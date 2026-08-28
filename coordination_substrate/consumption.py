"""Atomic consumption of a bounded isolated-disposable execution grant.

A published grant that says "one run" does not prevent five runs. P5-T001
demonstrated exactly that: the count was a field in an event, the runner was
independently invokable, and nothing stood between the two. This module turns
the grant into an unrepeatable filesystem transaction taken *at runner entry*.

The shape is deliberately small:

  * the grant is an already-published orchestrator event, revalidated here by
    its own bytes and digest rather than trusted from a caller;
  * one receipt directory per (action identity, grant) holds append-only
    receipts, and the number of receipts is the number of spends;
  * a single exclusive lock covers count-then-install, so two processes cannot
    both observe "zero spent";
  * the receipt is installed and fsynced **before** the runner proceeds, so a
    crash after installation still spends the grant.

That last rule is the point. Charging on success would make a crash loop free,
which is the failure mode worth preventing. A spend records that a consequential
action was *started*, not that it worked.

Identity, path confinement, locking and atomic install are reused from the
accepted substrate rather than reimplemented here.
"""

from __future__ import annotations

import argparse
import contextlib
import fcntl
import hashlib
import json
import re
import os
import subprocess
import sys
from pathlib import Path

import yaml

from .publisher import (
    _action_descriptor, _derived_action_identity, check_execution_grant)
from .topology import Topology, TopologyError, _require_relative

GRANT_FIELD = "isolated_disposable_runs"
HEX64 = re.compile(r"^[0-9a-f]{64}$")
HEX40 = re.compile(r"^[0-9a-f]{40}$")


class ConsumptionError(RuntimeError):
    """A typed refusal. Every one happens before the runner proceeds.

    ``kind`` is the machine-readable seam that refused, so a caller can tell a
    spent grant from a mistyped runner path without parsing prose.
    """

    def __init__(self, kind: str, detail: str) -> None:
        super().__init__(detail)
        self.kind = kind
        self.detail = detail


def _digest(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _no_symlink_component(root: Path, relative: str, label: str) -> Path:
    """Refuse a symlink anywhere along the path, not just at the leaf.

    A symlinked parent redirects the whole subtree, so checking only the final
    component would let a receipt slot be pointed somewhere else entirely.
    """
    current = root
    for part in Path(relative).parts:
        current = current / part
        if current.is_symlink():
            raise ConsumptionError(
                "unsafe-path", f"{label} contains a symlinked component: {part}")
    return current


def _read_confined(root: Path, relative: str, label: str) -> bytes:
    """Read a repository-relative path without escaping the root.

    Traversal and symlinks are refused rather than resolved: a grant that can be
    made to name a file outside the tree is not a grant about this tree.
    """
    # Convert the topology refusal into a typed consumption refusal. A caller at
    # a runner boundary must be able to tell "unsafe path" from "grant spent"
    # without catching a second exception type or reading prose.
    try:
        relative = _require_relative(relative, label)
    except TopologyError as exc:
        raise ConsumptionError("unsafe-path", str(exc)) from exc
    # Lexical first: a symlinked *parent* that resolves back inside the root
    # would otherwise be admitted, because resolution hides the redirection it
    # just followed. Every component is checked before anything is dereferenced.
    target = _no_symlink_component(root, relative, label)
    resolved = target.resolve()
    root_resolved = root.resolve()
    if resolved != root_resolved and root_resolved not in resolved.parents:
        raise ConsumptionError("unsafe-path", f"{label} resolves outside the root")
    if not resolved.is_file():
        raise ConsumptionError("absent-path", f"{label} is absent: {relative}")
    return resolved.read_bytes()


def validate_grant(event: dict) -> dict:
    """Refuse anything that is not a typed, self-consistent execution grant.

    The rules are the publisher's own, so consumption cannot admit a grant
    shape that publication never recognized. Only the error vocabulary is
    local: each shared kind is raised as this module's typed refusal.
    """
    if not isinstance(event, dict):
        raise ConsumptionError("malformed-grant", "grant event is not an object")
    grant = event.get("live_authorization")
    if not isinstance(grant, dict):
        raise ConsumptionError(
            "grant-contract", "grant event carries no live_authorization object")

    problem = check_execution_grant(grant)
    if problem is not None:
        raise ConsumptionError(*problem)

    descriptor = _action_descriptor(grant)
    return {
        "bound": grant[GRANT_FIELD],
        "action_identity": _derived_action_identity(descriptor),
        "candidate": grant["candidate"],
        "runner_path": grant["runner_path"],
        "runner_sha256": grant["runner_sha256"],
    }


def _git(repository: Path, *arguments: str) -> str:
    """Read repository state, translating every failure into a typed kind."""
    try:
        done = subprocess.run(
            ["git", "-C", str(repository), *arguments],
            capture_output=True, text=True, timeout=60,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise ConsumptionError(
            "repository-unreadable", f"git failed: {type(exc).__name__}") from exc
    if done.returncode != 0:
        raise ConsumptionError(
            "repository-unreadable",
            f"git {arguments[0]} failed: {done.stderr.strip()[:160]}")
    return done.stdout.strip()


def check_repository_root(supplied: Path) -> Path:
    """Refuse a symlinked repository root before it is dereferenced.

    resolve() silently follows the link, so a check on the resolved path can
    never fire. The operator-supplied path is judged as given, and only then
    normalized.
    """
    if supplied.is_symlink():
        raise ConsumptionError(
            "repository-unsafe", "repository root must not be a symlink")
    for parent in supplied.parents:
        if parent.is_symlink():
            raise ConsumptionError(
                "repository-unsafe",
                f"repository root has a symlinked parent: {parent.name}")
    return supplied.resolve()


def verify_repository(repository_root: Path, candidate: str) -> None:
    """Prove the repository *is* the granted candidate, cleanly.

    A caller repeating the candidate string proves nothing: that is the very
    identity the gate exists to establish. HEAD is read from the repository and
    the worktree must be clean, so a dirty tree or a different commit refuses
    before any receipt is installed.
    """
    if not repository_root.is_dir():
        raise ConsumptionError(
            "repository-unsafe", f"repository root is not a directory: {repository_root}")
    head = _git(repository_root, "rev-parse", "HEAD")
    if head != candidate:
        raise ConsumptionError(
            "candidate-mismatch",
            f"repository HEAD {head[:12]} is not the granted candidate {candidate[:12]}")
    if _git(repository_root, "status", "--porcelain"):
        raise ConsumptionError(
            "repository-dirty", "repository worktree is not clean")


def _require_in_event_dirs(topology: Topology, grant_path: object) -> str:
    """A grant is a published orchestrator event, not any file under the root.

    Confining to the topology's own event directories is what makes the grant
    authoritative: a caller-created YAML elsewhere in the tree can carry the
    right writer string and still be something the orchestrator never published.
    """
    try:
        relative = _require_relative(grant_path, "grant_path")
    except TopologyError as exc:
        raise ConsumptionError("unsafe-path", str(exc)) from exc
    parent = str(Path(relative).parent)
    if parent not in set(topology.event_dirs):
        raise ConsumptionError(
            "non-authoritative-grant",
            f"grant is not in a topology event directory: {parent}")
    return relative


def _receipts_root(root: Path, topology: Topology) -> Path:
    """The configured receipts directory, which must already exist."""
    if not topology.consumption_dir:
        raise ConsumptionError(
            "absent-topology", "topology declares no consumption_dir")
    _no_symlink_component(root, topology.consumption_dir, "consumption_dir")
    target = root / topology.consumption_dir
    if not target.is_dir():
        raise ConsumptionError(
            "absent-topology",
            f"consumption directory is absent: {topology.consumption_dir}")
    return target


@contextlib.contextmanager
def _exclusive(lock_path: Path):
    """One lock covering count-then-install.

    Counting outside the lock and installing inside it would let two processes
    both read zero and both install, which is precisely the race a bound exists
    to prevent.
    """
    if not lock_path.parent.is_dir():
        raise ConsumptionError(
            "absent-topology", f"receipt directory is absent: {lock_path.parent}")
    flags = os.O_RDWR | os.O_CREAT
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    fd = os.open(lock_path, flags, 0o600)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX)
        yield
    finally:
        fcntl.flock(fd, fcntl.LOCK_UN)
        os.close(fd)


def _install_receipt(directory: Path, ordinal: int, body: dict) -> tuple[Path, str]:
    """Install one append-only receipt, fsynced, never overwriting."""
    payload = json.dumps(body, sort_keys=True, separators=(",", ":")).encode("utf-8")
    path = directory / f"receipt-{ordinal:04d}.json"
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        fd = os.open(path, flags, 0o400)
    except FileExistsError as exc:
        # Another writer installed this ordinal. Refusing is correct: the count
        # under the lock said this slot was free, so the tree changed underneath
        # us and no spend should be inferred.
        raise ConsumptionError(
            "receipt-conflict", f"receipt {path.name} already exists") from exc
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
    except BaseException:
        path.unlink(missing_ok=True)
        raise
    dir_fd = os.open(directory, os.O_RDONLY)
    try:
        os.fsync(dir_fd)
    finally:
        os.close(dir_fd)
    return path, _digest(payload)


def consume(
    root: Path,
    topology: Topology,
    *,
    repository_root: Path,
    grant_path: str,
    grant_sha256: str,
    runner_path: str,
    consumer: str,
) -> dict:
    """Spend one unit of a bounded execution grant, or refuse.

    Two roots, deliberately. ``root`` is the coordination authority tree that
    holds the grant and the receipts; ``repository_root`` is the product
    repository the runner lives in. They are different trees, and conflating
    them would mean the gate could not bind the real runner at all.

    The candidate is not a parameter. It comes from the grant and is proved
    against the repository, because a caller repeating the expected string is
    not evidence about the repository's state.
    """
    grant_relative = _require_in_event_dirs(topology, grant_path)
    raw = _read_confined(root, grant_relative, "grant_path")
    observed = _digest(raw)
    if observed != grant_sha256:
        raise ConsumptionError(
            "grant-digest",
            f"grant bytes do not match the declared digest: {observed[:16]}")

    try:
        event = yaml.safe_load(raw)
    except yaml.YAMLError as exc:
        raise ConsumptionError("malformed-grant", "grant is not parseable") from exc

    if not isinstance(event, dict) or event.get("writer") != topology.orchestrator:
        raise ConsumptionError(
            "non-orchestrator-grant",
            "an execution grant must be written by the orchestrator")

    checked = validate_grant(event)

    # The repository must *be* the granted candidate, cleanly.
    verify_repository(repository_root, checked["candidate"])

    if runner_path != checked["runner_path"]:
        raise ConsumptionError(
            "runner-mismatch", "runner path does not match the granted runner")
    runner_bytes = _read_confined(repository_root, runner_path, "runner_path")
    if _digest(runner_bytes) != checked["runner_sha256"]:
        raise ConsumptionError(
            "runner-digest", "runner bytes do not match the granted runner digest")

    receipts_root = _receipts_root(root, topology)
    slot_relative = Path(topology.consumption_dir) / checked["action_identity"].replace(
        ":", "_") / grant_sha256
    _no_symlink_component(root, str(slot_relative), "receipt slot")
    slot = root / slot_relative
    slot.mkdir(parents=True, exist_ok=True)

    with _exclusive(slot / ".consume.lock"):
        spent = sorted(p for p in slot.glob("receipt-*.json"))
        if len(spent) >= checked["bound"]:
            raise ConsumptionError(
                "authority-exhausted",
                f"grant of {checked['bound']} is already spent {len(spent)} times")
        body = {
            "schema_version": 1,
            "action_identity": checked["action_identity"],
            "grant_path": grant_relative,
            "grant_sha256": grant_sha256,
            "candidate": checked["candidate"],
            "runner_path": runner_path,
            "runner_sha256": checked["runner_sha256"],
            "consumer": consumer,
            "ordinal": len(spent) + 1,
            "bound": checked["bound"],
        }
        path, digest = _install_receipt(slot, len(spent) + 1, body)

    return {
        "action": "consumed",
        "receipt": str(path.relative_to(root)),
        "receipt_sha256": digest,
        "ordinal": body["ordinal"],
        "bound": checked["bound"],
        "remaining": checked["bound"] - body["ordinal"],
    }


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description="Consume one bounded isolated-disposable execution grant.")
    parser.add_argument("--root", type=Path, required=True,
                        help="coordination authority root")
    parser.add_argument("--repository", type=Path, required=True,
                        help="product repository root the runner lives in")
    parser.add_argument("--topology", default=None)
    parser.add_argument("--grant-path", required=True)
    parser.add_argument("--grant-sha256", required=True)
    parser.add_argument("--runner-path", required=True)
    parser.add_argument("--consumer", required=True)
    args = parser.parse_args(argv)

    try:
        root = args.root.resolve()
        topology = (
            Topology.load(root, args.topology) if args.topology else Topology.load(root)
        )
        result = consume(
            root,
            topology,
            repository_root=check_repository_root(args.repository),
            grant_path=args.grant_path,
            grant_sha256=args.grant_sha256,
            runner_path=args.runner_path,
            consumer=args.consumer,
        )
    except ConsumptionError as exc:
        print(json.dumps({"action": "refused", "kind": exc.kind, "detail": exc.detail}),
              file=sys.stderr)
        return 1
    except TopologyError as exc:
        print(json.dumps({"action": "refused", "kind": "topology", "detail": str(exc)}),
              file=sys.stderr)
        return 1
    except (OSError, UnicodeDecodeError, ValueError, yaml.YAMLError) as exc:
        # Every remaining failure still leaves one typed JSON refusal and no
        # traceback: a runner boundary must never have to parse a stack trace to
        # learn that it may not proceed.
        print(json.dumps({"action": "refused", "kind": "unreadable-input",
                          "detail": f"{type(exc).__name__}"}), file=sys.stderr)
        return 1
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
