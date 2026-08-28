#!/usr/bin/env python3
"""Create, refresh, or release the one exclusive mutating lease.

The lease answers a single question: which process may currently mutate shared
state. It binds time to the host clock, serializes YAML from Python objects,
reparses before install, writes atomically, and authenticates operations
through the registered listener child lineage. Secret values are never
accepted or stored.

Exclusivity comes from ``mkdir`` on the lease directory, which is atomic on
POSIX filesystems. A plain file is not an adequate exclusive claim.

Extracted from the replaced coordination bridge.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
from pathlib import Path
import re
import shutil
import sys
import uuid

import yaml

from .topology import Topology, TopologyError

SHA256 = re.compile(r"^[0-9a-f]{64}$")


class LeaseError(RuntimeError):
    pass


def _now(clock=None) -> dt.datetime:
    value = (clock or (lambda: dt.datetime.now(dt.timezone.utc)))()
    if value.tzinfo is None:
        raise LeaseError("lease clock must be timezone-aware")
    return value.astimezone(dt.timezone.utc).replace(microsecond=0)


def _stamp(value: dt.datetime) -> str:
    return value.strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse_stamp(value: object) -> dt.datetime:
    if not isinstance(value, str):
        raise LeaseError("lease timestamp is invalid")
    try:
        return dt.datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(
            tzinfo=dt.timezone.utc
        )
    except ValueError as exc:
        raise LeaseError("lease timestamp is invalid") from exc


def _validate_identity(task_id: str, attempt: int, request_sha256: str, owner: str) -> None:
    if not task_id or task_id.strip() != task_id:
        raise LeaseError("task identity is empty or padded")
    if attempt < 1:
        raise LeaseError("attempt must be positive")
    if not SHA256.fullmatch(request_sha256):
        raise LeaseError("request sha256 is invalid")
    if not owner or owner.strip() != owner:
        raise LeaseError("owner identity is empty or padded")


def _dump(value: dict) -> bytes:
    body = yaml.safe_dump(value, sort_keys=False, width=100).encode("utf-8")
    if yaml.safe_load(body) != value:
        raise LeaseError("lease YAML failed round-trip validation")
    return body


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _atomic_replace(path: Path, value: dict) -> None:
    if path.is_symlink():
        raise LeaseError("lease file must not be a symlink")
    body = _dump(value)
    temporary = path.parent / f".{path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp"
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = os.open(temporary, flags, 0o600)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(body)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        _fsync_directory(path.parent)
    finally:
        temporary.unlink(missing_ok=True)


def _read(path: Path) -> dict:
    if path.is_symlink() or not path.is_file():
        raise LeaseError("lease is absent or unsafe")
    try:
        value = yaml.safe_load(path.read_bytes())
    except (OSError, yaml.YAMLError) as exc:
        raise LeaseError("lease is unreadable or invalid") from exc
    if not isinstance(value, dict):
        raise LeaseError("lease is not an object")
    return value


def _assert_owner(
    value: dict, *, task_id: str, attempt: int, request_sha256: str, owner: str
) -> None:
    expected = {
        "task_id": task_id,
        "attempt": attempt,
        "request_sha256": request_sha256,
        "owner_instance_id": owner,
    }
    if any(value.get(field) != wanted for field, wanted in expected.items()):
        raise LeaseError("lease owner tuple differs")


def _lease_paths(topology: Topology) -> tuple[Path, Path]:
    lease_dir = topology.root / topology.lease_dir
    return lease_dir, lease_dir / "lease.yaml"


def claim(
    topology: Topology,
    *,
    task_id: str,
    attempt: int,
    request_sha256: str,
    owner: str,
    harness: str,
    listener_pid: int,
    listener_child_pid: int,
    session_id: str,
    clock=None,
) -> dict:
    """Atomically acquire the lease, or replay an identical existing claim."""
    _validate_identity(task_id, attempt, request_sha256, owner)
    if not harness or harness.strip() != harness:
        raise LeaseError("harness identity is empty or padded")
    try:
        uuid.UUID(session_id)
    except (ValueError, AttributeError, TypeError) as exc:
        raise LeaseError("session identity is invalid") from exc
    lease_dir, lease_path = _lease_paths(topology)
    lease_dir.parent.mkdir(parents=True, exist_ok=True)
    try:
        lease_dir.mkdir(mode=0o700)
    except FileExistsError:
        current = _read(lease_path)
        _assert_owner(
            current,
            task_id=task_id,
            attempt=attempt,
            request_sha256=request_sha256,
            owner=owner,
        )
        # An expired lease must not silently replay: refresh already refuses one
        # (see refresh), so a re-claim replaying it would be the only path that
        # revives a stale claim at exit 0. Force the documented release-then-claim
        # recovery instead. (LEA-1.)
        if _parse_stamp(current.get("expires_at")) <= _now(clock):
            raise LeaseError("expired lease cannot be replayed; release then claim")
        return {"action": "claim-replayed", "lease": current}
    instant = _now(clock)
    value = {
        "schema_version": 1,
        "task_id": task_id,
        "attempt": attempt,
        "request_sha256": request_sha256,
        "owner_instance_id": owner,
        "harness": harness,
        "listener_pid": listener_pid,
        "listener_child_pid": listener_child_pid,
        "session_id": session_id,
        "acquired_at": _stamp(instant),
        "heartbeat_at": _stamp(instant),
        "expires_at": _stamp(instant + dt.timedelta(seconds=topology.lease_seconds)),
        "expiry_seconds": topology.lease_seconds,
    }
    # A pid alone is not an identity, because pids are reused. Pairing it with
    # the process start time makes a dead owner distinguishable from a live
    # stranger wearing its number. The keys are omitted rather than written null
    # when /proc cannot answer: absence means "never captured", which reap()
    # treats as unadjudicable, and a null identity on disk would be a claim we
    # did not establish.
    for field, pid in (
        ("listener_start_ticks", listener_pid),
        ("listener_child_start_ticks", listener_child_pid),
    ):
        ticks = process_start_ticks(pid)
        if ticks is not None:
            value[field] = ticks
    try:
        _atomic_replace(lease_path, value)
    except BaseException:
        shutil.rmtree(lease_dir, ignore_errors=True)
        raise
    return {"action": "claimed", "lease": value}


def refresh(
    topology: Topology,
    *,
    task_id: str,
    attempt: int,
    request_sha256: str,
    owner: str,
    clock=None,
) -> dict:
    """Extend an unexpired lease held by the same owner tuple."""
    _validate_identity(task_id, attempt, request_sha256, owner)
    _, path = _lease_paths(topology)
    value = _read(path)
    _assert_owner(
        value,
        task_id=task_id,
        attempt=attempt,
        request_sha256=request_sha256,
        owner=owner,
    )
    instant = _now(clock)
    if _parse_stamp(value.get("expires_at")) <= instant:
        raise LeaseError("expired lease cannot be refreshed")
    value["heartbeat_at"] = _stamp(instant)
    value["expires_at"] = _stamp(instant + dt.timedelta(seconds=topology.lease_seconds))
    value["expiry_seconds"] = topology.lease_seconds
    _atomic_replace(path, value)
    return {"action": "refreshed", "lease": value}


def release(
    topology: Topology,
    *,
    task_id: str,
    attempt: int,
    request_sha256: str,
    owner: str,
) -> dict:
    """Remove the lease. Only the recorded owner tuple may do this."""
    _validate_identity(task_id, attempt, request_sha256, owner)
    lease_dir, path = _lease_paths(topology)
    value = _read(path)
    _assert_owner(
        value,
        task_id=task_id,
        attempt=attempt,
        request_sha256=request_sha256,
        owner=owner,
    )
    path.unlink()
    _fsync_directory(lease_dir)
    lease_dir.rmdir()
    _fsync_directory(lease_dir.parent)
    return {"action": "released", "task_id": task_id, "attempt": attempt}


def read_lease(topology: Topology) -> dict | None:
    """Return the current lease, or None when the slot is free."""
    _, path = _lease_paths(topology)
    if not path.exists():
        return None
    return _read(path)


def process_start_ticks(pid: int, proc_root: Path = Path("/proc")) -> int | None:
    """Field 22 of /proc/<pid>/stat: when this process started, in clock ticks.

    A bare pid is not an identity. Pids are reused, so a reaper that trusts one
    will eventually act on an unrelated process, which is worse than never
    reaping at all. Pairing the pid with its start time makes the identity
    unambiguous for the uptime of the machine.

    Parsed positionally after the final ')', because the comm field is
    parenthesised and may itself contain spaces and parentheses.
    """
    try:
        raw = (proc_root / str(pid) / "stat").read_text()
    except OSError:
        return None
    close = raw.rfind(")")
    if close == -1:
        return None
    fields = raw[close + 1:].split()
    try:
        return int(fields[19])
    except (IndexError, ValueError):
        return None


def owner_is_alive(record: dict, proc_root: Path = Path("/proc")) -> bool | None:
    """Is the process that claimed this lease still the process running now?

    True when alive, False when provably gone, and None when the record carries
    no captured identity and the question cannot be answered.

    None is not False. A lease that cannot be adjudicated must never be reaped,
    or deploying this change would release every live lease claimed before it.
    """
    pid = record.get("listener_pid")
    expected = record.get("listener_start_ticks")
    if not isinstance(pid, int) or isinstance(pid, bool) or pid < 2:
        return None
    if not isinstance(expected, int) or isinstance(expected, bool):
        return None
    observed = process_start_ticks(pid, proc_root)
    if observed is None:
        return False
    return observed == expected


def reap(
    topology: Topology,
    *,
    proc_root: Path = Path("/proc"),
    clock=None,
) -> dict:
    """Release the lease only when its owner is provably gone.

    Refuses in every ambiguous case: no lease, owner alive, or an owner whose
    identity was never captured. The refusal reason is returned rather than
    raised, because a reaper that cannot decide is a normal outcome and not an
    error.
    """
    _, path = _lease_paths(topology)
    record = read_lease(topology)
    if record is None:
        return {"action": "reap-declined", "reason": "no-lease-present"}

    alive = owner_is_alive(record, proc_root)
    if alive is None:
        return {
            "action": "reap-declined",
            "reason": "owner-identity-not-captured",
            "detail": "the lease predates start-time capture; it can expire but not be reaped",
            "lease": record,
        }
    if alive:
        return {"action": "reap-declined", "reason": "owner-alive", "lease": record}

    expires = _parse_stamp(record.get("expires_at"))
    if _now(clock) < expires:
        return {
            "action": "reap-declined",
            "reason": "owner-gone-but-lease-unexpired",
            "detail": "an owner may die and be resumed inside its own lease window",
            "lease": record,
        }

    lease_dir = path.parent
    path.unlink()
    _fsync_directory(lease_dir)
    lease_dir.rmdir()
    _fsync_directory(lease_dir.parent)
    return {"action": "reaped", "reason": "owner-gone-and-lease-expired", "lease": record}


def _ancestors(pid: int | None = None, proc_root: Path = Path("/proc")) -> set[int]:
    result: set[int] = set()
    current = os.getpid() if pid is None else pid
    while current > 1 and current not in result:
        result.add(current)
        try:
            fields = (proc_root / str(current) / "stat").read_text().split()
            current = int(fields[3])
        except (OSError, ValueError, IndexError):
            break
    return result


def listener_identity(topology: Topology, role_name: str) -> dict:
    """Authenticate the caller against the registered listener child lineage."""
    role = topology.role(role_name)
    path = topology.root / role.listener_state
    try:
        value = json.loads(path.read_text())
    except (OSError, ValueError) as exc:
        raise LeaseError(f"{role_name} listener state is absent or invalid") from exc
    if value.get("status") not in {"running", "awaiting-transport-ack"}:
        raise LeaseError(f"{role_name} listener is not in a lease-authorizing state")
    child = value.get("child_pid")
    if not isinstance(child, int) or isinstance(child, bool) or child < 2:
        raise LeaseError(f"{role_name} listener child identity is invalid")
    if child not in _ancestors():
        raise LeaseError(f"caller is outside the registered {role_name} listener lineage")
    return value


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Manage the exclusive mutating lease.")
    parser.add_argument("action", choices=("claim", "refresh", "release", "read", "reap"))
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument("--topology", default="coordination/topology.yaml")
    parser.add_argument("--role")
    parser.add_argument("--task-id")
    parser.add_argument("--attempt", type=int)
    parser.add_argument("--request-sha256")
    parser.add_argument("--owner")
    parser.add_argument("--harness")
    parser.add_argument("--session-id")
    args = parser.parse_args(argv)
    try:
        topology = Topology.load(args.root, args.topology)
        if args.action == "read":
            print(json.dumps({"lease": read_lease(topology)}, sort_keys=True, default=str))
            return 0
        if args.action == "reap":
            # No owner tuple is required or accepted: reaping is adjudicated by
            # whether the recorded owner process still exists, never by a caller
            # asserting it may. A declined reap is a normal outcome, not an error.
            outcome = reap(topology)
            print(json.dumps(outcome, sort_keys=True, default=str))
            return 0
        role_name = args.role
        if role_name is None:
            holder = topology.lease_holder()
            if holder is None:
                raise LeaseError("no role declares holds_mutating_lease; pass --role")
            role_name = holder.name
        for required in ("task_id", "attempt", "request_sha256", "owner"):
            if getattr(args, required) is None:
                raise LeaseError(f"--{required.replace('_', '-')} is required")
        listener = listener_identity(topology, role_name)
        common = {
            "topology": topology,
            "task_id": args.task_id,
            "attempt": args.attempt,
            "request_sha256": args.request_sha256,
            "owner": args.owner,
        }
        if args.action == "claim":
            if not args.harness or not args.session_id:
                raise LeaseError("claim requires harness and session identity")
            result = claim(
                **common,
                harness=args.harness,
                listener_pid=listener["listener_pid"],
                listener_child_pid=listener["child_pid"],
                session_id=args.session_id,
            )
        elif args.action == "refresh":
            result = refresh(**common)
        else:
            result = release(**common)
        print(json.dumps(result, sort_keys=True))
        return 0
    except (LeaseError, TopologyError, OSError, ValueError) as exc:
        print(f"REFUSED: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
