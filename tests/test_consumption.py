"""Controls for atomic isolated-disposable run consumption.

The bound must hold against sequential replay and against genuinely concurrent
processes, and every identity seam must refuse with its own kind rather than a
generic failure. A control that only proved "something raised" would pass even
if the wrong thing refused for the wrong reason.
"""

from __future__ import annotations

import hashlib
import json
import os
import multiprocessing
import shutil
import subprocess
from pathlib import Path
import sys
import tempfile
import unittest

import support
import yaml

from coordination_substrate.consumption import (
    ConsumptionError,
    consume,
    validate_grant,
)
from coordination_substrate.publisher import _derived_action_identity

RUNNER_RELATIVE = "runners/topology_proof.py"
CANDIDATE = "a" * 40
DESCRIPTOR = {
    "provider": "local-docker",
    "packet_sha256": "b" * 64,
    "operation": "neo4j-round-trip-topology",
}


def _digest(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _git(repo: Path, *args):
    subprocess.run(["git", "-C", str(repo), *args], check=True,
                   capture_output=True, text=True)


def _make_repo(repo: Path, runner_body=b"print(1)\n") -> str:
    """A real repository, because the gate proves HEAD rather than trusting it."""
    repo.mkdir(parents=True, exist_ok=True)
    _git(repo, "init", "-q", "-b", "main")
    _git(repo, "config", "user.email", "clyde@example.invalid")
    _git(repo, "config", "user.name", "clyde")
    runner = repo / RUNNER_RELATIVE
    runner.parent.mkdir(parents=True, exist_ok=True)
    runner.write_bytes(runner_body)
    _git(repo, "add", "-A")
    _git(repo, "commit", "-q", "-m", "runner")
    return subprocess.run(["git", "-C", str(repo), "rev-parse", "HEAD"],
                          capture_output=True, text=True).stdout.strip()


def _seed(root: Path, repo: Path, *, bound=1, writer="boss", overrides=None,
          runner_body=b"print(1)\n"):
    """Install a runner repository and a published grant; return consume kwargs."""
    head = _make_repo(repo, runner_body)

    grant = {
        "isolated_disposable_runs": bound,
        "provider": DESCRIPTOR["provider"],
        "packet_sha256": DESCRIPTOR["packet_sha256"],
        "operation": DESCRIPTOR["operation"],
        "action_identity": _derived_action_identity(DESCRIPTOR),
        "candidate": head,
        "runner_path": RUNNER_RELATIVE,
        "runner_sha256": _digest(runner_body),
    }
    if overrides:
        grant.update(overrides)
    event = {"schema_version": 1, "writer": writer, "event": "execution-granted",
             "live_authorization": grant}
    relative = "bridge/events/grant.yaml"
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = yaml.safe_dump(event, sort_keys=False).encode("utf-8")
    path.write_bytes(payload)
    return {
        "repository_root": repo,
        "grant_path": relative,
        "grant_sha256": _digest(payload),
        "runner_path": RUNNER_RELATIVE,
        "consumer": "clyde-test",
    }


def _consume_in_child(root_str, kwargs, queue):
    """Run one consumption in a separate process, for the concurrency control."""
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from coordination_substrate.consumption import ConsumptionError, consume
    from coordination_substrate.topology import Topology

    root = Path(root_str)
    try:
        result = consume(root, Topology.load(root), **kwargs)
        queue.put(("ok", result["ordinal"]))
    except ConsumptionError as exc:
        queue.put(("refused", exc.kind))
    except Exception as exc:  # noqa: BLE001
        queue.put(("error", f"{type(exc).__name__}: {exc}"))


class ConsumptionTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name) / "authority"
        self.root.mkdir()
        self.repo = Path(self.tmp.name) / "repository"
        self.topology = support.build_bridge(self.root)
        # The receipts root is topology-owned; the fixture's lease_dir decides
        # where it lands, so the test must not hardcode a guess.
        self.receipts = self.root / self.topology.consumption_dir
        self.receipts.mkdir(parents=True, exist_ok=True)

    def _seed(self, **kwargs):
        """Each seed gets its own repository; repeated seeds must not collide."""
        self._seq = getattr(self, "_seq", 0) + 1
        repo = Path(self.tmp.name) / f"repository-{self._seq}"
        self.repo = repo
        return _seed(self.root, repo, **kwargs)

    def tearDown(self):
        self.tmp.cleanup()

    # -- success path -------------------------------------------------------

    def test_first_consumption_succeeds_and_installs_one_receipt(self):
        kwargs = self._seed()
        result = consume(self.root, self.topology, **kwargs)
        self.assertEqual(result["action"], "consumed")
        self.assertEqual(result["ordinal"], 1)
        self.assertEqual(result["remaining"], 0)
        receipts = list(self.receipts.rglob("receipt-*.json"))
        self.assertEqual(len(receipts), 1)

    def test_receipt_is_append_only_and_not_writable(self):
        kwargs = self._seed()
        consume(self.root, self.topology, **kwargs)
        receipt = next(self.receipts.rglob("receipt-*.json"))
        self.assertEqual(receipt.stat().st_mode & 0o222, 0)

    def test_receipt_binds_the_exact_identities(self):
        kwargs = self._seed()
        consume(self.root, self.topology, **kwargs)
        receipt = next(self.receipts.rglob("receipt-*.json"))
        body = json.loads(receipt.read_text())
        head = subprocess.run(["git","-C",str(self.repo),"rev-parse","HEAD"],
                              capture_output=True, text=True).stdout.strip()
        self.assertEqual(body["candidate"], head)
        self.assertEqual(body["runner_path"], RUNNER_RELATIVE)
        self.assertEqual(body["action_identity"], _derived_action_identity(DESCRIPTOR))
        self.assertEqual(body["grant_sha256"], kwargs["grant_sha256"])

    def test_a_bound_of_two_permits_exactly_two(self):
        kwargs = self._seed(bound=2)
        self.assertEqual(consume(self.root, self.topology, **kwargs)["ordinal"], 1)
        self.assertEqual(consume(self.root, self.topology, **kwargs)["ordinal"], 2)
        with self.assertRaises(ConsumptionError) as caught:
            consume(self.root, self.topology, **kwargs)
        self.assertEqual(caught.exception.kind, "authority-exhausted")

    # -- replay -------------------------------------------------------------

    def test_replay_is_refused_and_leaves_the_first_receipt_intact(self):
        kwargs = self._seed()
        consume(self.root, self.topology, **kwargs)
        receipt = next(self.receipts.rglob("receipt-*.json"))
        before = receipt.read_bytes()
        for _ in range(3):
            with self.assertRaises(ConsumptionError) as caught:
                consume(self.root, self.topology, **kwargs)
            self.assertEqual(caught.exception.kind, "authority-exhausted")
        self.assertEqual(receipt.read_bytes(), before)
        self.assertEqual(len(list(self.receipts.rglob("receipt-*.json"))), 1)

    # -- concurrency --------------------------------------------------------

    def test_two_concurrent_processes_yield_one_success(self):
        """The control that matters: separate processes, one bound."""
        kwargs = self._seed()
        queue = multiprocessing.Queue()
        procs = [
            multiprocessing.Process(
                target=_consume_in_child, args=(str(self.root), kwargs, queue)
            )
            for _ in range(2)
        ]
        for proc in procs:
            proc.start()
        for proc in procs:
            proc.join(timeout=60)
        outcomes = [queue.get(timeout=5) for _ in procs]
        kinds = sorted(kind for kind, _ in outcomes)
        self.assertEqual(kinds, ["ok", "refused"], outcomes)
        refusal = [detail for kind, detail in outcomes if kind == "refused"][0]
        self.assertEqual(refusal, "authority-exhausted", outcomes)
        self.assertEqual(len(list(self.receipts.rglob("receipt-*.json"))), 1)

    # -- identity negative controls ----------------------------------------

    def test_non_orchestrator_grant_refuses(self):
        kwargs = self._seed(writer="clyde-vscode-1")
        with self.assertRaises(ConsumptionError) as caught:
            consume(self.root, self.topology, **kwargs)
        self.assertEqual(caught.exception.kind, "non-orchestrator-grant")

    def test_wrong_grant_digest_refuses(self):
        kwargs = self._seed()
        kwargs["grant_sha256"] = "c" * 64
        with self.assertRaises(ConsumptionError) as caught:
            consume(self.root, self.topology, **kwargs)
        self.assertEqual(caught.exception.kind, "grant-digest")

    def test_wrong_action_identity_refuses(self):
        kwargs = self._seed(overrides={"action_identity": "v1:" + "d" * 64})
        with self.assertRaises(ConsumptionError) as caught:
            consume(self.root, self.topology, **kwargs)
        self.assertEqual(caught.exception.kind, "action-identity")

    def test_repository_head_differing_from_grant_refuses(self):
        kwargs = self._seed()
        (self.repo / "unrelated.txt").write_text("moves HEAD\n")
        _git(self.repo, "add", "-A")
        _git(self.repo, "commit", "-q", "-m", "second")
        with self.assertRaises(ConsumptionError) as caught:
            consume(self.root, self.topology, **kwargs)
        self.assertEqual(caught.exception.kind, "candidate-mismatch")

    def test_dirty_worktree_refuses_even_when_head_matches(self):
        kwargs = self._seed()
        (self.repo / "scratch.txt").write_text("uncommitted\n")
        with self.assertRaises(ConsumptionError) as caught:
            consume(self.root, self.topology, **kwargs)
        self.assertEqual(caught.exception.kind, "repository-dirty")

    def test_runner_under_repository_not_authority_root(self):
        """The runner lives in the repository; the authority root has no copy."""
        kwargs = self._seed()
        self.assertFalse((self.root / RUNNER_RELATIVE).exists())
        self.assertTrue((self.repo / RUNNER_RELATIVE).is_file())
        self.assertEqual(consume(self.root, self.topology, **kwargs)["ordinal"], 1)

    def test_grant_outside_event_directories_refuses(self):
        kwargs = self._seed()
        stray = self.root / "stray/grant.yaml"
        stray.parent.mkdir(parents=True, exist_ok=True)
        stray.write_bytes((self.root / kwargs["grant_path"]).read_bytes())
        kwargs["grant_path"] = "stray/grant.yaml"
        with self.assertRaises(ConsumptionError) as caught:
            consume(self.root, self.topology, **kwargs)
        self.assertEqual(caught.exception.kind, "non-authoritative-grant")

    def test_missing_action_identity_refuses(self):
        kwargs = self._seed(overrides={"action_identity": None})
        with self.assertRaises(ConsumptionError) as caught:
            consume(self.root, self.topology, **kwargs)
        self.assertEqual(caught.exception.kind, "action-identity")

    def test_non_hex_runner_digest_refuses(self):
        kwargs = self._seed(overrides={"runner_sha256": "Z" * 64})
        with self.assertRaises(ConsumptionError) as caught:
            consume(self.root, self.topology, **kwargs)
        self.assertEqual(caught.exception.kind, "grant-contract")

    def test_non_hex_packet_digest_refuses(self):
        kwargs = self._seed(overrides={"packet_sha256": "Z" * 64})
        with self.assertRaises(ConsumptionError) as caught:
            consume(self.root, self.topology, **kwargs)
        self.assertEqual(caught.exception.kind, "grant-contract")

    def test_symlinked_receipt_root_refuses(self):
        kwargs = self._seed()
        shutil.rmtree(self.receipts, ignore_errors=True)
        elsewhere = self.root / "elsewhere"
        elsewhere.mkdir(exist_ok=True)
        self.receipts.symlink_to(elsewhere)
        with self.assertRaises(ConsumptionError) as caught:
            consume(self.root, self.topology, **kwargs)
        self.assertEqual(caught.exception.kind, "unsafe-path")

    def test_wrong_runner_path_refuses(self):
        kwargs = self._seed()
        kwargs["runner_path"] = "runners/other.py"
        with self.assertRaises(ConsumptionError) as caught:
            consume(self.root, self.topology, **kwargs)
        self.assertEqual(caught.exception.kind, "runner-mismatch")

    def test_runner_digest_mismatch_refuses(self):
        """HEAD matches and the tree is clean; only the runner bytes disagree."""
        kwargs = self._seed(overrides={"runner_sha256": "f" * 64})
        with self.assertRaises(ConsumptionError) as caught:
            consume(self.root, self.topology, **kwargs)
        self.assertEqual(caught.exception.kind, "runner-digest")

    def test_missing_consumption_root_refuses(self):
        kwargs = self._seed()
        self.receipts.rmdir()
        with self.assertRaises(ConsumptionError) as caught:
            consume(self.root, self.topology, **kwargs)
        self.assertEqual(caught.exception.kind, "absent-topology")

    def test_malformed_grant_refuses(self):
        kwargs = self._seed()
        path = self.root / kwargs["grant_path"]
        path.write_bytes(b"{unparseable: [")
        kwargs["grant_sha256"] = _digest(path.read_bytes())
        with self.assertRaises(ConsumptionError) as caught:
            consume(self.root, self.topology, **kwargs)
        self.assertEqual(caught.exception.kind, "malformed-grant")

    def test_traversal_grant_path_refuses(self):
        kwargs = self._seed()
        kwargs["grant_path"] = "../outside.yaml"
        with self.assertRaises(ConsumptionError) as caught:
            consume(self.root, self.topology, **kwargs)
        self.assertEqual(caught.exception.kind, "unsafe-path")

    def test_symlinked_grant_refuses(self):
        kwargs = self._seed()
        target = self.root / "bridge/events/link.yaml"
        target.symlink_to(self.root / kwargs["grant_path"])
        kwargs["grant_path"] = "bridge/events/link.yaml"
        with self.assertRaises(ConsumptionError) as caught:
            consume(self.root, self.topology, **kwargs)
        self.assertEqual(caught.exception.kind, "unsafe-path")

    # -- grant contract -----------------------------------------------------

    def test_absent_bound_refuses_with_grant_contract(self):
        for bad in (0, -1, True, "1", None):
            event = {"writer": "boss",
                     "live_authorization": {**{"isolated_disposable_runs": bad}, **DESCRIPTOR}}
            with self.assertRaises(ConsumptionError) as caught:
                validate_grant(event)
            self.assertEqual(caught.exception.kind, "grant-contract", bad)

    def test_grant_without_live_authorization_refuses(self):
        with self.assertRaises(ConsumptionError) as caught:
            validate_grant({"writer": "boss"})
        self.assertEqual(caught.exception.kind, "grant-contract")

    def test_short_candidate_refuses(self):
        kwargs = self._seed(overrides={"candidate": "abc"})
        with self.assertRaises(ConsumptionError) as caught:
            consume(self.root, self.topology, **kwargs)
        self.assertEqual(caught.exception.kind, "grant-contract")

    def test_symlinked_parent_of_the_grant_refuses(self):
        """A symlinked *parent* that resolves back inside the root is not safe.

        Resolution hides the redirection it just followed, so only a lexical
        component check can catch this.
        """
        kwargs = self._seed()
        grant_rel = Path(kwargs["grant_path"])
        real = self.root / "real-events"
        real.mkdir(exist_ok=True)
        (real / grant_rel.name).write_bytes((self.root / grant_rel).read_bytes())
        parent = self.root / grant_rel.parent
        shutil.rmtree(parent)
        parent.symlink_to(real)
        with self.assertRaises(ConsumptionError) as caught:
            consume(self.root, self.topology, **kwargs)
        self.assertEqual(caught.exception.kind, "unsafe-path")

    def _cli(self, kwargs, repository):
        """Run the shipped CLI as a real process, so a traceback cannot hide."""
        program = (
            "import sys;"
            "from coordination_substrate.consumption import main;"
            "sys.exit(main(sys.argv[1:]))"
        )
        return subprocess.run(
            [sys.executable, "-c", program,
             "--root", str(self.root),
             "--repository", str(repository),
             "--grant-path", kwargs["grant_path"],
             "--grant-sha256", kwargs["grant_sha256"],
             "--runner-path", kwargs["runner_path"],
             "--consumer", "runner-entry"],
            capture_output=True, text=True,
            cwd=str(Path(__file__).resolve().parents[1]),
            env={**os.environ,
                 "PYTHONPATH": str(Path(__file__).resolve().parents[1]),
                 "PYTHONDONTWRITEBYTECODE": "1"},
        )

    def test_cli_refuses_a_symlinked_repository_root(self):
        """resolve() would follow the link, so the check must precede it."""
        kwargs = self._seed()
        link = Path(self.tmp.name) / "repository-link"
        link.symlink_to(self.repo)
        done = self._cli(kwargs, link)
        self.assertNotEqual(done.returncode, 0)
        self.assertNotIn("Traceback", done.stderr)
        refusal = json.loads(done.stderr.strip())
        self.assertEqual(refusal["action"], "refused")
        self.assertEqual(refusal["kind"], "repository-unsafe")

    def test_cli_admits_the_real_repository_root(self):
        """Negative control for the control: the same call succeeds unlinked."""
        kwargs = self._seed()
        done = self._cli(kwargs, self.repo)
        self.assertEqual(done.returncode, 0, done.stderr)
        self.assertEqual(json.loads(done.stdout)["action"], "consumed")


if __name__ == "__main__":
    unittest.main()
