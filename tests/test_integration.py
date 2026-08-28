"""End-to-end coverage: publisher -> pointer -> listener -> real subprocess.

These exercise the parts unit tests deliberately fake: the ``serve()`` loop,
subprocess supervision, stderr capture, and the interaction between what the
publisher writes and what the listener will accept.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
import shutil
import stat
import subprocess
import sys
import tempfile
import threading
import time
import unittest

import yaml

import conftest  # noqa: F401  (path setup)

from support import build_bridge, spec  # noqa: E402
from coordination_substrate.listener import (  # noqa: E402
    BridgeError,
    Listener,
    read_authoritative_pointer,
    serve,
)
from coordination_substrate.publisher import publish  # noqa: E402

FAKE_CLI = Path(__file__).resolve().parent / "fake_agent_cli.py"


class PublisherListenerContractTests(unittest.TestCase):
    """What the publisher writes must be exactly what the listener accepts."""

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.topology = build_bridge(self.root)

    def tearDown(self):
        self.temp.cleanup()

    def test_published_event_is_accepted_by_the_listener(self):
        result = publish(spec(sequence=1), topology=self.topology)
        pointer = read_authoritative_pointer(self.topology, "worker")
        self.assertEqual(pointer["sequence"], 1)
        self.assertEqual(pointer["event_path"], result["event_path"])
        self.assertEqual(pointer["event_sha256"], result["event_sha256"])

    def test_listener_refuses_a_tampered_event_after_publication(self):
        result = publish(spec(sequence=1), topology=self.topology)
        target = self.root / result["event_path"]
        target.write_bytes(target.read_bytes() + b"\ntampered: true\n")
        with self.assertRaises(BridgeError):
            read_authoritative_pointer(self.topology, "worker")

    def test_sequence_advances_across_several_publications(self):
        for sequence in (1, 2, 3):
            publish(spec(sequence=sequence), topology=self.topology)
            self.assertEqual(
                read_authoritative_pointer(self.topology, "worker")["sequence"], sequence
            )


class ServeLoopTests(unittest.TestCase):
    """Drive the real supervision loop against a real child process."""

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.topology = build_bridge(self.root)
        self.state = self.root / "bridge/listeners/worker.json"
        # Put an executable named like an agent CLI on PATH.
        self.bindir = self.root / "bin"
        self.bindir.mkdir()
        self.binary = self.bindir / "fake-agent"
        shutil.copy(FAKE_CLI, self.binary)
        self.binary.chmod(self.binary.stat().st_mode | stat.S_IXUSR)
        self.old_path = os.environ.get("PATH", "")
        os.environ["PATH"] = f"{self.bindir}{os.pathsep}{self.old_path}"
        self.env_keys: list[str] = []

    def tearDown(self):
        os.environ["PATH"] = self.old_path
        for key in self.env_keys:
            os.environ.pop(key, None)
        self.temp.cleanup()

    def set_env(self, key: str, value: str) -> None:
        os.environ[key] = value
        self.env_keys.append(key)

    def run_serve(self, timeout: float = 8.0, **kwargs) -> dict:
        """Run serve() in a thread and stop it once the wake is acknowledged."""
        error: dict = {}

        def target():
            try:
                serve(
                    "worker",
                    self.topology,
                    self.state,
                    executable=str(self.binary),
                    **kwargs,
                )
            except BaseException as exc:  # noqa: BLE001 - recorded for assertions
                error["exc"] = exc

        thread = threading.Thread(target=target, daemon=True)
        thread.start()

        deadline = time.time() + timeout
        state: dict = {}
        while time.time() < deadline:
            if "exc" in error:
                break
            if self.state.exists():
                try:
                    state = json.loads(self.state.read_text())
                except ValueError:
                    state = {}
                if state.get("last_acknowledged_sequence", 0) >= 1:
                    break
            time.sleep(0.05)

        # serve() installs SIGTERM/SIGINT handlers, but those only fire in the
        # main thread. Stop it by terminating the child, which the loop treats
        # as a fatal exit.
        child_pid = state.get("child_pid")
        if child_pid:
            try:
                os.kill(child_pid, 15)
            except (ProcessLookupError, PermissionError):
                pass
        thread.join(5)
        # Re-read after the loop has unwound: the polled snapshot predates any
        # terminal state the listener writes on its way out.
        final = state
        if self.state.exists():
            try:
                final = json.loads(self.state.read_text())
            except ValueError:
                pass
        return {"state": final, "error": error, "alive": thread.is_alive()}

    def test_serve_dispatches_and_records_a_real_replay_receipt(self):
        publish(spec(sequence=1), topology=self.topology)
        outcome = self.run_serve()
        self.assertEqual(outcome["state"].get("role"), "worker")
        self.assertEqual(outcome["state"].get("last_acknowledged_sequence"), 1)
        self.assertIsInstance(outcome["state"].get("child_pid"), int)

    def test_serve_captures_child_stderr_instead_of_discarding_it(self):
        publish(spec(sequence=1), topology=self.topology)
        self.set_env("FAKE_AGENT_STDERR", "startup diagnostic here\n")
        self.run_serve()
        log = self.state.parent / "worker.child-stderr.log"
        self.assertTrue(log.is_file(), "child stderr log was not created")
        self.assertIn("startup diagnostic here", log.read_text())

    def test_serve_reports_a_child_that_exits_immediately(self):
        publish(spec(sequence=1), topology=self.topology)
        self.set_env("FAKE_AGENT_EXIT", "3")
        outcome = self.run_serve(timeout=5)
        self.assertIn("exc", outcome["error"])
        self.assertIn("exited with", str(outcome["error"]["exc"]))

    def test_serve_times_out_when_the_child_never_replays(self):
        publish(spec(sequence=1), topology=self.topology)
        self.set_env("FAKE_AGENT_NO_REPLAY", "1")

        # A short ack timeout keeps the test fast; the property is the same.
        original = self.topology.ack_timeout_seconds
        object.__setattr__(self.topology, "ack_timeout_seconds", 1)
        try:
            outcome = self.run_serve(timeout=6)
        finally:
            object.__setattr__(self.topology, "ack_timeout_seconds", original)

        self.assertIn("exc", outcome["error"])
        self.assertIn("acknowledge", str(outcome["error"]["exc"]))
        self.assertEqual(outcome["state"].get("status"), "transport-ack-timeout")

    def test_a_second_listener_for_the_same_role_is_refused(self):
        publish(spec(sequence=1), topology=self.topology)
        first = self.run_serve()
        self.assertEqual(first["state"].get("last_acknowledged_sequence"), 1)
        # With the first listener stopped the lock is free again; hold it
        # manually to prove the singleton guard rejects a concurrent serve.
        import fcntl

        lock_path = self.state.parent / f".{self.state.name}.lock"
        fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            with self.assertRaises(BridgeError):
                serve("worker", self.topology, self.state, executable=str(self.binary))
        finally:
            fcntl.flock(fd, fcntl.LOCK_UN)
            os.close(fd)


class FakeAgentCliTests(unittest.TestCase):
    """The test double itself must behave, or the tests above prove nothing."""

    def test_replays_only_when_asked(self):
        message = json.dumps({"type": "user", "message": {"content": "PING abc"}}) + "\n"
        with_replay = subprocess.run(
            [sys.executable, str(FAKE_CLI), "--replay-user-messages"],
            input=message.encode(),
            capture_output=True,
            timeout=10,
        )
        self.assertIn("PING abc", with_replay.stdout.decode())

        without = subprocess.run(
            [sys.executable, str(FAKE_CLI)],
            input=message.encode(),
            capture_output=True,
            timeout=10,
        )
        self.assertEqual(without.stdout.decode().strip(), "")


if __name__ == "__main__":
    unittest.main()
