#!/usr/bin/env python3
"""A dead owner must be distinguishable from a live one, and must say so.

Three gaps, reported by an external reader of this substrate:

  * the child survives a parent that dies without running its finally block
  * a lease records its owner's pid and nothing ever checks whether it lives
  * failure reaches stderr and an exit code, never anything that reads the bridge

The tests that matter here are the refusals. A reaper that reaps everything
would satisfy every "it reaped" assertion while destroying live work, so the
declines are asserted at least as hard as the reap.
"""

from __future__ import annotations

import datetime as dt
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import textwrap
import time
import unittest

import conftest  # noqa: F401,E402  (path setup)

from coordination_substrate.lease import (  # noqa: E402
    claim,
    owner_is_alive,
    process_start_ticks,
    reap,
    read_lease,
)
from coordination_substrate.listener import write_terminal_state  # noqa: E402

from support import build_bridge  # noqa: E402

SHA = "b" * 64
LATER = dt.datetime(2026, 8, 1, 12, 0, 0, tzinfo=dt.timezone.utc)


class ProcessIdentityTests(unittest.TestCase):
    """A pid is not an identity. A pid plus its start time is."""

    def test_a_live_process_reports_start_ticks(self):
        self.assertIsInstance(process_start_ticks(os.getpid()), int)

    def test_the_value_is_stable_across_reads(self):
        first = process_start_ticks(os.getpid())
        time.sleep(0.05)
        self.assertEqual(process_start_ticks(os.getpid()), first)

    def test_an_absent_process_reports_nothing(self):
        self.assertIsNone(process_start_ticks(999_999_999))

    def test_a_comm_field_containing_spaces_and_parens_is_parsed(self):
        """Field 22 is positional after the final ')'. A naive split on
        whitespace mis-parses any process whose name contains a space or a
        parenthesis, which is exactly how a reaper starts reading the wrong
        number and killing the wrong process."""
        tmp = Path(self._proc())
        stat = tmp / "424242" / "stat"
        stat.parent.mkdir(parents=True)
        fields = ["424242", "(od d ) na)me)", "S"] + [str(n) for n in range(4, 23)]
        stat.write_text(" ".join(fields) + "\n", encoding="utf-8")
        self.assertEqual(process_start_ticks(424242, tmp), 22)

    def _proc(self):
        holder = tempfile.TemporaryDirectory()
        self.addCleanup(holder.cleanup)
        return holder.name


class OwnerLivenessTests(unittest.TestCase):
    def test_the_owner_that_claimed_is_reported_alive(self):
        pid = os.getpid()
        record = {"listener_pid": pid, "listener_start_ticks": process_start_ticks(pid)}
        self.assertIs(owner_is_alive(record), True)

    def test_a_reused_pid_is_not_the_original_owner(self):
        """The trap. Without start-time comparison this returns alive, and a
        reaper built on it eventually acts against an unrelated process."""
        pid = os.getpid()
        record = {
            "listener_pid": pid,
            "listener_start_ticks": process_start_ticks(pid) + 1,
        }
        self.assertIs(owner_is_alive(record), False)

    def test_a_departed_process_is_provably_gone(self):
        record = {"listener_pid": 999_999_999, "listener_start_ticks": 1}
        self.assertIs(owner_is_alive(record), False)

    def test_a_record_without_captured_identity_is_unadjudicable(self):
        """None, not False. A lease claimed before this change must never be
        reaped, or deploying it would release every live lease on the estate."""
        self.assertIsNone(owner_is_alive({"listener_pid": os.getpid()}))
        self.assertIsNone(owner_is_alive({}))


class ReapTests(unittest.TestCase):
    def setUp(self):
        holder = tempfile.TemporaryDirectory()
        self.addCleanup(holder.cleanup)
        self.topology = build_bridge(Path(holder.name))

    def claim_lease(self, pid, child_pid=None):
        return claim(
            self.topology,
            task_id="T-1",
            attempt=1,
            request_sha256=SHA,
            owner="owner-1",
            harness="harness",
            listener_pid=pid,
            listener_child_pid=child_pid if child_pid is not None else pid,
            session_id="00000000-0000-4000-8000-000000000000",
            clock=lambda: LATER,
        )

    def test_claiming_captures_the_owner_identity(self):
        lease = self.claim_lease(os.getpid())["lease"]
        self.assertEqual(
            lease["listener_start_ticks"], process_start_ticks(os.getpid())
        )

    def test_an_unknown_pid_omits_the_field_rather_than_writing_null(self):
        """Absence means never captured. A null on disk would assert an
        identity the code did not establish."""
        lease = self.claim_lease(999_999_999)["lease"]
        self.assertNotIn("listener_start_ticks", lease)

    def test_a_live_owner_is_not_reaped(self):
        self.claim_lease(os.getpid())
        outcome = reap(self.topology, clock=lambda: LATER)
        self.assertEqual(outcome["action"], "reap-declined")
        self.assertEqual(outcome["reason"], "owner-alive")
        self.assertIsNotNone(read_lease(self.topology))

    def test_an_unadjudicable_lease_is_not_reaped(self):
        self.claim_lease(999_999_999)
        outcome = reap(self.topology, clock=lambda: LATER)
        self.assertEqual(outcome["reason"], "owner-identity-not-captured")
        self.assertIsNotNone(read_lease(self.topology))

    def test_a_dead_owner_inside_its_lease_window_is_not_reaped(self):
        """An owner may die and be resumed inside its own window. Reaping
        early races the resume it was supposed to protect."""
        self.claim_lease(os.getpid())
        self._make_owner_look_dead()
        outcome = reap(self.topology, clock=lambda: LATER)
        self.assertEqual(outcome["reason"], "owner-gone-but-lease-unexpired")
        self.assertIsNotNone(read_lease(self.topology))

    def test_a_dead_owner_past_expiry_is_reaped(self):
        self.claim_lease(os.getpid())
        self._make_owner_look_dead()
        after = LATER + dt.timedelta(seconds=self.topology.lease_seconds + 1)
        outcome = reap(self.topology, clock=lambda: after)
        self.assertEqual(outcome["action"], "reaped")
        self.assertIsNone(read_lease(self.topology))

    def test_reaping_an_empty_slot_declines_rather_than_raising(self):
        outcome = reap(self.topology, clock=lambda: LATER)
        self.assertEqual(outcome["reason"], "no-lease-present")

    def test_reap_is_idempotent(self):
        self.claim_lease(os.getpid())
        self._make_owner_look_dead()
        after = LATER + dt.timedelta(seconds=self.topology.lease_seconds + 1)
        self.assertEqual(reap(self.topology, clock=lambda: after)["action"], "reaped")
        self.assertEqual(
            reap(self.topology, clock=lambda: after)["reason"], "no-lease-present"
        )

    def _make_owner_look_dead(self):
        """Rewrite the recorded start time so the live pid reads as a stranger.

        Cheaper and safer than killing a real process, and it exercises the
        same comparison a genuine death would.
        """
        _, path = (
            self.topology.root / self.topology.lease_dir,
            self.topology.root / self.topology.lease_dir / "lease.yaml",
        )
        import yaml

        value = yaml.safe_load(path.read_text())
        value["listener_start_ticks"] = value["listener_start_ticks"] + 1
        path.write_text(yaml.safe_dump(value, sort_keys=False), encoding="utf-8")


class TerminalStateTests(unittest.TestCase):
    def setUp(self):
        holder = tempfile.TemporaryDirectory()
        self.addCleanup(holder.cleanup)
        self.state = Path(holder.name) / "listener.json"

    def test_a_terminal_state_is_recorded_and_marked_terminal(self):
        write_terminal_state(self.state, "worker", "failed", "TypeError: boom")
        value = json.loads(self.state.read_text())
        self.assertEqual(value["status"], "failed")
        self.assertIs(value["terminal"], True)
        self.assertIn("TypeError", value["detail"])

    def test_it_replaces_a_stale_healthy_status(self):
        """The gap: a crashed listener whose file still says running is
        indistinguishable from a busy one."""
        self.state.write_text(json.dumps({"status": "running"}), encoding="utf-8")
        write_terminal_state(self.state, "worker", "failed", "gone")
        self.assertEqual(json.loads(self.state.read_text())["status"], "failed")

    def test_detail_is_bounded(self):
        write_terminal_state(self.state, "worker", "failed", "x" * 5000)
        self.assertLessEqual(len(json.loads(self.state.read_text())["detail"]), 500)

    def test_an_unknown_state_path_is_a_silent_no_op(self):
        write_terminal_state(None, "worker", "failed", "before the path was known")

    def test_a_recording_failure_never_replaces_the_original_failure(self):
        """It runs while something has already gone wrong. If it raised, it
        would mask the fault it exists to report."""
        write_terminal_state(Path("/proc/1/cannot-write-here"), "w", "failed", "x")


class UnexpectedExitTests(unittest.TestCase):
    """The whole point: an unanticipated death must still be visible."""

    def test_an_unexpected_exception_records_terminal_then_re_raises(self):
        holder = tempfile.TemporaryDirectory()
        self.addCleanup(holder.cleanup)
        state = Path(holder.name) / "listener.json"
        root = Path(__file__).resolve().parents[1]

        script = textwrap.dedent(
            f"""
            import sys
            sys.path.insert(0, {str(root)!r})
            from coordination_substrate import listener

            def explode(*a, **k):
                raise TypeError("an unanticipated defect")

            listener.serve = explode
            listener.Topology.load = staticmethod(lambda *a, **k: _T())

            class _R:
                listener_state = "listener.json"

            class _T:
                root = {str(state.parent)!r}
                def role(self, name):
                    return _R()

            listener.Path = __import__("pathlib").Path
            sys.exit(listener.main(["serve", "--role", "worker",
                                    "--state", {str(state)!r}]))
            """
        )
        result = subprocess.run([sys.executable, "-c", script],
                                capture_output=True, text=True)
        self.assertNotEqual(result.returncode, 0, "the failure must not be swallowed")
        self.assertIn("TypeError", result.stderr, "the traceback must survive")
        self.assertTrue(state.is_file(), "nothing recorded the death")
        value = json.loads(state.read_text())
        self.assertEqual(value["status"], "failed")
        self.assertIn("an unanticipated defect", value["detail"])


class ParentDeathSignalTests(unittest.TestCase):
    """The orphan case: the parent dies in a way its finally block cannot see."""

    ROOT = str(Path(__file__).resolve().parents[1])

    def _spawn(self, guarded: bool) -> tuple[int, int]:
        """Start a parent that spawns a long-lived child; return both pids."""
        script = textwrap.dedent(
            f"""
            import subprocess, sys, time
            sys.path.insert(0, {self.ROOT!r})
            from coordination_substrate.listener import _die_with_parent
            kwargs = {{"preexec_fn": _die_with_parent}} if {guarded!r} else {{}}
            child = subprocess.Popen(
                [sys.executable, "-c", "import time; time.sleep(120)"], **kwargs)
            print(child.pid, flush=True)
            time.sleep(120)
            """
        )
        parent = subprocess.Popen([sys.executable, "-c", script],
                                  stdout=subprocess.PIPE, text=True)
        child_pid = int(parent.stdout.readline().strip())
        self.addCleanup(parent.stdout.close)
        self.addCleanup(self._force_kill, parent.pid)
        self.addCleanup(self._force_kill, child_pid)
        return parent.pid, child_pid

    @staticmethod
    def _force_kill(pid: int) -> None:
        try:
            os.kill(pid, 9)
        except (ProcessLookupError, PermissionError):
            pass

    @staticmethod
    def _alive(pid: int) -> bool:
        for _ in range(50):
            if not Path(f"/proc/{pid}").exists():
                return False
            time.sleep(0.1)
        return True

    def test_the_child_dies_when_the_parent_is_hard_killed(self):
        """SIGKILL means no finally block runs. The kernel has to carry it."""
        parent_pid, child_pid = self._spawn(guarded=True)
        os.kill(parent_pid, 9)
        self.assertFalse(self._alive(child_pid),
                         "the child outlived a SIGKILLed parent: it is orphaned")

    def test_without_the_guard_the_child_is_orphaned(self):
        """The control. Without this, the test above could pass because of
        anything at all -- process groups, the harness, luck -- and would keep
        passing if the guard were deleted."""
        parent_pid, child_pid = self._spawn(guarded=False)
        os.kill(parent_pid, 9)
        self.assertTrue(self._alive(child_pid),
                        "the child died without the guard; the test above proves nothing")


if __name__ == "__main__":
    unittest.main()
