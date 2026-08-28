"""Listener invariants: validate before waking, require a replay receipt."""

from __future__ import annotations

import datetime as dt
import json
from pathlib import Path
import sys
import tempfile
import unittest

import conftest  # noqa: F401  (path setup)

from support import (  # noqa: E402
    FakeChild,
    build_bridge,
    install_event,
    publish_pointer,
    write_ledger,
)
from coordination_substrate.listener import (  # noqa: E402
    BridgeError,
    Listener,
    build_command,
    read_authoritative_pointer,
    session_conflict,
)


def _utc(offset_seconds: int = 0) -> str:
    base = dt.datetime(2026, 8, 1, 12, 0, 0, tzinfo=dt.timezone.utc)
    return (base + dt.timedelta(seconds=offset_seconds)).strftime("%Y-%m-%dT%H:%M:%SZ")


NOW = dt.datetime(2026, 8, 1, 12, 0, 0, tzinfo=dt.timezone.utc).timestamp()


class ListenerTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.topology = build_bridge(self.root)
        self.state = self.root / "bridge/listeners/worker.json"
        self.child = FakeChild()
        self.ticks = [0.0]

    def tearDown(self):
        self.temp.cleanup()

    def clock(self):
        return self.ticks[0]

    def listener(self, **kwargs):
        return Listener(
            role="worker",
            topology=self.topology,
            state_path=self.state,
            child=self.child,
            clock=self.clock,
            wall_clock=lambda: NOW,
            **kwargs,
        )

    def seed_inbound(self, sequence: int = 2, name: str = "task-assigned") -> None:
        rel, digest = install_event(
            self.root,
            "bridge/events",
            f"in-{sequence}.yaml",
            {"sequence": sequence, "event": name, "writer": "boss", "created_at": _utc()},
        )
        publish_pointer(
            self.root, "bridge/boss-to-worker.yaml", rel, digest, sequence, event_name=name
        )

    def seed_outbound(self, sequence: int = 3, *, age: int = 0, **fields) -> None:
        body = {
            "sequence": sequence,
            "event": "progress",
            "writer": "worker",
            "task_id": "T-1",
            "created_at": _utc(-age),
        }
        body.update(fields)
        rel, digest = install_event(
            self.root, "bridge/events", f"out-{sequence}.yaml", body
        )
        publish_pointer(
            self.root,
            "bridge/worker-to-boss.yaml",
            rel,
            digest,
            sequence,
            event_name="progress",
            writer="worker",
        )

    # ---- pointer rail --------------------------------------------------

    def test_validated_pointer_dispatched_once_and_replay_acks(self):
        self.seed_inbound(2)
        listener = self.listener()
        self.assertTrue(listener.dispatch_if_new())
        self.assertFalse(listener.dispatch_if_new())  # one outstanding wake only
        listener.accept_output(self.child.replay(self.child.last_transport_id()))
        self.assertEqual(listener.last_sequence, 2)
        self.assertIsNone(listener.pending)

    def test_a_non_user_record_quoting_the_id_does_not_ack(self):  # LIS-1
        import json
        self.seed_inbound(2)
        listener = self.listener()
        self.assertTrue(listener.dispatch_if_new())
        tid = self.child.last_transport_id()
        before = listener.last_sequence
        # A rejected input echoed in an error/system record quotes the id but is
        # not a user-message replay: it must not clear pending or advance the
        # delivered sequence.
        forged = json.dumps({"type": "system", "subtype": "error",
                             "message": {"role": "system",
                                         "content": f"rejected input: {tid}"}}).encode()
        self.assertFalse(listener.accept_output(forged))
        self.assertIsNotNone(listener.pending)
        self.assertEqual(listener.last_sequence, before)
        # A genuine user-message replay of the same id acks.
        listener.accept_output(self.child.replay(tid))
        self.assertEqual(listener.last_sequence, 2)
        self.assertIsNone(listener.pending)

    def test_pointer_rail_survives_a_transient_or_absent_pointer(self):  # LIS-2
        listener = self.listener()
        # No inbound pointer exists yet: read raises BridgeError; the rail must
        # skip this poll, not take down the listener.
        with self.assertRaises(BridgeError):
            read_authoritative_pointer(self.topology, "worker")
        self.assertFalse(listener.dispatch_if_new())
        # Normal dispatch + ack.
        self.seed_inbound(2)
        self.assertTrue(listener.dispatch_if_new())
        listener.accept_output(self.child.replay(self.child.last_transport_id()))
        self.assertEqual(listener.last_sequence, 2)
        # Prune the acknowledged event; the pointer now references a missing
        # event -- transient invalidity, not corruption: still no crash.
        for p in (self.root / "bridge/events").glob("in-2.yaml"):
            p.unlink()
        self.assertFalse(listener.dispatch_if_new())

    def test_new_pointer_queued_after_prior_ack(self):
        self.seed_inbound(2)
        listener = self.listener()
        listener.dispatch_if_new()
        listener.accept_output(self.child.replay(self.child.last_transport_id()))
        self.seed_inbound(3)
        self.assertTrue(listener.dispatch_if_new())

    def test_hash_mismatch_refused_before_dispatch(self):
        self.seed_inbound(2)
        publish_pointer(
            self.root, "bridge/boss-to-worker.yaml", "bridge/events/in-2.yaml", "0" * 64, 2
        )
        with self.assertRaises(BridgeError):
            read_authoritative_pointer(self.topology, "worker")

    def test_event_path_escape_refused(self):
        (self.root / "outside").mkdir()
        rel, digest = install_event(
            self.root, "outside", "evil.yaml", {"sequence": 2, "event": "x", "writer": "boss"}
        )
        publish_pointer(self.root, "bridge/boss-to-worker.yaml", rel, digest, 2)
        with self.assertRaises(BridgeError):
            read_authoritative_pointer(self.topology, "worker")

    def test_non_orchestrator_writer_refused_on_inbound(self):
        rel, digest = install_event(
            self.root,
            "bridge/events",
            "spoof.yaml",
            {"sequence": 2, "event": "x", "writer": "worker", "created_at": _utc()},
        )
        publish_pointer(
            self.root, "bridge/boss-to-worker.yaml", rel, digest, 2, event_name="x", writer="worker"
        )
        with self.assertRaises(BridgeError):
            read_authoritative_pointer(self.topology, "worker")

    def test_missing_transport_ack_expires(self):
        self.seed_inbound(2)
        listener = self.listener(ack_timeout=10)
        listener.dispatch_if_new()
        self.ticks[0] = 11
        with self.assertRaises(BridgeError):
            listener.assert_not_stale()

    def test_restart_redelivers_latest_pointer_idempotently(self):
        self.seed_inbound(2)
        first = self.listener()
        first.dispatch_if_new()
        first.accept_output(self.child.replay(self.child.last_transport_id()))
        # A fresh listener starts at sequence 0 and re-wakes on the same pointer.
        second = self.listener()
        self.assertTrue(second.dispatch_if_new())

    # ---- continuation rail ---------------------------------------------

    def test_continuation_wakes_once_per_role_sequence(self):
        self.seed_inbound(2)
        self.seed_outbound(3, next_pause="publication-candidate")
        listener = self.listener()
        self.assertTrue(listener.dispatch_continuation_if_due())
        listener.accept_output(self.child.replay(self.child.last_transport_id()))
        self.assertFalse(listener.dispatch_continuation_if_due())

    def test_closed_task_progress_cannot_wake_a_different_active_task(self):
        self.seed_outbound(
            3,
            age=600,
            task_id="T-CLOSED",
            next_pause="boss-selection",
        )
        listener = self.listener()
        self.assertFalse(listener.dispatch_self_wake_if_due())
        self.assertFalse(listener.dispatch_continuation_if_due())
        self.assertEqual(self.child.stdin.written, [])

    def test_end_turn_rearms_same_progress_but_caps_no_progress_turns(self):
        self.seed_inbound(2)
        self.seed_outbound(3, next_pause="publication-candidate")
        listener = self.listener()

        end_turn = json.dumps(
            {
                "type": "assistant",
                "message": {"role": "assistant", "stop_reason": "end_turn", "content": []},
            }
        ).encode()

        # First continuation is admitted by the new progress sequence.
        self.assertTrue(listener.dispatch_continuation_if_due())
        listener.accept_output(self.child.replay(self.child.last_transport_id()))
        self.assertFalse(listener.dispatch_continuation_if_due())

        # A completed active turn re-arms the same sequence. Two bounded follow-up
        # turns are allowed; the third end_turn cannot create an infinite loop.
        for expected_count in (2, 3):
            listener.accept_output(end_turn)
            self.assertTrue(listener.dispatch_continuation_if_due())
            self.assertEqual(
                listener.last_continuation_wake["dispatch_count"], expected_count
            )
            listener.accept_output(self.child.replay(self.child.last_transport_id()))
        listener.accept_output(end_turn)
        self.assertFalse(listener.dispatch_continuation_if_due())

    def test_live_stream_result_rearms_same_progress_sequence(self):
        self.seed_inbound(2)
        self.seed_outbound(3, next_pause="publication-candidate")
        listener = self.listener()

        self.assertTrue(listener.dispatch_continuation_if_due())
        listener.accept_output(self.child.replay(self.child.last_transport_id()))
        self.assertFalse(listener.dispatch_continuation_if_due())

        # Claude's live stream-json protocol ends a completed turn with result;
        # assistant/end_turn is the persisted-session representation.
        result = json.dumps(
            {"type": "result", "subtype": "success", "result": "done"}
        ).encode()
        listener.accept_output(result)
        self.assertTrue(listener.dispatch_continuation_if_due())
        self.assertEqual(listener.last_continuation_wake["dispatch_count"], 2)

    def test_new_progress_sequence_resets_continuation_cap(self):
        self.seed_inbound(2)
        self.seed_outbound(3, next_pause="first-pause")
        listener = self.listener()
        listener.last_continuation_wake = {
            "observed_role_sequence": 3,
            "dispatch_count": 3,
        }
        listener.continuation_rearm_ready = True
        self.assertFalse(listener.dispatch_continuation_if_due())

        self.seed_outbound(4, next_pause="second-pause")
        self.assertTrue(listener.dispatch_continuation_if_due())
        self.assertEqual(
            listener.last_continuation_wake,
            {"observed_role_sequence": 4, "dispatch_count": 1},
        )

    def test_continuation_suppressed_by_explicit_pause_signals(self):
        self.seed_inbound(2)
        for field in (
            {"task_paused": True},
            {"response_required": True},
            {"decision_required": True},
            {"terminal_decision": "FAIL"},
            {"lock_released": True},
        ):
            with self.subTest(field=field):
                self.seed_outbound(3, next_pause="something", **field)
                listener = self.listener()
                self.assertFalse(listener.dispatch_continuation_if_due())

    def test_continuation_requires_active_ledger_work(self):
        self.seed_inbound(2)
        self.seed_outbound(3, next_pause="publication-candidate")
        write_ledger(self.root, active_task="T-1", safe_pause=True)
        listener = self.listener()
        self.assertFalse(listener.dispatch_continuation_if_due())

    # ---- heartbeat rail -------------------------------------------------

    def test_heartbeat_fires_only_after_stale_window(self):
        self.seed_inbound(2)
        self.seed_outbound(3, age=100)
        listener = self.listener(heartbeat_stale=600)
        self.assertFalse(listener.dispatch_self_wake_if_due())
        self.seed_outbound(4, age=900)
        listener = self.listener(heartbeat_stale=600)
        self.assertTrue(listener.dispatch_self_wake_if_due())
        self.assertIn("BRIDGE_HEARTBEAT_WAKE", self.child.sent_texts()[-1])

    def test_idle_role_is_never_heartbeat_woken(self):
        self.seed_inbound(2)
        self.seed_outbound(3, age=900)
        write_ledger(self.root, active_task=None)
        listener = self.listener()
        self.assertFalse(listener.dispatch_self_wake_if_due())

    def test_unreadable_ledger_signals_health_without_task_authority(self):
        self.seed_inbound(2)
        (self.root / "state.yaml").write_text("{{{ not yaml")
        listener = self.listener()
        self.assertTrue(listener.dispatch_self_wake_if_due())
        text = self.child.sent_texts()[-1]
        self.assertIn("BRIDGE_HEALTH_WAKE", text)
        self.assertIn("grants no task", text)

    def test_epoch_gate_suppresses_pre_epoch_durations(self):
        import dataclasses

        self.topology = dataclasses.replace(
            self.topology,
            time_authority_epoch=dt.datetime(2027, 1, 1, tzinfo=dt.timezone.utc),
        )
        self.seed_inbound(2)
        self.seed_outbound(3, age=900)
        listener = self.listener()
        self.assertFalse(listener.dispatch_self_wake_if_due())

    # ---- messages and process safety ------------------------------------

    def test_every_wake_disclaims_authority(self):
        """A wake is scheduling. No rail may read as instruction or scope."""
        self.seed_inbound(2)
        self.seed_outbound(3, age=900, next_pause="publication-candidate")

        pointer_listener = self.listener()
        pointer_listener.dispatch_if_new()

        continuation_listener = self.listener()
        continuation_listener.dispatch_continuation_if_due()

        heartbeat_listener = self.listener()
        heartbeat_listener.last_continuation_wake = {"observed_role_sequence": 3}
        heartbeat_listener.dispatch_self_wake_if_due()

        texts = self.child.sent_texts()
        self.assertEqual(len(texts), 3)
        for text in texts:
            # Each rail must state that transport confers no authority, and
            # must not read as an instruction to begin or repeat work.
            self.assertRegex(
                text,
                r"(not treat this transport receipt as new scope|"
                r"grants no new scope|not scope or acceptance authority)",
            )
            self.assertIn("authority", text)

    def test_command_uses_streaming_input_and_replay(self):
        command = build_command("claude", None, None)
        self.assertIn("--replay-user-messages", command)
        self.assertIn("--input-format", command)
        self.assertIn("stream-json", command)
        with self.assertRaises(BridgeError):
            build_command("claude", "a", "b")

    def test_session_conflict_ignores_allowed_pids(self):
        proc = self.root / "proc"
        (proc / "5000").mkdir(parents=True)
        (proc / "5000" / "cmdline").write_bytes(b"claude\0--resume=abc\0")
        self.assertTrue(session_conflict("abc", proc_root=proc))
        self.assertFalse(session_conflict("abc", allowed_pids={5000}, proc_root=proc))
        self.assertFalse(session_conflict(None, proc_root=proc))


if __name__ == "__main__":
    unittest.main()
