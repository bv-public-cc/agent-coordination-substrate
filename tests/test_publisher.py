"""Publisher invariants: event first, pointer bound to installed bytes."""

from __future__ import annotations

import datetime as dt
import hashlib
import json
from pathlib import Path
import sys
import tempfile
import threading
import copy
import unittest

import support
from coordination_substrate.publisher import (
    ASSIGNMENT_REQUEST_KEYS, _activation_epoch)
from coordination_substrate.topology import Topology

import yaml

import conftest  # noqa: F401  (path setup)

from support import build_bridge, install_event, seed_pointer, spec, write_listener_state  # noqa: E402

from coordination_substrate.publisher import (  # noqa: E402
    PublishError, publish, _derived_action_identity)  # noqa: E402


class PublisherTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.topology = build_bridge(self.root)
        self.pointer = self.root / "bridge/boss-to-worker.yaml"
        seed_pointer(self.root, "bridge/boss-to-worker.yaml", 1)

    def tearDown(self):
        self.temp.cleanup()

    def publish(self, value=None, **kwargs):
        return publish(value or spec(), topology=self.topology, **kwargs)

    # ---- core binding -------------------------------------------------

    def test_event_first_hash_bound_pointer_and_colon_safe(self):
        result = self.publish()
        pointer = yaml.safe_load(self.pointer.read_bytes())
        event_path = self.root / result["event_path"]
        self.assertTrue(event_path.is_file())
        self.assertEqual(pointer["authoritative_event"]["path"], result["event_path"])
        self.assertEqual(pointer["authoritative_event"]["sha256"], result["event_sha256"])
        installed = yaml.safe_load(event_path.read_bytes())
        self.assertEqual(installed["note"], "prose with a colon: stays valid YAML")
        # The recorded digest must be of the bytes actually on disk.
        self.assertEqual(
            hashlib.sha256(event_path.read_bytes()).hexdigest(), result["event_sha256"]
        )

    def test_filename_timestamp_is_derived_from_the_authoritative_clock(self):
        moment = dt.datetime(2026, 8, 3, 4, 5, 6, tzinfo=dt.timezone.utc)
        value = spec(filename="20990101T000000Z-2-boss-test.yaml")
        original = value["event_filename"]
        result = self.publish(value, now=moment)
        self.assertNotEqual(original, Path(result["event_path"]).name)
        self.assertEqual(
            Path(result["event_path"]).name,
            "20260803T040506Z-2-boss-test.yaml",
        )

    def test_publisher_not_caller_creates_authoritative_event(self):
        value = spec()
        value["pointer"]["authoritative_event"] = {"path": "x", "sha256": "0" * 64}
        with self.assertRaises(PublishError):
            self.publish(value)

    # ---- failure atomicity --------------------------------------------

    def test_failure_after_event_never_changes_pointer(self):
        before = self.pointer.read_bytes()
        with self.assertRaises(PublishError):
            self.publish(failpoint="after_event")
        self.assertEqual(self.pointer.read_bytes(), before)
        self.assertEqual(
            len(list((self.root / "bridge/events").glob("*-2-boss-test.yaml"))),
            1,
        )

    def test_failure_before_replace_never_changes_pointer(self):
        before = self.pointer.read_bytes()
        with self.assertRaises(PublishError):
            self.publish(failpoint="before_pointer_replace")
        self.assertEqual(self.pointer.read_bytes(), before)

    def test_existing_event_is_never_overwritten(self):
        result = self.publish()
        target = self.root / result["event_path"]
        body = target.read_bytes()
        with self.assertRaises(PublishError):
            self.publish()
        self.assertEqual(target.read_bytes(), body)

    def test_orphaned_sequence_cannot_republish_under_a_new_clock_filename(self):
        first = dt.datetime(2026, 8, 3, 4, 5, 6, tzinfo=dt.timezone.utc)
        second = first + dt.timedelta(seconds=1)
        with self.assertRaisesRegex(PublishError, "injected failure"):
            self.publish(now=first, failpoint="after_event")
        with self.assertRaisesRegex(
            PublishError,
            "writer sequence already exists in append-only history",
        ):
            self.publish(now=second)
        self.assertEqual(
            len(list((self.root / "bridge/events").glob("*-2-boss-*.yaml"))),
            1,
        )

    # ---- sequence and route ownership ---------------------------------

    def test_rejects_nonmonotonic_and_mismatched_identity(self):
        for mutate in (
            lambda v: v["event"].update(sequence=1),
            lambda v: v["pointer"].update(event="different"),
            lambda v: v["event"].update(writer="worker"),
            lambda v: v.update(event_filename="20260801T000000Z-99-boss-test.yaml"),
            lambda v: v.update(pointer_path="bridge/not-a-route.yaml"),
        ):
            value = spec()
            mutate(value)
            with self.assertRaises(PublishError):
                self.publish(value)

    def test_writer_must_own_the_route(self):
        seed_pointer(self.root, "bridge/worker-to-boss.yaml", 1, writer="worker")
        write_listener_state(self.root, "worker", child_pid=1)
        value = spec(pointer="bridge/worker-to-boss.yaml", writer="boss", sequence=2)
        with self.assertRaises(PublishError):
            self.publish(value)

    def test_orchestrator_sequence_is_global_across_routes(self):
        seed_pointer(self.root, "bridge/boss-to-auditor.yaml", 1)
        self.publish(spec(sequence=5))
        # 4 is greater than the auditor pointer alone but below the global max.
        with self.assertRaises(PublishError):
            self.publish(spec(pointer="bridge/boss-to-auditor.yaml", sequence=4))
        self.publish(spec(pointer="bridge/boss-to-auditor.yaml", sequence=6))

    def test_string_sequence_on_orchestrator_route_is_a_typed_refusal(self):  # PUB-4
        # A string sequence previously raised a raw TypeError out of publish()
        # (the orchestrator-sequence check runs before the sequence type is
        # validated). It must be a typed PublishError.
        with self.assertRaises(PublishError):
            self.publish(spec(sequence="7"))

    def test_task_assignment_also_requires_credential_contract(self):  # PUB-3
        # `-assignment` is an assignment for layer purposes; it must not escape
        # the credential contract that `-assigned` triggers.
        with self.assertRaises(PublishError):
            self.publish(spec(event_name="task-assignment"))

    def test_concurrent_lower_sequence_cannot_clobber_pointer(self):
        """Two publishers race; the lock must cover check *and* replace.

        The higher sequence enters first and parks inside the critical section
        after its monotonicity check. The lower sequence then tries to publish
        concurrently. Without a lock spanning both operations it would validate
        against the stale pointer and replace last, regressing the sequence.
        """
        inside = threading.Event()
        finished = threading.Event()
        loser: dict = {}

        def park():
            inside.set()
            # While this thread holds the critical section, a correct lock keeps
            # the racer blocked. Assert that directly: if the racer completes in
            # this window, the lock does not span check-and-replace.
            blocked = not finished.wait(0.5)
            loser["blocked_while_locked"] = blocked

        def publish_lower():
            inside.wait(5)
            try:
                self.publish(spec(sequence=8))
                loser["result"] = "published"
            except PublishError as exc:
                loser["result"] = f"refused: {exc}"
            finally:
                finished.set()

        racer = threading.Thread(target=publish_lower)
        racer.start()
        self.publish(spec(sequence=9), after_sequence_check=park)
        racer.join(5)

        self.assertFalse(racer.is_alive(), "racing publisher deadlocked")
        self.assertTrue(
            loser.get("blocked_while_locked"),
            "racer was not blocked: the lock does not cover check-and-replace",
        )
        self.assertTrue(
            loser.get("result", "").startswith("refused:"),
            f"lower sequence was not refused: {loser.get('result')}",
        )
        self.assertEqual(yaml.safe_load(self.pointer.read_bytes())["sequence"], 9)

    def test_the_race_harness_actually_detects_a_missing_lock(self):
        """Negative control: prove the race test above can fail.

        A test that cannot fail is not evidence. This runs the same structure
        with the lock disabled and asserts the harness catches it.
        """
        from contextlib import contextmanager
        from unittest import mock

        @contextmanager
        def no_lock(_path):
            yield

        inside = threading.Event()
        finished = threading.Event()
        observed: dict = {}

        def park():
            inside.set()
            observed["blocked_while_locked"] = not finished.wait(0.5)

        def publish_lower():
            inside.wait(5)
            try:
                self.publish(spec(sequence=8))
                observed["result"] = "published"
            except PublishError as exc:
                observed["result"] = f"refused: {exc}"
            finally:
                finished.set()

        with mock.patch("coordination_substrate.publisher._pointer_lock", no_lock):
            racer = threading.Thread(target=publish_lower)
            racer.start()
            self.publish(spec(sequence=9), after_sequence_check=park)
            racer.join(5)

        # Without the lock the racer runs straight through the critical section.
        self.assertFalse(
            observed.get("blocked_while_locked"),
            "harness cannot distinguish a missing lock",
        )

    def test_missing_route_directory_fails_closed(self):
        """A topology naming an absent directory must refuse, never mkdir."""
        import shutil

        shutil.rmtree(self.root / "bridge/events")
        with self.assertRaises(PublishError):
            self.publish()
        self.assertFalse((self.root / "bridge/events").exists())

    # ---- nested authority references ----------------------------------

    def test_nested_reference_hash_is_verified(self):
        rel, digest = install_event(
            self.root, "bridge/events", "prior.yaml", {"sequence": 1, "event": "seed"}
        )
        good = spec(extra={"responds_to": {"path": rel, "sha256": digest, "sequence": 1}})
        self.publish(good)

        bad = spec(
            sequence=3,
            extra={"responds_to": {"path": rel, "sha256": "0" * 64, "sequence": 1}},
        )
        with self.assertRaises(PublishError):
            self.publish(bad)

    def test_nested_reference_auto_binds_from_bytes(self):
        rel, digest = install_event(
            self.root, "bridge/events", "prior2.yaml", {"sequence": 1, "event": "seed"}
        )
        value = spec(extra={"responds_to": {"path": rel, "sha256": "auto", "sequence": 1}})
        result = self.publish(value)
        installed = yaml.safe_load((self.root / result["event_path"]).read_bytes())
        self.assertEqual(installed["responds_to"]["sha256"], digest)
        self.assertEqual(installed["responds_to"]["reference_intent"], "immutable_ref")

    def test_nested_reference_outside_event_dirs_is_refused(self):
        (self.root / "elsewhere").mkdir()
        rel, digest = install_event(self.root, "elsewhere", "x.yaml", {"sequence": 1})
        value = spec(extra={"responds_to": {"path": rel, "sha256": digest, "sequence": 1}})
        with self.assertRaises(PublishError):
            self.publish(value)

    def test_declared_snapshot_reference_is_verified(self):
        target = self.root / "doc.md"
        target.write_text("hello")
        digest = hashlib.sha256(b"hello").hexdigest()
        good = spec(
            extra={
                "governance": {
                    "path": "doc.md",
                    "sha256": digest,
                    "reference_intent": "snapshot_at_publication",
                }
            }
        )
        self.publish(good)
        target.write_text("changed")
        stale = spec(
            sequence=3,
            extra={
                "governance": {
                    "path": "doc.md",
                    "sha256": digest,
                    "reference_intent": "snapshot_at_publication",
                }
            },
        )
        with self.assertRaises(PublishError):
            self.publish(stale)

    def test_invalid_reference_intent_is_refused(self):
        (self.root / "doc2.md").write_text("x")
        value = spec(
            extra={
                "governance": {
                    "path": "doc2.md",
                    "sha256": "auto",
                    "reference_intent": "whatever",
                }
            }
        )
        with self.assertRaises(PublishError):
            self.publish(value)

    # ---- clock binding -------------------------------------------------

    def test_created_at_is_publisher_bound_when_omitted(self):
        moment = dt.datetime(2026, 8, 1, 12, 0, 0, tzinfo=dt.timezone.utc)
        result = self.publish(now=moment)
        installed = yaml.safe_load((self.root / result["event_path"]).read_bytes())
        self.assertEqual(installed["created_at"], "2026-08-01T12:00:00Z")

    def test_future_created_at_beyond_skew_is_refused(self):
        moment = dt.datetime(2026, 8, 1, 12, 0, 0, tzinfo=dt.timezone.utc)
        value = spec()
        value["event"]["created_at"] = "2026-08-01T13:00:00Z"
        value["pointer"]["created_at"] = "2026-08-01T13:00:00Z"
        with self.assertRaises(PublishError):
            self.publish(value, now=moment)

    def test_event_and_pointer_created_at_must_agree(self):
        value = spec()
        value["event"]["created_at"] = "2026-08-01T12:00:00Z"
        value["pointer"]["created_at"] = "2026-08-01T12:00:01Z"
        with self.assertRaises(PublishError):
            self.publish(value)

    # ---- stop and decision contracts -----------------------------------

    def test_stop_shaped_event_requires_pause_scope(self):
        value = spec(event_name="safety-stop")
        with self.assertRaises(PublishError):
            self.publish(value)

    def test_task_wide_stop_requires_overlap_proof(self):
        value = spec(
            event_name="safety-stop",
            extra={
                "pause_scope": {
                    "blocked_action": "write",
                    "shared_state": "worktree",
                    "last_verified_state": "clean",
                    "continuation_lanes": [],
                    "resume_condition": "operator",
                }
            },
        )
        with self.assertRaises(PublishError):
            self.publish(value)
        value["event"]["pause_scope"]["task_wide_stop_justification"] = "everything overlaps"
        value["event"]["pause_scope"]["overlap_proof"] = ["shared worktree"]
        self.publish(value)

    def test_decision_required_needs_a_contract_that_changes_action(self):
        # A decision-required event is also stop-shaped, so it must carry both
        # the pause scope and the decision contract.
        scope = {
            "blocked_action": "write",
            "shared_state": "worktree",
            "last_verified_state": "clean",
            "continuation_lanes": ["docs"],
            "resume_condition": "orchestrator decision",
        }
        base = {
            "class": "ambiguity",
            "question": "which?",
            "next_material_decision": "pick one",
            "reversible_default": "assume A",
            "next_action_reversible": False,
            "bounded_check": {"method": "read", "budget": "1", "stop_condition": "done"},
        }
        same = dict(base, outcome_actions=[
            {"outcome": "a", "action": "proceed"},
            {"outcome": "b", "action": "proceed"},
        ])
        value = spec(
            event_name="decision-required",
            extra={"decision_contract": same, "pause_scope": scope},
        )
        with self.assertRaises(PublishError):
            self.publish(value)

        differs = dict(base, outcome_actions=[
            {"outcome": "a", "action": "proceed"},
            {"outcome": "b", "action": "stop"},
        ])
        ok = spec(
            event_name="decision-required",
            extra={"decision_contract": differs, "pause_scope": scope},
        )
        self.publish(ok)

    def test_non_ambiguity_decision_also_requires_distinct_actions(self):  # PUB-2
        scope = {
            "blocked_action": "write", "shared_state": "worktree",
            "last_verified_state": "clean", "continuation_lanes": ["docs"],
            "resume_condition": "orchestrator decision",
        }
        # A 'security'-class decision needs no bounded_check, but the flagship
        # "outcomes must reach >=2 distinct actions" rule still applies.
        base = {"class": "security", "question": "grant?",
                "next_material_decision": "allow or deny",
                "reversible_default": "deny"}
        same = dict(base, outcome_actions=[
            {"outcome": "a", "action": "escalate"},
            {"outcome": "b", "action": "escalate"}])
        with self.assertRaises(PublishError):
            self.publish(spec(event_name="decision-required",
                              extra={"decision_contract": same, "pause_scope": scope}))
        # No outcome_actions at all is likewise refused (previously published clean).
        with self.assertRaises(PublishError):
            self.publish(spec(event_name="decision-required",
                              extra={"decision_contract": base, "pause_scope": scope}))
        differs = dict(base, outcome_actions=[
            {"outcome": "a", "action": "allow"},
            {"outcome": "b", "action": "deny"}])
        self.publish(spec(event_name="decision-required",
                          extra={"decision_contract": differs, "pause_scope": scope}))

    def test_task_assignment_requires_credential_disposition(self):
        value = spec(event_name="task-assigned")
        value["event"].pop("credential_contract")
        with self.assertRaisesRegex(PublishError, "credential_contract"):
            self.publish(value)

        value["event"]["credential_contract"] = {"mode": "none"}
        self.publish(value)

    def test_research_assignment_has_the_same_credential_invariant(self):
        value = spec(event_name="research-assigned")
        with self.assertRaisesRegex(PublishError, "credential_contract"):
            self.publish(value)

        value["event"]["credential_contract"] = {"mode": "none"}
        self.publish(value)

    def test_protected_assignment_rejects_embedded_secret_values(self):
        contract = {
            "mode": "protected-interfaces",
            "interfaces": [{
                "env_name": "PROJECT_TOKEN",
                "scope": "one project",
                "transport": "protected-file-or-askpass",
                "never_expose_in": ["argv", "logs", "events", "artifacts", "helper-body"],
                "prohibited_names": ["ADMIN_TOKEN"],
            }],
        }
        value = spec(event_name="task-assigned", extra={"credential_contract": contract})
        self.publish(value)

        bad = spec(
            sequence=3,
            event_name="task-assigned",
            extra={"credential_contract": contract},
        )
        bad["event"]["credential_contract"]["interfaces"][0]["token"] = "not-a-real-token"
        with self.assertRaisesRegex(PublishError, "never contain"):
            self.publish(bad)

        wrong_transport = spec(
            sequence=3,
            event_name="task-assigned",
            extra={"credential_contract": contract},
        )
        wrong_transport["event"]["credential_contract"]["interfaces"][0]["transport"] = "process-argv"
        with self.assertRaisesRegex(PublishError, "not approved"):
            self.publish(wrong_transport)

    # ---- listener lineage ----------------------------------------------

    def test_agent_route_requires_registered_listener_lineage(self):
        seed_pointer(self.root, "bridge/worker-to-boss.yaml", 1, writer="worker")
        value = spec(pointer="bridge/worker-to-boss.yaml", writer="worker", sequence=2)
        # No listener state at all.
        with self.assertRaises(PublishError):
            self.publish(value)
        # A listener whose child is not in this process's ancestry.
        write_listener_state(self.root, "worker", child_pid=999999)
        with self.assertRaises(PublishError):
            self.publish(value)
        # A listener whose child is this process is accepted.
        import os

        write_listener_state(self.root, "worker", child_pid=os.getpid())
        self.publish(value)


if __name__ == "__main__":
    unittest.main()


class LedgerDerivedAdmissionTests(unittest.TestCase):
    """Identity is what the action *is*, not the task it was filed under."""

    PACKET = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    OTHER_PACKET = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.topology = build_bridge(self.root)
        seed_pointer(self.root, "bridge/boss-to-worker.yaml", 1)

    def tearDown(self):
        self.temp.cleanup()

    def authority(self, sequence=2, admission=None, packet=None,
                  operation="board-review", task_id="TASK", count=1):
        extra = {"task_id": task_id,
                 "live_authorization": {"provider": "codex",
                                        "packet_sha256": packet or self.PACKET,
                                        "operation": operation,
                                        "process_invocations_permitted": count}}
        if admission is not None:
            extra["failure_loop_admission"] = dict(admission)
        return spec(sequence=sequence, extra=extra)

    def mislabel(self, sequence, name):
        install_event(self.root, "bridge/events", name,
                      {"schema_version": 1, "sequence": sequence,
                       "event": "task-terminal", "writer": "boss", "task_id": "TASK",
                       "provider": "codex", "packet_sha256": self.PACKET,
                       "operation": "board-review", "outcome": "failed",
                       "action_identity": "v1:" + "0" * 64})

    def identity(self, provider="codex", packet_sha256=None, operation="board-review"):
        return _derived_action_identity(
            {"provider": provider, "packet_sha256": packet_sha256 or self.PACKET,
             "operation": operation})

    def terminal(self, sequence, outcome, name, task_id="TASK", packet=None,
                 operation="board-review"):
        descriptor = {"provider": "codex", "packet_sha256": packet or self.PACKET,
                      "operation": operation}
        body = {"schema_version": 1, "sequence": sequence, "event": "task-terminal",
                "writer": "boss", "task_id": task_id,
                "action_identity": self.identity(**descriptor)}
        body.update(descriptor)
        if outcome is not None:
            body["outcome"] = outcome
        relative, digest = install_event(self.root, "bridge/events", name, body)
        return {"path": relative, "sha256": digest, "sequence": sequence}

    def remediation(self, name="r.json"):
        identity = self.identity()
        body = json.dumps({"action_identity": identity, "decision": "adapt"},
                          indent=2, sort_keys=True).encode() + b"\n"
        directory = self.root / "coordination/remediation"
        directory.mkdir(parents=True, exist_ok=True)
        (directory / name).write_bytes(body)
        return {"path": "coordination/remediation/" + name,
                "sha256": hashlib.sha256(body).hexdigest(),
                "reference_intent": "immutable_ref"}

    def publish(self, **kw):
        return publish(self.authority(**kw), topology=self.topology)

    def refusal(self, **kw):
        with self.assertRaises(PublishError) as caught:
            publish(self.authority(**kw), topology=self.topology)
        return str(caught.exception)

    def test_two_stored_failures_for_this_action_require_an_admission(self):
        self.terminal(90, "failed", "a.yaml")
        self.terminal(91, "failed", "b.yaml")
        self.assertIn("requires failure_loop_admission", self.refusal())

    def test_a_renamed_task_cannot_mint_a_fresh_identity(self):
        """The exact cross-task control: one descriptor, three task names."""
        self.terminal(92, "failed", "c.yaml", task_id="TASK-ONE")
        self.terminal(93, "failed", "d.yaml", task_id="TASK-TWO")
        self.assertIn("requires failure_loop_admission",
                      self.refusal(sequence=3, task_id="TASK-THREE"))

    def test_a_different_packet_digest_is_a_distinct_action(self):
        self.terminal(94, "failed", "e.yaml")
        self.terminal(95, "failed", "f.yaml")
        self.publish(sequence=4, packet=self.OTHER_PACKET)

    def test_a_different_operation_is_a_distinct_action(self):
        self.terminal(96, "failed", "g.yaml")
        self.terminal(97, "failed", "h.yaml")
        self.publish(sequence=5, operation="dry-run")

    def test_a_declared_count_is_refused_rather_than_believed(self):
        self.terminal(98, "failed", "i.yaml")
        self.terminal(99, "failed", "j.yaml")
        self.assertIn("must not be declared",
                      self.refusal(sequence=6,
                                   admission={"terminal_failure_count": 0}))

    def test_the_complete_derived_set_plus_remediation_is_admitted(self):
        first = self.terminal(100, "failed", "k.yaml")
        second = self.terminal(101, "failed", "l.yaml")
        self.publish(sequence=7, admission={
            "terminal_predecessors": [first, second],
            "remediation_inventory": self.remediation()})

    def test_a_subset_of_the_derived_set_is_refused(self):
        first = self.terminal(102, "failed", "m.yaml")
        self.terminal(103, "failed", "n.yaml")
        self.assertIn("exactly the derived terminal predecessors",
                      self.refusal(sequence=8, admission={
                          "terminal_predecessors": [first],
                          "remediation_inventory": self.remediation()}))

    def test_a_succeeded_terminal_does_not_count(self):
        self.terminal(104, "failed", "o.yaml")
        self.terminal(105, "succeeded", "p.yaml")
        self.publish(sequence=9)

    def test_a_matching_terminal_without_an_explicit_outcome_refuses(self):
        self.terminal(106, "failed", "q.yaml")
        self.terminal(107, None, "r.yaml")
        self.assertIn("no explicit failed or succeeded outcome",
                      self.refusal(sequence=10))

    def test_a_first_action_needs_no_envelope(self):
        self.publish(sequence=11)

    def test_an_unrelated_malformed_record_does_not_freeze_a_first_action(self):
        (self.root / "bridge/events/legacy.yaml").write_bytes(b"{ bad: [")
        self.publish(sequence=12)

    def test_a_positive_authority_without_a_descriptor_is_refused(self):
        self.assertIn("requires provider, packet_sha256 and operation",
                      self.refusal(sequence=13, packet="short"))

    def test_a_delimiter_in_a_field_cannot_collide_two_actions(self):
        """A join would make these one action; a digest does not."""
        left = self.identity(provider="a:b", operation="c")
        right = self.identity(provider="a", operation="b:c")
        self.assertNotEqual(left, right)

    def test_a_terminal_whose_label_contradicts_its_descriptor_refuses(self):
        self.terminal(120, "failed", "z1.yaml")
        self.mislabel(121, "z2.yaml")
        self.assertIn("does not match its descriptor", self.refusal(sequence=30))

    def terminal_spec(self, sequence, descriptor=True, label=True):
        extra = {"outcome": "failed"}
        if descriptor:
            extra.update({"provider": "codex", "packet_sha256": self.PACKET,
                          "operation": "board-review"})
        extra["action_identity"] = (self.identity() if label else "v1:" + "0" * 64)
        return spec(sequence=sequence, event_name="task-terminal", extra=extra)

    def test_a_malformed_typed_terminal_never_reaches_the_ledger(self):
        """Fail closed where the bad record enters, not where it is used."""
        pointer = self.root / "bridge/boss-to-worker.yaml"
        before = pointer.read_bytes()
        for sequence, kwargs, reason in (
            (40, {"descriptor": False}, "requires provider, packet_sha256 and operation"),
            (41, {"label": False}, "does not match its descriptor"),
        ):
            value = self.terminal_spec(sequence, **kwargs)
            with self.assertRaises(PublishError) as caught:
                publish(value, topology=self.topology)
            self.assertIn(reason, str(caught.exception))
            installed = self.root / "bridge/events" / value["event_filename"]
            self.assertFalse(installed.exists(), "a refused terminal was installed")
        self.assertEqual(pointer.read_bytes(), before,
                         "a refused terminal moved the pointer")

    def test_a_well_formed_typed_terminal_publishes(self):
        publish(self.terminal_spec(42), topology=self.topology)

    def test_a_record_without_an_outcome_is_left_alone(self):
        value = self.terminal_spec(43, descriptor=False)
        del value["event"]["outcome"]
        del value["event"]["action_identity"]
        publish(value, topology=self.topology)


LAYER_TOPOLOGY = {
    "vocabulary": ["platform-build", "mission-execution"],
    "activation_value": "platform-build",
    "activation_event": "ledger-layer-activated",
}


class LedgerLayerContractTests(unittest.TestCase):
    """The two-population partition, expressed against topology not constants.

    The live standalone publisher carries the same six decisive rules inline
    because it has no package to import from. Neither implementation imports the
    other; what makes them one contract is that the rules and their refusal
    identifiers are identical, which the controls below state literally.
    """

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.history = {}
        self.quarantine = {}
        self.topology = self.build(LAYER_TOPOLOGY)

    def tearDown(self):
        self.temp.cleanup()

    def build(self, layer_contract):
        description = copy.deepcopy(support.TOPOLOGY)
        if layer_contract is not None:
            description["layer_contract"] = dict(
                layer_contract,
                quarantined_parse_defects=dict(self.quarantine),
                pre_activation_layer_records=dict(self.history),
            )
        for relative in ("bridge/events", "bridge/auditor-events",
                         "bridge/listeners", "bridge/locks"):
            (self.root / relative).mkdir(parents=True, exist_ok=True)
        (self.root / "coordination").mkdir(parents=True, exist_ok=True)
        (self.root / "coordination/topology.yaml").write_text(
            yaml.safe_dump(description))
        support.write_ledger(self.root)
        return Topology.load(self.root)

    def install_historic_layer_record(self, sequence=758, layer="execution"):
        relative = f"bridge/events/20260731T000000Z-{sequence}-boss-historic.yaml"
        (self.root / relative).write_text(json.dumps(
            {"schema_version": 1, "sequence": sequence, "event": "historic",
             "writer": "boss", "layer": layer}))
        self.history[relative] = {"writer": "boss", "sequence": sequence,
                                  "layer": layer}
        self.topology = self.build(LAYER_TOPOLOGY)
        return relative

    def install_defect(self, name="20260731T000000Z-9-boss-broken.yaml",
                       body="{unterminated: [", quarantine=True):
        relative = f"bridge/events/{name}"
        (self.root / relative).write_text(body)
        if quarantine:
            self.quarantine[relative] = hashlib.sha256(
                (self.root / relative).read_bytes()).hexdigest()
            self.topology = self.build(LAYER_TOPOLOGY)
        return relative

    def activation_body(self, sequence=900, **overrides):
        body = {
            "layer": "platform-build",
            "activation_sequence": sequence,
            "layer_vocabulary": list(LAYER_TOPOLOGY["vocabulary"]),
            "pre_activation_layer_records": [
                dict(entry, path=path) for path, entry in self.history.items()],
            "quarantined_parse_defects": [
                {"path": path, "sha256": digest}
                for path, digest in sorted(self.quarantine.items())],
        }
        body.update(overrides)
        return body

    def layer_spec(self, sequence=2, *, event_name="task-result-accepted",
                   extra=None, pointer_layer=None, filename=None):
        value = support.spec(sequence=sequence, event_name=event_name,
                             filename=filename, extra=extra or {})
        if pointer_layer is not None:
            value["pointer"]["layer"] = pointer_layer
        return value

    def publish(self, value):
        return publish(value, topology=self.topology)

    def refusal(self, value):
        with self.assertRaises(PublishError) as caught:
            self.publish(value)
        return str(caught.exception)

    def install_activation(self, sequence=900):
        body = {"schema_version": 1, "sequence": sequence,
                "event": "ledger-layer-activated", "writer": "boss"}
        body.update(self.activation_body(sequence))
        (self.root / f"bridge/events/20260731T000000Z-{sequence}-boss-activate.yaml"
         ).write_text(json.dumps(body))

    # -- the contract is opt-in -------------------------------------------

    def test_a_topology_without_a_vocabulary_does_not_run_the_contract(self):
        self.topology = self.build(None)
        self.assertFalse(self.topology.layer_contract_enabled)
        self.publish(self.layer_spec(2, extra={"layer": "anything"}))

    # -- before activation -------------------------------------------------

    def test_layer_before_activation_is_premature(self):
        self.assertIn("premature",
                      self.refusal(self.layer_spec(2, extra={"layer": "platform-build"})))

    def test_absence_before_activation_is_valid(self):
        self.publish(self.layer_spec(2))

    # -- activation admission ----------------------------------------------

    def test_a_well_formed_activation_is_admitted_and_becomes_the_epoch(self):
        self.install_historic_layer_record()
        spec_value = self.layer_spec(
            900, event_name="ledger-layer-activated",
            extra=self.activation_body(900), pointer_layer="platform-build",
            filename="20260801T000000Z-900-boss-activate.yaml")
        self.publish(spec_value)
        self.assertEqual(_activation_epoch(self.topology), 900)

    def test_an_activation_must_state_the_exact_vocabulary(self):
        spec_value = self.layer_spec(
            900, event_name="ledger-layer-activated",
            extra=self.activation_body(900, layer_vocabulary=["platform-build"]),
            pointer_layer="platform-build",
            filename="20260801T000000Z-900-boss-activate.yaml")
        self.assertIn("exact vocabulary", self.refusal(spec_value))

    def test_an_incomplete_layer_enumeration_is_refused_at_admission(self):
        self.install_historic_layer_record()
        spec_value = self.layer_spec(
            900, event_name="ledger-layer-activated",
            extra=self.activation_body(900, pre_activation_layer_records=[]),
            pointer_layer="platform-build",
            filename="20260801T000000Z-900-boss-activate.yaml")
        self.assertIn("pre-activation sequences", self.refusal(spec_value))

    def test_an_incomplete_defect_enumeration_is_refused_at_admission(self):
        self.install_defect()
        spec_value = self.layer_spec(
            900, event_name="ledger-layer-activated",
            extra=self.activation_body(900, quarantined_parse_defects=[]),
            pointer_layer="platform-build",
            filename="20260801T000000Z-900-boss-activate.yaml")
        self.assertIn("quarantined defect enumeration differs",
                      self.refusal(spec_value))

    def test_a_second_activation_is_refused(self):
        self.install_activation()
        spec_value = self.layer_spec(
            901, event_name="ledger-layer-activated",
            extra=self.activation_body(901), pointer_layer="platform-build",
            filename="20260801T000000Z-901-boss-activate.yaml")
        self.assertIn("already activated", self.refusal(spec_value))

    # -- unreadable and drifting history -----------------------------------

    def test_an_unreadable_activation_never_reads_as_pre_activation(self):
        self.install_activation()
        (self.root / "bridge/events/20260731T000000Z-900-boss-activate.yaml"
         ).write_text("{unterminated: [")
        self.assertIn("unquarantined ledger parse defect",
                      self.refusal(self.layer_spec(2)))

    def test_an_extra_defect_fails_closed(self):
        self.install_defect(name="20260731T000000Z-8-boss-extra.yaml",
                            quarantine=False)
        self.assertIn("unquarantined ledger parse defect",
                      self.refusal(self.layer_spec(2)))

    def test_a_changed_quarantined_defect_fails_closed(self):
        relative = self.install_defect()
        (self.root / relative).write_text("{different: [")
        self.assertIn("has changed", self.refusal(self.layer_spec(2)))

    def test_a_vanished_quarantined_defect_fails_closed(self):
        relative = self.install_defect()
        (self.root / relative).unlink()
        self.assertIn("absent or now parses", self.refusal(self.layer_spec(2)))

    def test_a_deleted_activation_never_reads_as_pre_activation(self):
        self.install_activation()
        self.publish(self.layer_spec(2, extra={"layer": "platform-build"},
                                     pointer_layer="platform-build"))
        (self.root / "bridge/events/20260731T000000Z-900-boss-activate.yaml").unlink()
        with self.assertRaises(PublishError) as caught:
            _activation_epoch(self.topology)
        self.assertIn("no layer activation record", str(caught.exception))

    # -- after activation ---------------------------------------------------

    def test_a_missing_event_layer_fails_closed(self):
        self.install_activation()
        self.assertIn("event layer is required",
                      self.refusal(self.layer_spec(2, pointer_layer="platform-build")))

    def test_a_missing_pointer_layer_fails_closed(self):
        self.install_activation()
        self.assertIn("pointer layer is required",
                      self.refusal(self.layer_spec(2, extra={"layer": "platform-build"})))

    def test_an_unadmitted_value_fails_closed(self):
        self.install_activation()
        self.assertIn("not an admitted value",
                      self.refusal(self.layer_spec(2, extra={"layer": "evidence"},
                                                   pointer_layer="evidence")))

    def test_a_mismatched_pair_fails_closed(self):
        self.install_activation()
        self.assertIn("layer differ",
                      self.refusal(self.layer_spec(2, extra={"layer": "platform-build"},
                                                   pointer_layer="mission-execution")))

    def test_two_consecutive_post_activation_publications_both_succeed(self):
        """The growth case: the frozen enumeration must not be re-derived."""
        self.install_activation()
        self.publish(self.layer_spec(2, extra={"layer": "platform-build"},
                                     pointer_layer="platform-build"))
        self.publish(self.layer_spec(3, extra={"layer": "mission-execution"},
                                     pointer_layer="mission-execution"))
        self.publish(self.layer_spec(4, extra={"layer": "platform-build"},
                                     pointer_layer="platform-build"))

    def test_an_assignment_request_must_carry_the_same_layer(self):
        self.install_activation()
        request = self.root / "coordination/request.json"
        request.write_text(json.dumps({"task_id": "t", "layer": "mission-execution"}))
        reference = {"path": "coordination/request.json",
                     "sha256": hashlib.sha256(request.read_bytes()).hexdigest(),
                     "reference_intent": "immutable_ref"}
        value = self.layer_spec(
            2, event_name="task-assigned", pointer_layer="platform-build",
            extra={"layer": "platform-build", "request": reference,
                   "credential_contract": {"mode": "none"}})
        self.assertIn("differs from the event layer", self.refusal(value))

    def test_every_enumerated_request_key_is_actually_checked(self):
        """Both keys, not just the common one.

        The live instance found `assurance_request` by scanning its real ledger;
        a portable library has no ledger to scan, so without this control a key
        could be dropped from the enumeration and every substrate test would
        still pass. That is exactly the silent skip the enumeration exists to
        prevent, so it is asserted here per key rather than assumed.
        """
        self.install_activation()
        request = self.root / "coordination/request.json"
        request.write_text(json.dumps({"task_id": "t", "layer": "mission-execution"}))
        reference = {"path": "coordination/request.json",
                     "sha256": hashlib.sha256(request.read_bytes()).hexdigest(),
                     "reference_intent": "immutable_ref"}
        # Written out, not read from ASSIGNMENT_REQUEST_KEYS. A guard that
        # derives its expectation from the thing it guards agrees with any
        # change to that thing, including dropping a key -- the first version of
        # this control did exactly that and passed under mutation.
        expected_keys = ("request", "assurance_request")
        self.assertEqual(tuple(ASSIGNMENT_REQUEST_KEYS), expected_keys)
        for index, key in enumerate(expected_keys):
            value = self.layer_spec(
                2 + index, event_name="task-assigned",
                pointer_layer="platform-build",
                extra={"layer": "platform-build", key: reference,
                       "credential_contract": {"mode": "none"}})
            self.assertIn("differs from the event layer", self.refusal(value), key)

    def test_the_refusal_identifiers_match_the_live_publisher(self):
        """One contract in two implementations, not two similar ones."""
        for phrase in ("layer is premature before the ledger layer activation",
                       "event and pointer layer differ",
                       "the ledger carries more than one layer activation record",
                       "an unquarantined ledger parse defect is present",
                       "layer-bearing records exist with no layer activation record"):
            self.assertIn(phrase, PUBLISHER_SOURCE, phrase)


PUBLISHER_SOURCE = (Path(__file__).resolve().parents[1]
                    / "coordination_substrate" / "publisher.py").read_text()


class ExecutionGrantPublicationTests(unittest.TestCase):
    """t1: the authority publisher, not only the consumer, admits the grant.

    Without this seam cs-consume could validate a typed grant shape that the
    publisher never recognized and never checked.
    """

    CANDIDATE = "9f" * 20
    RUNNER_DIGEST = "3c" * 32
    PACKET = "7a" * 32

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.topology = build_bridge(self.root)
        self.pointer_rel = "bridge/boss-to-worker.yaml"
        self.pointer = self.root / self.pointer_rel
        seed_pointer(self.root, self.pointer_rel, 1)

    def tearDown(self):
        self.temp.cleanup()

    def _grant(self, **overrides):
        descriptor = {
            "provider": "isolated-runner",
            "packet_sha256": self.PACKET,
            "operation": "neo4j-topology-proof",
        }
        grant = dict(descriptor)
        grant.update({
            "isolated_disposable_runs": 1,
            "action_identity": _derived_action_identity(descriptor),
            "candidate": self.CANDIDATE,
            "runner_path": "scripts/run-proof.py",
            "runner_sha256": self.RUNNER_DIGEST,
        })
        grant.update(overrides)
        return grant

    def _publish(self, grant, sequence=2):
        value = spec(sequence=sequence,
                     extra={"live_authorization": grant})
        return publish(value, topology=self.topology)

    def _installed_events(self):
        return sorted(p.name for p in (self.root / "bridge/events").glob("*.yaml"))

    def test_a_well_formed_grant_publishes(self):
        result = self._publish(self._grant())
        installed = yaml.safe_load((self.root / result["event_path"]).read_bytes())
        grant = installed["live_authorization"]
        self.assertEqual(grant["isolated_disposable_runs"], 1)
        self.assertEqual(grant["candidate"], self.CANDIDATE)

    def test_the_bound_is_positive_authority_vocabulary(self):
        from coordination_substrate.publisher import POSITIVE_AUTHORITY_FIELDS
        self.assertIn("isolated_disposable_runs", POSITIVE_AUTHORITY_FIELDS)

    def test_malformed_grants_refuse_without_event_or_pointer_movement(self):
        """Each variant must fail closed: no event installed, pointer unmoved."""
        variants = {
            "zero-bound": ({"isolated_disposable_runs": 0}, "grant-contract"),
            "boolean-bound": ({"isolated_disposable_runs": True}, "grant-contract"),
            "negative-bound": ({"isolated_disposable_runs": -1}, "grant-contract"),
            "string-bound": ({"isolated_disposable_runs": "1"}, "grant-contract"),
            "absent-provider": ({"provider": ""}, "grant-contract"),
            "uppercase-packet": ({"packet_sha256": "7A" * 32}, "grant-contract"),
            "short-packet": ({"packet_sha256": "7a" * 16}, "grant-contract"),
            "untrimmed-operation": ({"operation": " proof "}, "grant-contract"),
            "absent-identity": ({"action_identity": None}, "action-identity"),
            "mismatched-identity": (
                {"action_identity": "v1:" + "0" * 64}, "action-identity"),
            "short-candidate": ({"candidate": "9f" * 10}, "grant-contract"),
            "uppercase-candidate": ({"candidate": "9F" * 20}, "grant-contract"),
            "absolute-runner": ({"runner_path": "/etc/passwd"}, "unsafe-path"),
            "traversal-runner": ({"runner_path": "../../escape.py"}, "unsafe-path"),
            "absent-runner": ({"runner_path": ""}, "grant-contract"),
            "uppercase-runner-digest": ({"runner_sha256": "3C" * 32}, "grant-contract"),
            "short-runner-digest": ({"runner_sha256": "3c" * 16}, "grant-contract"),
        }
        before_pointer = self.pointer.read_bytes()
        before_events = self._installed_events()
        for label, (override, expected_kind) in variants.items():
            with self.subTest(variant=label):
                with self.assertRaises(PublishError) as caught:
                    self._publish(self._grant(**override))
                # The exact reason, not merely that something raised: a control
                # that accepts any PublishError would pass for the wrong cause.
                self.assertIn(f"({expected_kind})", str(caught.exception), label)
                self.assertEqual(self._installed_events(), before_events, label)
                self.assertEqual(self.pointer.read_bytes(), before_pointer, label)

    def test_events_without_the_bound_are_untouched(self):
        """The admission taxes only events that actually claim the grant."""
        result = publish(spec(), topology=self.topology)
        self.assertTrue((self.root / result["event_path"]).is_file())
