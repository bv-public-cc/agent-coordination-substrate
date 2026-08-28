#!/usr/bin/env python3
"""P2-G004 fixture tests. Stdlib unittest only."""
import hashlib, importlib.util, inspect, os, shutil, tempfile, unittest
import pathlib
import tempfile

_HERE = os.path.dirname(os.path.abspath(__file__))
# Extracted layout: the calculator lives in the package, not beside its tests.
_SPEC = importlib.util.spec_from_file_location(
    "tm2",
    os.path.join(_HERE, os.pardir, "coordination_substrate", "metrics_reference.py"),
)
tm = importlib.util.module_from_spec(_SPEC); _SPEC.loader.exec_module(tm)


class Fx:
    def __init__(self):
        self.root = tempfile.mkdtemp(prefix="p2g004-")
        self.events = os.path.join(self.root, "coordination", "bridge", "events")
        os.makedirs(self.events)

    def write(self, name, body):
        with open(os.path.join(self.events, name), "w", encoding="utf-8") as h:
            h.write(body)

    def target(self, rel, body):
        full = os.path.join(self.root, rel)
        os.makedirs(os.path.dirname(full), exist_ok=True)
        with open(full, "w", encoding="utf-8") as h:
            h.write(body)
        return hashlib.sha256(body.encode()).hexdigest()

    def ref_event(self, name, rel, sha, intent=None, key="responds_to"):
        extra = f"  reference_intent: {intent}\n" if intent else ""
        self.write(name, "schema_version: 1\nsequence: 1\nevent: task-assigned\n"
                   "writer: boss\ncreated_at: \"2026-08-01T04:00:00Z\"\n"
                   f"task_id: T\n{key}:\n  path: {rel}\n  sha256: \"{sha}\"\n{extra}")

    def compute(self):
        return tm.compute(self.root, ["coordination/bridge/events"],
                          "coordination/current-state.yaml")

    def cleanup(self):
        shutil.rmtree(self.root, ignore_errors=True)


class Base(unittest.TestCase):
    def setUp(self):
        self.fx = Fx(); self.addCleanup(self.fx.cleanup)


class TestClassifierCannotSeeOutcome(unittest.TestCase):
    def test_signature_accepts_no_digest_or_match_information(self):
        params = set(inspect.signature(tm.classify_reference_intent).parameters)
        self.assertEqual(params, {"rel_path", "declared_intent", "authority_bearing"})
        for forbidden in ("actual", "expected", "sha256", "matched", "observed", "digest"):
            self.assertNotIn(forbidden, params)

    def test_same_path_classifies_identically_regardless_of_content(self):
        a = tm.classify_reference_intent("coordination/bridge/events/x.yaml")
        b = tm.classify_reference_intent("coordination/bridge/events/x.yaml")
        self.assertEqual(a, b, "classification must be content-independent")


class TestImmutableRef(Base):
    def test_exact_match_passes(self):
        sha = self.fx.target("coordination/bridge/events/t.yaml", "x\n")
        self.fx.ref_event("a-1-boss-r.yaml", "coordination/bridge/events/t.yaml", sha)
        i = self.fx.compute()["integrity"]
        self.assertEqual(i["counts"]["immutable_failures"], 0)

    def test_drift_fails_closed(self):
        self.fx.target("coordination/bridge/events/t.yaml", "x\n")
        self.fx.ref_event("a-1-boss-r.yaml", "coordination/bridge/events/t.yaml", "a"*64)
        i = self.fx.compute()["integrity"]
        self.assertEqual(i["counts"]["immutable_failures"], 1)
        self.assertTrue(i["fail_closed"])

    def test_missing_target_fails_closed(self):
        self.fx.ref_event("a-1-boss-r.yaml", "coordination/bridge/events/gone.yaml", "b"*64)
        i = self.fx.compute()["integrity"]
        self.assertEqual(i["counts"]["missing_targets"], 1)
        self.assertTrue(i["fail_closed"])

    def test_malformed_digest_fails_closed(self):
        self.fx.write("a-1-boss-r.yaml", "schema_version: 1\nsequence: 1\n"
                      "event: task-assigned\nwriter: boss\n"
                      "created_at: \"2026-08-01T04:00:00Z\"\ntask_id: T\n"
                      "responds_to:\n  path: coordination/bridge/events/t.yaml\n"
                      "  sha256: " + "0"*64 + "\n")
        i = self.fx.compute()["integrity"]
        self.assertEqual(i["counts"]["malformed_bindings"], 1)
        self.assertTrue(i["fail_closed"])

    def test_path_escape_fails_closed(self):
        self.fx.ref_event("a-1-boss-r.yaml", "../../etc/passwd", "c"*64,
                          intent="immutable_ref")
        i = self.fx.compute()["integrity"]
        self.assertEqual(i["counts"]["malformed_bindings"], 1)
        self.assertEqual(i["malformed_bindings"][0]["observed"], "PATH-ESCAPE")
        self.assertTrue(i["fail_closed"])


class TestSnapshotDoesNotAlarm(Base):
    def test_snapshot_drift_visible_but_silent(self):
        self.fx.target(".agents.md", "governance v1\n")
        self.fx.ref_event("a-1-boss-r.yaml", ".agents.md", "d"*64)
        i = self.fx.compute()["integrity"]
        self.assertEqual(i["counts"]["snapshot_drift"], 1)
        self.assertEqual(i["counts"]["immutable_failures"], 0)
        self.assertFalse(i["fail_closed"])
        drift = i["snapshot_drift"][0]
        for field in ("origin", "expected", "observed", "reference_intent", "reason"):
            self.assertIn(field, drift)

    def test_declared_snapshot_intent_is_honoured(self):
        self.fx.target("coordination/whatever.yaml", "v1\n")
        self.fx.ref_event("a-1-boss-r.yaml", "coordination/whatever.yaml", "e"*64,
                          intent="snapshot_at_publication")
        i = self.fx.compute()["integrity"]
        self.assertEqual(i["counts"]["snapshot_drift"], 1)
        self.assertFalse(i["fail_closed"])


class TestUnknownIntentFailsClosed(Base):
    def test_unclassifiable_path_fails_closed(self):
        self.fx.target("random/unclassifiable.bin", "v1\n")
        # Deliberately NOT an authority-bearing key: `responds_to` would
        # correctly classify as authority. This exercises the fallback's
        # inability to decide, which must fail closed rather than guess.
        self.fx.ref_event("a-1-boss-r.yaml", "random/unclassifiable.bin", "f"*64,
                          key="some_plain_reference")
        i = self.fx.compute()["integrity"]
        self.assertEqual(i["counts"]["unknown_references"], 1)
        self.assertTrue(i["fail_closed"])

    def test_bogus_declared_intent_fails_closed(self):
        self.fx.target("coordination/x.yaml", "v1\n")
        self.fx.ref_event("a-1-boss-r.yaml", "coordination/x.yaml", "0"*63 + "a",
                          intent="totally_benign_trust_me", key="some_plain_reference")
        i = self.fx.compute()["integrity"]
        self.assertEqual(i["counts"]["unknown_references"], 1)
        self.assertTrue(i["fail_closed"])


class TestRogueWriterClass(Base):
    """Event-371 class: a corrective record must not be sanitized to a snapshot."""

    def test_authority_bearing_reference_to_a_draft_is_not_sanitized(self):
        self.fx.target("coordination/tasks/T/result.notdraft.yaml", "v1\n")
        self.fx.write("a-371-clyde-correction.yaml",
                      "schema_version: 1\nsequence: 371\nevent: draft-hash-correction\n"
                      "writer: clyde-vscode\ncreated_at: \"2026-08-01T04:00:00Z\"\n"
                      "task_id: T\nsupersedes_draft_hashes_in:\n"
                      "  path: coordination/tasks/T/result.notdraft.yaml\n"
                      "  sha256: \"" + "9"*64 + "\"\n")
        i = self.fx.compute()["integrity"]
        self.assertEqual(i["counts"]["immutable_failures"], 1)
        self.assertEqual(i["counts"]["snapshot_drift"], 0)
        self.assertTrue(i["fail_closed"])
        self.assertTrue(i["immutable_failures"][0]["authority_bearing"])

    def test_corrective_record_pointing_at_an_event_alarms_on_drift(self):
        self.fx.target("coordination/bridge/events/t-370.yaml", "original\n")
        self.fx.write("a-371-clyde-correction.yaml",
                      "schema_version: 1\nsequence: 371\nevent: draft-hash-correction\n"
                      "writer: clyde-vscode\ncreated_at: \"2026-08-01T04:00:00Z\"\n"
                      "task_id: T\nsupersedes_draft_hashes_in:\n"
                      "  path: coordination/bridge/events/t-370.yaml\n"
                      "  sha256: \"" + "8"*64 + "\"\n")
        i = self.fx.compute()["integrity"]
        self.assertEqual(i["counts"]["immutable_failures"], 1)
        self.assertTrue(i["fail_closed"])


class TestKnownGaps(Base):
    def test_unparseable_record_becomes_a_declared_gap(self):
        self.fx.write("a-1-boss-broken.yaml", "schema_version: 1\nevent: [unclosed\n")
        i = self.fx.compute()["integrity"]
        self.assertEqual(len(i["known_gaps"]), 1)
        gap = i["known_gaps"][0]
        for field in ("path", "byte_sha256", "parser_failure", "chronology_basis",
                      "impact", "limitation"):
            self.assertIn(field, gap)
        self.assertIn("no reconstruction", gap["limitation"].lower())
        self.assertEqual(len(gap["byte_sha256"]), 64)


class TestPredecessorInvariantsPreserved(Base):
    def test_epoch_and_availability_unchanged(self):
        result = self.fx.compute()
        self.assertEqual(result["time_authority"]["epoch_event"], 272)
        self.assertEqual(result["time_authority"]["epoch"], "2026-08-01T03:38:48Z")
        for name in ("duplicate_work", "post_gate_rework", "mission_predicates_advanced"):
            entry = result["metrics"][name]
            self.assertEqual(entry["state"], tm.UNAVAILABLE)
            self.assertNotIn("value", entry)

    def test_volume_still_segregated(self):
        result = self.fx.compute()
        self.assertIn("never progress", result["diagnostic_signals"]["note"].lower())


if __name__ == "__main__":
    unittest.main(verbosity=1)


class MedianBranchActivation(unittest.TestCase):
    """Drive the two compute branches that escaped to the live ledger.

    The accepted 16-fixture suite never reached the median call sites, so a
    missing import survived review and only failed against real data. Boss
    sequence 410 rejected a direct median() call as activation: it proves the
    symbol resolves, not that either branch executes. This builds synthetic
    post-epoch event pairs and asserts the exact medians compute produces.
    """

    def _event(self, root, name, *, event, writer, task, attempt, when):
        path = root / "coordination" / "bridge" / "events" / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            "schema_version: 1\n"
            f"event: {event}\nwriter: {writer}\n"
            f"task_id: {task}\nattempt: {attempt}\n"
            f"created_at: '{when}'\n")
        return path

    def test_both_measured_branches_execute_with_exact_medians(self):
        with tempfile.TemporaryDirectory() as raw:
            root = pathlib.Path(raw)
            # Two attempts so each median is over n=2 and is a real average.
            plan = [("T-A", 1, "04:00:00", "04:00:10", "04:10:00", "04:10:30"),
                    ("T-B", 1, "05:00:00", "05:00:30", "05:10:00", "05:11:30")]
            for idx, (task, att, a0, a1, g0, g1) in enumerate(plan):
                for tag, ev, wr, when in (
                        ("assign", "task-assigned", "boss", a0),
                        ("accept", "publish-approved", "boss", a1),
                        ("ack", "assurance-acknowledged", "james", g0),
                        ("term", "terminal-gate-finding", "james", g1)):
                    self._event(root, f"20260801T{when.replace(':','')}Z-{idx}{tag}.yaml",
                                event=ev, writer=wr, task=task, attempt=att,
                                when=f"2026-08-01T{when}Z")
            state = root / "coordination" / "current-state.yaml"
            state.write_text("schema_version: 1\n")
            result = tm.compute(root, ["coordination/bridge/events"],
                                "coordination/current-state.yaml")
            metrics = result["metrics"]
            cycle = metrics["critical_path_cycle_time"]
            wait = metrics["assurance_gate_wait"]
            self.assertEqual(cycle["state"], tm.MEASURED, cycle)
            self.assertEqual(wait["state"], tm.MEASURED, wait)
            # 10s and 30s -> median 20.0 ; 30s and 90s -> median 60.0
            self.assertEqual(cycle["value"]["count"], 2)
            self.assertEqual(cycle["value"]["median_seconds"], 20.0)
            self.assertEqual(wait["value"]["count"], 2)
            self.assertEqual(wait["value"]["median_seconds"], 60.0)
