"""Integrity engine: the alarm must fire on tampering and stay quiet otherwise."""

from __future__ import annotations

import hashlib
from pathlib import Path
import tempfile
import unittest

import yaml

import conftest  # noqa: F401  (path setup)

from support import build_bridge, spec  # noqa: E402
from coordination_substrate import integrity  # noqa: E402
from coordination_substrate.publisher import publish  # noqa: E402

EVENT_DIRS = ["bridge/events", "bridge/auditor-events"]


class IntegrityTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.topology = build_bridge(self.root)

    def tearDown(self):
        self.temp.cleanup()

    def write_event(self, name: str, body: dict, directory: str = "bridge/events") -> Path:
        path = self.root / directory / name
        path.write_bytes(yaml.safe_dump(body, sort_keys=False).encode())
        return path

    def verify(self):
        return integrity.verify(self.root, EVENT_DIRS)

    # ---- clean ledger ---------------------------------------------------

    def test_clean_ledger_does_not_fail_closed(self):
        publish(spec(sequence=1), topology=self.topology)
        publish(spec(sequence=2), topology=self.topology)
        report = self.verify()
        self.assertEqual(report.loaded, 2)
        self.assertEqual(report.rejected, 0)
        self.assertEqual(report.immutable_failures, 0)
        self.assertFalse(report.fail_closed)

    # ---- the distinction that makes the signal usable --------------------

    def test_immutable_reference_tampering_fails_closed(self):
        target = self.root / "artifact.txt"
        target.write_text("original")
        digest = hashlib.sha256(b"original").hexdigest()
        self.write_event(
            "e-1.yaml",
            {
                "sequence": 1,
                "writer": "boss",
                "evidence": {
                    "path": "artifact.txt",
                    "sha256": digest,
                    "reference_intent": "immutable_ref",
                },
            },
        )
        self.assertFalse(self.verify().fail_closed)

        target.write_text("tampered")
        report = self.verify()
        self.assertEqual(report.immutable_failures, 1)
        self.assertTrue(report.fail_closed)

    def test_snapshot_drift_is_reported_but_never_alarms(self):
        target = self.root / "governance.md"
        target.write_text("v1")
        self.write_event(
            "e-2.yaml",
            {
                "sequence": 2,
                "writer": "boss",
                "governance": {
                    "path": "governance.md",
                    "sha256": hashlib.sha256(b"v1").hexdigest(),
                    "reference_intent": "snapshot_at_publication",
                },
            },
        )
        target.write_text("v2 — legitimately revised")
        report = self.verify()
        self.assertEqual(report.snapshot_drift, 1)
        self.assertEqual(report.immutable_failures, 0)
        self.assertFalse(
            report.fail_closed,
            "expected drift must not alarm; that is what made the old signal useless",
        )

    def test_unknown_intent_is_surfaced_separately(self):
        (self.root / "thing.txt").write_text("x")
        self.write_event(
            "e-3.yaml",
            {
                "sequence": 3,
                "writer": "boss",
                "ref": {
                    "path": "thing.txt",
                    "sha256": hashlib.sha256(b"x").hexdigest(),
                    "reference_intent": "something-else",
                },
            },
        )
        report = self.verify()
        self.assertEqual(report.unknown_intent, 1)
        self.assertEqual(report.immutable_failures, 0)

    def test_absent_immutable_target_fails_closed(self):
        self.write_event(
            "e-4.yaml",
            {
                "sequence": 4,
                "writer": "boss",
                "evidence": {
                    "path": "never-existed.txt",
                    "sha256": "0" * 64,
                    "reference_intent": "immutable_ref",
                },
            },
        )
        report = self.verify()
        self.assertEqual(report.immutable_failures, 1)
        self.assertTrue(report.fail_closed)

    def test_nested_bindings_are_found_at_any_depth(self):
        (self.root / "deep.txt").write_text("d")
        digest = hashlib.sha256(b"d").hexdigest()
        self.write_event(
            "e-5.yaml",
            {
                "sequence": 5,
                "writer": "boss",
                "outer": {
                    "list": [
                        {"inner": {"path": "deep.txt", "sha256": digest,
                                   "reference_intent": "immutable_ref"}}
                    ]
                },
            },
        )
        self.assertEqual(self.verify().bindings_checked, 1)

    # ---- parse failures and identity -------------------------------------

    def test_unparseable_event_is_counted_and_fails_closed(self):
        (self.root / "bridge/events/broken.yaml").write_text("key: value: nope\n")
        report = self.verify()
        self.assertEqual(report.rejected, 1)
        self.assertTrue(report.fail_closed)
        self.assertTrue(any(d.kind == "unparseable" for d in report.defects))

    def test_duplicate_yaml_keys_are_rejected_not_silently_merged(self):
        (self.root / "bridge/events/dupe.yaml").write_text(
            "sequence: 1\nwriter: boss\nstatus: a\nstatus: b\n"
        )
        self.assertEqual(self.verify().rejected, 1)

    def test_duplicate_writer_sequence_identities_are_reported(self):
        for name in ("d-1.yaml", "d-2.yaml"):
            self.write_event(name, {"sequence": 7, "writer": "boss", "event": "x"})
        report = self.verify()
        self.assertEqual(len(report.duplicate_identities), 1)
        self.assertEqual(report.duplicate_identities[0]["sequence"], 7)
        self.assertTrue(report.fail_closed)

    def test_same_sequence_from_different_writers_is_not_a_duplicate(self):
        self.write_event("w-1.yaml", {"sequence": 7, "writer": "boss"})
        self.write_event(
            "w-2.yaml", {"sequence": 7, "writer": "auditor"}, "bridge/auditor-events"
        )
        self.assertEqual(self.verify().duplicate_identities, [])

    def test_missing_event_directory_is_a_defect_not_a_crash(self):
        report = integrity.verify(self.root, ["bridge/events", "bridge/nope"])
        self.assertTrue(any(d.kind == "missing-event-directory" for d in report.defects))

    # ---- rendering -------------------------------------------------------

    def test_markdown_separates_alarming_defects_from_expected_drift(self):
        target = self.root / "gov.md"
        target.write_text("v1")
        self.write_event(
            "r-1.yaml",
            {
                "sequence": 1,
                "writer": "boss",
                "gov": {"path": "gov.md", "sha256": hashlib.sha256(b"v1").hexdigest(),
                        "reference_intent": "snapshot_at_publication"},
            },
        )
        target.write_text("v2")
        text = integrity.render(self.verify())
        self.assertIn("fail_closed: false", text)
        self.assertIn("is not a defect", text)
        self.assertNotIn("Defects requiring attention", text)

    # ---- fail-open regressions from the external audit (INT-1..5) --------

    def _binding_event(self, name, binding, seq=1):
        self.write_event(name, {"sequence": seq, "writer": "boss", "ref": binding})

    def test_unknown_intent_value_fails_closed(self):  # INT-1
        self.write_event("bad.md", {}, directory="bridge/events")  # dummy so dir exists
        (self.root / "t.md").write_text("x")
        self._binding_event("e.yaml", {
            "path": "t.md", "sha256": hashlib.sha256(b"x").hexdigest(),
            "reference_intent": "immutable-ref"})  # typo, not immutable_ref
        report = self.verify()
        self.assertEqual(report.unknown_intent, 1)
        self.assertTrue(report.fail_closed, "an unknown reference intent must fail closed")

    def test_non_string_sha256_is_malformed_not_dropped(self):  # INT-2
        (self.root / "t.md").write_text("x")
        # An all-digit sha256 parses as an int; it must not vanish from checking.
        self._binding_event("e.yaml", {
            "path": "t.md", "sha256": 123456789, "reference_intent": "immutable_ref"})
        report = self.verify()
        self.assertTrue(any(d.kind == "malformed-binding" for d in report.defects))
        self.assertTrue(report.fail_closed)

    def test_symlinked_target_fails_closed(self):  # INT-3a
        (self.root / "real.md").write_text("x")
        (self.root / "link.md").symlink_to(self.root / "real.md")
        self._binding_event("e.yaml", {
            "path": "link.md", "sha256": hashlib.sha256(b"x").hexdigest(),
            "reference_intent": "immutable_ref"})
        report = self.verify()
        self.assertTrue(any(d.kind == "symlinked-target" for d in report.defects))
        self.assertTrue(report.fail_closed)

    def test_missing_event_directory_fails_closed(self):  # INT-3b
        report = integrity.verify(self.root, ["bridge/does-not-exist"])
        self.assertTrue(any(d.kind == "missing-event-directory" for d in report.defects))
        self.assertTrue(report.fail_closed)

    def test_path_escape_is_refused_and_not_followed(self):  # INT-4
        outside = self.root.parent / "outside-secret.txt"
        outside.write_text("secret")
        try:
            self._binding_event("e.yaml", {
                "path": "../outside-secret.txt",
                "sha256": hashlib.sha256(b"secret").hexdigest(),
                "reference_intent": "immutable_ref"})
            report = self.verify()
            self.assertTrue(any(d.kind == "path-escape" for d in report.defects),
                            "a reference resolving outside the root must be refused")
            self.assertTrue(report.fail_closed)
        finally:
            outside.unlink()

    def test_string_sequence_identity_is_flagged(self):  # INT-5
        self.write_event("e.yaml", {"sequence": "7", "writer": "boss"})
        report = self.verify()
        self.assertTrue(any(d.kind == "malformed-identity" for d in report.defects))
        self.assertTrue(report.fail_closed)

    def test_an_unsequenced_heartbeat_is_not_flagged_as_malformed_identity(self):  # INT-5 control
        self.write_event("hb.yaml", {"writer": "clyde", "event": "heartbeat"})  # no sequence
        report = self.verify()
        self.assertFalse(any(d.kind == "malformed-identity" for d in report.defects),
                         "an unsequenced heartbeat is not an identity collision")

    def test_strict_exit_code_reflects_fail_closed(self):  # TCI-3
        (self.root / "t.md").write_text("x")
        self._binding_event("e.yaml", {
            "path": "t.md", "sha256": hashlib.sha256(b"x").hexdigest(),
            "reference_intent": "immutable-ref"})  # unknown intent -> fail closed
        args = ["--root", str(self.root), "--event-dir", "bridge/events", "--format", "json"]
        self.assertEqual(integrity.main(args + ["--strict"]), 1)
        # A clean ledger exits 0 even with --strict.
        (self.root / "e2" ).mkdir()
        self.assertEqual(integrity.main(["--root", str(self.root), "--event-dir", "e2", "--strict"]), 0)


if __name__ == "__main__":
    unittest.main()
