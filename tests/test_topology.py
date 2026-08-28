"""Topology validation: the shape the other modules rely on."""

from __future__ import annotations

import copy
import datetime as dt
from pathlib import Path
import sys
import tempfile
import support
import unittest

import yaml

import conftest  # noqa: F401  (path setup)

from support import TOPOLOGY, build_bridge  # noqa: E402
from coordination_substrate.topology import (  # noqa: E402
    ActiveWorkProbe,
    Topology,
    TopologyError,
)


class TopologyTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)

    def tearDown(self):
        self.temp.cleanup()

    def parse(self, mutate=None):
        value = copy.deepcopy(TOPOLOGY)
        if mutate:
            mutate(value)
        return Topology.parse(value, self.root)

    def test_derived_route_tables(self):
        topology = self.parse()
        routes = topology.routes
        self.assertEqual(routes["bridge/boss-to-worker.yaml"], ("bridge/events", "boss"))
        self.assertEqual(routes["bridge/worker-to-boss.yaml"], ("bridge/events", "worker"))
        self.assertEqual(
            routes["bridge/auditor-to-boss.yaml"], ("bridge/auditor-events", "auditor")
        )
        self.assertEqual(topology.agent_routes["bridge/worker-to-boss.yaml"], "worker")
        self.assertEqual(
            topology.orchestrator_routes,
            frozenset({"bridge/boss-to-worker.yaml", "bridge/boss-to-auditor.yaml"}),
        )
        self.assertIn("bridge/auditor-events", topology.event_dirs)
        self.assertEqual(topology.lease_holder().name, "worker")

    def test_default_listener_state_path(self):
        topology = self.parse()
        self.assertEqual(
            topology.role("auditor").listener_state, "bridge/listeners/auditor.json"
        )

    def test_duplicate_pointer_is_refused(self):
        with self.assertRaises(TopologyError):
            self.parse(
                lambda v: v["roles"]["auditor"].update(
                    inbound_pointer="bridge/boss-to-worker.yaml"
                )
            )

    def test_role_may_not_impersonate_the_orchestrator(self):
        with self.assertRaises(TopologyError):
            self.parse(lambda v: v["roles"]["worker"].update(writer_prefix="boss"))

    def test_only_one_lease_holder(self):
        with self.assertRaises(TopologyError):
            self.parse(
                lambda v: v["roles"]["auditor"].update(holds_mutating_lease=True)
            )

    def test_absolute_and_traversing_paths_refused(self):
        with self.assertRaises(TopologyError):
            self.parse(lambda v: v.update(ledger="/etc/passwd"))
        with self.assertRaises(TopologyError):
            self.parse(lambda v: v.update(ledger="../escape.yaml"))

    def test_schema_version_is_required(self):
        with self.assertRaises(TopologyError):
            self.parse(lambda v: v.update(schema_version=2))

    def test_epoch_parsing_requires_timezone(self):
        topology = self.parse(
            lambda v: v.update(time_authority_epoch="2026-08-01T03:38:48Z")
        )
        self.assertEqual(
            topology.time_authority_epoch,
            dt.datetime(2026, 8, 1, 3, 38, 48, tzinfo=dt.timezone.utc),
        )
        with self.assertRaises(TopologyError):
            self.parse(lambda v: v.update(time_authority_epoch="2026-08-01T03:38:48"))

    def test_no_epoch_means_no_gate(self):
        self.assertIsNone(self.parse().time_authority_epoch)

    def test_load_refuses_symlinked_topology(self):
        build_bridge(self.root)
        target = self.root / "coordination/topology.yaml"
        moved = self.root / "real-topology.yaml"
        target.rename(moved)
        target.symlink_to(moved)
        with self.assertRaises(TopologyError):
            Topology.load(self.root)


class ActiveWorkProbeTests(unittest.TestCase):
    def test_task_and_pause_paths(self):
        probe = ActiveWorkProbe.parse(
            {"task_path": ["execution", "active_task"], "pause_path": ["execution", "safe_pause"]},
            "probe",
        )
        self.assertTrue(probe.is_active({"execution": {"active_task": "T-1"}}))
        self.assertFalse(probe.is_active({"execution": {"active_task": None}}))
        self.assertFalse(probe.is_active({"execution": {"active_task": "  "}}))
        self.assertFalse(
            probe.is_active({"execution": {"active_task": "T-1", "safe_pause": True}})
        )

    def test_nested_probe(self):
        probe = ActiveWorkProbe.parse(
            {"task_path": ["execution", "auditor", "active_task"]}, "probe"
        )
        self.assertTrue(
            probe.is_active({"execution": {"auditor": {"active_task": "A-1"}}})
        )
        self.assertFalse(probe.is_active({"execution": {"auditor": {}}}))

    def test_missing_intermediate_object_is_an_error(self):
        probe = ActiveWorkProbe.parse({"task_path": ["a", "b"]}, "probe")
        with self.assertRaises(TopologyError):
            probe.is_active({"a": "not-an-object"})

    def test_empty_task_path_refused(self):
        with self.assertRaises(TopologyError):
            ActiveWorkProbe.parse({"task_path": []}, "probe")


class ExampleTopologyTests(unittest.TestCase):
    def test_shipped_example_parses(self):
        example = (
            Path(__file__).resolve().parents[1]
            / "examples/mission-platform-topology.yaml"
        )
        value = yaml.safe_load(example.read_bytes())
        topology = Topology.parse(value, Path("/tmp"))
        self.assertEqual(topology.orchestrator, "boss")
        self.assertEqual(sorted(topology.roles), ["clyde", "james"])
        self.assertEqual(topology.lease_holder().name, "clyde")
        self.assertEqual(
            topology.role("james").events_dir, "coordination/bridge/james-events"
        )




class LayerContractTopologyTests(unittest.TestCase):
    """The contract's terms are instance description, so they are validated.

    Hard-coding one instance's unreadable records into the publisher would make
    the module unusable elsewhere and would excuse the wrong files when reused.
    Moving them into topology only helps if the topology refuses nonsense.
    """

    def build(self, layer_contract):
        root = Path(self.temp.name)
        for relative in ("bridge/events", "bridge/auditor-events",
                         "bridge/listeners", "bridge/locks"):
            (root / relative).mkdir(parents=True, exist_ok=True)
        (root / "coordination").mkdir(parents=True, exist_ok=True)
        description = copy.deepcopy(support.TOPOLOGY)
        if layer_contract is not None:
            description["layer_contract"] = layer_contract
        (root / "coordination/topology.yaml").write_text(yaml.safe_dump(description))
        support.write_ledger(root)
        return Topology.load(root)

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()

    def tearDown(self):
        self.temp.cleanup()

    def test_the_contract_is_absent_unless_a_vocabulary_is_declared(self):
        self.assertFalse(self.build(None).layer_contract_enabled)

    def test_a_declared_vocabulary_enables_the_contract(self):
        topology = self.build({"vocabulary": ["platform-build", "mission-execution"],
                               "activation_value": "platform-build"})
        self.assertTrue(topology.layer_contract_enabled)
        self.assertEqual(topology.layer_vocabulary,
                         ("platform-build", "mission-execution"))

    def test_an_activation_value_outside_the_vocabulary_is_refused(self):
        with self.assertRaises(TopologyError) as caught:
            self.build({"vocabulary": ["platform-build"], "activation_value": "other"})
        self.assertIn("activation_value", str(caught.exception))

    def test_a_vocabulary_with_no_activation_value_is_refused(self):
        with self.assertRaises(TopologyError) as caught:
            self.build({"vocabulary": ["platform-build"]})
        self.assertIn("activation_value", str(caught.exception))

    def test_a_repeated_vocabulary_value_is_refused(self):
        with self.assertRaises(TopologyError) as caught:
            self.build({"vocabulary": ["platform-build", "platform-build"],
                        "activation_value": "platform-build"})
        self.assertIn("repeats", str(caught.exception))

    def test_an_invalid_quarantine_digest_is_refused(self):
        with self.assertRaises(TopologyError) as caught:
            self.build({"vocabulary": ["platform-build"],
                        "activation_value": "platform-build",
                        "quarantined_parse_defects": {"bridge/events/a.yaml": "short"}})
        self.assertIn("digest is invalid", str(caught.exception))

    def test_an_absolute_quarantine_path_is_refused(self):
        with self.assertRaises(TopologyError):
            self.build({"vocabulary": ["platform-build"],
                        "activation_value": "platform-build",
                        "quarantined_parse_defects": {"/etc/passwd": "0" * 64}})

    def test_the_contract_lock_must_differ_from_the_sequence_lock(self):
        """Different scopes, so they cannot share one file.

        The contract lock spans every route; the sequence lock spans only the
        orchestrator's. Sharing one would make the narrower scope silently
        inherit the wider one and hide which invariant a wait belongs to.
        """
        sequence_lock = self.build(None).global_sequence_lock
        with self.assertRaises(TopologyError) as caught:
            self.build({"vocabulary": ["platform-build"],
                        "activation_value": "platform-build",
                        "contract_lock": sequence_lock})
        self.assertIn("must differ", str(caught.exception))

    def test_an_absolute_contract_lock_is_refused(self):
        """`root / absolute` discards the root entirely, so it never reaches disk."""
        with self.assertRaises(TopologyError) as caught:
            self.build({"vocabulary": ["platform-build"],
                        "activation_value": "platform-build",
                        "contract_lock": "/tmp/layer-contract"})
        self.assertIn("contract_lock", str(caught.exception))

    def test_an_upward_traversal_contract_lock_is_refused(self):
        with self.assertRaises(TopologyError):
            self.build({"vocabulary": ["platform-build"],
                        "activation_value": "platform-build",
                        "contract_lock": "../outside/layer-contract"})

    def test_a_padded_or_empty_activation_event_is_refused(self):
        for value in (" ledger-layer-activated", "", "activated "):
            with self.assertRaises(TopologyError):
                self.build({"vocabulary": ["platform-build"],
                            "activation_value": "platform-build",
                            "activation_event": value})

    def test_a_sixty_four_character_non_hex_digest_is_refused(self):
        """Length alone admits 64 characters of anything at all."""
        for digest in ("g" * 64, "A" * 64, "-" * 64):
            with self.assertRaises(TopologyError) as caught:
                self.build({"vocabulary": ["platform-build"],
                            "activation_value": "platform-build",
                            "quarantined_parse_defects": {"bridge/events/a.yaml": digest}})
            self.assertIn("digest is invalid", str(caught.exception), digest)

    def test_an_absolute_history_key_is_refused(self):
        with self.assertRaises(TopologyError):
            self.build({"vocabulary": ["platform-build"],
                        "activation_value": "platform-build",
                        "pre_activation_layer_records":
                            {"/etc/passwd": {"layer": "platform-build"}}})

if __name__ == "__main__":
    unittest.main()
