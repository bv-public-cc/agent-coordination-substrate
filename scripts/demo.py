#!/usr/bin/env python3
"""End-to-end smoke demo: build a bridge, publish, verify, lease, release.

Runs entirely in a temporary directory and prints what each step proved.
"""

from __future__ import annotations

import datetime as dt
import hashlib
from pathlib import Path
import sys
import tempfile

import yaml

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from coordination_substrate.lease import claim, read_lease, release  # noqa: E402
from coordination_substrate.publisher import PublishError, publish  # noqa: E402
from coordination_substrate.topology import Topology  # noqa: E402

TOPOLOGY = {
    "schema_version": 1,
    "orchestrator": "boss",
    "orchestrator_events_dir": "bridge/events",
    "ledger": "state.yaml",
    "lease_dir": "bridge/locks/mutating.lock",
    "defaults": {"events_dir": "bridge/events", "listeners_dir": "bridge/listeners"},
    "roles": {
        "worker": {
            "inbound_pointer": "bridge/boss-to-worker.yaml",
            "outbound_pointer": "bridge/worker-to-boss.yaml",
            "holds_mutating_lease": True,
            "active_work": {"task_path": ["execution", "active_task"]},
        }
    },
}


def step(number: int, text: str) -> None:
    print(f"  {number}. {text}")


def main() -> int:
    with tempfile.TemporaryDirectory() as name:
        root = Path(name)
        for relative in ("bridge/events", "bridge/listeners", "bridge/locks", "coordination"):
            (root / relative).mkdir(parents=True, exist_ok=True)
        (root / "coordination/topology.yaml").write_text(yaml.safe_dump(TOPOLOGY))
        (root / "state.yaml").write_text(yaml.safe_dump({"execution": {"active_task": "T-1"}}))
        topology = Topology.load(root)
        print(f"\nbridge root: {root}\n")

        result = publish(
            {
                "pointer_path": "bridge/boss-to-worker.yaml",
                "event_filename": "20260801T120000Z-1-boss-task-assigned.yaml",
                "event": {
                    "schema_version": 1,
                    "sequence": 1,
                    "event": "task-assigned",
                    "writer": "boss",
                    "task_id": "T-1",
                    "credential_contract": {"mode": "none"},
                    "note": "a colon: in prose stays valid",
                },
                "pointer": {
                    "schema_version": 1,
                    "sequence": 1,
                    "event": "task-assigned",
                    "writer": "boss",
                },
            },
            topology=topology,
        )
        step(1, f"published event {result['event_path']}")

        pointer = yaml.safe_load((root / "bridge/boss-to-worker.yaml").read_bytes())
        on_disk = hashlib.sha256((root / result["event_path"]).read_bytes()).hexdigest()
        assert pointer["authoritative_event"]["sha256"] == on_disk
        step(2, f"pointer digest matches installed bytes ({on_disk[:12]}…)")

        try:
            publish(
                {
                    "pointer_path": "bridge/boss-to-worker.yaml",
                    "event_filename": "20260801T120100Z-1-boss-replay.yaml",
                    "event": {"schema_version": 1, "sequence": 1, "event": "x", "writer": "boss"},
                    "pointer": {"schema_version": 1, "sequence": 1, "event": "x", "writer": "boss"},
                },
                topology=topology,
            )
            raise SystemExit("FAILED: non-monotonic sequence was accepted")
        except PublishError as exc:
            step(3, f"refused non-monotonic sequence: {exc}")

        request_digest = hashlib.sha256(b"request").hexdigest()
        identity = {
            "task_id": "T-1",
            "attempt": 1,
            "request_sha256": request_digest,
            "owner": "worker-1",
        }
        claim(
            topology,
            **identity,
            harness="demo",
            listener_pid=1,
            listener_child_pid=2,
            session_id="b4a20b7d-100b-4f13-a509-9340559ed468",
            clock=lambda: dt.datetime.now(dt.timezone.utc),
        )
        step(4, f"claimed lease, expires {read_lease(topology)['expires_at']}")

        try:
            claim(
                topology,
                **{**identity, "owner": "intruder"},
                harness="demo",
                listener_pid=1,
                listener_child_pid=2,
                session_id="b4a20b7d-100b-4f13-a509-9340559ed468",
            )
            raise SystemExit("FAILED: second owner acquired a held lease")
        except Exception as exc:  # LeaseError
            step(5, f"refused second owner: {exc}")

        release(topology, **identity)
        assert read_lease(topology) is None
        step(6, "released lease; slot is free")

        print("\nall demo assertions held\n")
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
