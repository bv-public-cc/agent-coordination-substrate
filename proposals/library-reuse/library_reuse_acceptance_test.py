#!/usr/bin/env python3
"""Reuse acceptance: the library holds its invariants under a DIFFERENT scope.

The library is only "reusable in another project" if its safety properties hold
when instantiated with roles and paths that are not this project's. Passing under
the default (boss/worker) topology proves nothing about generality; this test
builds a captain/scout/signals bridge with zero 'boss' anywhere and asserts the
core invariants still FIRE — route/writer authority, orchestrator sequence
monotonicity, and the lease payload-owner tuple. The refusals are the controls:
each asserts the invariant rejects, not merely that a valid call succeeds.

Run directly; stdlib + the library only.
"""
import datetime as dt
import sys
import tempfile
import uuid
import os
from pathlib import Path

sys.path.insert(0, "/srv/coordination-substrate")
import yaml  # noqa: E402
from coordination_substrate.topology import Topology  # noqa: E402
from coordination_substrate.publisher import publish, PublishError  # noqa: E402
from coordination_substrate import lease  # noqa: E402
from coordination_substrate.lease import LeaseError  # noqa: E402

# A scope that shares nothing with this project's names.
TOPOLOGY = {
    "schema_version": 1,
    "orchestrator": "captain",
    "orchestrator_events_dir": "bridge/events",
    "ledger": "fleet-state.yaml",
    "lease_dir": "bridge/locks/scout-mutating.lock",
    "global_sequence_lock": "bridge/locks/captain-global-sequence",
    "defaults": {"events_dir": "bridge/events", "listeners_dir": "bridge/listeners"},
    "roles": {
        "scout": {
            "inbound_pointer": "bridge/captain-to-scout.yaml",
            "outbound_pointer": "bridge/scout-to-captain.yaml",
            "events_dir": "bridge/events",
            "holds_mutating_lease": True,
            "active_work": {"task_path": ["ops", "active"], "pause_path": ["ops", "hold"]},
        },
        "signals": {
            "inbound_pointer": "bridge/captain-to-signals.yaml",
            "outbound_pointer": "bridge/signals-to-captain.yaml",
            "events_dir": "bridge/signals-events",
            "active_work": {"task_path": ["ops", "signals", "active"]},
        },
    },
}


def build(root: Path) -> Topology:
    for rel in ("bridge/events", "bridge/signals-events", "bridge/listeners", "bridge/locks", "coordination"):
        (root / rel).mkdir(parents=True, exist_ok=True)
    (root / "coordination/topology.yaml").write_text(yaml.safe_dump(TOPOLOGY))
    (root / "fleet-state.yaml").write_text(yaml.safe_dump(
        {"ops": {"active": "OP-1", "hold": False, "signals": {"active": None}}}))
    return Topology.load(root)


def spec(seq, *, writer="captain", pointer="bridge/captain-to-scout.yaml", event="task-assigned"):
    body = {"schema_version": 1, "sequence": seq, "event": event, "writer": writer,
            "note": "prose with a colon: stays valid"}
    if event == "task-assigned":
        body["credential_contract"] = {"mode": "none"}
    return {"pointer_path": pointer,
            "event_filename": f"20260801T000000Z-{seq}-{writer}-x.yaml",
            "event": body,
            "pointer": {"schema_version": 1, "sequence": seq, "event": event, "writer": writer}}


def expect_refusal(fn, kind):
    try:
        fn()
    except (PublishError, LeaseError) as exc:
        return str(exc)
    raise AssertionError(f"{kind}: expected a refusal, got none")


def main() -> int:
    checks = []
    with tempfile.TemporaryDirectory() as d:
        root = Path(d)
        # Generality precondition: no 'boss' token anywhere in this scope.
        assert "boss" not in yaml.safe_dump(TOPOLOGY).lower(), "test scope leaked 'boss'"
        topo = build(root)
        assert topo.orchestrator == "captain" and set(topo.roles) == {"scout", "signals"}
        checks.append("topology loads under a captain/scout/signals scope")

        # 1. A valid orchestrator publish succeeds under the new scope.
        publish(spec(1), topology=topo)
        assert (root / "bridge/captain-to-scout.yaml").exists()
        checks.append("valid captain->scout publish succeeds, pointer written")

        # 2. CONTROL: a non-orchestrator writer on the captain route is refused.
        why = expect_refusal(lambda: publish(spec(2, writer="intruder"), topology=topo),
                             "route/writer authority")
        checks.append(f"wrong-writer publish refused ({why[:48]}...)")

        # 3. CONTROL: orchestrator sequence must strictly increase.
        why = expect_refusal(lambda: publish(spec(1), topology=topo), "sequence monotonicity")
        checks.append(f"replayed/lower sequence refused ({why[:48]}...)")
        publish(spec(2), topology=topo)  # a higher sequence is admitted
        checks.append("higher sequence admitted")

        # 4. Lease payload-owner tuple under the custom lease_dir.
        owner_kw = dict(harness="h1", listener_pid=os.getpid(),
                        listener_child_pid=os.getpid(), session_id=str(uuid.uuid4()))
        lease.claim(topo, task_id="OP-1", attempt=1, request_sha256="a" * 64,
                    owner="scout-instance", **owner_kw)
        checks.append("scout acquired the mutating lease")
        # CONTROL: a different owner tuple cannot take the held lease.
        why = expect_refusal(
            lambda: lease.claim(topo, task_id="OP-9", attempt=1, request_sha256="b" * 64,
                                owner="other-instance", **owner_kw),
            "lease payload-owner")
        checks.append(f"second owner refused ({why[:48]}...)")
        lease.release(topo, task_id="OP-1", attempt=1, request_sha256="a" * 64, owner="scout-instance")
        checks.append("recorded owner released the lease")

    print("REUSE ACCEPTANCE — library holds its invariants under a different scope:")
    for c in checks:
        print(f"  [ok] {c}")
    print("\nRESULT: the substrate is role-neutral and portable (no boss-coupling in the tested surface).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
