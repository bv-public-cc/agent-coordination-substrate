"""Shared fixtures: build a real bridge on disk for the substrate tests."""

from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path
import sys

import yaml

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from coordination_substrate.topology import Topology  # noqa: E402


TOPOLOGY = {
    "schema_version": 1,
    "orchestrator": "boss",
    "orchestrator_events_dir": "bridge/events",
    "ledger": "state.yaml",
    "lease_dir": "bridge/locks/mutating.lock",
    "lease_seconds": 600,
    "ack_timeout_seconds": 300,
    "heartbeat_stale_seconds": 600,
    "defaults": {"events_dir": "bridge/events", "listeners_dir": "bridge/listeners"},
    "roles": {
        "worker": {
            "inbound_pointer": "bridge/boss-to-worker.yaml",
            "outbound_pointer": "bridge/worker-to-boss.yaml",
            "events_dir": "bridge/events",
            "holds_mutating_lease": True,
            "active_work": {
                "task_path": ["execution", "active_task"],
                "pause_path": ["execution", "safe_pause"],
            },
        },
        "auditor": {
            "inbound_pointer": "bridge/boss-to-auditor.yaml",
            "outbound_pointer": "bridge/auditor-to-boss.yaml",
            "events_dir": "bridge/auditor-events",
            "active_work": {"task_path": ["execution", "auditor", "active_task"]},
        },
    },
}


def build_bridge(root: Path, *, active_task: str | None = "T-1", safe_pause: bool = False) -> Topology:
    """Create directories, a topology file, and a ledger under ``root``."""
    for relative in (
        "bridge/events",
        "bridge/auditor-events",
        "bridge/listeners",
        "bridge/locks",
    ):
        (root / relative).mkdir(parents=True, exist_ok=True)
    (root / "coordination").mkdir(parents=True, exist_ok=True)
    (root / "coordination/topology.yaml").write_text(yaml.safe_dump(TOPOLOGY))
    write_ledger(root, active_task=active_task, safe_pause=safe_pause)
    return Topology.load(root)


def write_ledger(
    root: Path,
    *,
    active_task: str | None = "T-1",
    safe_pause: bool = False,
    auditor_task: str | None = None,
) -> None:
    (root / "state.yaml").write_text(
        yaml.safe_dump(
            {
                "execution": {
                    "active_task": active_task,
                    "safe_pause": safe_pause,
                    "auditor": {"active_task": auditor_task},
                }
            }
        )
    )


def spec(
    *,
    pointer: str = "bridge/boss-to-worker.yaml",
    sequence: int = 2,
    writer: str = "boss",
    event_name: str = "task-assigned",
    filename: str | None = None,
    extra: dict | None = None,
) -> dict:
    body = {
        "schema_version": 1,
        "sequence": sequence,
        "event": event_name,
        "writer": writer,
        "note": "prose with a colon: stays valid YAML",
    }
    if event_name == "task-assigned":
        body["credential_contract"] = {"mode": "none"}
    if extra:
        body.update(extra)
    return {
        "pointer_path": pointer,
        "event_filename": filename or f"20260801T000000Z-{sequence}-{writer}-test.yaml",
        "event": body,
        "pointer": {
            "schema_version": 1,
            "sequence": sequence,
            "event": event_name,
            "writer": writer,
        },
    }


def seed_pointer(root: Path, pointer: str, sequence: int = 1, writer: str = "boss") -> None:
    (root / pointer).write_text(
        yaml.safe_dump({"schema_version": 1, "sequence": sequence, "event": "seed", "writer": writer})
    )


def install_event(
    root: Path, relative_dir: str, filename: str, body: dict
) -> tuple[str, str]:
    """Write an event directly and return (relative path, sha256)."""
    path = root / relative_dir / filename
    encoded = yaml.safe_dump(body, sort_keys=False).encode()
    path.write_bytes(encoded)
    return f"{relative_dir}/{filename}", hashlib.sha256(encoded).hexdigest()


def publish_pointer(
    root: Path, pointer: str, event_rel: str, digest: str, sequence: int, *,
    event_name: str = "progress", writer: str = "boss",
) -> None:
    (root / pointer).write_text(
        yaml.safe_dump(
            {
                "schema_version": 1,
                "sequence": sequence,
                "event": event_name,
                "writer": writer,
                "authoritative_event": {"path": event_rel, "sha256": digest},
            }
        )
    )


def write_listener_state(
    root: Path, role: str, *, child_pid: int, status: str = "running", listener_pid: int = 1
) -> Path:
    path = root / "bridge/listeners" / f"{role}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "role": role,
                "status": status,
                "listener_pid": listener_pid,
                "child_pid": child_pid,
                "last_acknowledged_sequence": 0,
            }
        )
    )
    return path


class FakeStdin:
    def __init__(self):
        self.written: list[bytes] = []

    def write(self, data: bytes) -> None:
        self.written.append(data)

    def flush(self) -> None:
        pass


class FakeChild:
    """Stand-in for the agent subprocess."""

    pid = 4242

    def __init__(self):
        self.stdin = FakeStdin()

    def sent_texts(self) -> list[str]:
        return [
            json.loads(chunk)["message"]["content"] for chunk in self.stdin.written
        ]

    def last_transport_id(self) -> str:
        """Extract the 32-hex transport id without depending on word order."""
        match = re.search(r"\b[0-9a-f]{32}\b", self.sent_texts()[-1])
        if not match:
            raise AssertionError("no transport id found in the wake message")
        return match.group(0)

    def replay(self, transport_id: str) -> bytes:
        return json.dumps(
            {"type": "user", "message": {"role": "user", "content": f"echo {transport_id}"}}
        ).encode()
