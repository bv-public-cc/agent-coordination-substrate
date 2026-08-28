#!/usr/bin/env python3
"""Wake one durable agent listener from validated bridge pointers.

The bridge owns a long-lived, streaming-input agent subprocess. It never
touches an IDE process or private socket. A new authoritative pointer is
hash-validated, queued as one concise user turn, and transport-acknowledged
from the agent CLI's ``--replay-user-messages`` stream.

Three independent wake reasons exist:

``pointer``
    A new orchestrator event appeared. This is the primary rail.
``continuation``
    The role's own last event says work continues to a named next pause, so
    the next turn is queued immediately instead of waiting out a stale timer.
``heartbeat``
    The ledger still declares the role active but no durable role event has
    arrived within the stale window.

Extracted from the replaced coordination bridge; role names, paths,
and the ledger probe come from a :class:`Topology`.
"""

from __future__ import annotations

import argparse
from contextlib import contextmanager
import datetime as dt
import fcntl
import hashlib
import json
import os
from pathlib import Path
import queue
import shutil
import signal
import subprocess
import sys
import threading
import time
import uuid

import yaml

from .topology import Topology, TopologyError

POLL_SECONDS = 1
PUBLISHING_STATES = {"running", "awaiting-transport-ack"}


class BridgeError(RuntimeError):
    pass


def _sha256(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def _atomic_json(path: Path, value: dict) -> None:
    body = (json.dumps(value, sort_keys=True, indent=2) + "\n").encode("utf-8")
    if json.loads(body) != value:
        raise BridgeError("listener state JSON failed round-trip validation")
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.parent / f".{path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp"
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    fd = os.open(temp, flags, 0o600)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(body)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp, path)
        dir_fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)
    finally:
        temp.unlink(missing_ok=True)


def write_terminal_state(
    state_path: "Path | None", role: str, status: str, detail: str
) -> None:
    """Record how a listener ended, on every exit path including unexpected ones.

    Failure previously reached stderr and an exit code only. Nothing that reads
    the bridge could tell a crashed listener from a busy one, so a crash
    presented as a hang and the lease it held sat untouched until expiry.

    Best effort by construction: this runs while something has already gone
    wrong, and a failure to record must never replace the original failure.
    """
    if state_path is None:
        return
    try:
        _atomic_json(
            state_path,
            {
                "schema_version": 1,
                "role": role,
                "status": status,
                "terminal": True,
                "detail": detail[:500],
                "listener_pid": os.getpid(),
                "child_pid": None,
                "recorded_at": dt.datetime.now(dt.timezone.utc).strftime(
                    "%Y-%m-%dT%H:%M:%SZ"
                ),
            },
        )
    except Exception:
        pass


def _die_with_parent() -> None:
    """Ask the kernel to signal this child when its parent dies.

    The terminate/kill escalation in serve()'s finally block covers every exit
    the parent can observe. It cannot cover the ones it cannot: SIGKILL, the OOM
    killer, a container stop. In those the finally never runs and the child is
    reparented to init, still holding the session and still writing.

    PR_SET_PDEATHSIG moves that guarantee into the kernel, so the child dies
    with the parent regardless of how the parent went. Deliberately not
    start_new_session=True, which does the opposite: it detaches the child so it
    survives, which is the orphan this exists to prevent.

    Linux only, and best effort. Where prctl is unavailable the process simply
    keeps the pre-existing behaviour rather than failing to start.
    """
    try:
        import ctypes

        PR_SET_PDEATHSIG = 1
        ctypes.CDLL("libc.so.6", use_errno=True).prctl(
            PR_SET_PDEATHSIG, signal.SIGTERM, 0, 0, 0
        )
    except Exception:
        pass


@contextmanager
def _singleton(state_path: Path):
    """Refuse a second listener for the same role, without blocking."""
    lock_path = state_path.parent / f".{state_path.name}.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    flags = os.O_RDWR | os.O_CREAT
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    fd = os.open(lock_path, flags, 0o600)
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise BridgeError("another listener bridge already owns this role") from exc
        yield
    finally:
        fcntl.flock(fd, fcntl.LOCK_UN)
        os.close(fd)


def _load_pointer(root: Path, pointer_rel: str, label: str) -> dict:
    pointer_path = root / pointer_rel
    if pointer_path.is_symlink():
        raise BridgeError(f"{label} pointer route must not be a symlink")
    try:
        pointer = yaml.safe_load(pointer_path.read_bytes())
    except (OSError, yaml.YAMLError) as exc:
        raise BridgeError(f"{label} pointer is absent or invalid") from exc
    if not isinstance(pointer, dict):
        raise BridgeError(f"{label} pointer is not an object")
    return pointer


def _verified_event(
    root: Path, pointer: dict, event_dir_rel: str, label: str
) -> tuple[dict, str, str]:
    """Resolve, containment-check, and hash-verify a pointer's event."""
    sequence = pointer.get("sequence")
    reference = pointer.get("authoritative_event")
    if not isinstance(sequence, int) or isinstance(sequence, bool) or sequence < 1:
        raise BridgeError(f"{label} pointer sequence is invalid")
    if not isinstance(reference, dict):
        raise BridgeError(f"{label} pointer has no authoritative event reference")
    event_rel = reference.get("path")
    expected = reference.get("sha256")
    if not isinstance(event_rel, str) or not isinstance(expected, str):
        raise BridgeError(f"{label} pointer event reference is invalid")
    event_path = (root / event_rel).resolve()
    allowed = (root / event_dir_rel).resolve()
    if event_path.parent != allowed:
        raise BridgeError(f"{label} event reference is outside its admitted directory")
    try:
        body = event_path.read_bytes()
        event = yaml.safe_load(body)
    except (OSError, yaml.YAMLError) as exc:
        raise BridgeError(f"{label} referenced event is absent or invalid") from exc
    if _sha256(body) != expected:
        raise BridgeError(f"{label} referenced event hash differs")
    if not isinstance(event, dict) or event.get("sequence") != sequence:
        raise BridgeError(f"{label} pointer and event sequence differ")
    if event.get("event") != pointer.get("event"):
        raise BridgeError(f"{label} pointer and event type differ")
    return event, event_rel, expected


def read_authoritative_pointer(topology: Topology, role_name: str) -> dict:
    """Validate the inbound orchestrator pointer before it may wake anything."""
    role = topology.role(role_name)
    root = topology.root
    pointer = _load_pointer(root, role.inbound_pointer, "inbound")
    event, event_rel, expected = _verified_event(
        root, pointer, topology.orchestrator_events_dir, "inbound"
    )
    if (
        pointer.get("writer") != topology.orchestrator
        or event.get("writer") != topology.orchestrator
    ):
        raise BridgeError("inbound route is not orchestrator-authored")
    return {
        "sequence": pointer["sequence"],
        "event": pointer.get("event"),
        "event_path": event_rel,
        "event_sha256": expected,
    }


def _parse_publisher_time(value: object) -> dt.datetime:
    if not isinstance(value, str):
        raise BridgeError("role event has no publisher-bound created_at")
    try:
        parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise BridgeError("role event created_at is invalid") from exc
    if parsed.tzinfo is None:
        raise BridgeError("role event created_at has no timezone")
    return parsed.astimezone(dt.timezone.utc)


def read_role_progress(topology: Topology, role_name: str) -> dict:
    """Validate the role's durable outbound pointer for heartbeat scheduling."""
    role = topology.role(role_name)
    root = topology.root
    pointer = _load_pointer(root, role.outbound_pointer, "role")
    event, event_rel, expected = _verified_event(root, pointer, role.events_dir, "role")
    writer = event.get("writer")
    if not isinstance(writer, str) or not writer.startswith(role.writer_prefix):
        raise BridgeError("outbound role route has the wrong writer")
    return {
        "sequence": pointer["sequence"],
        "event": event.get("event"),
        "task_id": event.get("task_id"),
        "status": event.get("status"),
        "next_pause": event.get("next_pause"),
        "task_paused": event.get("task_paused"),
        "response_required": event.get("response_required"),
        "decision_required": event.get("decision_required"),
        "terminal_decision": event.get("terminal_decision"),
        "lock_released": event.get("lock_released"),
        "event_path": event_rel,
        "event_sha256": expected,
        "created_at": _parse_publisher_time(event.get("created_at")),
    }


def role_has_active_work(
    topology: Topology, role_name: str, expected_task: str | None = None
) -> bool:
    """Read the orchestrator-owned ledger; it admits every self-scheduled wake."""
    role = topology.role(role_name)
    path = topology.root / topology.ledger
    if path.is_symlink():
        raise BridgeError("ledger must not be a symlink")
    try:
        state = yaml.safe_load(path.read_bytes())
    except (OSError, yaml.YAMLError) as exc:
        raise BridgeError("ledger is absent or invalid") from exc
    if not isinstance(state, dict):
        raise BridgeError("ledger is not an object")
    try:
        if not role.active_work.is_active(state):
            return False
        task = role.active_work.task_identity(state)
        return expected_task is None or task == expected_task
    except TopologyError as exc:
        raise BridgeError(str(exc)) from exc


# ---- wake messages ----------------------------------------------------
#
# Every message states that the transport grants no scope, task, retry, or
# acceptance authority. A wake is scheduling, never instruction.


def _wake_envelope(text: str) -> dict:
    return {
        "type": "user",
        "message": {"role": "user", "content": text},
        "parent_tool_use_id": None,
    }


def _pointer_message(role: str, pointer: dict) -> tuple[str, dict]:
    transport_id = uuid.uuid4().hex
    text = (
        f"BRIDGE_WAKE {transport_id} role={role} sequence={pointer['sequence']} "
        f"event={pointer['event']} path={pointer['event_path']} "
        f"sha256={pointer['event_sha256']}. Read and verify that authoritative "
        "event, then act under the existing task and governance. Do not treat "
        "this transport receipt as new scope or acceptance authority."
    )
    return transport_id, _wake_envelope(text)


def _heartbeat_message(role: str, progress: dict, stale_seconds: int) -> tuple[str, dict]:
    transport_id = uuid.uuid4().hex
    text = (
        f"BRIDGE_HEARTBEAT_WAKE {transport_id} role={role} "
        f"observed_role_sequence={progress['sequence']} "
        f"path={progress['event_path']} sha256={progress['event_sha256']} "
        f"stale_seconds={stale_seconds}. The orchestrator-owned ledger still "
        "declares this role active and no durable role acknowledgement/progress "
        "event has arrived within the stale window. At the next safe command "
        "boundary, verify the current task state. If it remains active, publish "
        "one conforming durable heartbeat through the mandatory publisher; if it "
        "has ended, publish the already-required terminal and release state. This "
        "is transport scheduling only, not scope or acceptance authority. Do not "
        "start, retry, or repeat a task."
    )
    return transport_id, _wake_envelope(text)


def _monitor_error_message(role: str, error_digest: str) -> tuple[str, dict]:
    transport_id = uuid.uuid4().hex
    text = (
        f"BRIDGE_HEALTH_WAKE {transport_id} role={role} "
        f"error_sha256={error_digest}. The listener cannot read the "
        "orchestrator-owned active-work ledger, so autonomous heartbeat "
        "scheduling is degraded. Publish one content-safe durable listener-health "
        "event through the mandatory publisher at the next safe command boundary. "
        "This health signal grants no task, retry, sequence, or acceptance "
        "authority. Do not start, retry, repeat, or stop product work because of "
        "this transport wake."
    )
    return transport_id, _wake_envelope(text)


def _continuation_message(role: str, progress: dict) -> tuple[str, dict]:
    transport_id = uuid.uuid4().hex
    text = (
        f"BRIDGE_CONTINUE_WAKE {transport_id} role={role} "
        f"observed_role_sequence={progress['sequence']} "
        f"path={progress['event_path']} sha256={progress['event_sha256']} "
        f"next_pause={progress['next_pause']}. The orchestrator-owned ledger "
        "still declares this exact task active and your durable progress event "
        "says implementation is continuing. Resume that existing task to its "
        "already declared next pause. This wake grants no new scope, task, retry, "
        "repeat, canonical execution, or acceptance authority."
    )
    return transport_id, _wake_envelope(text)


def _text_from_message(value: dict) -> str:
    message = value.get("message") if isinstance(value, dict) else None
    content = message.get("content") if isinstance(message, dict) else None
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        return " ".join(item.get("text", "") for item in content if isinstance(item, dict))
    return ""


def session_conflict(
    session_id: str | None,
    *,
    allowed_pids: set[int] | None = None,
    proc_root: Path = Path("/proc"),
) -> bool:
    """True when a process outside this listener claims the same agent session."""
    if not session_id:
        return False
    needle = f"--resume={session_id}"
    alternate = f"--session-id={session_id}"
    allowed = {os.getpid()} if allowed_pids is None else {os.getpid(), *allowed_pids}
    try:
        entries = list(proc_root.iterdir())
    except OSError:
        return False
    for entry in entries:
        if not entry.name.isdigit() or int(entry.name) in allowed:
            continue
        try:
            command = entry.joinpath("cmdline").read_bytes().replace(b"\0", b" ").decode()
        except (OSError, UnicodeDecodeError):
            continue
        if needle in command or alternate in command:
            return True
    return False


class Listener:
    """Dispatch at-most-one outstanding wake and require a replay receipt."""

    def __init__(
        self,
        *,
        role: str,
        topology: Topology,
        state_path: Path,
        child,
        clock=time.monotonic,
        wall_clock=time.time,
        ack_timeout: float | None = None,
        heartbeat_stale: float | None = None,
    ):
        self.role = role
        self.topology = topology
        self.root = topology.root
        self.state_path = state_path
        self.child = child
        self.clock = clock
        self.wall_clock = wall_clock
        self.ack_timeout = (
            topology.ack_timeout_seconds if ack_timeout is None else ack_timeout
        )
        self.heartbeat_stale = (
            topology.heartbeat_stale_seconds if heartbeat_stale is None else heartbeat_stale
        )
        # Delivery is intentionally at-least-once across bridge restarts. The
        # task idempotency key prevents duplicate execution, while replaying the
        # latest pointer is what wakes a resumed process after an interruption.
        self.last_sequence = 0
        self.pending = None
        self.last_self_wake = self._load_wake("last_self_wake", ("observed_role_sequence",))
        self.last_monitor_error_wake = self._load_wake(
            "last_monitor_error_wake", ("error_sha256",)
        )
        self.last_continuation_wake = self._load_wake(
            "last_continuation_wake", ("observed_role_sequence",), require_time=False
        )
        if self.last_continuation_wake is not None:
            self.last_continuation_wake.setdefault("dispatch_count", 1)
        self.continuation_rearm_ready = False
        self.heartbeat_monitor_error = None

    def _load_wake(
        self, key: str, required: tuple[str, ...], *, require_time: bool = True
    ) -> dict | None:
        try:
            value = json.loads(self.state_path.read_text())
        except (OSError, ValueError, TypeError):
            return None
        if not isinstance(value, dict) or value.get("role") != self.role:
            return None
        wake = value.get(key)
        if not isinstance(wake, dict):
            return None
        result: dict = {}
        for name in required:
            item = wake.get(name)
            if name.endswith("_sequence"):
                if not isinstance(item, int) or isinstance(item, bool) or item < 1:
                    return None
            elif name.endswith("_sha256"):
                if not isinstance(item, str) or len(item) != 64:
                    return None
            result[name] = item
        if require_time:
            sent_at = wake.get("sent_wall_time")
            if not isinstance(sent_at, (int, float)) or isinstance(sent_at, bool):
                return None
            result["sent_wall_time"] = float(sent_at)
        dispatch_count = wake.get("dispatch_count")
        if (
            isinstance(dispatch_count, int)
            and not isinstance(dispatch_count, bool)
            and dispatch_count >= 1
        ):
            result["dispatch_count"] = dispatch_count
        return result

    def _send(self, message: dict) -> None:
        encoded = (json.dumps(message, separators=(",", ":")) + "\n").encode()
        self.child.stdin.write(encoded)
        self.child.stdin.flush()

    def dispatch_if_new(self) -> bool:
        """Primary rail: a newer validated orchestrator pointer."""
        try:
            pointer = read_authoritative_pointer(self.topology, self.role)
        except BridgeError:
            # An absent pointer (no inbound yet), a pointer caught mid-replace, or
            # a pruned already-acknowledged event is transient, not corruption:
            # skip this poll rather than taking down the listener. A persistently
            # invalid pointer simply keeps the rail idle -- it never dispatches an
            # unvalidated wake -- which is fail-safe. This mirrors the containment
            # the continuation and heartbeat rails already have. (LIS-2.)
            return False
        if pointer["sequence"] <= self.last_sequence:
            return False
        if self.pending is not None:
            return False
        transport_id, message = _pointer_message(self.role, pointer)
        self._send(message)
        self.pending = {
            **pointer,
            "kind": "pointer",
            "transport_id": transport_id,
            "sent_monotonic": self.clock(),
        }
        self._write_state("awaiting-transport-ack")
        return True

    def dispatch_self_wake_if_due(self) -> bool:
        """Stale-timer rail, admitted only by the orchestrator-owned ledger."""
        if self.pending is not None:
            return False
        try:
            if not role_has_active_work(self.topology, self.role):
                self.heartbeat_monitor_error = None
                return False
            progress = read_role_progress(self.topology, self.role)
            progress_task = progress.get("task_id")
            if not isinstance(progress_task, str) or not progress_task.strip():
                self.heartbeat_monitor_error = None
                return False
            if not role_has_active_work(
                self.topology, self.role, progress_task
            ):
                self.heartbeat_monitor_error = None
                return False
        except BridgeError as exc:
            # The auxiliary scheduler never takes down the proven pointer rail.
            # Its observation defect is persisted and signalled through the child
            # at most once per stale interval, without task authority.
            self.heartbeat_monitor_error = str(exc)
            now = float(self.wall_clock())
            digest = hashlib.sha256(str(exc).encode()).hexdigest()
            if self.last_monitor_error_wake is not None:
                same_error = self.last_monitor_error_wake["error_sha256"] == digest
                wake_age = now - self.last_monitor_error_wake["sent_wall_time"]
                if same_error and wake_age < self.heartbeat_stale:
                    self._write_state("running")
                    return False
            transport_id, message = _monitor_error_message(self.role, digest)
            self._send(message)
            self.last_monitor_error_wake = {
                "error_sha256": digest,
                "sent_wall_time": now,
            }
            self.pending = {
                "kind": "heartbeat-monitor-error",
                "sequence": 0,
                "event": "listener-health",
                "event_path": "",
                "event_sha256": digest,
                "transport_id": transport_id,
                "sent_monotonic": self.clock(),
            }
            self._write_state("awaiting-monitor-error-wake-ack")
            return True
        self.heartbeat_monitor_error = None
        epoch = self.topology.time_authority_epoch
        if epoch is not None and progress["created_at"] < epoch:
            return False
        now = float(self.wall_clock())
        age = now - progress["created_at"].timestamp()
        if age < self.heartbeat_stale:
            return False
        if self.last_self_wake is not None:
            same_progress = (
                self.last_self_wake["observed_role_sequence"] == progress["sequence"]
            )
            wake_age = now - self.last_self_wake["sent_wall_time"]
            if same_progress and wake_age < self.heartbeat_stale:
                return False
        transport_id, message = _heartbeat_message(self.role, progress, int(age))
        self._send(message)
        self.last_self_wake = {
            "observed_role_sequence": progress["sequence"],
            "sent_wall_time": now,
        }
        self.pending = {
            **progress,
            "kind": "heartbeat",
            "transport_id": transport_id,
            "sent_monotonic": self.clock(),
        }
        self._write_state("awaiting-heartbeat-wake-ack")
        return True

    def dispatch_continuation_if_due(self) -> bool:
        """Queue the next turn immediately after an explicitly continuing event.

        A durable progress event often ends the agent's current turn. A stale
        timer is appropriate for missing progress but wasteful when that event
        itself states that work continues to a named next pause. One persisted
        wake per role sequence closes that idle gap without orchestrator
        scheduling or any expansion of task authority.
        """
        if self.pending is not None:
            return False
        try:
            if not role_has_active_work(self.topology, self.role):
                return False
            progress = read_role_progress(self.topology, self.role)
            progress_task = progress.get("task_id")
            if not isinstance(progress_task, str) or not progress_task.strip():
                return False
            if not role_has_active_work(
                self.topology, self.role, progress_task
            ):
                return False
        except BridgeError:
            # The monitor-error path owns visibility for invalid ledger state.
            return False
        next_pause = progress.get("next_pause")
        if not isinstance(next_pause, str) or not next_pause.strip():
            return False
        # Event and status words are role- and task-specific. Positive word
        # matching previously stranded both an assignment acknowledgement and an
        # assurance acknowledgement. Continue from the orchestrator-owned active
        # task unless the role event carries an explicit pause/end/decision
        # signal. This is semantic state, not an open-ended vocabulary list.
        if (
            progress.get("task_paused") is True
            or progress.get("response_required") is True
            or progress.get("decision_required") is True
            or progress.get("terminal_decision") is not None
            or progress.get("lock_released") is True
        ):
            return False
        same_progress = (
            self.last_continuation_wake is not None
            and self.last_continuation_wake["observed_role_sequence"] == progress["sequence"]
        )
        dispatch_count = (
            self.last_continuation_wake.get("dispatch_count", 1)
            if same_progress
            else 0
        )
        # A role may need more than one Claude turn to reach its declared pause.
        # The first design suppressed every later continuation for the same role
        # sequence, so a turn that ended after a correction or a test finding sat
        # idle until the 600-second stale wake -- exactly when its 600-second task
        # lease expired. Rearm only after an observed end_turn, and cap the number
        # of no-progress turns so a broken agent cannot spin forever.
        if same_progress and (
            not self.continuation_rearm_ready or dispatch_count >= 3
        ):
            return False
        transport_id, message = _continuation_message(self.role, progress)
        self._send(message)
        self.last_continuation_wake = {
            "observed_role_sequence": progress["sequence"],
            "dispatch_count": dispatch_count + 1,
        }
        self.continuation_rearm_ready = False
        self.pending = {
            **progress,
            "kind": "continuation",
            "transport_id": transport_id,
            "sent_monotonic": self.clock(),
        }
        self._write_state("awaiting-continuation-wake-ack")
        return True

    def accept_output(self, line: bytes) -> bool:
        """Consume the replay receipt that proves the turn was ingested."""
        try:
            value = json.loads(line)
        except (TypeError, ValueError):
            return False
        message = value.get("message") if isinstance(value, dict) else None
        # Claude's persisted session records carry assistant/end_turn, while
        # the live --output-format stream-json rail terminates a turn with a
        # top-level result message. Accept both representations; observing only
        # the session form left the real listener idle after a productive turn.
        turn_completed = value.get("type") == "result" or (
            value.get("type") == "assistant"
            and isinstance(message, dict)
            and message.get("stop_reason") == "end_turn"
        )
        if turn_completed:
            self.continuation_rearm_ready = True
            if self.pending is None:
                self._write_state("running")
        if self.pending is None:
            return False
        # The receipt must be the agent replaying the user wake back -- a
        # type:"user" record carrying the unique transport id -- not merely any
        # record that quotes it. A rejected input echoed in an error/system/result
        # record would otherwise clear pending and, for a pointer wake, advance
        # last_sequence, recording the event delivered though it was never
        # ingested (silent until a restart re-delivers). (LIS-1.)
        if value.get("type") != "user":
            return False
        if self.pending["transport_id"] not in _text_from_message(value):
            return False
        if self.pending["kind"] == "pointer":
            self.last_sequence = self.pending["sequence"]
        self.pending = None
        self._write_state("running")
        return True

    def assert_not_stale(self) -> None:
        if self.pending and self.clock() - self.pending["sent_monotonic"] > self.ack_timeout:
            self._write_state("transport-ack-timeout")
            raise BridgeError(
                f"listener did not acknowledge wake within {int(self.ack_timeout)} seconds"
            )

    def _write_state(self, status: str) -> None:
        value = {
            "schema_version": 1,
            "role": self.role,
            "status": status,
            "listener_pid": os.getpid(),
            "child_pid": getattr(self.child, "pid", None),
            "last_acknowledged_sequence": self.last_sequence,
        }
        if self.pending:
            value["pending"] = {
                key: self.pending[key]
                for key in (
                    "kind",
                    "sequence",
                    "event",
                    "event_path",
                    "event_sha256",
                    "transport_id",
                )
            }
        if self.last_self_wake:
            value["last_self_wake"] = self.last_self_wake
        if self.last_monitor_error_wake:
            value["last_monitor_error_wake"] = self.last_monitor_error_wake
        if self.last_continuation_wake:
            value["last_continuation_wake"] = self.last_continuation_wake
        if self.continuation_rearm_ready:
            value["continuation_rearm_ready"] = True
        if self.heartbeat_monitor_error:
            value["heartbeat_monitor_error"] = self.heartbeat_monitor_error
        _atomic_json(self.state_path, value)


def _reader(stream, output_queue: queue.Queue):
    for line in iter(stream.readline, b""):
        output_queue.put(line)
    output_queue.put(None)


def build_command(
    executable: str,
    resume_session: str | None,
    new_session: str | None = None,
    extra: list[str] | None = None,
) -> list[str]:
    if resume_session and new_session:
        raise BridgeError("resume and new session identities are mutually exclusive")
    command = [
        executable,
        "-p",
        "--input-format",
        "stream-json",
        "--output-format",
        "stream-json",
        "--verbose",
        "--replay-user-messages",
    ]
    command.extend(extra or [])
    if resume_session:
        command.append(f"--resume={resume_session}")
    elif new_session:
        command.append(f"--session-id={new_session}")
    return command


def serve(
    role: str,
    topology: Topology,
    state_path: Path,
    resume_session: str | None = None,
    new_session: str | None = None,
    *,
    executable: str = "claude",
    extra_args: list[str] | None = None,
    stderr_log: Path | None = None,
) -> None:
    resolved = shutil.which(executable)
    if not resolved:
        raise BridgeError(f"{executable} executable is unavailable")
    session_id = resume_session or new_session
    if session_conflict(session_id):
        raise BridgeError("the requested agent session is already owned by another process")
    # Child diagnostics are preserved rather than discarded; a listener that
    # cannot start is otherwise indistinguishable from one that exited cleanly.
    if stderr_log is None:
        stderr_log = state_path.parent / f"{role}.child-stderr.log"
    stderr_log.parent.mkdir(parents=True, exist_ok=True)
    with _singleton(state_path), open(stderr_log, "ab", buffering=0) as stderr_handle:
        child = subprocess.Popen(
            build_command(resolved, resume_session, new_session, extra_args),
            cwd=topology.root,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=stderr_handle,
            preexec_fn=_die_with_parent,
        )
        output: queue.Queue = queue.Queue()
        reader = threading.Thread(target=_reader, args=(child.stdout, output), daemon=True)
        reader.start()
        listener = Listener(
            role=role, topology=topology, state_path=state_path, child=child
        )

        stopping = False

        def stop(_signum, _frame):
            nonlocal stopping
            stopping = True

        # Signal handlers can only be installed from the main thread. Serving
        # from a worker thread is legitimate (supervisors, tests), so degrade to
        # cooperative shutdown rather than refusing to run at all.
        if threading.current_thread() is threading.main_thread():
            signal.signal(signal.SIGTERM, stop)
            signal.signal(signal.SIGINT, stop)
        try:
            while not stopping:
                if session_conflict(session_id, allowed_pids={child.pid}):
                    listener._write_state("session-ownership-conflict")
                    raise BridgeError("another process claimed the listener's agent session")
                listener.dispatch_if_new()
                listener.dispatch_continuation_if_due()
                listener.dispatch_self_wake_if_due()
                try:
                    line = output.get(timeout=POLL_SECONDS)
                except queue.Empty:
                    line = b""
                if line is None:
                    raise BridgeError(f"agent listener exited with {child.poll()}")
                if line:
                    listener.accept_output(line)
                listener.assert_not_stale()
        finally:
            if child.poll() is None:
                child.terminate()
                try:
                    child.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    child.kill()
                    child.wait(timeout=5)
            # Close the pipes explicitly; leaving them to the garbage collector
            # leaks descriptors in any process that serves more than one role.
            for stream in (child.stdin, child.stdout):
                if stream is not None:
                    try:
                        stream.close()
                    except OSError:
                        pass


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Serve one durable agent listener.")
    parser.add_argument("command", choices=["serve"])
    parser.add_argument("--role", required=True)
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument("--topology", default="coordination/topology.yaml")
    parser.add_argument("--state", type=Path)
    parser.add_argument("--executable", default="claude")
    parser.add_argument(
        "--arg",
        action="append",
        default=[],
        dest="extra",
        help="extra argument passed to the agent CLI; repeatable",
    )
    session_group = parser.add_mutually_exclusive_group()
    session_group.add_argument("--resume-session")
    session_group.add_argument("--new-session")
    args = parser.parse_args(argv)
    state_path: Path | None = None
    try:
        topology = Topology.load(args.root, args.topology)
        role = topology.role(args.role)
        state = args.state or topology.root / role.listener_state
        state_path = Path(state).resolve()
        serve(
            args.role,
            topology,
            state_path,
            args.resume_session,
            args.new_session,
            executable=args.executable,
            extra_args=args.extra,
        )
    except (BridgeError, TopologyError, OSError, ValueError, json.JSONDecodeError) as exc:
        print(f"REFUSED: {exc}", file=sys.stderr)
        write_terminal_state(state_path, args.role, "refused", str(exc))
        return 1
    except BaseException as exc:
        # A listener that dies of something unanticipated must still say so.
        # Without this the state file keeps its last healthy status, and a dead
        # listener is indistinguishable from a busy one for as long as anyone
        # cares to wait -- which is the hang this exists to prevent. Re-raised
        # after recording, so the traceback and exit code are unchanged.
        write_terminal_state(
            state_path, args.role, "failed", f"{type(exc).__name__}: {exc}"
        )
        raise
    write_terminal_state(state_path, args.role, "stopped", "listener exited normally")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
