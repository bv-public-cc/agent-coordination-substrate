#!/usr/bin/env python3
"""Declarative description of one coordination bridge.

The extracted substrate keeps every safety property of the original
implementation but replaces its hard-coded role names and paths with this
object. A topology names one orchestrator, one or more agent roles, and the
filesystem locations that carry authority between them.

Nothing here performs I/O against the bridge; it only resolves and validates
the shape that :mod:`publisher`, :mod:`listener`, and :mod:`lease` enforce.
"""

from __future__ import annotations

from dataclasses import dataclass, field
import datetime as dt
from pathlib import Path
from typing import Mapping, Sequence

import yaml


DEFAULT_TOPOLOGY_RELATIVE = "coordination/topology.yaml"


class TopologyError(RuntimeError):
    pass


def _require_relative(value: object, label: str) -> str:
    if not isinstance(value, str) or not value:
        raise TopologyError(f"{label} must be a non-empty string")
    if Path(value).is_absolute():
        raise TopologyError(f"{label} must be a repository-relative path")
    if ".." in Path(value).parts:
        raise TopologyError(f"{label} must not traverse upward")
    return value


def _require_identifier(value: object, label: str) -> str:
    if not isinstance(value, str) or not value or value.strip() != value:
        raise TopologyError(f"{label} must be a non-empty unpadded string")
    return value


@dataclass(frozen=True)
class ActiveWorkProbe:
    """Where the orchestrator-owned ledger declares a role busy.

    ``task_path`` walks the parsed ledger to a value that is truthy when the
    role has work. ``pause_path`` is optional; when it resolves to a truthy
    value the role counts as idle even if a task is named. This keeps the
    scheduler's admission source declarative instead of role-specific code.
    """

    task_path: tuple[str, ...]
    pause_path: tuple[str, ...] = ()

    @staticmethod
    def parse(value: object, label: str) -> "ActiveWorkProbe":
        if not isinstance(value, Mapping):
            raise TopologyError(f"{label} must be an object")
        raw_task = value.get("task_path")
        if not isinstance(raw_task, Sequence) or isinstance(raw_task, str) or not raw_task:
            raise TopologyError(f"{label}.task_path must be a non-empty list")
        raw_pause = value.get("pause_path", [])
        if isinstance(raw_pause, str) or not isinstance(raw_pause, Sequence):
            raise TopologyError(f"{label}.pause_path must be a list")
        return ActiveWorkProbe(
            task_path=tuple(_require_identifier(k, f"{label}.task_path") for k in raw_task),
            pause_path=tuple(_require_identifier(k, f"{label}.pause_path") for k in raw_pause),
        )

    def _walk(self, state: object, path: tuple[str, ...]) -> object:
        current = state
        for key in path:
            if not isinstance(current, Mapping):
                raise TopologyError(f"ledger has no object at '{key}'")
            current = current.get(key)
        return current

    def is_active(self, state: object) -> bool:
        task = self.task_identity(state)
        active = isinstance(task, str) and bool(task.strip())
        if not active:
            return False
        if self.pause_path:
            if self._walk(state, self.pause_path):
                return False
        return True

    def task_identity(self, state: object) -> str | None:
        """Return the exact orchestrator-owned task identity, if active-shaped."""
        task = self._walk(state, self.task_path)
        return task if isinstance(task, str) and bool(task.strip()) else None


@dataclass(frozen=True)
class Role:
    """One non-orchestrator participant on the bridge."""

    name: str
    writer_prefix: str
    inbound_pointer: str
    outbound_pointer: str
    events_dir: str
    listener_state: str
    active_work: ActiveWorkProbe
    holds_mutating_lease: bool = False

    @staticmethod
    def parse(name: str, value: object, defaults: Mapping[str, object]) -> "Role":
        if not isinstance(value, Mapping):
            raise TopologyError(f"role '{name}' must be an object")
        label = f"roles.{name}"
        events_dir = value.get("events_dir", defaults.get("events_dir"))
        listeners_dir = defaults.get("listeners_dir")
        listener_state = value.get("listener_state")
        if listener_state is None:
            if not isinstance(listeners_dir, str):
                raise TopologyError(f"{label}.listener_state has no default")
            listener_state = f"{listeners_dir.rstrip('/')}/{name}.json"
        return Role(
            name=_require_identifier(name, "role name"),
            writer_prefix=_require_identifier(
                value.get("writer_prefix", name), f"{label}.writer_prefix"
            ),
            inbound_pointer=_require_relative(
                value.get("inbound_pointer"), f"{label}.inbound_pointer"
            ),
            outbound_pointer=_require_relative(
                value.get("outbound_pointer"), f"{label}.outbound_pointer"
            ),
            events_dir=_require_relative(events_dir, f"{label}.events_dir"),
            listener_state=_require_relative(listener_state, f"{label}.listener_state"),
            active_work=ActiveWorkProbe.parse(
                value.get("active_work"), f"{label}.active_work"
            ),
            holds_mutating_lease=bool(value.get("holds_mutating_lease", False)),
        )


@dataclass(frozen=True)
class Topology:
    """A validated bridge description rooted at one directory."""

    root: Path
    orchestrator: str
    orchestrator_events_dir: str
    roles: Mapping[str, Role]
    ledger: str
    lease_dir: str
    # Every orchestrator route shares one sequence space, so they must share one
    # lock. Deriving this from the routes would move the lock whenever a role is
    # added, briefly leaving two locks live and reopening the race it closes.
    global_sequence_lock: str
    # Where consumption receipts live. Configured like every other authority
    # path rather than hard-coded in the consumer, so an extracted instance
    # owns its own tree instead of inheriting one module's assumption.
    consumption_dir: str = ""
    lease_seconds: int = 600
    ack_timeout_seconds: int = 300
    heartbeat_stale_seconds: int = 600
    time_authority_epoch: dt.datetime | None = None
    reference_intents: frozenset = field(
        default_factory=lambda: frozenset({"immutable_ref", "snapshot_at_publication"})
    )
    # The ledger-layer contract, expressed as instance description rather than
    # constants in the publisher. An extracted instance has its own unreadable
    # records and its own pre-contract layer history; hard-coding one instance's
    # paths here would make the module unusable anywhere else and would quietly
    # excuse the wrong files when it was reused.
    layer_vocabulary: tuple = ()
    layer_activation_event: str = "ledger-layer-activated"
    layer_activation_value: str = ""
    layer_contract_lock: str = ""
    quarantined_parse_defects: Mapping[str, str] = field(default_factory=dict)
    pre_activation_layer_records: Mapping[str, Mapping] = field(default_factory=dict)

    @property
    def layer_contract_enabled(self) -> bool:
        """A topology that declares no vocabulary does not run this contract."""
        return bool(self.layer_vocabulary)

    # ---- derived maps used by the enforcing modules -------------------

    @property
    def event_dirs(self) -> tuple[str, ...]:
        seen = [self.orchestrator_events_dir]
        for role in self.roles.values():
            if role.events_dir not in seen:
                seen.append(role.events_dir)
        return tuple(seen)

    @property
    def routes(self) -> dict[str, tuple[str, str]]:
        """pointer path -> (event directory, required writer prefix)."""
        table: dict[str, tuple[str, str]] = {}
        for role in self.roles.values():
            table[role.inbound_pointer] = (
                self.orchestrator_events_dir,
                self.orchestrator,
            )
            table[role.outbound_pointer] = (role.events_dir, role.writer_prefix)
        return table

    @property
    def agent_routes(self) -> dict[str, str]:
        """Outbound pointer -> role name. These require listener lineage."""
        return {role.outbound_pointer: role.name for role in self.roles.values()}

    @property
    def orchestrator_routes(self) -> frozenset:
        """Inbound pointers sharing one global orchestrator sequence."""
        return frozenset(role.inbound_pointer for role in self.roles.values())

    def role(self, name: str) -> Role:
        try:
            return self.roles[name]
        except KeyError as exc:
            raise TopologyError(f"unknown role '{name}'") from exc

    def lease_holder(self) -> Role | None:
        for role in self.roles.values():
            if role.holds_mutating_lease:
                return role
        return None

    # ---- loading ------------------------------------------------------

    @staticmethod
    def parse(value: object, root: Path) -> "Topology":
        if not isinstance(value, Mapping):
            raise TopologyError("topology must be an object")
        if value.get("schema_version") != 1:
            raise TopologyError("topology schema_version must be 1")
        defaults = value.get("defaults") or {}
        if not isinstance(defaults, Mapping):
            raise TopologyError("defaults must be an object")
        raw_roles = value.get("roles")
        if not isinstance(raw_roles, Mapping) or not raw_roles:
            raise TopologyError("topology requires at least one role")
        roles = {
            name: Role.parse(name, spec, defaults) for name, spec in raw_roles.items()
        }

        epoch_raw = value.get("time_authority_epoch")
        epoch = None
        if epoch_raw is not None:
            if isinstance(epoch_raw, dt.datetime):
                epoch = epoch_raw
            elif isinstance(epoch_raw, str):
                try:
                    epoch = dt.datetime.fromisoformat(epoch_raw.replace("Z", "+00:00"))
                except ValueError as exc:
                    raise TopologyError("time_authority_epoch is invalid") from exc
            else:
                raise TopologyError("time_authority_epoch must be a UTC timestamp")
            if epoch.tzinfo is None:
                raise TopologyError("time_authority_epoch must be timezone-aware")
            epoch = epoch.astimezone(dt.timezone.utc)

        raw_intents = value.get("reference_intents")
        if raw_intents is None:
            intents = frozenset({"immutable_ref", "snapshot_at_publication"})
        else:
            if isinstance(raw_intents, str) or not isinstance(raw_intents, Sequence):
                raise TopologyError("reference_intents must be a list")
            intents = frozenset(
                _require_identifier(item, "reference_intents") for item in raw_intents
            )
            if "immutable_ref" not in intents:
                raise TopologyError("reference_intents must include immutable_ref")

        raw_layer = value.get("layer_contract") or {}
        if not isinstance(raw_layer, Mapping):
            raise TopologyError("layer_contract must be an object")
        raw_vocabulary = raw_layer.get("vocabulary")
        if raw_vocabulary is None:
            vocabulary = ()
        else:
            if isinstance(raw_vocabulary, str) or not isinstance(raw_vocabulary, Sequence):
                raise TopologyError("layer_contract.vocabulary must be a list")
            vocabulary = tuple(
                _require_identifier(item, "layer_contract.vocabulary")
                for item in raw_vocabulary
            )
            if len(set(vocabulary)) != len(vocabulary):
                raise TopologyError("layer_contract.vocabulary repeats a value")
        activation_value = raw_layer.get("activation_value", "")
        if vocabulary and activation_value not in vocabulary:
            raise TopologyError("layer_contract.activation_value is not in the vocabulary")
        quarantine = raw_layer.get("quarantined_parse_defects") or {}
        if not isinstance(quarantine, Mapping):
            raise TopologyError("layer_contract.quarantined_parse_defects must be an object")
        for path, digest in quarantine.items():
            _require_relative(path, "quarantined_parse_defects")
            if (not isinstance(digest, str) or len(digest) != 64
                    or any(character not in "0123456789abcdef" for character in digest)):
                # Length alone admits 64 characters of anything, including an
                # uppercase or truncated-and-padded value that would never match
                # a computed digest and would therefore fail closed for the
                # wrong reason, or worse, be silently mistyped and never noticed.
                raise TopologyError("a quarantined parse defect digest is invalid")
        history = raw_layer.get("pre_activation_layer_records") or {}
        if not isinstance(history, Mapping):
            raise TopologyError("layer_contract.pre_activation_layer_records must be an object")
        for path, entry in history.items():
            _require_relative(path, "pre_activation_layer_records")
            if not isinstance(entry, Mapping) or "layer" not in entry:
                raise TopologyError("a pre-activation layer record is invalid")

        lock = value.get("global_sequence_lock")
        if lock is None:
            lock = f"{Path(_require_relative(value.get('lease_dir'), 'lease_dir')).parent}/orchestrator-global-sequence"

        topology = Topology(
            root=Path(root).resolve(),
            orchestrator=_require_identifier(value.get("orchestrator"), "orchestrator"),
            orchestrator_events_dir=_require_relative(
                value.get("orchestrator_events_dir", defaults.get("events_dir")),
                "orchestrator_events_dir",
            ),
            roles=roles,
            ledger=_require_relative(value.get("ledger"), "ledger"),
            lease_dir=_require_relative(value.get("lease_dir"), "lease_dir"),
            consumption_dir=_require_relative(
                value.get(
                    "consumption_dir",
                    f"{Path(_require_relative(value.get('lease_dir'), 'lease_dir')).parent}"
                    "/consumption"),
                "consumption_dir"),
            global_sequence_lock=_require_relative(lock, "global_sequence_lock"),
            lease_seconds=int(value.get("lease_seconds", 600)),
            ack_timeout_seconds=int(value.get("ack_timeout_seconds", 300)),
            heartbeat_stale_seconds=int(value.get("heartbeat_stale_seconds", 600)),
            time_authority_epoch=epoch,
            reference_intents=intents,
            layer_vocabulary=vocabulary,
            layer_activation_event=_require_identifier(
                raw_layer.get("activation_event", "ledger-layer-activated"),
                "layer_contract.activation_event"),
            layer_activation_value=activation_value,
            # Relative, like every other configured path. An absolute value
            # would make `root / lock` escape the instance root entirely, and a
            # traversal would place the lock outside the tree it serializes.
            layer_contract_lock=_require_relative(
                raw_layer.get(
                    "contract_lock",
                    f"{Path(_require_relative(value.get('lease_dir'), 'lease_dir')).parent}"
                    "/layer-contract"),
                "layer_contract.contract_lock"),
            quarantined_parse_defects=dict(quarantine),
            pre_activation_layer_records={
                path: dict(entry) for path, entry in history.items()},
        )
        topology.validate()
        return topology

    def validate(self) -> None:
        pointers: dict[str, str] = {}
        for role in self.roles.values():
            for pointer, label in (
                (role.inbound_pointer, f"{role.name}.inbound"),
                (role.outbound_pointer, f"{role.name}.outbound"),
            ):
                if pointer in pointers:
                    raise TopologyError(
                        f"pointer '{pointer}' is claimed by {pointers[pointer]} and {label}"
                    )
                pointers[pointer] = label
            if role.writer_prefix == self.orchestrator:
                raise TopologyError(
                    f"role '{role.name}' writer_prefix collides with the orchestrator"
                )
        if self.lease_seconds < 1 or self.ack_timeout_seconds < 1:
            raise TopologyError("lease and ack timeouts must be positive")
        if sum(1 for r in self.roles.values() if r.holds_mutating_lease) > 1:
            raise TopologyError("at most one role may hold the mutating lease")
        if self.layer_vocabulary and not self.layer_activation_value:
            raise TopologyError("a layer contract requires an activation_value")
        if self.layer_vocabulary and self.layer_contract_lock == self.global_sequence_lock:
            # Two different scopes must not share one lock file: the contract
            # lock spans every route, the sequence lock only the orchestrator's.
            raise TopologyError("layer_contract lock must differ from the sequence lock")

    @staticmethod
    def load(root: Path, relative: str = DEFAULT_TOPOLOGY_RELATIVE) -> "Topology":
        root = Path(root).resolve()
        path = root / relative
        if path.is_symlink():
            raise TopologyError("topology file must not be a symlink")
        try:
            value = yaml.safe_load(path.read_bytes())
        except (OSError, yaml.YAMLError) as exc:
            raise TopologyError(f"topology is absent or invalid: {path}") from exc
        return Topology.parse(value, root)
