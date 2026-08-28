#!/usr/bin/env python3
"""Atomically publish one validated event and its current pointer.

Input is JSON; this module alone serializes YAML. A valid append-only event is
installed first without overwrite, then the pointer is atomically replaced with
that event's exact path and SHA-256. A failure can leave a valid orphan event,
but can never publish a pointer to missing, invalid, or mismatched content.

Extracted from the replaced coordination bridge. Role names and paths
come from a :class:`~coordination_substrate.topology.Topology` instead of
module constants; every validation is otherwise unchanged.
"""

from __future__ import annotations

import copy
from contextlib import contextmanager
import datetime as dt
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import sys
import uuid

import yaml

from .integrity import load_events
from .topology import Topology, TopologyError, _require_relative

# Fields whose values always name append-only events in this bridge. They are
# authority-bearing, so their digests are verified at publication time rather
# than trusted from hand transcription.
EVENT_REFERENCE_FIELDS = {
    "responds_to",
    "corrects",
    "corrected_responds_to",
    "supersedes",
    "supersedes_draft_hashes_in",
    "rebound",
    "reconciles",
    "authoritative_event",
}
MAX_CLOCK_SKEW_SECONDS = 60
EVENT_FILENAME_TIME = re.compile(r"^\d{8}T\d{6}Z-(.+\.yaml)$")


class PublishError(RuntimeError):
    pass


def _stamp_created_at(spec: dict, now: dt.datetime | None = None) -> dict:
    """Bind event time to the publisher clock, not hand-authored prose.

    Callers may omit ``created_at`` and receive the publisher's current UTC
    second. A supplied value is accepted only when event and pointer agree and
    it is within a small clock-skew window. This prevents a valid sequence from
    carrying a future timestamp that corrupts duration metrics.
    """
    stamped = copy.deepcopy(spec)
    try:
        event = stamped["event"]
        pointer = stamped["pointer"]
    except (KeyError, TypeError) as exc:
        raise PublishError("spec requires event and pointer") from exc
    if not isinstance(event, dict) or not isinstance(pointer, dict):
        raise PublishError("event and pointer must be objects")
    instant = now or dt.datetime.now(dt.timezone.utc)
    if instant.tzinfo is None:
        raise PublishError("publisher clock must be timezone-aware")
    instant = instant.astimezone(dt.timezone.utc).replace(microsecond=0)
    supplied_event = event.get("created_at")
    supplied_pointer = pointer.get("created_at")
    if supplied_event is None and supplied_pointer is None:
        authoritative = instant.strftime("%Y-%m-%dT%H:%M:%SZ")
    else:
        if supplied_event != supplied_pointer:
            raise PublishError("event and pointer created_at differ")
        if not isinstance(supplied_event, str):
            raise PublishError("created_at must be a UTC string")
        try:
            parsed = dt.datetime.strptime(
                supplied_event, "%Y-%m-%dT%H:%M:%SZ"
            ).replace(tzinfo=dt.timezone.utc)
        except ValueError as exc:
            raise PublishError("created_at must use UTC second format") from exc
        if abs((parsed - instant).total_seconds()) > MAX_CLOCK_SKEW_SECONDS:
            raise PublishError("created_at exceeds publisher clock-skew bound")
        authoritative = supplied_event
    event["created_at"] = authoritative
    pointer["created_at"] = authoritative
    filename = stamped.get("event_filename")
    if not isinstance(filename, str):
        raise PublishError("event_filename must be a string")
    matched = EVENT_FILENAME_TIME.fullmatch(filename)
    if matched is None:
        raise PublishError("event filename must begin with a UTC timestamp")
    filename_time = dt.datetime.strptime(
        authoritative, "%Y-%m-%dT%H:%M:%SZ"
    ).strftime("%Y%m%dT%H%M%SZ")
    stamped["event_filename"] = f"{filename_time}-{matched.group(1)}"
    return stamped


def _process_ancestors(pid: int | None = None, proc_root: Path = Path("/proc")) -> set[int]:
    """Return the caller lineage used to bind an agent publish to its listener."""
    ancestors: set[int] = set()
    current = os.getpid() if pid is None else pid
    while current > 1 and current not in ancestors:
        ancestors.add(current)
        try:
            fields = (proc_root / str(current) / "stat").read_text().split()
            current = int(fields[3])
        except (OSError, ValueError, IndexError):
            break
    return ancestors


def _validate_agent_sender(
    pointer_rel: str,
    topology: Topology,
    *,
    ancestors: set[int] | None = None,
) -> None:
    """Refuse agent-authored events outside the registered listener lineage.

    A role lock alone does not authenticate a process: another process can
    reuse the same agent session and claim the same writer/instance fields.
    Binding outbound publication to the durable listener's child-process
    ancestry makes that duplicate process unable to publish under the
    listener's identity.
    """
    role_name = topology.agent_routes.get(pointer_rel)
    if role_name is None:
        return
    role = topology.role(role_name)
    state_path = topology.root / role.listener_state
    try:
        state = json.loads(state_path.read_text())
    except (OSError, ValueError) as exc:
        raise PublishError(f"{role_name} listener state is absent or invalid") from exc
    child_pid = state.get("child_pid") if isinstance(state, dict) else None
    status = state.get("status") if isinstance(state, dict) else None
    if not isinstance(child_pid, int) or isinstance(child_pid, bool) or child_pid < 2:
        raise PublishError(f"{role_name} listener child identity is invalid")
    if status not in {"running", "awaiting-transport-ack"}:
        raise PublishError(f"{role_name} listener is not in a publishing state")
    lineage = _process_ancestors() if ancestors is None else ancestors
    if child_pid not in lineage:
        raise PublishError(
            f"caller is outside the registered {role_name} listener process lineage"
        )


def _validate_global_orchestrator_sequence(
    pointer_rel: str, event: dict, topology: Topology
) -> None:
    """Keep one monotonic orchestrator sequence across every outbound route."""
    routes = topology.orchestrator_routes
    if pointer_rel not in routes:
        return
    sequence = event.get("sequence")
    # This runs before _validated_inputs checks the sequence type, and main()'s
    # handler does not catch TypeError, so a string sequence would otherwise raise
    # a raw `'<=' not supported between str and int` out of publish(). Refuse it
    # as a typed error here. (PUB-4.)
    if not isinstance(sequence, int) or isinstance(sequence, bool):
        raise PublishError("event sequence must be an integer")
    observed: list[int] = []
    for route in routes:
        path = topology.root / route
        if not path.exists():
            continue
        try:
            pointer = yaml.safe_load(path.read_bytes())
        except (OSError, yaml.YAMLError) as exc:
            raise PublishError("orchestrator route pointer is invalid") from exc
        value = pointer.get("sequence") if isinstance(pointer, dict) else None
        if not isinstance(value, int) or isinstance(value, bool):
            raise PublishError("orchestrator route pointer sequence is invalid")
        observed.append(value)
    if observed and sequence <= max(observed):
        raise PublishError("sequence is not greater than the global orchestrator sequence")


@contextmanager
def _pointer_lock(pointer_path: Path):
    """Serialize sequence validation and pointer replacement for one route.

    The lock directory is never created here. A route whose directory is absent
    is a topology error and must fail closed rather than silently materialize a
    new tree before validation has run.
    """
    lock_path = pointer_path.parent / f".{pointer_path.name}.publish.lock"
    if not lock_path.parent.is_dir():
        raise PublishError("bridge route directory is absent")
    flags = os.O_RDWR | os.O_CREAT
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    fd = os.open(lock_path, flags, 0o600)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX)
        yield
    finally:
        fcntl.flock(fd, fcntl.LOCK_UN)
        os.close(fd)


def _dump(value: dict) -> bytes:
    body = yaml.safe_dump(
        value, sort_keys=False, default_flow_style=False, width=100
    ).encode("utf-8")
    if yaml.safe_load(body) != value:
        raise PublishError("YAML round-trip validation failed")
    return body


def _validate_pause_scope(event: dict) -> None:
    """Require explicit blast-radius analysis before a workflow can stop.

    Ambiguous safety language previously allowed a local integrity concern to
    halt an entire task. Stop-shaped events must identify the exact boundary
    and the work that continues. A task-wide stop needs positive overlap proof;
    an empty continuation list is never accepted by omission.
    """
    event_name = str(event.get("event", "")).lower()
    status = str(event.get("status", "")).lower()
    stop_shaped = (
        "safety-stop" in event_name
        or "decision-required" in event_name
        or status.startswith("halted")
        or status.startswith("stopped")
    )
    if not stop_shaped:
        return
    scope = event.get("pause_scope")
    if not isinstance(scope, dict):
        raise PublishError("stop-shaped event requires pause_scope")
    for field in (
        "blocked_action",
        "shared_state",
        "last_verified_state",
        "continuation_lanes",
        "resume_condition",
    ):
        value = scope.get(field)
        if value is None or value == "" or value == {}:
            raise PublishError(f"pause_scope.{field} is required")
    lanes = scope["continuation_lanes"]
    if not isinstance(lanes, list):
        raise PublishError("pause_scope.continuation_lanes must be a list")
    if not lanes:
        justification = scope.get("task_wide_stop_justification")
        proof = scope.get("overlap_proof")
        if not isinstance(justification, str) or not justification.strip():
            raise PublishError("task-wide stop requires task_wide_stop_justification")
        if not isinstance(proof, list) or not proof:
            raise PublishError("task-wide stop requires non-empty overlap_proof")


def _validate_decision_contract(event: dict) -> None:
    """Prevent ambiguity from becoming an unbounded research or stop cycle."""
    event_name = str(event.get("event", "")).lower()
    if "decision-required" not in event_name:
        return
    contract = event.get("decision_contract")
    if not isinstance(contract, dict):
        raise PublishError("decision-required event requires decision_contract")
    decision_class = contract.get("class")
    admitted = {
        "ambiguity",
        "authority",
        "security",
        "destructive",
        "canonical-budget",
        "architecture",
        "acceptance-contract",
    }
    if decision_class not in admitted:
        raise PublishError("decision_contract.class is invalid")
    for field in ("question", "next_material_decision", "reversible_default"):
        value = contract.get(field)
        if not isinstance(value, str) or not value.strip():
            raise PublishError(f"decision_contract.{field} is required")
    # The flagship rule applies to EVERY decision-required event, not only the
    # ambiguity class: an escalation whose outcomes all lead to one action does
    # not change what happens next, so it is not a decision. DESIGN.md states this
    # unconditionally; the code previously enforced it for ambiguity alone. (PUB-2.)
    outcomes = contract.get("outcome_actions")
    if not isinstance(outcomes, list) or len(outcomes) < 2:
        raise PublishError("a decision-required event requires at least two outcome_actions")
    actions = set()
    for item in outcomes:
        if not isinstance(item, dict):
            raise PublishError("each outcome_action must be an object")
        outcome = item.get("outcome")
        action = item.get("action")
        if not isinstance(outcome, str) or not outcome.strip():
            raise PublishError("each outcome_action requires outcome")
        if not isinstance(action, str) or not action.strip():
            raise PublishError("each outcome_action requires action")
        actions.add(action.strip().lower())
    if len(actions) < 2:
        raise PublishError(
            "the decision does not change the next action; proceed without escalation"
        )
    # Ambiguity is the reversible case: it must additionally proceed under a
    # recorded assumption bounded by a check, so it cannot become an unbounded
    # research or stop cycle. These extras stay ambiguity-specific.
    if decision_class != "ambiguity":
        return
    if contract.get("next_action_reversible") is not False:
        raise PublishError("reversible ambiguity must proceed under a recorded assumption")
    check = contract.get("bounded_check")
    if not isinstance(check, dict):
        raise PublishError("ambiguity requires bounded_check")
    for field in ("method", "budget", "stop_condition"):
        value = check.get(field)
        if not isinstance(value, str) or not value.strip():
            raise PublishError(f"bounded_check.{field} is required")


def _validate_task_assignment_credentials(event: dict) -> None:
    """Refuse assignments that leave credential authority implicit.

    The contract is carried in the first assignment event so an executor never
    has to improvise a secret transport while waiting for a later correction.
    """
    event_name = event.get("event")
    # Use the same suffix set the layer logic uses (ASSIGNMENT_EVENT_SUFFIXES).
    # Checking only "-assigned" let a `task-assignment` event -- an assignment for
    # layer purposes -- escape the credential contract entirely. (PUB-3.)
    if not isinstance(event_name, str) or not event_name.endswith(ASSIGNMENT_EVENT_SUFFIXES):
        return
    contract = event.get("credential_contract")
    if not isinstance(contract, dict):
        raise PublishError("assignment event requires credential_contract")
    mode = contract.get("mode")
    if mode == "none":
        if set(contract) != {"mode"}:
            raise PublishError("credential_contract mode none permits no interfaces")
        return
    if mode != "protected-interfaces":
        raise PublishError("credential_contract.mode is invalid")
    interfaces = contract.get("interfaces")
    if not isinstance(interfaces, list) or not interfaces:
        raise PublishError("protected credential contract requires interfaces")
    for interface in interfaces:
        if not isinstance(interface, dict):
            raise PublishError("credential interface must be an object")
        for field in (
            "env_name",
            "scope",
            "transport",
            "never_expose_in",
            "prohibited_names",
        ):
            value = interface.get(field)
            if value is None or value == "" or value == []:
                raise PublishError(f"credential interface requires {field}")
        if (
            not isinstance(interface["env_name"], str)
            or not interface["env_name"].isidentifier()
        ):
            raise PublishError("credential interface env_name is invalid")
        if not isinstance(interface["scope"], str):
            raise PublishError("credential interface scope must be text")
        if interface["transport"] != "protected-file-or-askpass":
            raise PublishError("credential interface transport is not approved")
        exposure = interface["never_expose_in"]
        required_exposure = {"argv", "logs", "events", "artifacts", "helper-body"}
        if not isinstance(exposure, list) or not required_exposure.issubset(exposure):
            raise PublishError("credential interface exposure prohibitions are incomplete")
        prohibited = interface["prohibited_names"]
        if (
            not isinstance(prohibited, list)
            or not prohibited
            or not all(isinstance(name, str) and name for name in prohibited)
        ):
            raise PublishError("credential interface prohibited_names is invalid")
        if {"value", "token", "secret", "password"}.intersection(interface):
            raise PublishError("credential interface must never contain a secret value")


def _admitted_event_dirs(topology: Topology) -> set[Path]:
    return {(topology.root / directory).resolve() for directory in topology.event_dirs}


# The shapes that grant a consequential spend. An event carrying one of these
# with a positive count is authorising something that costs.
POSITIVE_AUTHORITY_FIELDS = (
    "provider_main_processes", "process_invocations_permitted",
    "model_requests", "target_actions", "canonical_runs",
    "isolated_disposable_runs",
)
REMEDIATION_DECISIONS = frozenset({"adopt", "adapt", "build"})
# A typed terminal states explicitly whether the action failed or succeeded.
# Activation is prospective: a record written before this field existed is read
# as neither, and never as evidence that nothing failed.
TERMINAL_OUTCOMES = frozenset({"failed", "succeeded"})


EXECUTION_GRANT_FIELD = "isolated_disposable_runs"
_HEX64 = re.compile(r"^[0-9a-f]{64}$")
_HEX40 = re.compile(r"^[0-9a-f]{40}$")


def check_execution_grant(grant):
    """The one rule set for a typed isolated-disposable execution grant.

    Returns None when the grant is well formed, else a (kind, detail) pair.
    Publication turns the pair into a PublishError and consumption turns it
    into its own typed refusal, so a grant can never be admissible under one
    module's rules and inadmissible under the other's.
    """
    if not isinstance(grant, dict):
        return ("grant-contract", "live_authorization must be an object")

    bound = grant.get(EXECUTION_GRANT_FIELD)
    if isinstance(bound, bool) or not isinstance(bound, int) or bound < 1:
        return ("grant-contract",
                f"{EXECUTION_GRANT_FIELD} must be a positive integer bound")

    descriptor = _action_descriptor(grant)
    if descriptor is None:
        return ("grant-contract",
                "grant requires provider, lowercase-hex packet_sha256 and a "
                "trimmed operation")

    declared = grant.get("action_identity")
    if not isinstance(declared, str) or not declared:
        return ("action-identity", "grant does not declare an action_identity")
    if declared != _derived_action_identity(descriptor):
        return ("action-identity",
                "declared action_identity does not match its descriptor")

    candidate = grant.get("candidate")
    if not isinstance(candidate, str) or not _HEX40.fullmatch(candidate):
        return ("grant-contract",
                "grant candidate must be a full lowercase git object id")

    runner = grant.get("runner_path")
    if not isinstance(runner, str) or not runner:
        return ("grant-contract", "grant runner_path is missing")
    try:
        _require_relative(runner, "runner_path")
    except TopologyError as exc:
        return ("unsafe-path", str(exc))

    digest = grant.get("runner_sha256")
    if not isinstance(digest, str) or not _HEX64.fullmatch(digest):
        return ("grant-contract",
                "runner_sha256 must be 64 lowercase hex characters")
    return None


def _validate_execution_grant(event: dict) -> None:
    """Refuse a malformed grant before the event is installed.

    Only a grant that actually claims the bound is checked; every other event
    is untouched. Because this runs in the pre-install validation block, a
    refusal leaves no event file behind and never moves the route pointer.
    """
    grant = event.get("live_authorization")
    if not isinstance(grant, dict) or EXECUTION_GRANT_FIELD not in grant:
        return
    problem = check_execution_grant(grant)
    if problem is not None:
        kind, detail = problem
        raise PublishError(f"execution grant is invalid ({kind}): {detail}")


def _positive_authority(event: dict) -> bool:
    scopes = [event]
    grant = event.get("live_authorization")
    if isinstance(grant, dict):
        scopes.append(grant)
    for scope in scopes:
        for field in POSITIVE_AUTHORITY_FIELDS:
            value = scope.get(field)
            if isinstance(value, bool):
                continue
            if isinstance(value, int) and value > 0:
                return True
            if isinstance(value, dict):
                for inner in value.values():
                    if isinstance(inner, int) and not isinstance(inner, bool) and inner > 0:
                        return True
    return False


def _derive_matching_failures(identity: str, topology: Topology) -> list:
    """Count prior failures for one action from the ledger, not from the caller.

    An earlier version of this rule validated whatever the caller declared, so
    a caller declaring zero was believed and the brake could be removed by the
    party it was meant to brake. Nothing here reads that declaration.

    Derivation reads only parsed records that carry the exact action identity.
    An unparseable record cannot carry one, so it is not evidence about this
    action in either direction: it is neither counted as a failure nor read as
    proof that none occurred, and it does not freeze an unrelated action. The
    known malformed events predate this gate and could not have carried a typed
    identity anyway.

    Where uncertainty is genuinely about this action, it refuses rather than
    resolving downward: a parsed matching terminal with no explicit outcome is
    an ambiguity refusal, because guessing whether it was a failure is the thing
    this exists to remove. Only an explicit success reduces the count.
    """
    records, _defects = load_events(topology.root, topology.event_dirs)
    failures = []
    for record in records:
        body = record.body
        if not isinstance(body, dict):
            continue
        descriptor = _action_descriptor(body)
        if descriptor is None:
            # Historical records predate the typed descriptor and are evidence
            # in neither direction.
            continue
        if _derived_action_identity(descriptor) != identity:
            continue
        if not _terminal_label_matches(body, descriptor):
            raise PublishError(
                "a matching terminal's action_identity does not match its descriptor")
        outcome = body.get("outcome")
        if outcome == "succeeded":
            continue
        if outcome == "failed":
            failures.append(record)
            continue
        raise PublishError(
            "a matching terminal has no explicit failed or succeeded outcome")
    return failures


def _action_descriptor(scope: dict):
    """The three fields that say what an action *is*.

    Not the task. A task is a name someone chose, and keying on it means the
    same provider running the same operation over the same packet becomes a
    brand new action the moment the task is renamed -- which is exactly the
    bypass this has to refuse.
    """
    if not isinstance(scope, dict):
        return None
    provider = scope.get("provider")
    packet = scope.get("packet_sha256")
    operation = scope.get("operation")
    if not isinstance(provider, str) or not provider.strip():
        return None
    if not isinstance(packet, str) or len(packet) != 64:
        return None
    if any(c not in "0123456789abcdef" for c in packet):
        return None
    if not isinstance(operation, str) or operation != operation.strip() or not operation:
        return None
    return {"provider": provider, "packet_sha256": packet, "operation": operation}


ACTION_IDENTITY_VERSION = "v1"


def _derived_action_identity(descriptor: dict) -> str:
    """One canonical name for an authorized action, as a versioned digest.

    Not a delimiter join: a provider or operation may legitimately contain the
    delimiter, and then two different actions encode to the same string. A
    digest over canonical JSON has no such seam, and the version prefix leaves
    room to change the encoding later without silently re-identifying history.
    """
    canonical = json.dumps(descriptor, sort_keys=True, separators=(",", ":"))
    return ACTION_IDENTITY_VERSION + ":" + hashlib.sha256(
        canonical.encode("utf-8")).hexdigest()


def _terminal_label_matches(body: dict, descriptor: dict) -> bool:
    """A post-activation terminal must carry the identity its descriptor implies.

    A record that names a descriptor but labels itself as some other action is
    not evidence about either one, so it is refused rather than counted.
    """
    return body.get("action_identity") == _derived_action_identity(descriptor)



def _granted_action_identity(event: dict) -> str:
    grant = event.get("live_authorization")
    descriptor = _action_descriptor(grant) if isinstance(grant, dict) else None
    if descriptor is None:
        descriptor = _action_descriptor(event)
    if descriptor is None:
        raise PublishError(
            "a positive authority requires provider, packet_sha256 and operation")
    return _derived_action_identity(descriptor)



def _validate_typed_terminal(event: dict) -> None:
    """A typed terminal must be well formed before it is installed, not after.

    The reader already refuses a terminal whose label contradicts its
    descriptor, but by then the bad record is in the ledger and the cost lands
    on whoever next tries to derive an authority. Refusing here keeps it out.

    Only post-activation records are in scope: an event with no explicit
    outcome predates this and is left alone rather than retroactively
    invalidated.
    """
    outcome = event.get("outcome")
    if outcome not in TERMINAL_OUTCOMES:
        return
    descriptor = _action_descriptor(event)
    if descriptor is None:
        raise PublishError(
            "a typed terminal requires provider, packet_sha256 and operation")
    if event.get("action_identity") != _derived_action_identity(descriptor):
        raise PublishError(
            "a typed terminal's action_identity does not match its descriptor")



# ---------------------------------------------------------------------------
# Ledger layer contract.
#
# The same six decisive rules as the live standalone publisher, expressed
# against topology instead of constants. Neither implementation imports the
# other: this one reuses its own integrity loader, and the live one carries the
# rules inline because it has no package to import from.


ASSIGNMENT_EVENT_SUFFIXES = ("-assigned", "-assignment")
ASSIGNMENT_REQUEST_KEYS = ("request", "assurance_request")


def _assignment_request_reference(event: dict):
    """The request an assignment issues, or None when it issues none.

    Citing a request is not issuing one: acknowledgements and acceptances carry
    the same reference. And a name ending in the same word does not force an
    assignment to invent a request it never had.
    """
    name = event.get("event")
    if not isinstance(name, str) or not name.endswith(ASSIGNMENT_EVENT_SUFFIXES):
        return None
    for key in ASSIGNMENT_REQUEST_KEYS:
        reference = event.get(key)
        if isinstance(reference, dict):
            return reference
    return None


def _admitted_layer(value: object, where: str, topology: Topology) -> str:
    if value is None:
        raise PublishError(f"{where} layer is required after activation")
    if value not in topology.layer_vocabulary:
        raise PublishError(f"{where} layer is not an admitted value")
    return value


def _observed_parse_defects(topology: Topology) -> dict:
    _records, defects = load_events(topology.root, topology.event_dirs)
    observed = {}
    for defect in defects:
        path = topology.root / defect.path
        try:
            observed[defect.path] = hashlib.sha256(path.read_bytes()).hexdigest()
        except OSError:
            observed[defect.path] = None
    return observed


def _validate_defect_quarantine(topology: Topology) -> None:
    """Concluding absence is a claim about having read the whole ledger.

    An unreadable record has no type, so it cannot be recognised as an
    activation event. Discarding it is how an irreversible activation reverts.
    """
    observed = _observed_parse_defects(topology)
    quarantine = topology.quarantined_parse_defects
    for relative, digest in sorted(observed.items()):
        expected = quarantine.get(relative)
        if expected is None:
            raise PublishError(f"an unquarantined ledger parse defect is present: {relative}")
        if digest != expected:
            raise PublishError(f"a quarantined ledger parse defect has changed: {relative}")
    for relative in sorted(quarantine):
        if relative not in observed:
            raise PublishError(
                f"an admitted quarantine record is absent or now parses: {relative}")


def _layer_bearing_records(records) -> dict:
    """Path -> identity and value. Path, because sequence repeats per writer."""
    found = {}
    for record in records:
        body = record.body if hasattr(record, "body") else record[1]
        relative = record.path if hasattr(record, "path") else record[0]
        if isinstance(body, dict) and "layer" in body:
            found[relative] = {"writer": body.get("writer"),
                               "sequence": body.get("sequence"),
                               "layer": body.get("layer")}
    return found


def _validate_activation_record(body: dict, topology: Topology, records=None,
                                *, admission: bool = False) -> int:
    sequence = body.get("sequence")
    if not isinstance(sequence, int) or isinstance(sequence, bool) or sequence < 1:
        raise PublishError("the layer activation record has no valid sequence")
    if body.get("layer") != topology.layer_activation_value:
        raise PublishError("the layer activation record must carry the activation value")
    if body.get("activation_sequence") != sequence:
        raise PublishError("the layer activation record must state its own sequence")
    vocabulary = body.get("layer_vocabulary")
    if not isinstance(vocabulary, list) or tuple(vocabulary) != topology.layer_vocabulary:
        raise PublishError("the layer activation record must state the exact vocabulary")

    declared = body.get("pre_activation_layer_records")
    if not isinstance(declared, list):
        raise PublishError(
            "the layer activation record must list the pre-activation layer records")
    stated = {}
    for entry in declared:
        if not isinstance(entry, dict) or not isinstance(entry.get("path"), str):
            raise PublishError("a pre-activation layer record entry is malformed")
        stated[entry["path"]] = {"writer": entry.get("writer"),
                                 "sequence": entry.get("sequence"),
                                 "layer": entry.get("layer")}
    if set(stated) != set(topology.pre_activation_layer_records):
        raise PublishError(
            "the layer activation record must list the pre-activation sequences")

    entries = body.get("quarantined_parse_defects")
    if not isinstance(entries, list):
        raise PublishError(
            "the layer activation record must list the quarantined parse defects")
    stated_defects = {}
    for entry in entries:
        if not isinstance(entry, dict) or not isinstance(entry.get("path"), str):
            raise PublishError("a quarantined parse defect entry is malformed")
        stated_defects[entry["path"]] = entry.get("sha256")
    if stated_defects != _observed_parse_defects(topology):
        raise PublishError("the quarantined defect enumeration differs from the ledger")

    # Recomputed once, at admission. Afterwards the ledger legitimately grows:
    # every post-activation event carries a layer, so re-deriving the frozen
    # pre-activation set on each publication would refuse the second one.
    if admission:
        if records is None:
            records, _defects = load_events(topology.root, topology.event_dirs)
        observed_layers = {relative: item for relative, item
                           in _layer_bearing_records(records).items()
                           if item.get("sequence") != sequence}
        if stated != observed_layers:
            raise PublishError("the pre-activation layer enumeration differs from the ledger")
    return sequence


def _activation_epoch(topology: Topology):
    records, _defects = load_events(topology.root, topology.event_dirs)
    _validate_defect_quarantine(topology)
    found = []
    for record in records:
        body = record.body if hasattr(record, "body") else record[1]
        if isinstance(body, dict) and body.get("event") == topology.layer_activation_event:
            found.append(body)
    if len(found) > 1:
        raise PublishError("the ledger carries more than one layer activation record")
    if not found:
        unexplained = sorted(relative for relative in _layer_bearing_records(records)
                             if relative not in topology.pre_activation_layer_records)
        if unexplained:
            raise PublishError("layer-bearing records exist with no layer activation record")
        return None
    return _validate_activation_record(found[0], topology, records=records)


def _validate_layer_contract(event: dict, pointer: dict, topology: Topology) -> None:
    """Enforced on the record's existence, never on a sequence comparison.

    Each writer carries its own counter, so comparing a new event's sequence
    against the activation sequence would leave one writer permanently
    pre-activation and another permanently post.
    """
    if not topology.layer_contract_enabled:
        return
    epoch = _activation_epoch(topology)
    if event.get("event") == topology.layer_activation_event:
        if epoch is not None:
            raise PublishError("the ledger layer contract is already activated")
        _validate_activation_record(event, topology, admission=True)
        if pointer.get("layer") != topology.layer_activation_value:
            raise PublishError("the layer activation pointer must carry the activation value")
        return
    if epoch is None:
        if "layer" in event or "layer" in pointer:
            raise PublishError("layer is premature before the ledger layer activation")
        return
    layer = _admitted_layer(event.get("layer"), "event", topology)
    if _admitted_layer(pointer.get("layer"), "pointer", topology) != layer:
        raise PublishError("event and pointer layer differ")
    reference = _assignment_request_reference(event)
    if reference is None:
        return
    relative = reference.get("path")
    if not isinstance(relative, str):
        raise PublishError("an assignment requires its immutable request reference")
    target = (topology.root / relative).resolve()
    try:
        target.relative_to(topology.root.resolve())
    except ValueError as exc:
        raise PublishError("the referenced request is outside the bridge root") from exc
    try:
        body = yaml.safe_load(target.read_bytes())
    except (OSError, yaml.YAMLError) as exc:
        raise PublishError("the referenced assignment request is invalid") from exc
    if not isinstance(body, dict):
        raise PublishError("the referenced assignment request is not an object")
    if _admitted_layer(body.get("layer"), "request", topology) != layer:
        raise PublishError("the referenced request layer differs from the event layer")


@contextmanager
def _layer_contract_lock(topology: Topology):
    """One lock shared by every route, taken before any per-route pointer lock.

    Route locks alone cannot serialize this: an agent can read pre-activation on
    its own route while the orchestrator installs activation on another. The
    order is fixed -- contract lock first, pointer lock second -- so nesting
    cannot deadlock against the existing per-route monotonicity.
    """
    if not topology.layer_contract_enabled:
        yield
        return
    lock_path = topology.root / f".{Path(topology.layer_contract_lock).name}.lock"
    if not lock_path.parent.is_dir():
        raise PublishError("bridge root is absent")
    flags = os.O_RDWR | os.O_CREAT
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    fd = os.open(lock_path, flags, 0o600)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX)
        yield
    finally:
        fcntl.flock(fd, fcntl.LOCK_UN)
        os.close(fd)


def _validate_failure_loop_admission(event: dict, topology: Topology) -> None:
    """Cap an action after two derived failures. Not an envelope on every grant.

    A first or second authority carries nothing extra: requiring an admission
    there would tax every grant for a condition that has not occurred. The cap
    engages only once the ledger itself shows two matching failures, and the
    identity it looks up is derived from the granted task and provider before
    any history is read, so the caller cannot choose which history applies.
    """
    if not _positive_authority(event):
        return
    identity = _granted_action_identity(event)
    derived = _derive_matching_failures(identity, topology)
    if len(derived) < 2:
        return

    admission = event.get("failure_loop_admission")
    if not isinstance(admission, dict):
        raise PublishError(
            "a third authority for one action identity requires failure_loop_admission")
    # Refused rather than ignored, so a caller cannot believe it was honoured.
    if "terminal_failure_count" in admission:
        raise PublishError(
            "terminal_failure_count is derived from the ledger and must not be declared")
    declared_identity = admission.get("action_identity")
    if declared_identity is not None and declared_identity != identity:
        raise PublishError(
            "failure_loop_admission action_identity differs from the derived action")

    predecessors = admission.get("terminal_predecessors", [])
    if not isinstance(predecessors, list):
        raise PublishError("failure_loop_admission terminal_predecessors must be a list")
    for index, reference in enumerate(predecessors):
        _validate_one_event_reference(
            f"failure_loop_admission.terminal_predecessors[{index}]", reference,
            topology)
    declared = {r.get("path") for r in predecessors if isinstance(r, dict)}
    if declared != {record.path for record in derived}:
        raise PublishError(
            "the admission must reference exactly the derived terminal predecessors")

    inventory = admission.get("remediation_inventory")
    if not isinstance(inventory, dict):
        raise PublishError(
            "a third authority for one action identity requires remediation_inventory")
    _validate_confined_reference(
        "failure_loop_admission.remediation_inventory", inventory,
        topology)
    if inventory.get("reference_intent") != "immutable_ref":
        raise PublishError(
            "remediation_inventory reference_intent must be immutable_ref")
    body = (topology.root / inventory["path"]).resolve().read_bytes()
    if hashlib.sha256(body).hexdigest() != inventory["sha256"]:
        raise PublishError("remediation_inventory hash differs")
    try:
        record = json.loads(body)
    except ValueError as exc:
        raise PublishError("remediation_inventory is not valid JSON") from exc
    if not isinstance(record, dict):
        raise PublishError("remediation_inventory is not an object")
    if record.get("action_identity") != identity:
        raise PublishError("remediation_inventory names a different action identity")
    if record.get("decision") not in REMEDIATION_DECISIONS:
        raise PublishError("remediation_inventory decision is not adopt, adapt or build")


def _validate_confined_reference(field: str, reference: object, topology: Topology) -> None:
    """A path plus a 64-hex digest resolving to a regular file inside the root."""
    if not isinstance(reference, dict):
        raise PublishError(f"{field} must be an object")
    path_value = reference.get("path")
    digest = reference.get("sha256")
    if not isinstance(path_value, str) or not path_value:
        raise PublishError(f"{field} requires path")
    if (not isinstance(digest, str) or len(digest) != 64
            or any(c not in "0123456789abcdef" for c in digest)):
        raise PublishError(f"{field} sha256 is invalid")
    relative = Path(path_value)
    if relative.is_absolute():
        raise PublishError(f"{field} path must be relative")
    target = topology.root / relative
    if target.is_symlink():
        raise PublishError(f"{field} must not be a symlink")
    # Root-relative is not confined: "../x" is relative and still leaves the tree.
    resolved = target.resolve()
    root = topology.root.resolve()
    if resolved != root and root not in resolved.parents:
        raise PublishError(f"{field} resolves outside the coordination root")
    if not resolved.is_file():
        raise PublishError(f"{field} is absent or not a regular file")


def _validate_one_event_reference(field: str, reference: object, topology: Topology) -> None:
    """Bind authority-bearing nested references before installing the event.

    The publisher already made its outer event/pointer binding atomic, but a
    manually transcribed ``responds_to`` digest could still be wrong inside an
    otherwise valid event. These fields always name append-only events, so they
    can and must be verified at publication time.
    """
    if not isinstance(reference, dict):
        raise PublishError(f"{field} event reference must be an object")
    path_value = reference.get("path")
    expected = reference.get("sha256")
    sequence = reference.get("sequence")
    if not isinstance(path_value, str) or not path_value:
        raise PublishError(f"{field} event reference requires path")
    if (
        not isinstance(expected, str)
        or len(expected) != 64
        or any(character not in "0123456789abcdef" for character in expected)
    ):
        raise PublishError(f"{field} event reference sha256 is invalid")
    if not isinstance(sequence, int) or isinstance(sequence, bool) or sequence < 1:
        raise PublishError(f"{field} event reference sequence is invalid")
    relative = Path(path_value)
    if relative.is_absolute():
        raise PublishError(f"{field} event reference path must be relative")
    target = topology.root / relative
    if target.is_symlink():
        raise PublishError(f"{field} event reference must not be a symlink")
    resolved = target.resolve()
    if resolved.parent not in _admitted_event_dirs(topology):
        raise PublishError(f"{field} event reference is outside admitted event directories")
    try:
        body = resolved.read_bytes()
        parsed = yaml.safe_load(body)
    except (OSError, yaml.YAMLError) as exc:
        raise PublishError(f"{field} referenced event is absent or invalid") from exc
    if hashlib.sha256(body).hexdigest() != expected:
        raise PublishError(f"{field} referenced event hash differs")
    if not isinstance(parsed, dict) or parsed.get("sequence") != sequence:
        raise PublishError(f"{field} referenced event sequence differs")


def _autobind_one_event_reference(field: str, reference: object, topology: Topology) -> None:
    """Replace an explicit ``sha256: auto`` with the installed event digest.

    Exact nested hashes should come from bytes, not human transcription. The
    resulting event contains the ordinary 64-character digest; ``auto`` is only
    an input convenience inside the same gated publisher transaction.
    """
    if not isinstance(reference, dict):
        return
    intent = reference.get("reference_intent")
    if intent is None:
        # These fields are structurally limited to append-only events. Install
        # the explicit classification so prospective metrics never have to
        # infer it from a path or from whether the current hash matches.
        reference["reference_intent"] = "immutable_ref"
    elif intent != "immutable_ref":
        raise PublishError(f"{field} event reference must be immutable_ref")
    if reference.get("sha256") != "auto":
        return
    path_value = reference.get("path")
    if not isinstance(path_value, str) or not path_value:
        raise PublishError(f"{field} auto-bound reference requires path")
    relative = Path(path_value)
    if relative.is_absolute():
        raise PublishError(f"{field} auto-bound reference path must be relative")
    target = topology.root / relative
    if target.is_symlink():
        raise PublishError(f"{field} auto-bound reference must not be a symlink")
    resolved = target.resolve()
    if resolved.parent not in _admitted_event_dirs(topology):
        raise PublishError(f"{field} auto-bound reference is outside admitted event directories")
    try:
        body = resolved.read_bytes()
    except OSError as exc:
        raise PublishError(f"{field} auto-bound event is absent") from exc
    reference["sha256"] = hashlib.sha256(body).hexdigest()


def _event_reference_values(value: object):
    """Yield reserved authority-reference fields at every event-tree depth."""
    if isinstance(value, dict):
        for field, child in value.items():
            if field in EVENT_REFERENCE_FIELDS:
                yield field, child
            yield from _event_reference_values(child)
    elif isinstance(value, list):
        for child in value:
            yield from _event_reference_values(child)


def _autobind_event_references(event: dict, topology: Topology) -> None:
    for field, value in _event_reference_values(event):
        references = value if isinstance(value, list) else [value]
        for reference in references:
            _autobind_one_event_reference(field, reference, topology)


def _validate_event_references(event: dict, topology: Topology) -> None:
    """Validate every direct authority-bearing event reference or reference list."""
    for field, value in _event_reference_values(event):
        references = value if isinstance(value, list) else [value]
        if not references:
            raise PublishError(f"{field} event reference list must not be empty")
        for reference in references:
            _validate_one_event_reference(field, reference, topology)


def _declared_reference_bindings(value: object):
    """Yield every explicitly typed path/hash binding in an event tree."""
    if isinstance(value, dict):
        if "reference_intent" in value:
            yield value
        for child in value.values():
            yield from _declared_reference_bindings(child)
    elif isinstance(value, list):
        for child in value:
            yield from _declared_reference_bindings(child)


def _reference_target(reference: dict, topology: Topology) -> Path:
    intent = reference.get("reference_intent")
    if intent not in topology.reference_intents:
        raise PublishError("declared reference_intent is invalid")
    path_value = reference.get("path")
    if not isinstance(path_value, str) or not path_value:
        raise PublishError("declared reference requires path")
    relative = Path(path_value)
    if relative.is_absolute():
        raise PublishError("declared reference path must be relative")
    target = topology.root / relative
    if target.is_symlink():
        raise PublishError("declared reference must not be a symlink")
    resolved = target.resolve()
    try:
        resolved.relative_to(topology.root)
    except ValueError as exc:
        raise PublishError("declared reference is outside the coordination root") from exc
    if not resolved.is_file():
        raise PublishError("declared reference target is absent or not a regular file")
    return resolved


def _autobind_declared_references(event: dict, topology: Topology) -> None:
    """Resolve ``sha256: auto`` for any explicitly typed file reference."""
    for reference in _declared_reference_bindings(event):
        if reference.get("sha256") != "auto":
            continue
        target = _reference_target(reference, topology)
        reference["sha256"] = hashlib.sha256(target.read_bytes()).hexdigest()


def _validate_declared_references(event: dict, topology: Topology) -> None:
    """Verify every immutable or publication-snapshot binding before install."""
    for reference in _declared_reference_bindings(event):
        target = _reference_target(reference, topology)
        expected = reference.get("sha256")
        if (
            not isinstance(expected, str)
            or len(expected) != 64
            or any(character not in "0123456789abcdef" for character in expected)
        ):
            raise PublishError("declared reference sha256 is invalid")
        if hashlib.sha256(target.read_bytes()).hexdigest() != expected:
            raise PublishError("declared reference hash differs")
        if reference.get("validate_declared_contents") is True:
            try:
                nested = yaml.safe_load(target.read_bytes())
            except yaml.YAMLError as exc:
                raise PublishError("declared authority index is invalid YAML") from exc
            if not isinstance(nested, (dict, list)):
                raise PublishError("declared authority index is not structured")
            _validate_declared_references(nested, topology)


def _write_fsynced(path: Path, body: bytes) -> None:
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    fd = os.open(path, flags, 0o600)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(body)
            handle.flush()
            os.fsync(handle.fileno())
    except BaseException:
        path.unlink(missing_ok=True)
        raise


def _fsync_directory(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _validated_inputs(spec: dict, topology: Topology) -> tuple[dict, dict, Path, Path, str]:
    try:
        pointer_rel = spec["pointer_path"]
        filename = spec["event_filename"]
        event = spec["event"]
        pointer = spec["pointer"]
    except (KeyError, TypeError) as exc:
        raise PublishError(
            "spec requires pointer_path, event_filename, event, and pointer"
        ) from exc

    routes = topology.routes
    if pointer_rel not in routes:
        raise PublishError("pointer path is not an admitted bridge route")
    event_dir_rel, writer_prefix = routes[pointer_rel]
    event_dir = topology.root / event_dir_rel
    if (
        not isinstance(filename, str)
        or Path(filename).name != filename
        or not filename.endswith(".yaml")
    ):
        raise PublishError("event filename must be one YAML basename")
    if not isinstance(event, dict) or not isinstance(pointer, dict):
        raise PublishError("event and pointer must be objects")

    sequence = event.get("sequence")
    if not isinstance(sequence, int) or isinstance(sequence, bool) or sequence < 1:
        raise PublishError("event sequence must be a positive integer")
    if f"-{sequence}-" not in filename:
        raise PublishError("event filename must contain its exact sequence")
    existing_sequence = list(
        event_dir.glob(f"*-{sequence}-{writer_prefix}*.yaml")
    )
    if existing_sequence:
        raise PublishError("writer sequence already exists in append-only history")
    if event.get("event") != pointer.get("event") or sequence != pointer.get("sequence"):
        raise PublishError("event and pointer identity differ")
    _validate_pause_scope(event)
    _validate_decision_contract(event)
    _validate_task_assignment_credentials(event)
    _validate_execution_grant(event)
    _validate_declared_references(event, topology)
    _validate_event_references(event, topology)
    _validate_typed_terminal(event)
    _validate_failure_loop_admission(event, topology)
    _validate_layer_contract(event, pointer, topology)
    for value in (event.get("writer"), pointer.get("writer")):
        if not isinstance(value, str) or not value.startswith(writer_prefix):
            raise PublishError("writer does not own this bridge route")
    if "authoritative_event" in pointer:
        raise PublishError("publisher, not caller, creates authoritative_event")

    pointer_path = topology.root / pointer_rel
    if not event_dir.is_dir() or not pointer_path.parent.is_dir():
        raise PublishError("bridge route directory is absent")
    if pointer_path.exists():
        current = yaml.safe_load(pointer_path.read_bytes())
        current_sequence = current.get("sequence") if isinstance(current, dict) else None
        if not isinstance(current_sequence, int) or sequence <= current_sequence:
            raise PublishError("sequence is not greater than the current pointer")

    return event, pointer, event_dir / filename, pointer_path, f"{event_dir_rel}/{filename}"


def publish(
    spec: dict,
    *,
    topology: Topology,
    failpoint: str | None = None,
    after_sequence_check=None,
    now: dt.datetime | None = None,
) -> dict:
    """Install one event then atomically bind the pointer to its exact digest."""
    spec = copy.deepcopy(spec)
    event_for_binding = spec.get("event") if isinstance(spec, dict) else None
    if not isinstance(event_for_binding, dict):
        raise PublishError("spec requires event")
    _autobind_declared_references(event_for_binding, topology)
    _autobind_event_references(event_for_binding, topology)
    spec = _stamp_created_at(spec, now=now)
    try:
        pointer_rel = spec["pointer_path"]
    except (KeyError, TypeError) as exc:
        raise PublishError("spec requires pointer_path") from exc
    if pointer_rel not in topology.routes:
        raise PublishError("pointer path is not an admitted bridge route")
    pointer_path = topology.root / pointer_rel
    # Orchestrator routes share one sequence space and therefore one explicitly
    # declared lock. A derived target would move when a role is added.
    lock_target = (
        topology.root / topology.global_sequence_lock
        if pointer_rel in topology.orchestrator_routes
        else pointer_path
    )

    # The lock covers both the monotonicity check and replace. Without it, two
    # valid concurrent publications can both validate against the same old
    # pointer and the lower sequence can replace the higher sequence last.
    #
    # The contract lock is shared by every route and is always taken first, so
    # an agent cannot read pre-activation on its own route while the
    # orchestrator installs the activation record on another.
    with _layer_contract_lock(topology), _pointer_lock(lock_target):
        _validate_agent_sender(pointer_rel, topology)
        _validate_global_orchestrator_sequence(pointer_rel, spec.get("event", {}), topology)
        event, pointer, event_path, pointer_path, event_rel = _validated_inputs(spec, topology)
        if after_sequence_check is not None:
            after_sequence_check()
        event_body = _dump(event)

        event_tmp = event_path.parent / f".{event_path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp"
        _write_fsynced(event_tmp, event_body)
        try:
            # A hard link installs the fully written event atomically and
            # refuses overwrite if another publisher already claimed the name.
            os.link(event_tmp, event_path, follow_symlinks=False)
        except FileExistsError as exc:
            raise PublishError("append-only event already exists") from exc
        finally:
            event_tmp.unlink(missing_ok=True)
        _fsync_directory(event_path.parent)

        placed = event_path.read_bytes()
        if yaml.safe_load(placed) != event:
            raise PublishError("installed event failed read-back validation")
        digest = hashlib.sha256(placed).hexdigest()
        if failpoint == "after_event":
            raise PublishError("injected failure after event")

        final_pointer = dict(pointer)
        final_pointer["authoritative_event"] = {"path": event_rel, "sha256": digest}
        pointer_body = _dump(final_pointer)
        reparsed = yaml.safe_load(pointer_body)
        reference = reparsed.get("authoritative_event", {})
        if reference.get("path") != event_rel or reference.get("sha256") != digest:
            raise PublishError("pointer reference validation failed")
        if (
            not event_path.is_file()
            or hashlib.sha256(event_path.read_bytes()).hexdigest() != digest
        ):
            raise PublishError("event changed before pointer publication")

        pointer_tmp = (
            pointer_path.parent / f".{pointer_path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp"
        )
        _write_fsynced(pointer_tmp, pointer_body)
        try:
            if failpoint == "before_pointer_replace":
                raise PublishError("injected failure before pointer replacement")
            os.replace(pointer_tmp, pointer_path)
            _fsync_directory(pointer_path.parent)
        finally:
            pointer_tmp.unlink(missing_ok=True)

        return {"event_path": event_rel, "event_sha256": digest}


def main(argv: list[str] | None = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(description="Publish one validated bridge event.")
    parser.add_argument("spec", type=Path, help="JSON specification file")
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument("--topology", default=None)
    args = parser.parse_args(argv)
    try:
        relative = args.topology or "coordination/topology.yaml"
        topology = Topology.load(args.root, relative)
        with open(args.spec, "r", encoding="utf-8") as handle:
            result = publish(json.load(handle), topology=topology)
    except (OSError, ValueError, yaml.YAMLError, PublishError, TopologyError) as exc:
        print(f"REFUSED: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
