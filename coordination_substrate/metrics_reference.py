#!/usr/bin/env python3
"""P2-G004: reference-intent successor to the accepted P2-G003 calculator.

Extends the accepted predecessor rather than replacing it. Everything P2-G003
established -- fail-closed parsing, absence-is-not-zero, volume-is-not-progress,
one calculation two renderings, the time-authority epoch, verified rebound
reconciliation -- is carried forward unchanged.

What is new is EXPLICIT REFERENCE INTENT:

  immutable_ref            content-addressed authority: event files, frozen
                           candidates, committed artifacts. Recomputed, and
                           FAILS CLOSED on missing, malformed, out-of-root,
                           changed, or unknown classification.

  snapshot_at_publication  mutable-by-design: drafts, governance documents,
                           ledgers, leases. Recomputed and fully reported with
                           origin, expected, observed, classification and
                           reason -- but drift NEVER raises an integrity alarm.

The classifier is decided by declared intent and path form ONLY. It is defined
before any digest is computed and it never receives a match outcome. A
classifier that can see whether the hash matched can always find a benign
reading of a mismatch, which would let real corruption classify itself away.

Unknown or absent intent fails closed. Absence of a classification is not a
benign classification.


Design rules that are deliberate and load-bearing:

  * FAIL CLOSED. Duplicate YAML keys, malformed timestamps, duplicate
    (writer, sequence) identities, and hash-bound records whose target does not
    match are DEFECTS, reported as such. They are never silently skipped, because
    a metric computed over a silently truncated ledger reads as complete.
  * ABSENCE IS NOT ZERO. Every metric carries an availability state:
    measured | derived | unavailable | assumption-backed. A metric with no
    supporting records is reported unavailable, never 0.
  * VOLUME IS NOT PROGRESS. Event and revision counts are emitted only under
    `diagnostic_signals` and are never combined into any progress figure.
  * ONE CALCULATION, TWO RENDERINGS. JSON and Markdown are produced from the
    same result object so the narrative cannot drift from the numbers.

DEPENDENCY (declared accurately, C1): this tool requires **PyYAML**. It is not
standard-library-only; an earlier version of this docstring claimed that and the
claim was false. PyYAML is imported through a guarded import that fails with an
explicit, actionable message rather than a bare ImportError traceback.

Otherwise read-only. No network, model, or service access.
"""

from __future__ import annotations

import argparse
import datetime as _dt
import glob
import hashlib
import json
import os
import sys
from statistics import median

try:
    import yaml
except ImportError as _exc:  # C1: declare and fail clearly, never pretend
    raise SystemExit(
        "throughput_metrics requires PyYAML, which is NOT part of the standard "
        "library. Install it (python3 -m pip install PyYAML) or run this tool "
        "with an interpreter that provides it. This dependency is declared "
        "rather than silently assumed."
    ) from _exc


SCHEMA_VERSION = 1

# --- Time-authority epoch (C7, Boss sequence 273) -------------------------
# Before this instant, bridge `created_at` values were manually authored and the
# publisher did not bind them to the host clock. A bounded Boss audit found 123
# of 715 timestamps outside filesystem install time by more than 60 seconds, 96
# of them future-dated, with a maximum future delta of 3074 seconds. A duration
# computed across such endpoints is not a measurement.
#
# Boss event 272 established publisher clock binding with 60-second skew
# refusal. Its own timestamp is the epoch boundary. A duration is reported
# `measured` ONLY when BOTH endpoints are at or after this instant; otherwise it
# is `unavailable`. Filesystem mtime is never substituted as an event timestamp.
TIME_AUTHORITY_EPOCH = _dt.datetime(2026, 8, 1, 3, 38, 48,
                                    tzinfo=_dt.timezone.utc)
TIME_AUTHORITY_EPOCH_EVENT = 272


def is_post_epoch(moment):
    """True when a timestamp carries publisher-bound time authority."""
    return moment is not None and moment >= TIME_AUTHORITY_EPOCH
MEASURED = "measured"
DERIVED = "derived"
UNAVAILABLE = "unavailable"
ASSUMED = "assumption-backed"


class LedgerDefect(Exception):
    """A ledger integrity problem that must surface rather than be absorbed."""


class _StrictLoader(yaml.SafeLoader):
    """SafeLoader that refuses duplicate mapping keys.

    PyYAML silently keeps the last duplicate. In an append-only audit ledger a
    duplicated key means one of the two values is invisible, so the record can
    assert one thing and be read as another.
    """


def _no_duplicate_keys(loader, node, deep=False):
    mapping = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=deep)
        if key in mapping:
            raise LedgerDefect(f"duplicate key {key!r}")
        mapping[key] = loader.construct_object(value_node, deep=deep)
    return mapping


_StrictLoader.add_constructor(
    yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, _no_duplicate_keys
)


def parse_timestamp(value, where):
    """Parse a UTC bridge timestamp, failing closed on anything ambiguous."""
    if value is None:
        return None
    if isinstance(value, _dt.datetime):
        # C4: a timezone-naive YAML datetime is AMBIGUOUS. Silently calling it
        # UTC is an assumption wearing a measurement's clothes, and it
        # contradicts this parser's fail-closed contract.
        if value.tzinfo is None:
            raise LedgerDefect(
                f"{where}: timezone-naive datetime {value!r}; UTC is not assumed")
        return value
    if not isinstance(value, str):
        raise LedgerDefect(f"{where}: timestamp is {type(value).__name__}, not a string")
    text = value.strip()
    for fmt in ("%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S.%fZ"):
        try:
            return _dt.datetime.strptime(text, fmt).replace(tzinfo=_dt.timezone.utc)
        except ValueError:
            continue
    raise LedgerDefect(f"{where}: malformed timestamp {text!r}")


def load_events(root, event_dirs):
    """Load every event, returning (records, defects). Never raises for one bad file."""
    records, defects = [], []
    paths = []
    for rel in event_dirs:
        paths.extend(sorted(glob.glob(os.path.join(root, rel, "*.yaml"))))
    for path in paths:
        rel = os.path.relpath(path, root)
        try:
            with open(path, "r", encoding="utf-8") as handle:
                data = yaml.load(handle, Loader=_StrictLoader)
        except LedgerDefect as exc:
            defects.append({"kind": "duplicate-key", "path": rel, "detail": str(exc)})
            continue
        except yaml.YAMLError as exc:
            defects.append({"kind": "unparseable", "path": rel,
                            "detail": type(exc).__name__})
            continue
        except OSError as exc:
            defects.append({"kind": "unreadable", "path": rel,
                            "detail": type(exc).__name__})
            continue
        if not isinstance(data, dict):
            defects.append({"kind": "not-a-mapping", "path": rel, "detail": ""})
            continue
        try:
            created = parse_timestamp(data.get("created_at"), rel)
        except LedgerDefect as exc:
            defects.append({"kind": "malformed-timestamp", "path": rel,
                            "detail": str(exc)})
            continue
        records.append({"path": rel, "data": data, "created_at": created,
                        "writer": data.get("writer"), "sequence": data.get("sequence"),
                        "event": data.get("event"), "task_id": data.get("task_id"),
                        "attempt": data.get("attempt")})
    return records, defects


def check_identities(records):
    """Duplicate (writer, sequence) pairs. Ambiguous provenance, reported not merged."""
    seen, dups = {}, []
    for rec in records:
        if rec["sequence"] is None or rec["writer"] is None:
            continue
        key = (rec["writer"], rec["sequence"])
        seen.setdefault(key, []).append(rec["path"])
    for key, paths in sorted(seen.items(), key=lambda kv: (str(kv[0][0]), kv[0][1])):
        if len(paths) > 1:
            dups.append({"writer": key[0], "sequence": key[1], "paths": sorted(paths)})
    return dups


# --- Reference intent (P2-G004) ------------------------------------------
IMMUTABLE_REF = "immutable_ref"
SNAPSHOT_AT_PUBLICATION = "snapshot_at_publication"
UNKNOWN_INTENT = "unknown"

# Authority-bearing records assert corrective authority over another record's
# hashes. Their references to append-only events are immutable regardless of
# what else they name, so a snapshot classification can never sanitize an
# unauthorized writer or a corrective claim (rogue-writer class, event 371).
AUTHORITY_BEARING_KEYS = (
    "supersedes", "supersedes_draft_hashes_in", "responds_to",
    "authoritative_event", "rebound", "reconciles",
)

# Mutable-by-design path forms. Declared BEFORE any digest is computed and
# never consulted against a match outcome.
# Committed, content-addressed artifacts: published task evidence and results.
# Mutable drafts are excluded by the snapshot markers, which are evaluated first.
COMMITTED_ARTIFACT_MARKERS = (
    "/artifacts/", "/evidence/", "evidence-manifest", "result.yaml",
    ".patch", ".diff", "/decisions/", "/reviews/",
)

SNAPSHOT_PATH_MARKERS = (
    ".draft.", "/drafts/", "-draft", ".agents.md", "current-state.yaml",
    "backlog.yaml", "james-queue.yaml", "BRIDGE-PROTOCOL.md",
    "ASSURANCE-PROTOCOL.md", "PROJECT-CHARTER.md", "/locks/", "lease.yaml",
)


def classify_reference_intent(rel_path, declared_intent=None,
                              authority_bearing=False):
    """Decide reference intent from DECLARED INTENT and PATH FORM only.

    This function deliberately accepts no digest, no expected value, and no
    match outcome. It cannot know whether the reference matched, so it cannot
    be tempted to call a mismatch benign.

    Returns one of IMMUTABLE_REF, SNAPSHOT_AT_PUBLICATION, UNKNOWN_INTENT.
    UNKNOWN_INTENT fails closed downstream; it is not a benign class.
    """
    if declared_intent is not None:
        if declared_intent in (IMMUTABLE_REF, SNAPSHOT_AT_PUBLICATION):
            return declared_intent
        return UNKNOWN_INTENT
    if not isinstance(rel_path, str) or not rel_path:
        return UNKNOWN_INTENT
    # Historical deterministic fallback, documented before evaluation.
    if rel_path.startswith(IMMUTABLE_TARGET_PREFIXES):
        return IMMUTABLE_REF
    # Snapshot form is evaluated BEFORE the authority override. A lease or a
    # draft is mutable by nature; being named by an authority-bearing record
    # does not freeze it, and treating it as authority would manufacture alarms
    # that train readers to ignore the class.
    if any(marker in rel_path for marker in SNAPSHOT_PATH_MARKERS):
        return SNAPSHOT_AT_PUBLICATION
    if authority_bearing:
        # A reference sitting directly under an authority-bearing key asserts
        # or corrects authority. Never sanitized to a snapshot.
        return IMMUTABLE_REF
    if any(marker in rel_path for marker in COMMITTED_ARTIFACT_MARKERS):
        return IMMUTABLE_REF
    return UNKNOWN_INTENT


def escapes_root(rel_path):
    """True when a reference path would resolve outside the ledger root."""
    if not isinstance(rel_path, str) or not rel_path:
        return True
    if rel_path.startswith("/") or rel_path.startswith("~"):
        return True
    parts = rel_path.replace("\\", "/").split("/")
    depth = 0
    for part in parts:
        if part in ("", "."):
            continue
        if part == "..":
            depth -= 1
            if depth < 0:
                return True
        else:
            depth += 1
    return False


IMMUTABLE_TARGET_PREFIXES = (
    "coordination/bridge/events/",
    "coordination/bridge/james-events/",
)


def classify_target(rel_path):
    """Immutable append-only event, or a mutable file, decided by PATH ALONE.

    C5 requires that immutability is never inferred from the target's current
    content -- content is exactly what drift changes, so inferring from it would
    make the classification agree with whatever it found. Append-only event
    directories are structurally immutable; everything else may legitimately
    change after it was hashed.
    """
    return ("immutable"
            if rel_path.startswith(IMMUTABLE_TARGET_PREFIXES) else "mutable")


REBOUND_EVENTS = {"global-sequence-rebound"}


def reconcile_duplicates(root, records, duplicates):
    """Split duplicate identities into reconciled and unresolved (C6).

    A duplicate is reconciled ONLY by a later rebound event that:
      * names exactly one of the duplicate's paths in `supersedes.path`,
      * carries a `supersedes.sha256` matching that file's actual digest, and
      * leaves exactly one retained path in the duplicate set.

    Anything else -- a missing target, a mismatched hash, a path outside the
    duplicate set, or rebounds that between them would retire every path --
    stays UNRESOLVED and keeps failing closed. A rebound is an assertion about
    the ledger, so it is verified rather than believed; accepting one on its own
    say-so would let a bad rebound erase a real ambiguity.
    """
    rebounds = []
    for rec in records:
        if rec["event"] not in REBOUND_EVENTS:
            continue
        superseded = rec["data"].get("supersedes")
        if not isinstance(superseded, dict):
            continue
        target = superseded.get("path")
        digest = superseded.get("sha256")
        if isinstance(target, str) and isinstance(digest, str):
            rebounds.append({"rebound_path": rec["path"], "rebound_sequence":
                             rec["sequence"], "target": target, "sha256": digest})

    reconciled, unresolved = [], []
    for dup in duplicates:
        paths = set(dup["paths"])
        superseded_paths, evidence, rejected = set(), [], []
        for reb in rebounds:
            if reb["target"] not in paths:
                continue
            actual = None
            full = os.path.join(root, reb["target"])
            try:
                with open(full, "rb") as handle:
                    actual = hashlib.sha256(handle.read()).hexdigest()
            except OSError:
                actual = None
            if actual is None:
                rejected.append({"rebound": reb["rebound_path"],
                                 "why": "superseded target absent"})
                continue
            if actual != reb["sha256"]:
                rejected.append({"rebound": reb["rebound_path"],
                                 "why": "superseded hash does not match the file"})
                continue
            superseded_paths.add(reb["target"])
            evidence.append(reb)
        retained = sorted(paths - superseded_paths)
        entry = dict(dup)
        entry["rebound_evidence"] = evidence
        entry["rejected_rebounds"] = rejected
        entry["retained_paths"] = retained
        if evidence and len(retained) == 1 and not rejected:
            entry["resolution"] = "reconciled"
            reconciled.append(entry)
        else:
            if rejected:
                entry["resolution"] = "unresolved: a rebound failed verification"
            elif not evidence:
                entry["resolution"] = "unresolved: no verified rebound"
            elif len(retained) != 1:
                entry["resolution"] = (
                    f"unresolved: rebound would leave {len(retained)} retained paths, "
                    "exactly one is required")
            unresolved.append(entry)
    return reconciled, unresolved


def check_hash_bindings(root, records, limit=None):
    """Recompute every reference, classified by intent BEFORE hashing (P2-G004).

    Returns separate buckets so a low alarm count can never erase a diagnostic
    fact: immutable failures alarm; snapshot drift is fully reported and does
    not; unknown intent, missing targets, malformed digests and path escapes all
    fail closed.
    """
    checked = 0
    immutable_failures, snapshot_drift = [], []
    unknown_references, missing_targets, malformed_bindings = [], [], []
    cache = {}

    def digest(rel):
        if rel not in cache:
            target = os.path.join(root, rel)
            try:
                with open(target, "rb") as handle:
                    cache[rel] = hashlib.sha256(handle.read()).hexdigest()
            except OSError:
                cache[rel] = None
        return cache[rel]

    def walk(node, origin, authority_bearing, key_path=""):
        nonlocal checked
        if isinstance(node, dict):
            # Authority applies to the reference sitting directly under an
            # authority-bearing key, not to every descendant of the record.
            # Propagating it everywhere would mark an entire event's references
            # as authority and bury the distinction this class exists to draw.
            here_authority = authority_bearing
            path_value = node.get("path")
            sha_value = node.get("sha256")
            if sha_value is None:
                sha_value = node.get("sha256_declared")
            declared = node.get("reference_intent")
            if isinstance(path_value, str) and sha_value is not None:
                if limit is None or checked < limit:
                    checked += 1
                    # Intent is decided here, before any comparison exists.
                    intent = classify_reference_intent(
                        path_value, declared, here_authority)
                    record = {"origin": origin, "path": path_value,
                              "expected": sha_value if isinstance(sha_value, str)
                              else f"<non-string {type(sha_value).__name__}>",
                              "reference_intent": intent,
                              "declared_intent": declared,
                              "authority_bearing": here_authority,
                              "key_path": key_path}
                    if not isinstance(sha_value, str):
                        record["observed"] = "MALFORMED-BINDING"
                        record["reason"] = ("digest is not a string; an all-digit "
                                            "sha256 parses as an int and would "
                                            "otherwise vanish")
                        malformed_bindings.append(record)
                    elif escapes_root(path_value):
                        record["observed"] = "PATH-ESCAPE"
                        record["reason"] = "reference resolves outside the ledger root"
                        malformed_bindings.append(record)
                    elif intent == UNKNOWN_INTENT:
                        record["observed"] = digest(path_value) or "ABSENT"
                        record["reason"] = ("intent could not be determined; "
                                            "absence of a classification is not "
                                            "a benign classification")
                        unknown_references.append(record)
                    else:
                        actual = digest(path_value)
                        record["observed"] = actual or "ABSENT"
                        if actual is None:
                            record["reason"] = "referenced target is absent"
                            missing_targets.append(record)
                        elif actual != sha_value:
                            record["reason"] = (
                                "content differs from the recorded digest")
                            if intent == IMMUTABLE_REF:
                                immutable_failures.append(record)
                            else:
                                snapshot_drift.append(record)
            for key, value in node.items():
                walk(value, key in AUTHORITY_BEARING_KEYS,
                     f"{key_path}.{key}" if key_path else str(key)) if False else \
                    walk(value, origin, key in AUTHORITY_BEARING_KEYS,
                         f"{key_path}.{key}" if key_path else str(key))
        elif isinstance(node, list):
            for index, value in enumerate(node):
                walk(value, origin, authority_bearing, f"{key_path}[{index}]")

    for rec in records:
        walk(rec["data"], rec["path"], False)

    return {"checked": checked,
            "immutable_failures": immutable_failures,
            "snapshot_drift": snapshot_drift,
            "unknown_references": unknown_references,
            "missing_targets": missing_targets,
            "malformed_bindings": malformed_bindings}


def metric(state, value=None, unit=None, sources=None, records=0, note=None):
    """Uniform metric envelope. `unavailable` never carries a numeric value."""
    entry = {"state": state, "records": records}
    if state in (MEASURED, DERIVED, ASSUMED):
        entry["value"] = value
        if unit:
            entry["unit"] = unit
    if sources:
        entry["sources"] = sorted(set(sources))
    if note:
        entry["note"] = note
    return entry


def group_attempts(records):
    """Group records by (task_id, attempt). Records lacking a task are excluded."""
    groups = {}
    for rec in records:
        if not rec["task_id"]:
            continue
        groups.setdefault((rec["task_id"], rec["attempt"]), []).append(rec)
    for key in groups:
        groups[key].sort(key=lambda r: (r["created_at"] or _dt.datetime.min.replace(
            tzinfo=_dt.timezone.utc), r["path"]))
    return groups


ASSIGN_EVENTS = {"task-assigned", "assurance-assigned", "assurance-assignment",
                 "assurance-tooling-assigned"}
# C2: Clyde completion is NOT Boss acceptance. .agents.md states that a reported
# `complete` status is not acceptance and that Boss independently verifies. So
# `result-ready` (Clyde-authored completion) is deliberately EXCLUDED, and every
# acceptance event must additionally be authored by Boss.
ACCEPT_EVENTS = {"publish-approved", "failure-accepted-publish-and-release",
                 "task-accepted", "integration-accepted", "publish-approved-read-only"}
ACCEPT_WRITER = "boss"
NON_ACCEPTANCE_COMPLETION_EVENTS = {"result-ready", "publication-candidate",
                                    "corrected-publication-candidate"}
REVISION_EVENTS = {"revision-required"}
GATE_ACK_EVENTS = {"assurance-acknowledged"}
GATE_TERMINAL_EVENTS = {"terminal-gate-finding", "terminal-finding"}


def compute(root, event_dirs, current_state_rel):
    records, defects = load_events(root, event_dirs)
    duplicates = check_identities(records)
    reconciled_dups, unresolved_dups = reconcile_duplicates(root, records, duplicates)
    bindings = check_hash_bindings(root, records)
    groups = group_attempts(records)

    # Known-gap declaration: authority that cannot be reconstructed, only
    # declared. Never a substitute event and never a repair.
    known_gaps = []
    for defect in defects:
        if defect["kind"] in ("unparseable", "duplicate-key", "not-a-mapping"):
            target = os.path.join(root, defect["path"])
            try:
                with open(target, "rb") as handle:
                    byte_hash = hashlib.sha256(handle.read()).hexdigest()
            except OSError:
                byte_hash = None
            known_gaps.append({
                "path": defect["path"],
                "byte_sha256": byte_hash,
                "parser_failure": defect["detail"] or defect["kind"],
                "chronology_basis": ("filename timestamp prefix only; the record "
                                     "body cannot be parsed, so its created_at is "
                                     "unavailable"),
                "impact": ("excluded from every metric and from reference "
                           "recomputation; its assertions are not represented"),
                "limitation": ("no reconstruction is attempted or possible; this "
                               "declares lost authority rather than repairing it"),
                "pre_publisher": True,
            })

    event_counts = {}
    for rec in records:
        event_counts[rec["event"]] = event_counts.get(rec["event"], 0) + 1

    # --- cycle time: assignment -> first acceptance, per attempt -------------
    cycles, revised_attempts, clean_attempts = [], 0, 0
    pre_epoch_cycles, pre_epoch_waits = [], []
    revision_counts = {}
    for (task, attempt), recs in groups.items():
        assigned = next((r for r in recs if r["event"] in ASSIGN_EVENTS), None)
        accepted = next((r for r in recs if r["event"] in ACCEPT_EVENTS
                         and r["writer"] == ACCEPT_WRITER), None)
        revs = [r for r in recs if r["event"] in REVISION_EVENTS]
        if revs:
            revision_counts[f"{task}#{attempt}"] = len(revs)
        if assigned and accepted and assigned["created_at"] and accepted["created_at"]:
            delta = (accepted["created_at"] - assigned["created_at"]).total_seconds()
            if delta >= 0:
                entry = {"task": task, "attempt": attempt, "seconds": delta,
                         "from": assigned["path"], "to": accepted["path"]}
                if is_post_epoch(assigned["created_at"]) and \
                        is_post_epoch(accepted["created_at"]):
                    cycles.append(entry)
                else:
                    pre_epoch_cycles.append(entry)
        if accepted:
            if revs:
                revised_attempts += 1
            else:
                clean_attempts += 1

    # --- assurance gate wait: james ack -> james terminal --------------------
    waits = []
    for (task, attempt), recs in groups.items():
        ack = next((r for r in recs if r["event"] in GATE_ACK_EVENTS
                    and r["writer"] == "james"), None)
        term = next((r for r in recs if r["event"] in GATE_TERMINAL_EVENTS
                     and r["writer"] == "james"), None)
        if ack and term and ack["created_at"] and term["created_at"]:
            delta = (term["created_at"] - ack["created_at"]).total_seconds()
            if delta >= 0:
                entry = {"task": task, "seconds": delta,
                         "from": ack["path"], "to": term["path"]}
                if is_post_epoch(ack["created_at"]) and \
                        is_post_epoch(term["created_at"]):
                    waits.append(entry)
                else:
                    pre_epoch_waits.append(entry)

    result = {
        "schema_version": SCHEMA_VERSION,
        "generator": "throughput_metrics.py",
        "inputs": {"root": root, "event_directories": list(event_dirs),
                   "files_loaded": len(records), "files_rejected": len(defects)},
        "integrity": {
            "ledger_defects": defects,
            "duplicate_writer_sequence": unresolved_dups,
            "reconciled_writer_sequence": reconciled_dups,
            "duplicate_identities_total": len(duplicates),
            "references_checked": bindings["checked"],
            "immutable_failures": bindings["immutable_failures"],
            "snapshot_drift": bindings["snapshot_drift"],
            "unknown_references": bindings["unknown_references"],
            "missing_targets": bindings["missing_targets"],
            "malformed_bindings": bindings["malformed_bindings"],
            "counts": {
                "immutable_failures": len(bindings["immutable_failures"]),
                "snapshot_drift": len(bindings["snapshot_drift"]),
                "unknown_references": len(bindings["unknown_references"]),
                "missing_targets": len(bindings["missing_targets"]),
                "malformed_bindings": len(bindings["malformed_bindings"]),
            },
            # Snapshot drift is deliberately absent from this expression. It is
            # fully reported above and never contributes to the alarm.
            "fail_closed": bool(
                defects or unresolved_dups
                or bindings["immutable_failures"]
                or bindings["unknown_references"]
                or bindings["missing_targets"]
                or bindings["malformed_bindings"]),
            "interpretation": (
                "fail_closed reflects parse defects, unresolved duplicate "
                "writer-sequence identities, immutable_ref failures, unknown "
                "reference intent, missing targets and malformed bindings. "
                "snapshot_at_publication drift is recomputed and fully reported "
                "but never alarms, because a mutable target legitimately changes "
                "after a record hashed it."),
            "known_gaps": known_gaps,
        },
        "time_authority": {
            "epoch_event": TIME_AUTHORITY_EPOCH_EVENT,
            "epoch": TIME_AUTHORITY_EPOCH.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "rule": ("A duration is `measured` only when both endpoints are at or "
                     "after the epoch. Pre-epoch created_at values are not "
                     "sufficient evidence for elapsed time and filesystem mtime is "
                     "never substituted."),
            "pre_epoch_cycle_pairs_excluded": len(pre_epoch_cycles),
            "pre_epoch_gate_wait_pairs_excluded": len(pre_epoch_waits),
            "withdrawn_claims": [
                "P2-G003 initial measured critical-path cycle-time baseline",
                "P2-G003 initial measured assurance-gate wait baseline"],
        },
        "metrics": {},
        "diagnostic_signals": {
            "note": ("Volume only. Never progress, never productivity. Recorded "
                     "because trends are useful for spotting coordination cost."),
            "total_events_loaded": len(records),
            "events_by_type": dict(sorted(event_counts.items(),
                                          key=lambda kv: (-kv[1], str(kv[0])))),
            "revision_counts_by_attempt": dict(sorted(revision_counts.items())),
        },
    }

    m = result["metrics"]
    if cycles:
        values = sorted(c["seconds"] for c in cycles)
        m["critical_path_cycle_time"] = metric(
            MEASURED, {"count": len(values), "median_seconds": median(values),
                       "min_seconds": values[0], "max_seconds": values[-1],
                       "samples": sorted(cycles, key=lambda c: c["seconds"])[:8]},
            "seconds", [c["from"] for c in cycles], len(cycles),
            "Assignment event to first acceptance event within the same task attempt.")
    else:
        m["critical_path_cycle_time"] = metric(
            UNAVAILABLE, records=0,
            note=("No attempt had an assignment/acceptance pair with BOTH endpoints "
                  f"at or after the time-authority epoch (Boss event "
                  f"{TIME_AUTHORITY_EPOCH_EVENT}, "
                  f"{TIME_AUTHORITY_EPOCH.strftime('%Y-%m-%dT%H:%M:%SZ')}). "
                  f"{len(pre_epoch_cycles)} pre-epoch pair(s) exist and are "
                  "deliberately excluded: their created_at values were manually "
                  "authored without publisher clock binding, so a duration across "
                  "them is not a measurement."))

    total_accepted = clean_attempts + revised_attempts
    if total_accepted:
        m["first_pass_acceptance_rate"] = metric(
            DERIVED, {"accepted_attempts": total_accepted,
                      "without_revision": clean_attempts,
                      "with_revision": revised_attempts,
                      "rate": round(clean_attempts / total_accepted, 4)},
            "ratio", records=total_accepted,
            note="An attempt counts as first-pass when it reached acceptance with zero revision-required events.")
    else:
        m["first_pass_acceptance_rate"] = metric(
            UNAVAILABLE, records=0, note="No accepted attempts found.")

    if waits:
        values = sorted(w["seconds"] for w in waits)
        m["assurance_gate_wait"] = metric(
            MEASURED, {"count": len(values), "median_seconds": median(values),
                       "min_seconds": values[0], "max_seconds": values[-1],
                       "samples": waits},
            "seconds", [w["from"] for w in waits], len(waits),
            "James acknowledgement to James terminal finding. Measures gate duration, not queue wait before assignment.")
    else:
        m["assurance_gate_wait"] = metric(
            UNAVAILABLE, records=0,
            note=("No acknowledgement/terminal pair has BOTH endpoints at or after "
                  f"the time-authority epoch (Boss event {TIME_AUTHORITY_EPOCH_EVENT}). "
                  f"{len(pre_epoch_waits)} pre-epoch pair(s) exist and are excluded. "
                  "The previously published n=2 median of 450s was computed across "
                  "James-authored pre-epoch timestamps and is withdrawn."))

    total_revisions = sum(revision_counts.values())
    m["material_revisions"] = metric(
        MEASURED if revision_counts else UNAVAILABLE,
        {"total": total_revisions, "attempts_with_revisions": len(revision_counts)}
        if revision_counts else None,
        "count", records=total_revisions,
        note=("Counted from revision-required events. Material/expands_contract "
              "classification is only present on events published after that "
              "governance field existed, so this total is not split by materiality."))

    m["post_gate_rework"] = metric(
        UNAVAILABLE, records=0,
        note=("No event type distinguishes rework occurring after a protected-event "
              "gate closed. Deriving it from revision timing would be an assumption, "
              "not a measurement, so it is reported unavailable."))
    m["duplicate_work"] = metric(
        UNAVAILABLE, records=0,
        note=("No explicit duplicate-work marker exists in the ledger. Repeated "
              "(task_id, attempt) execution cannot be distinguished from an "
              "authorized continuation without an idempotency-key field on every event."))
    m["listener_and_lease_idle"] = metric(
        UNAVAILABLE, records=0,
        note=("Lease acquire/release timestamps are recorded inside lock directories "
              "that are removed on release, so elapsed idle time is not reconstructable "
              "from the append-only ledger alone."))
    m["mission_predicates_advanced"] = metric(
        UNAVAILABLE, records=0,
        note=("mission_predicates became a required result field only under the "
              "governance active at Boss 229. Earlier accepted results predate it, "
              "so a per-task count would understate history rather than measure it."))
    m["governance_rule_activations"] = metric(
        UNAVAILABLE, records=0,
        note=("Rule activations are not explicitly recorded as such. Counting "
              "revision events as activations would conflate a rule firing with a "
              "reviewer noticing, which is exactly the conflation these metrics exist to avoid."))
    return result


def render_markdown(result):
    lines = ["# Phase-2 throughput and governance baseline", ""]
    inputs = result["inputs"]
    lines += [f"Generated by `{result['generator']}` over `{inputs['root']}`.", "",
              f"- Event files loaded: **{inputs['files_loaded']}**",
              f"- Event files rejected: **{inputs['files_rejected']}**", ""]
    integrity = result["integrity"]
    lines += ["## Ledger integrity", ""]
    if integrity["fail_closed"]:
        lines.append("**The ledger does not read as complete.** Metrics below are computed "
                     "over the records that loaded.")
        lines.append("")
        for defect in integrity["ledger_defects"]:
            lines.append(f"- `{defect['kind']}` — `{defect['path']}` {defect['detail']}")
        for dup in integrity["duplicate_writer_sequence"]:
            lines.append(f"- `duplicate-identity` — writer `{dup['writer']}` sequence "
                         f"`{dup['sequence']}` used by {len(dup['paths'])} events")
            for path in dup["paths"]:
                lines.append(f"    - `{path}`")
        for bad in integrity["immutable_failures"]:
            lines.append(f"- `immutable-failure` — `{bad['path']}` referenced by "
                         f"`{bad['origin']}` ({bad.get('reason','')})")
        for bad in integrity["unknown_references"]:
            lines.append(f"- `unknown-intent` — `{bad['path']}` referenced by `{bad['origin']}`")
        for bad in integrity["missing_targets"]:
            lines.append(f"- `missing-target` — `{bad['path']}` referenced by `{bad['origin']}`")
        for bad in integrity["malformed_bindings"]:
            lines.append(f"- `malformed-binding` — `{bad['path']}` ({bad.get('observed','')})")
    else:
        lines.append("No defects detected.")
    counts = integrity["counts"]
    lines += ["", f"References recomputed: **{integrity['references_checked']}**", "",
              "| class | count | alarms |", "| --- | ---: | :---: |",
              f"| immutable_ref failures | {counts['immutable_failures']} | yes |",
              f"| unknown intent | {counts['unknown_references']} | yes |",
              f"| missing targets | {counts['missing_targets']} | yes |",
              f"| malformed bindings | {counts['malformed_bindings']} | yes |",
              f"| snapshot drift | {counts['snapshot_drift']} | **no** |", "",
              f"Known gaps (unrecoverable pre-publisher records): "
              f"**{len(integrity['known_gaps'])}**", "",
              "## Metrics", ""]
    for name, entry in result["metrics"].items():
        label = name.replace("_", " ")
        state = entry["state"]
        if state == UNAVAILABLE:
            lines.append(f"### {label} — _unavailable_")
            lines.append("")
            lines.append(f"{entry.get('note','')}")
        else:
            lines.append(f"### {label} — _{state}_")
            lines.append("")
            lines.append("```json")
            lines.append(json.dumps(entry.get("value"), indent=2, sort_keys=True, default=str))
            lines.append("```")
            if entry.get("note"):
                lines.append(entry["note"])
            lines.append(f"Records: {entry['records']}")
        lines.append("")
    diag = result["diagnostic_signals"]
    lines += ["## Diagnostic signals (never progress)", "", diag["note"], "",
              f"- Total events loaded: {diag['total_events_loaded']}", ""]
    lines.append("| event | count |")
    lines.append("| --- | ---: |")
    for name, count in list(diag["events_by_type"].items())[:15]:
        lines.append(f"| `{name}` | {count} |")
    lines.append("")
    return "\n".join(lines)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--root", default="/home/rocky/base_directory")
    parser.add_argument("--event-dir", action="append", default=None)
    parser.add_argument("--current-state", default="coordination/current-state.yaml")
    parser.add_argument("--json-out")
    parser.add_argument("--markdown-out")
    args = parser.parse_args(argv)
    event_dirs = args.event_dir or ["coordination/bridge/events",
                                    "coordination/bridge/james-events"]
    result = compute(args.root, event_dirs, args.current_state)
    payload = json.dumps(result, indent=2, sort_keys=True, default=str)
    markdown = render_markdown(result)
    if args.json_out:
        with open(args.json_out, "w", encoding="utf-8") as handle:
            handle.write(payload + "\n")
    if args.markdown_out:
        with open(args.markdown_out, "w", encoding="utf-8") as handle:
            handle.write(markdown + "\n")
    if not args.json_out and not args.markdown_out:
        print(payload)
    return 1 if result["integrity"]["fail_closed"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
