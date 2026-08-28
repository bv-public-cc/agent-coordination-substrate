#!/usr/bin/env python3
"""Orient: does the effort's premise still hold, and has a trigger fired?

The vocabulary-independent distillation of the source project's ``orient_gate``
tool. It answers the execution-phase question that assessment does not — not
"is the work good?" but "do the assumptions the whole effort rests on still
hold, and has a pre-committed decision trigger (a CCIR / kill-criterion) fired?"

Doctrine (OODA-Orient, RDSP, JP 5-0 assumption-validation and MOP/MOE
branch-and-sequel triggers): during execution a force must continuously
re-check whether the world the plan assumed still exists, and re-decide when it
does not. Encoded as prose, that check decays and the effort drifts past its own
kill-criteria on momentum. Encoded here, it fires.

Fail-closed. The verdict is never ``CONTINUE`` by default; severity orders the
lattice ``CONTINUE < HOLD-VALIDATE < REFRAME < SEQUEL < ABANDON`` and the most
severe active condition wins:

- an empty subject is ``NOT_ESTABLISHED_VACUOUS`` (HOLD-VALIDATE) — you cannot
  orient over a scope that declares nothing;
- an unvalidated or stale assumption HOLDS the effort (HOLD-VALIDATE);
- a failed assumption or a tripped CCIR emits its declared decision;
- MOP-passing while a MOE is undemonstrated past its window is a divergence that
  routes to a decision instrument (REFRAME).

This module decides; it does not read a clock or a CLI. Pass ``now`` explicitly
so the verdict is a pure function of (registry, scope, now).
"""
from __future__ import annotations

from dataclasses import dataclass, field
import datetime as _dt
from typing import Any, Optional

DECISIONS = ("CONTINUE", "HOLD-VALIDATE", "REFRAME", "SEQUEL", "ABANDON")
_SEVERITY = {d: i for i, d in enumerate(DECISIONS)}


@dataclass
class OrientVerdict:
    decision: str
    allow: bool
    scope: str
    findings: list = field(default_factory=list)
    problems: list = field(default_factory=list)
    reason: Optional[str] = None
    counts: dict = field(default_factory=dict)


def _parse_iso(s: Optional[str]) -> Optional[_dt.datetime]:
    if not s:
        return None
    try:
        d = _dt.datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        return None
    return d if d.tzinfo else d.replace(tzinfo=_dt.timezone.utc)


def _worse(a: str, b: str) -> str:
    return a if _SEVERITY[a] >= _SEVERITY[b] else b


def decide(registry: dict, scope: Optional[str], now: _dt.datetime) -> OrientVerdict:
    """Return the typed orient verdict. ``allow`` is True only for CONTINUE."""
    verticals = set(registry.get("verticals", {}))
    if scope is not None and scope not in verticals:
        return OrientVerdict("HOLD-VALIDATE", False, scope or "ALL",
                             problems=[f"UNKNOWN_VERTICAL: {scope!r}"])

    def keep(row: dict) -> bool:
        return scope is None or row.get("vertical") == scope

    assumptions = [a for a in registry.get("assumptions", []) if keep(a)]
    ccirs = [c for c in registry.get("ccirs", []) if keep(c)]
    moes = [m for m in registry.get("moe", []) if keep(m)]

    if not assumptions and not ccirs and not moes:
        return OrientVerdict("HOLD-VALIDATE", False, scope or "ALL",
                             reason="NOT_ESTABLISHED_VACUOUS",
                             problems=["NOT_ESTABLISHED_VACUOUS: empty subject"])

    problems: list = []
    findings: list = []
    decision = "CONTINUE"

    for c in ccirs:
        on_trip = c.get("on_trip", "REFRAME")
        if on_trip not in _SEVERITY:
            problems.append(f"MALFORMED_CCIR: {c.get('id')} on_trip={on_trip!r}")
            decision = _worse(decision, "HOLD-VALIDATE")
            continue
        if c.get("tripped"):
            if not c.get("evidence"):
                problems.append(f"CCIR_WITHOUT_EVIDENCE: {c.get('id')}")
            findings.append({"kind": "CCIR_TRIPPED", "id": c.get("id"),
                             "decision": on_trip, "evidence": c.get("evidence")})
            decision = _worse(decision, on_trip)

    for a in assumptions:
        st = a.get("status")
        if st not in ("holds", "failed", "unvalidated"):
            problems.append(f"MALFORMED_ASSUMPTION: {a.get('id')} status={st!r}")
            decision = _worse(decision, "HOLD-VALIDATE")
            continue
        if st == "failed":
            on_fail = a.get("on_fail", "REFRAME")
            on_fail = on_fail if on_fail in _SEVERITY else "REFRAME"
            findings.append({"kind": "ASSUMPTION_FAILED", "id": a.get("id"),
                             "decision": on_fail, "evidence": a.get("evidence")})
            decision = _worse(decision, on_fail)
            continue
        lv = _parse_iso(a.get("last_validated"))
        max_age = a.get("max_age_seconds")
        stale = st == "unvalidated" or lv is None or (
            max_age is not None and (now - lv).total_seconds() > max_age)
        if stale:
            findings.append({"kind": "ASSUMPTION_UNVALIDATED", "id": a.get("id"),
                             "validation": a.get("validation")})
            decision = _worse(decision, "HOLD-VALIDATE")

    for m in moes:
        if m.get("demonstrated"):
            continue
        opened = _parse_iso(m.get("window_opened"))
        matured = opened is None or m.get("window_seconds") is None or (
            (now - opened).total_seconds() > m["window_seconds"])
        if matured:
            findings.append({"kind": "MOE_UNDEMONSTRATED", "id": m.get("id"),
                             "route": "assess"})
            decision = _worse(decision, "REFRAME")

    if problems:
        decision = _worse(decision, "HOLD-VALIDATE")

    return OrientVerdict(decision, decision == "CONTINUE" and not problems,
                         scope or "ALL", findings=findings, problems=problems,
                         counts={"assumptions": len(assumptions),
                                 "ccirs": len(ccirs), "moe": len(moes)})
