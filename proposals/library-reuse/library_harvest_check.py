#!/usr/bin/env python3
"""Keep the reusable library accurate against the authoritative live tool.

The live tool (coordination/tools/publish_bridge_event.py) is where lessons are
learned; the library (coordination_substrate/publisher.py) is the portable,
generalized distillation reused in other project scopes. This checker mechanizes
"keep accuracy" so it cannot rot into a manual mirroring discipline:

  * every validator in the tool is classified — ported / missing / project-specific;
    a tool validator that matches no class is UNCLASSIFIED and fails the check,
    so a live-learned lesson cannot silently skip the library (guard-the-guard);
  * the library must stay general — a project-specific token (a role literal, the
    layer vocabulary, this run's ledger sequences) appearing in the library fails
    the check (the reuse-integrity guard);
  * `missing` is the harvest backlog, reported but not itself a failure.

Fail-closed exit 1 on an unclassified validator or a generalization leak.
Run `--self-test` for the negative controls that prove each refusal fires.

Stdlib only.
"""
from __future__ import annotations

import ast
import re
import sys
from pathlib import Path

TOOL = Path("/home/rocky/base_directory/coordination/tools/publish_bridge_event.py")
LIB = Path("/srv/coordination-substrate/coordination_substrate/publisher.py")

VALIDATOR = re.compile(r"^(_?validate|check)_")

# Each tool validator, classified from the grounded tool<->library diff.
#   in_sync           — a generalized counterpart exists in the library
#   missing           — a general mechanism to HARVEST (extract invariant, drop project policy)
#   project_specific  — this project's policy; must NOT be baked into the library
CLASSIFICATION = {
    "_validate_agent_sender":               ("in_sync", "library _validate_agent_sender"),
    "_validate_global_orchestration-agent_sequence":       ("in_sync", "library _validate_global_orchestrator_sequence (role generalized)"),
    "_validate_pause_scope":                ("in_sync", "library _validate_pause_scope"),
    "_validate_decision_contract":          ("in_sync", "library _validate_decision_contract"),
    "_validate_task_assignment_credentials":("in_sync", "library _validate_task_assignment_credentials"),
    "_validate_one_bridge_event_reference": ("in_sync", "library _validate_one_event_reference"),
    "_validate_bridge_event_references":    ("in_sync", "library _validate_event_references"),
    "_validate_declared_references":        ("in_sync", "library _validate_declared_references"),

    "_validate_orchestration-agent_completion_estimate":   ("missing", "general completion-estimate shape; the library does not yet enforce it (harvest backlog)"),
    "_validate_assignment_git_objects":     ("project_specific", "assignment-request-file plumbing; the general core is the inline git-object check"),
    "_validate_inline_repository_git_objects":("missing", "general inline git-object binding; not yet in the library (harvest backlog)"),
    "check_execution_grant":                ("in_sync", "library check_execution_grant"),
    "_validate_execution_grant":            ("in_sync", "library _validate_execution_grant"),
    "_validate_typed_terminal":             ("in_sync", "library _validate_typed_terminal"),
    "_validate_failure_loop_admission":     ("in_sync", "library _validate_failure_loop_admission"),
    "_validate_declared_reference_shape":   ("in_sync", "library _reference_target enforces the same shape invariant"),

    "_validate_defect_quarantine":          ("project_specific", "parse-defect quarantine state (this run's remediation)"),
    "_validate_activation_record":          ("project_specific", "layer-activation record (this project's layering)"),
    "_validate_layer_contract":             ("project_specific", "LAYER_VOCABULARY"),
}

# Tokens that must never appear in the general library (reuse-integrity guard).
FORBIDDEN_IN_LIBRARY = [
    (re.compile(r"\borchestration-agent\b", re.I),           "role literal 'orchestration-agent' (library must be role-neutral)"),
    (re.compile(r"platform-build"),           "project layer vocabulary"),
    (re.compile(r"mission-execution"),        "project layer vocabulary"),
    (re.compile(r"PRE_ACTIVATION_LAYER_SEQUENCES"), "this run's ledger sequence policy"),
    (re.compile(r"QUARANTINE_TOPOLOGY"),      "project quarantine state"),
]


def validators(source: str) -> set[str]:
    tree = ast.parse(source)
    return {n.name for n in tree.body
            if isinstance(n, ast.FunctionDef) and VALIDATOR.match(n.name)}


def classify(tool_src: str, lib_src: str):
    tool_v = validators(tool_src)
    report = {"in_sync": [], "missing": [], "project_specific": [], "unclassified": [],
              "generalization_leaks": []}
    for name in sorted(tool_v):
        cls = CLASSIFICATION.get(name)
        if cls is None:
            report["unclassified"].append(name)
        else:
            report[cls[0]].append((name, cls[1]))
    for pattern, why in FORBIDDEN_IN_LIBRARY:
        if pattern.search(lib_src):
            report["generalization_leaks"].append(why)
    return report


def failed(report) -> bool:
    return bool(report["unclassified"] or report["generalization_leaks"])


def render(report) -> str:
    out = []
    out.append(f"HARVEST BACKLOG (port to library, generalized) — {len(report['missing'])}:")
    for n, why in report["missing"]:
        out.append(f"  - {n}: {why}")
    out.append(f"\nPROJECT-SPECIFIC (keep out of library) — {len(report['project_specific'])}:")
    for n, why in report["project_specific"]:
        out.append(f"  - {n}: {why}")
    out.append(f"\nIN SYNC — {len(report['in_sync'])}:")
    for n, why in report["in_sync"]:
        out.append(f"  - {n} -> {why}")
    if report["unclassified"]:
        out.append(f"\nUNCLASSIFIED (drift — a tool validator nobody triaged) — {len(report['unclassified'])}:")
        for n in report["unclassified"]:
            out.append(f"  - {n}: classify as ported/missing/project_specific")
    if report["generalization_leaks"]:
        out.append(f"\nGENERALIZATION LEAKS (library is no longer portable) — {len(report['generalization_leaks'])}:")
        for why in report["generalization_leaks"]:
            out.append(f"  - {why}")
    return "\n".join(out)


def self_test() -> int:
    tool_src = TOOL.read_text(encoding="utf-8")
    lib_src = LIB.read_text(encoding="utf-8")

    # Positive control: the real inputs classify cleanly and do not leak.
    base = classify(tool_src, lib_src)
    assert not base["unclassified"], f"real tool has unclassified validators: {base['unclassified']}"
    assert not base["generalization_leaks"], f"real library leaks: {base['generalization_leaks']}"
    # A cleared backlog (0 missing) is the success state, not an error: every tool
    # validator is now ported, reclassified, or in sync.

    # Negative control 1: a new tool validator nobody classified must fail.
    mutated_tool = tool_src + "\n\ndef _validate_a_new_lesson(event):\n    return None\n"
    assert mutated_tool != tool_src, "mutation changed nothing"
    m1 = classify(mutated_tool, lib_src)
    assert "_validate_a_new_lesson" in m1["unclassified"], "unclassified drift not detected"
    assert failed(m1), "an unclassified validator must fail the check"

    # Negative control 2: a project-specific token in the library must fail.
    mutated_lib = lib_src + "\n# orchestration-agent-only shortcut\n"
    assert mutated_lib != lib_src, "mutation changed nothing"
    m2 = classify(tool_src, mutated_lib)
    assert m2["generalization_leaks"], "generalization leak not detected"
    assert failed(m2), "a leaked project token must fail the check"

    print("self-test: OK (baseline clean; unclassified-drift and generalization-leak controls both fired)")
    return 0


def main(argv) -> int:
    if "--self-test" in argv[1:]:
        return self_test()
    report = classify(TOOL.read_text(encoding="utf-8"), LIB.read_text(encoding="utf-8"))
    print(render(report))
    if failed(report):
        print("\nRESULT: FAIL (drift — resolve unclassified/leaks above)", file=sys.stderr)
        return 1
    print(f"\nRESULT: coverage clean — {len(report['missing'])} items in the harvest backlog to port")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
