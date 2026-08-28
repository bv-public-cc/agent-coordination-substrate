# Contributing: adding an invariant without eroding reuse

This library is the **generalized distillation** of a live coordination tool.
Lessons are learned in a running system; they arrive here only after being
stripped of one project's policy and re-expressed against a `Topology`. The
whole value of the library is that it stays portable. These rules exist so an
addition cannot quietly un-portable it.

Read `docs/ASSURANCE.md` first — it is the loop these rules serve.

## The harvest direction is one-way

A live tool is authoritative; the library is downstream of it. You **harvest**
tool → library by *extract-the-invariant, parameterize-the-policy*. You never
mirror the tool literally, and you never edit the library to match a project's
literals.

- **Extract the invariant.** "An orchestrator's sequence must be monotonic" is
  an invariant. "`boss` events must be monotonic" is a project literal — the role
  name is policy, so it becomes `topology.orchestrator`.
- **Parameterize the policy.** A vocabulary (remediation decisions, layer names),
  a route table, a ledger's concrete sequences — these are inputs the library
  *consumes*, never constants it *contains*.

## Every invariant ships a firing negative control

An addition is not done when the check passes on good input. It is done when a
deliberately-broken input makes the check **fail**, and you have watched it fail.
A check that has never rejected anything may be asserting nothing (see
`docs/ASSURANCE.md`, non-negotiable #1). Provide the control as a test, or as a
`--self-test` mutation for a CLI (as `library_harvest_check.py` does), or a
negative-control test in the suite (as `integrity` and the publisher lock-race
do).

## Two guards protect the reuse boundary — run them

Both live in `proposals/opord08-library-reuse/`; `library_harvest_check.py` ships
a `--self-test`, and `library_reuse_acceptance_test.py` is a runnable acceptance
test:

- **`library_harvest_check.py`** — fails when the tool grows a validator nobody
  triaged (a live lesson silently skipping the library) **or** a project token
  (`boss`, a layer vocabulary, a run's ledger sequences) leaks into the library.
  When you port a validator, classify it in that file's table; when you add a
  library symbol, keep it free of the forbidden tokens.
- **`library_reuse_acceptance_test.py`** — instantiates the library under a
  *different* topology (a `captain`/`scout`/`signals` bridge with no `boss`) and
  asserts the invariants still hold. Passing under the original scope proves
  nothing; the foreign-scope pass is the acceptance. Add your new invariant to
  it.

```bash
python3 proposals/opord08-library-reuse/library_harvest_check.py --self-test
python3 proposals/opord08-library-reuse/library_reuse_acceptance_test.py
```

## Checklist for a new invariant

1. It is stated as a property of *any* bridge, not this one.
2. Its policy inputs come from `Topology` (or an explicit parameter), not
   constants in the module.
3. It has a test that proves the refusal **fires** on a witness input, plus a
   positive control proving it is not fail-dead.
4. `library_harvest_check.py --self-test` passes and the validator is classified.
5. `library_reuse_acceptance_test.py` exercises it under a foreign topology.
6. `PROVENANCE.md` and the test count in `README.md` are updated.
7. If it is a *shape* an event must have, prefer a schema in `schemas/` and say
   plainly what the schema cannot check (see `schemas/README.md`).

## What does not belong here

Project policy: role names, layer vocabularies, a specific run's ledger
sequences, remediation taxonomies, credential-mode requirements. These are
configuration the library reads. If you find yourself typing a proper noun from
one deployment into a module, it belongs in that deployment's `topology.yaml` or
a parameter, not in the library.
