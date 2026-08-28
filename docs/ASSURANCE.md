# Assurance: the loop that keeps the mechanisms honest

> **Status: distilled, pending ratification.** This document is a portable
> distillation of the operating discipline in
> `coordination/ASSURANCE-PROTOCOL.md` of the originating replaced
> project. That file is authoritative; this one travels with the library so a
> future adopter inherits the *why*, not only the code. The refutation-cadence
> rule below (exhaustive-per-pass) is proposed under FRAGO-09 and not yet
> ratified upstream — treat it as recommended, not settled. When the upstream
> protocol ratifies, re-harvest this file rather than editing it in place.

The rest of this repository gives you **mechanisms** — a publisher that binds a
pointer to its event's digest, a lease that is one atomic `mkdir`, an integrity
check that refuses a ledger whose bindings no longer hold. Mechanisms answer
*"is this record internally consistent?"*

They do **not** answer *"do I trust the process that produced it?"* That is a
property of the loop the humans and agents run, not of any file. A team that
vendors the mechanisms but drops the loop gets gates that look authoritative and
are never adversarially tested — the most dangerous state, because a green check
now means less than no check at all. This file is the loop.

## The loop

Four moves, in order, per unit of work:

1. **Verify-first.** Before building, write down what would *prove* the thing
   works and what would *disprove* it. If you cannot state a disproof, you do
   not yet understand the claim well enough to build it.
2. **Mechanize.** Turn the proof into a check that runs without a human
   remembering to be careful. A discipline that lives only in a doc has already
   decayed; the only durable form of a rule is a check that fails when it is
   broken.
3. **Refute.** An *independent* party attacks the candidate — not the author,
   not the author's logs, its own checkout at the exact candidate. The refuter's
   job is to make the claim false, not to confirm it.
4. **Assess (RATE).** Decide per objective: **R**eframe (wrong problem),
   **A**dapt (right problem, wrong approach), **T**erminate (met, or provably
   won't be), **E**xecute (on track). Distinguish *are we doing the right
   things* (effect vs. end state) from *are we doing things right* (task
   completion). A green build answers only the second.

## The non-negotiables

These have teeth. Each is here because dropping it produced a real, wrong
"green" in the originating project.

1. **Every gate ships a firing negative control.** A check must come with a
   deliberate mutation that proves the check *fails* when the property is
   violated. A gate whose control has never fired is decorative — it may be
   asserting nothing. In this repo those controls are the suite's negative-control
   tests (integrity's tampering and `--strict` cases, the publisher lock-race
   falsification harness) and the `--self-test` on `library_harvest_check.py` in
   `proposals/`; run them, and watch them fail on the injected mutation, before
   you believe them.

2. **Close the class, not the case.** When refutation finds a defect, fix the
   category it belongs to, not the single instance. A patch that satisfies the
   one failing input while leaving its siblings open just reschedules the bug.

3. **Refute exhaustively per pass.** One pass attacks *every* applicable lens —
   trust chain, provenance and digest completeness, authority dominance,
   vacuity/empty-input, bypass, reproducible unit-of-check — and returns the
   *complete* forced-change set. Surfacing one defect per round when more were
   visible in the same pass is a defect of the pass: it costs round-trips
   without buying rigor. Aim for ≤2 rounds per object. This *raises* per-pass
   rigor; it never trades depth for speed. (Pending ratification, per the status
   banner.)

4. **Independence is structural, not attitudinal.** The refuter uses its own
   read-only checkout at the exact candidate, never edits the implementer's
   tree, and never treats the implementer's logs as independent execution
   evidence. "I ran it and it passed" from the author is a claim to be tested,
   not evidence.

5. **Structured output is not evidence.** A schema-conforming receipt proves the
   model can fill a schema, nothing more. Bind acceptance to a value the actor
   could not have produced *without* actually doing the work — a digest of a
   real artifact, a git object id, a measured duration.

6. **Claims must not outrun their proof.** State exactly the property the
   commands you ran establish — no broader. "By construction" is not a proof; it
   is a promise to show the construction. If a field would require a value you
   have not computed, omit it and state the absence rather than inventing one.

7. **Derive, detect, enforce — never invent a world-claim.** A checker is sound
   about a property only if no two possible worlds share its input while the
   property differs. It may *derive* from bound evidence, *detect* contradictions
   in declared-and-pinned semantics, and *enforce* evidence-bound policy. It must
   never turn an underdetermined record into a unique claim about the world. See
   `DESIGN.md` for the full treatment (the "straddle" law) — it is the reason the
   mechanisms in this repo refuse rather than guess.

## How the code already embodies this

You do not have to take the loop on faith; parts of it are already mechanized
here, and those are the parts to imitate when you extend:

| Principle | Where it already lives |
|---|---|
| Firing negative control | negative-control tests in the suite (integrity tampering/`--strict`, the publisher lock-race harness) and `library_harvest_check.py --self-test` |
| Detect-don't-invent | every refusal in `publisher.py`; the straddle law in `DESIGN.md` |
| Independence of the check | `integrity` knows nothing about event *names*; it only re-checks bindings |
| Guard the guard | `proposals/library_harvest_check.py` fails when a lesson skips the library |
| Reuse under a foreign scope | `proposals/library_reuse_acceptance_test.py` proves role-neutrality |

## What this file is not

It is not a substitute for the upstream protocol, which additionally governs
budgets, lease scoping, protected-event closure, and the orchestrator's
authority — concerns that belong to a *running* fleet, not a reused library. If
you operate a fleet on top of this substrate, read the source protocol. If you
only vendor the mechanisms, this file is the minimum discipline that keeps them
worth trusting. See `CONTRIBUTING.md` for how to add an invariant without
eroding it.
