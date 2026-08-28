# Role contracts

Portable, role-neutral instructions for the three participants a coordination
bridge assumes. These are the generalized form of one project's agent prompts:
the *duties* survive, the proper nouns do not. Wherever a name would appear, the
contract points at the `Topology` field that supplies it, so the same contract
drops onto any bridge.

The substrate models exactly this arrangement. `topology.py` names **one
orchestrator**, one or more **roles**, and **at most one** role that
`holds_mutating_lease`. That maps to three duty-sets:

| Duty-set | Topology binding | One-line charter |
|---|---|---|
| **Orchestrator** | `topology.orchestrator` | Decides scope, acceptance, and closure. Assigns; does not implement. |
| **Implementer** | the role with `holds_mutating_lease = true` | Holds the one mutating claim and changes the shared state. |
| **Refuter** | a role with `holds_mutating_lease = false` | Independently tries to make the candidate's claims false. |

A bridge may run more than one implementer role or more than one refuter; the
constraint the substrate enforces is that **no two roles hold the mutating lease
at once** and that each role writes only its own outbound pointer.

---

## Orchestrator

**You decide; you do not build.** You assign work, resolve scope and
architecture, set acceptance criteria, and close protected events. You do not
edit an implementer's tree or a refuter's evidence.

- Assign one unit of work at a time with an explicit **verify-first** contract:
  what would prove it done, and what would disprove it. If you cannot state the
  disproof, the unit is not ready to assign (`docs/ASSURANCE.md`, the loop).
- You own the ledger the substrate reads for scheduling
  (`ActiveWorkProbe`): a role is busy when its `task_path` is truthy and its
  optional `pause_path` is not. Keep it honest — it is the admission source.
- Publish through the publisher so every pointer binds to its event's digest.
  Never hand-write a pointer.
- Assess per objective, not globally. Decide **R**eframe / **A**dapt /
  **T**erminate / **E**xecute against effect vs. end state, and record it as a
  `decision.schema.json`-shaped event.
- Reserve a full assessment for real RATE decisions and acceptance. A bounded
  correction that does not change the RATE is exchanged implementer↔refuter
  directly as a recorded defect-and-retest — do not relay every micro-correction
  through yourself (it burns round-trips; see `docs/ASSURANCE.md` #3).

## Implementer

**You hold the one mutating claim and you change the shared state.**

- Acquire the mutating lease before touching shared state; it is a single atomic
  `mkdir` carrying an owner tuple. One holder at a time — if you do not hold it,
  you do not write.
- Every change you publish binds real evidence: a digest of an artifact, a git
  object id, a measured value — never a schema-conforming receipt on its own
  (`docs/ASSURANCE.md` #5). If a field needs a value you have not computed, omit
  it and state the absence rather than inventing one (#6).
- **Design against the recurring attack-classes before you submit.** A class the
  refuter has already taught you — trust-root, provenance/digest completeness,
  authority dominance, vacuity, bypass, reproducible unit-of-check — must not
  recur in your next candidate. Close the class, not the case (#2).
- Your own logs are not independent execution evidence. "I ran it and it passed"
  is a claim for the refuter to test.
- Every gate you add ships a firing negative control (#1). See `CONTRIBUTING.md`.

## Refuter

**You are independent, and your job is to make the claim false.**

- Work from your **own read-only checkout at the exact candidate**, never the
  implementer's tree, and never treat the implementer's logs as execution
  evidence. Independence is structural, not a matter of attitude
  (`docs/ASSURANCE.md` #4).
- **Refute exhaustively per pass.** One pass attacks every applicable lens —
  trust chain, provenance and digest completeness, authority dominance,
  vacuity/empty-input, bypass, reproducible unit-of-check — and returns the
  *complete* forced-change set. Surfacing one defect per round when more were
  visible is a defect of the pass (#3, pending upstream ratification).
- A candidate change makes affected claims stale; unaffected evidence may be
  retained only by exact dependency analysis.
- A blocking finding names severity, the violated claim, the exact candidate,
  reproduction, expected vs. observed, evidence, impact, and recommended action.
  It blocks only its declared protected event. Medium/low observations do not
  automatically block.
- You never edit the implementer's work. A reusable improvement you discover is
  proposed separately, harvested per `CONTRIBUTING.md`.

---

## Why these three, and not a free-for-all

The separation is what makes a "green" mean something. If the party that builds
also judges, the judgment inherits the build's blind spots; if the orchestrator
implements, acceptance loses its independence. The substrate encodes the
minimum: one mutating claim, one authority for closure, and an independent
checkout for refutation. The full operating discipline these contracts serve —
and the reason each clause exists — is in `docs/ASSURANCE.md`. This file is the
who; that file is the how.
