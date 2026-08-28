# Schemas

Reusable, role-neutral shapes for events that cross a coordination bridge. Each
is harvested from the originating project and generalized: role names, pinned
vocabularies, and identifiers are **policy the deployment supplies**, not values
fixed in the schema.

| Schema | What it shapes | Grounded in |
|---|---|---|
| `decision.schema.json` | A typed RATE assessment/decision event | `proposals/opord08-library-reuse/substrate_decision_schema.json` (design record) |

## The one thing to understand before you trust a schema

**A schema validates shape. Shape is not correctness, and a schema-conforming
record is not evidence that anything happened.** This is the single most
expensive lesson behind this library (see `docs/ASSURANCE.md`, non-negotiable
#5). Concretely, for `decision.schema.json` the schema *can* enforce:

- a decision carries a typed `rate` from the fixed four;
- every objective has a diagnosis, and every diagnosis has a `type` and a
  `falsification_condition` — no un-falsifiable claims;
- a waiver is dated, cited, and carries an expiry;
- a disposition binds a 64-hex `governing_digest`.

It **cannot** enforce, and these remain code obligations wherever the events are
actually published or read:

- **Readiness.** "Zero contradicted-and-open" is computed over classification by
  an algorithm; a schema cannot count. A contradicted row whose disposition
  names no closure evidence is open regardless of applicability or waiver.
- **Waiver expiry.** The schema requires an `expires_at`; only code comparing it
  to now can *refuse* an expired one.
- **Digest freshness.** The schema requires `governing_digest` to be present;
  only code comparing it to the current standard can tell a *stale* disposition
  from a current one.
- **Diagnosis truth.** Undecidable. `diagnosis_truth` is always
  `not_established`; underdetermined cases route to discrimination, never to a
  truth verdict.
- **Vocabulary membership.** The pinned diagnosis `type` set is a project's
  closed set; enforce membership in a project overlay or in code.

If your deployment enforces those obligations in the live publisher, keep the
schema as the *documented shape* and the code as the *authority* — do not let a
schema and a validator become two drifting enumerations of the same rule. The
schema is the portable contract; the code is where a value the actor could not
have produced without doing the work gets bound and checked.

## Using a schema

```python
import json, jsonschema  # jsonschema is not a library dependency; validate in your project
schema = json.load(open("schemas/decision.schema.json"))
jsonschema.validate(instance=my_event, schema=schema)
```

To narrow a vocabulary for your deployment, overlay rather than edit: load the
schema and inject your closed `enum` at `$defs/diagnosis/properties/type`, so
the portable file stays general and your project pins its own policy.
