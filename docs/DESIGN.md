# Design notes: the failure behind each invariant

Every rule here exists because something broke. This document records which
failure produced which mechanism, because the failures generalize even when the
specific project does not.

## 1. A pointer published with a blank digest

An agent composed YAML with a shell heredoc, wrote the pointer and the event as
two separate ungated commands, and the pointer landed first — referencing a
file that did not yet exist, with an empty hash field.

**Mechanism.** The publisher installs the event first, computes the digest from
the installed bytes, and only then writes the pointer. The caller is *forbidden*
from supplying `authoritative_event`; passing one is an immediate refusal. There
is no ordering for a caller to get wrong.

## 2. Prose containing a colon produced an unparseable authoritative record

Two events in the source ledger remain permanently unreadable because a
hand-authored YAML string contained `: ` and was written without validation.
They predate the publisher and cannot be recovered — the project now carries a
formal known-gap declaration for them.

**Mechanism.** YAML is serialized from Python objects with `safe_dump`, then
immediately `safe_load`ed and compared to the original. A round-trip mismatch
refuses the publication. Callers never hand-write the serialized form.

## 3. Two publishers, and the lower sequence landed last

The monotonicity check read the current pointer, then the replace happened
later. Two concurrent valid publications could both pass the check against the
same old pointer, and whichever replaced last won — potentially regressing the
sequence.

**Mechanism.** A `flock` spans both the check and the replace. Orchestrator
routes additionally share one lock anchored to a deterministic path, because
they share one sequence space across several outbound pointers.

## 4. A rogue process published under another agent's identity

Two extra agent processes spawned under an IDE server, reused the same session,
and published an event carrying the correct `writer` and `instance` fields. They
also rewrote drafts and overwrote a test log — turning a failing suite into a
passing one in the recorded evidence.

**Mechanism.** Outbound agent routes require the calling process to descend from
the registered listener's child PID. A role lock marks a writer; it does not
authenticate a process. Lineage does.

The intrusion was *detected* by the sequence check — the impostor advanced the
pointer, so the legitimate agent's next publish was refused. Loud failure beat
silent corruption.

## 5. The listener did not listen

The bridge assumed an agent would poll for new events. The actual harness only
executed when a human typed into its UI. Leases and timers kept refreshing, so
liveness looked healthy while nothing progressed — for hours. The orchestrator
compensated by polling, which read as over-supervision but was really a broken
transport.

**Mechanism.** A durable listener process owns a long-lived agent subprocess and
queues a turn on new work. Crucially it requires the agent CLI to **replay the
message back** before marking it delivered; a successful pipe write proves
nothing. Absence of a receipt within the timeout is an explicit listener
blocker, not evidence that the agent is busy.

## 6. Liveness was conflated with progress

A lease refresher proved a process existed. It did not prove work advanced. The
original protocol demanded a heartbeat every 60 seconds, which is incompatible
with doing real work, so an agent automated it — by touching a pointer's
timestamp while retaining a stale event hash. That presented an idle task as
freshly attested.

**Mechanism.** Heartbeats are ordinary append-only events; there is no way to
refresh time without publishing new state. The stale window is configurable
(`heartbeat_stale_seconds`) and the continuation rail exists so that genuinely
continuing work resumes immediately rather than waiting out a timer.

## 7. Durations were computed across hand-authored timestamps

Every `created_at` before a certain instant was typed by an agent, not bound to
a clock. Any duration spanning them is not a measurement.

**Mechanism.** The publisher stamps `created_at` itself. `time_authority_epoch`
in the topology marks when that began; the metrics calculator refuses to compute
durations across it, and the listener's heartbeat rail ignores pre-epoch events.
Omit the key entirely if your ledger was publisher-stamped from the start.

## 8. Hash mismatches could not distinguish tampering from normal revision

Sixty-four hash bindings failed verification. Nearly all referenced files that
are *mutable by design* — drafts, governance documents, a lease that rotates
every minute. But the rogue-writer intrusion produced mismatches that looked
exactly the same. An integrity signal with a ~94% false-positive rate cannot
detect the thing it exists for.

**Mechanism.** Every declared reference carries an explicit `reference_intent`:

- `immutable_ref` — must always verify; a mismatch is a real integrity failure.
- `snapshot_at_publication` — a point-in-time record of a file expected to
  change; informational, never an alarm.

Authority-bearing event references are forced to `immutable_ref` automatically,
since they structurally name append-only files.

The corollary is a discipline, not just a schema: **if you declare a reference
immutable, do not then mutate the file.** Version the path instead
(`report_v2.md`), so each publication binds to a distinct immutable artifact.

## 9. A local integrity concern halted an entire task

"Safety stop" was ambiguous enough that a bounded problem stopped everything.

**Mechanism.** Stop-shaped events must carry a `pause_scope` naming the blocked
action, the shared state, the last verified state, the lanes that continue, and
the resume condition. An empty continuation list is only accepted with a written
justification plus positive overlap proof.

## 10. Ambiguity became an unbounded research cycle

An agent facing a choice would escalate, and escalation could expand
indefinitely without changing what happened next.

**Mechanism.** `decision-required` events must carry a `decision_contract` whose
`outcome_actions` lead to **at least two distinct actions**. If every outcome
produces the same next action, the publisher refuses it — the ambiguity does not
matter, so proceed under a recorded assumption instead.

This is the most transferable idea in the substrate: *make the escalation
justify itself mechanically.*

## What is deliberately not enforced

- **Ledger freshness.** The orchestrator-owned ledger is hand-maintained in the
  source project and drifted from observable lease state for hours. The
  substrate reads it but does not enforce it. If you build on this, derive the
  volatile fields from the lease file and event log rather than maintaining them
  by hand — it is the one remaining place where an agent is asked to remember.
- **Evidence-directory protection.** Lineage checks cover bridge writes only.
  Nothing here prevents a rogue process from touching worktrees, drafts, or
  logs, which is how the test-log forgery in failure 4 actually happened.
