# Walkthrough: build a working two-agent bridge from nothing

This builds a complete coordination bridge step by step, runs it, and then
breaks it on purpose so you can see each guarantee actually fire.

Everything happens under one throwaway directory. Nothing here touches a real
repository, and you can delete the whole thing at the end.

**Cast.** An orchestrator called `orchestration-agent` assigns work. An agent called `worker`
does it and holds the mutating lease. That is the smallest arrangement that
exercises every component.

---

## 0. Prerequisites

```bash
python3 --version          # 3.9 or newer
python3 -c "import yaml"   # PyYAML installed
```

Put the package on your path for the session:

```bash
export PYTHONPATH=/srv/coordination-substrate:$PYTHONPATH
```

---

## 1. Lay out the bridge

```bash
export BRIDGE=/tmp/bridge-demo
mkdir -p $BRIDGE/{bridge/events,bridge/listeners,bridge/locks,coordination,work}
cd $BRIDGE
```

Four directories carry authority:

| Directory | Holds |
|---|---|
| `bridge/events/` | append-only event files — the actual history |
| `bridge/listeners/` | one JSON state file per running listener |
| `bridge/locks/` | the mutating lease and the sequence locks |
| `coordination/` | the topology description |

The substrate **never creates these for you**. A missing directory is a refusal,
because silently materializing a tree is how a typo becomes a second, empty,
authoritative-looking bridge.

---

## 2. Describe the bridge

```bash
cat > coordination/topology.yaml <<'YAML'
schema_version: 1

orchestrator: orchestration-agent
orchestrator_events_dir: bridge/events
ledger: state.yaml
lease_dir: bridge/locks/mutating.lock

defaults:
  events_dir: bridge/events
  listeners_dir: bridge/listeners

roles:
  worker:
    inbound_pointer: bridge/orchestration-agent-to-worker.yaml
    outbound_pointer: bridge/worker-to-orchestration-agent.yaml
    holds_mutating_lease: true
    active_work:
      task_path: [execution, active_task]
      pause_path: [execution, safe_pause]
YAML
```

Read that as: *orchestration-agent writes to `orchestration-agent-to-worker.yaml`; worker writes to
`worker-to-orchestration-agent.yaml`; worker is busy when `state.yaml` names an
`execution.active_task` and does not declare `execution.safe_pause`.*

Create the ledger the orchestrator owns:

```bash
cat > state.yaml <<'YAML'
execution:
  active_task: null
  safe_pause: true
YAML
```

Confirm it parses:

```bash
python3 -c "
from coordination_substrate.topology import Topology
t = Topology.load('$BRIDGE')
print('orchestrator:', t.orchestrator)
print('roles:', sorted(t.roles))
print('lease holder:', t.lease_holder().name)
print('routes:'); [print(' ', k, '->', v) for k, v in sorted(t.routes.items())]
"
```

---

## 3. Assign work (orchestrator side)

An assignment is one event plus one pointer update, published together.

```bash
cat > /tmp/assign.json <<'JSON'
{
  "pointer_path": "bridge/orchestration-agent-to-worker.yaml",
  "event_filename": "20260801T120000Z-1-orchestration-agent-task-assigned.yaml",
  "event": {
    "schema_version": 1,
    "sequence": 1,
    "event": "task-assigned",
    "writer": "orchestration-agent",
    "task_id": "T-1",
    "instruction": "Add a health endpoint. Note: this prose has a colon.",
    "next_pause": "publication-candidate"
  },
  "pointer": {
    "schema_version": 1,
    "sequence": 1,
    "event": "task-assigned",
    "writer": "orchestration-agent"
  }
}
JSON

python3 -m coordination_substrate.publisher /tmp/assign.json --root $BRIDGE
```

Output is the event path and its digest:

```json
{"event_path": "bridge/events/20260801T120000Z-1-orchestration-agent-task-assigned.yaml",
 "event_sha256": "…"}
```

Look at what landed:

```bash
cat bridge/orchestration-agent-to-worker.yaml
```

```yaml
schema_version: 1
sequence: 1
event: task-assigned
writer: orchestration-agent
created_at: '2026-08-01T12:00:00Z'
authoritative_event:
  path: bridge/events/20260801T120000Z-1-orchestration-agent-task-assigned.yaml
  sha256: 8f2a…
```

Four things happened that you did not ask for:

1. **`created_at` was stamped by the publisher**, not by you. Hand-authored
   timestamps make durations unmeasurable, so the clock is not negotiable.
2. **The event filename's timestamp is derived from `created_at`.** The publisher
   rewrites the timestamp portion of the name you supplied to match the stamped
   `created_at`, so the two can never disagree. Here they match because this run's
   clock produced `12:00:00Z`; a live run stamps the current clock, so your
   installed filename will carry *that* time — read it back from
   `ls bridge/events/` rather than assuming the name you passed.
3. **`authoritative_event` was added.** You are *forbidden* from supplying it —
   the digest comes from the bytes actually on disk, after they were written.
4. **The colon in your prose survived**, because the YAML was serialized from an
   object and re-parsed before install.

Prove the binding yourself:

```bash
python3 - <<'PY'
import hashlib, os, yaml, pathlib
root = pathlib.Path(os.environ["BRIDGE"])
p = yaml.safe_load((root/"bridge/orchestration-agent-to-worker.yaml").read_bytes())
ref = p["authoritative_event"]
actual = hashlib.sha256((root/ref["path"]).read_bytes()).hexdigest()
print("pointer says:", ref["sha256"])
print("disk says:   ", actual)
print("MATCH" if actual == ref["sha256"] else "MISMATCH")
PY
```

---

## 4. Mark the worker busy

The ledger is what admits self-scheduled wakes. Until it names a task, the
listener will never wake the agent on a timer.

```bash
cat > state.yaml <<'YAML'
execution:
  active_task: T-1
  safe_pause: false
YAML
```

---

## 5. Take the lease (agent side)

Before mutating anything shared, claim the single mutating slot.

```bash
REQ=$(sha256sum /tmp/assign.json | cut -d' ' -f1)

python3 - <<PY
from coordination_substrate.topology import Topology
from coordination_substrate.lease import claim, read_lease
t = Topology.load("$BRIDGE")
print(claim(t,
    task_id="T-1", attempt=1, request_sha256="$REQ",
    owner="worker-instance-1", harness="demo",
    listener_pid=1, listener_child_pid=2,
    session_id="b4a20b7d-100b-4f13-a509-9340559ed468")["action"])
print("expires:", read_lease(t)["expires_at"])
PY
```

Now try to take it as someone else:

```bash
python3 - <<PY
from coordination_substrate.topology import Topology
from coordination_substrate.lease import claim, LeaseError
t = Topology.load("$BRIDGE")
try:
    claim(t, task_id="T-1", attempt=1, request_sha256="$REQ",
          owner="a-different-agent", harness="demo",
          listener_pid=9, listener_child_pid=9,
          session_id="b4a20b7d-100b-4f13-a509-9340559ed468")
    print("BUG: second owner got the lease")
except LeaseError as exc:
    print("refused:", exc)
PY
```

The slot is a directory created with `mkdir`, which is atomic on POSIX. Two
agents racing for it cannot both win, and an identical re-claim by the *same*
owner replays instead of duplicating — so a retried assignment is safe.

---

## 6. Report progress (agent side)

The agent publishes to its own route. Same publisher, different pointer.

```bash
cat > /tmp/progress.json <<'JSON'
{
  "pointer_path": "bridge/worker-to-orchestration-agent.yaml",
  "event_filename": "20260801T120500Z-1-worker-progress.yaml",
  "event": {
    "schema_version": 1,
    "sequence": 1,
    "event": "progress",
    "writer": "worker",
    "task_id": "T-1",
    "status": "implementing",
    "next_pause": "publication-candidate",
    "responds_to": {
      "path": "bridge/events/20260801T120000Z-1-orchestration-agent-task-assigned.yaml",
      "sha256": "auto",
      "sequence": 1
    }
  },
  "pointer": {
    "schema_version": 1,
    "sequence": 1,
    "event": "progress",
    "writer": "worker"
  }
}
JSON
```

Note `"sha256": "auto"`. The publisher resolves it from the referenced file's
real bytes, so a reply can never cite a digest someone mistyped.

This publish is **refused** unless it comes from a registered listener process:

```bash
python3 -m coordination_substrate.publisher /tmp/progress.json --root $BRIDGE
# REFUSED: worker listener state is absent or invalid
```

That is the guard against a stray process publishing under an agent's identity.
In production the real listener writes that file; here, register the current
shell as the listener.

> **Stay in this shell from here on.** The registration records `$$`, and the
> publisher checks that the caller descends from that pid. A new terminal will
> be refused — which is the guard working, not a bug.

```bash
cat > bridge/listeners/worker.json <<JSON
{"schema_version": 1, "role": "worker", "status": "running",
 "listener_pid": $$, "child_pid": $$, "last_acknowledged_sequence": 0}
JSON

python3 -m coordination_substrate.publisher /tmp/progress.json --root $BRIDGE
```

Inspect the resolved reference:

```bash
grep -A3 responds_to bridge/events/20260801T120500Z-1-worker-progress.yaml
```

`sha256: auto` is gone, replaced by the real digest and an explicit
`reference_intent: immutable_ref`.

---

## 7. Watch the guarantees fire

### Sequence cannot go backwards

```bash
sed 's/"sequence": 1/"sequence": 1/' /tmp/assign.json > /tmp/replay.json
python3 -m coordination_substrate.publisher /tmp/replay.json --root $BRIDGE
# REFUSED: sequence is not greater than the global orchestrator sequence
```

### An event file cannot be overwritten

Events install with `os.link`, which refuses an existing name. Append-only is
enforced by the filesystem, not by convention.

### A stop must say what it stops

```bash
cat > /tmp/stop.json <<'JSON'
{
  "pointer_path": "bridge/orchestration-agent-to-worker.yaml",
  "event_filename": "20260801T121000Z-2-orchestration-agent-safety-stop.yaml",
  "event": {"schema_version": 1, "sequence": 2, "event": "safety-stop",
            "writer": "orchestration-agent", "reason": "shared state looks wrong"},
  "pointer": {"schema_version": 1, "sequence": 2, "event": "safety-stop",
              "writer": "orchestration-agent"}
}
JSON

python3 -m coordination_substrate.publisher /tmp/stop.json --root $BRIDGE
# REFUSED: stop-shaped event requires pause_scope
```

Add the scope and it goes through:

```bash
python3 - <<'PY'
import json
s = json.load(open("/tmp/stop.json"))
s["event"]["pause_scope"] = {
  "blocked_action": "worktree mutation for T-1",
  "shared_state": "the T-1 worktree",
  "last_verified_state": "clean at sequence 1",
  "continuation_lanes": ["documentation", "read-only analysis"],
  "resume_condition": "orchestrator decision",
}
json.dump(s, open("/tmp/stop-ok.json", "w"))
PY

python3 -m coordination_substrate.publisher /tmp/stop-ok.json --root $BRIDGE
```

A vague "safety stop" once halted an entire task over a bounded problem. Now a
stop must name the blocked action, the shared state, the last verified state,
the lanes that continue, and the resume condition — and an empty continuation
list needs written justification plus overlap proof.

> This example uses the **orchestration-agent** route deliberately. On an agent route the
> listener-lineage check runs first, so you would see that refusal instead —
> which is the correct order, but it hides the lesson.

### An escalation must change the outcome

A `decision-required` event needs a `decision_contract` whose `outcome_actions`
lead to **at least two different actions**. If every outcome produces the same
next step, the publisher refuses it: the ambiguity does not matter, so proceed
under a recorded assumption instead. This is the single most transferable idea
in the substrate — *make escalation justify itself mechanically.*

---

## 8. Check ledger integrity

```bash
python3 -m coordination_substrate.integrity --root $BRIDGE \
  --event-dir bridge/events
```

```
- events loaded: **2**
- events rejected: **0**
- reference bindings checked: **1**
- immutable failures: **0**
- snapshot drift (expected): **0**
- duplicate identities: **0**

**fail_closed: false**
```

Now tamper with history and re-run:

```bash
echo "tampered: true" >> bridge/events/20260801T120000Z-1-orchestration-agent-task-assigned.yaml
python3 -m coordination_substrate.integrity --root $BRIDGE --event-dir bridge/events
```

```
- immutable failures: **1**
**fail_closed: true**

## Defects requiring attention
- `immutable-failure` `bridge/events/…-1-orchestration-agent-task-assigned.yaml` — content differs from the recorded digest (referenced by `…-1-worker-progress.yaml`)
```

Undo it (`sed -i '$ d' …`) before continuing.

**The distinction that makes this usable:** files you declare
`snapshot_at_publication` — drafts, governance docs, anything expected to change
— are reported as drift and never alarm. Only `immutable_ref` failures,
unparseable events, and duplicate identities set `fail_closed`. An integrity
check that fires on routine edits gets ignored, and then it cannot catch the one
event that matters.

Add `--strict` to exit non-zero for CI:

```bash
python3 -m coordination_substrate.integrity --root $BRIDGE \
  --event-dir bridge/events --strict
```

---

## 9. Release the lease

```bash
python3 - <<PY
from coordination_substrate.topology import Topology
from coordination_substrate.lease import release, read_lease
t = Topology.load("$BRIDGE")
print(release(t, task_id="T-1", attempt=1,
              request_sha256="$REQ", owner="worker-instance-1")["action"])
print("lease now:", read_lease(t))
PY
```

Then mark the worker idle so no scheduler wakes it:

```bash
cat > state.yaml <<'YAML'
execution:
  active_task: null
  safe_pause: true
YAML
```

> **Keep this in sync.** A ledger that still says "active" after a role
> finished will make the heartbeat rail wake an idle agent indefinitely. In the
> source project that exact drift ran for nearly four hours. If you build on
> this, derive these fields from the lease file and event log rather than
> maintaining them by hand.

---

## 10. Run a live listener

Everything above was manual. In production a listener watches the pointer and
wakes a real agent process.

```bash
python3 -m coordination_substrate.listener serve \
  --role worker --root $BRIDGE \
  --executable claude \
  --arg --permission-mode --arg acceptEdits
```

The loop:

1. Read `bridge/orchestration-agent-to-worker.yaml` and validate the whole chain — pointer
   sequence, referenced event exists, digest matches, the event lives inside the
   admitted directory, and it was authored by the orchestrator.
2. If the sequence advanced, write one turn into the agent's stdin.
3. **Wait for the agent CLI to replay that message back.** A successful pipe
   write is not delivery. Only the echo counts.
4. On timeout (`ack_timeout_seconds`, default 300) write
   `status: transport-ack-timeout` and exit loudly.

Watch it live from another shell:

```bash
watch -n1 cat $BRIDGE/bridge/listeners/worker.json
```

Two more rails run alongside the pointer rail:

| Rail | Fires when | Guard against runaway |
|---|---|---|
| `continuation` | the agent's own last event names a `next_pause` and shows no stop signal | one wake per role sequence |
| `heartbeat` | the ledger says active but nothing arrived within `heartbeat_stale_seconds` | one wake per stale window |

Both are admitted **only** by the orchestrator-owned ledger, so an agent cannot
schedule itself into perpetual activity. And every wake message says, in the
text the agent reads, that transport grants no scope, task, retry, or acceptance
authority. A wake is scheduling, never instruction.

Any CLI works if it accepts stream-JSON on stdin, emits stream-JSON on stdout,
and echoes user messages. If yours cannot echo, you need a different receipt — a
sentinel file, a status endpoint — but **do not substitute a successful write for
a receipt.** That is the exact failure the transport exists to prevent.

---

## 11. Clean up

```bash
rm -rf $BRIDGE /tmp/assign.json /tmp/progress.json /tmp/stop.json /tmp/replay.json
```

---

## Where to go next

- [DESIGN.md](DESIGN.md) — the production failure behind each mechanism. Worth
  reading before you decide any of them is over-engineered.
- [PORTING.md](PORTING.md) — retargeting to your own roles, ledger shape, agent
  CLI, and timer sizing.
- `examples/minimal-topology.yaml` — the smallest config, with every default
  spelled out.


### Two habits worth adopting with it

**If you declare a reference immutable, never mutate that file.** Version the
path instead (`report_v2.md`). The integrity check is only as useful as that
discipline.

**When you want to add a rule, check whether the publisher can just refuse it.**
Nearly every validation here started as a paragraph in a governance document
that kept getting violated. Prose degrades; mechanisms do not.
