# Porting the substrate to your own bridge

## The three tools that port cleanly

`publisher`, `listener`, and `lease` need only a `topology.yaml`. Write one,
point them at your root, and they work. Nothing else in them names a role.

Checklist:

1. Create the directories your topology declares — event dirs, listeners dir,
   the lease dir's parent. The modules do not create event directories for you;
   a missing route directory is a refusal, not a silent `mkdir`.
2. Seed each pointer with a starting `sequence`, or let the first publication
   create it (any sequence ≥ 1 is accepted when no pointer exists).
3. Omit `time_authority_epoch` unless you have a genuine cutover instant before
   which timestamps were not clock-bound.
4. Decide which single role, if any, `holds_mutating_lease`.

`integrity` also ports cleanly — it deliberately knows nothing about event
names. Use it for "is my ledger still telling the truth"; use
`metrics_reference` only for throughput, and only after retargeting.

```bash
python3 -m coordination_substrate.integrity --root /path/to/root --strict
```

## The one tool that does not: `metrics_reference`

It computes integrity and throughput over an append-only log, but it classifies
events using one team's vocabulary. Its integrity half is superseded by
`integrity.py`; only its throughput metrics need this table:

| Constant | What it means |
|---|---|
| `TIME_AUTHORITY_EPOCH` | when timestamps became publisher-bound |
| `IMMUTABLE_TARGET_PREFIXES` | directories whose contents must never change |
| `SNAPSHOT_PATH_MARKERS` | filenames that are expected to change |
| `COMMITTED_ARTIFACT_MARKERS` | paths treated as committed artifacts |
| `ASSIGN_EVENTS` | event names that start an attempt |
| `ACCEPT_EVENTS` | event names that accept one |
| `ACCEPT_WRITER` | who may accept (`"boss"`) |
| `REVISION_EVENTS` | what counts as a material revision |
| `GATE_ACK_EVENTS`, `GATE_TERMINAL_EVENTS` | assurance gate boundaries |
| `REBOUND_EVENTS` | events that reconcile duplicate identities |

The assurance-gate-wait metric additionally hard-codes `writer == "james"`
(search for `ACCEPT_WRITER` and the assurance-gate block).

**Do not "fix" this by inventing plausible mappings.** The calculator's most
valuable behavior is that it reports `unavailable` with a reason rather than a
number it cannot defend. If your vocabulary does not map, leaving a metric
unavailable is the correct outcome — it is what the tool is for.

## Adapting the ledger probe

`ActiveWorkProbe` walks your ledger to decide whether a role is busy. It takes
a path of keys and an optional pause path:

```yaml
active_work:
  task_path: [execution, active_task]     # truthy string => has work
  pause_path: [execution, safe_pause]     # truthy => idle anyway
```

For a flat ledger, `task_path: [worker_task]` is fine. For per-role nesting,
`task_path: [roles, auditor, active_task]`.

If your ledger cannot express "is this role busy," the heartbeat and
continuation rails will not work — but the pointer rail, which is the primary
one, does not depend on the ledger at all.

## Using a different agent CLI

The listener assumes a CLI that:

- accepts newline-delimited JSON on stdin (`--input-format stream-json`),
- emits newline-delimited JSON on stdout (`--output-format stream-json`),
- and echoes user messages back (`--replay-user-messages`).

The replay echo is the load-bearing requirement — it is what turns "we wrote to
a pipe" into "the agent ingested the turn." If your CLI cannot echo, you need a
different receipt: a sentinel file the agent writes, or a status endpoint. Do
not substitute a successful write for a receipt; that is the exact failure the
transport exists to prevent.

Pass CLI-specific flags with `--arg`:

```bash
python3 -m coordination_substrate.listener serve --role worker \
  --root /path/to/root \
  --arg --permission-mode --arg acceptEdits
```

## Sizing the timers

| Setting | Default | Guidance |
|---|---|---|
| `ack_timeout_seconds` | 300 | How long an agent may take to echo a queued turn. Too low produces spurious blockers during a long tool call. |
| `heartbeat_stale_seconds` | 600 | How long silence is tolerated from an agent the ledger says is active. Match it to real work granularity — a value below a few minutes forces agents to automate reporting, which is how faked liveness gets invented. |
| `lease_seconds` | 600 | Refresh at roughly a tenth of this. |

## A note on adding rules

The source project's strongest lesson was that prose rules degrade and
mechanisms do not. If you find yourself writing a policy document telling agents
to be careful about something in this layer, check first whether the publisher
can simply refuse it. Most of the validations here started life as a paragraph
that kept getting violated.
