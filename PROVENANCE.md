# Provenance

Extracted 2026-08-01 from the replaced coordination bridge at
`/home/rocky/base_directory/coordination/`.

## Source mapping

| Extracted | Source | Lines (source) |
|---|---|---|
| `coordination_substrate/publisher.py` | `tools/publish_bridge_event.py` | 671 |
| `coordination_substrate/listener.py` | `tools/claude_listener_bridge.py` | 771 |
| `coordination_substrate/lease.py` | `tools/manage_bridge_lease.py` | 320 |
| `coordination_substrate/metrics_reference.py` | `tasks/P2-G004-.../corrections/2/artifacts/throughput_metrics_v2_1.py` | 852 |
| `tests/test_metrics_reference.py` | the same correction's `test_throughput_metrics_v2_1.py` | 253 |
| `coordination_substrate/topology.py` | **new** — replaces hard-coded constants | — |

`metrics_reference.py` is byte-identical to the accepted source artifact:

```
8d41e7ecf3fdd59e88b567e0745a9cc89e028939ed6ebb80015ac6c816b1e819
```

That digest is the one the source project's own verifying shim pins, so the
copy is provably the accepted implementation.

## What was generalized

The source hard-coded one bridge: an orchestrator named `orchestration-agent`, roles named
`orchestration-agent2` and `orchestration-agent3`, and paths under `coordination/bridge/`. All of it now comes
from a `Topology`:

| Was | Now |
|---|---|
| `ROUTES`, `AGENT_ROUTES`, `orchestration-agent_ROUTES` | `topology.routes` / `.agent_routes` / `.orchestrator_routes` |
| `BRIDGE_EVENT_DIRECTORIES` | `topology.event_dirs` |
| `ROLES`, `ROLE_OUTPUTS` | `topology.roles` |
| `coordination/bridge/listeners/{role}.json` | `role.listener_state` |
| `LEASE_RELATIVE` | `topology.lease_dir` |
| `TIME_AUTHORITY_EPOCH` constant | `topology.time_authority_epoch`, optional |
| `role_has_active_work` with per-role branches | declarative `ActiveWorkProbe` |
| `writer != "orchestration-agent"` | `writer != topology.orchestrator` |
| `implementer_instance_id`, `orchestration-agent2_session` | `owner_instance_id`, `session_id` |

Every validation, ordering guarantee, lock, and refusal message is otherwise
preserved. The extraction changes *what the code is told*, not *what it checks*.

## Deliberate behavioral changes

All address findings raised during review of the source implementation or during
this extraction. Nothing else differs.

1. **Child stderr is preserved.** The source passed `stderr=subprocess.DEVNULL`,
   so an agent CLI that failed to start reported only `exited with N` and the
   diagnostic was lost. The listener now appends to
   `<listeners_dir>/<role>.child-stderr.log`.

2. **Agent CLI policy flags are caller-supplied.** The source hard-coded
   `--permission-mode auto`, `--setting-sources=user,project,local`, and
   `--no-chrome`. Those are deployment policy — notably, `auto` grants
   unattended permission on a host with passwordless sudo — not substrate
   behavior. They now come from `--arg` / `extra_args`. To reproduce the source
   configuration exactly:

   ```bash
   python3 -m coordination_substrate.listener serve --role worker \
     --arg --permission-mode --arg auto \
     --arg --setting-sources=user,project,local \
     --arg --no-chrome
   ```

3. **`serve()` no longer requires the main thread.** The source called
   `signal.signal()` unconditionally, which raises `ValueError` off the main
   thread and made the function impossible to embed in a supervisor or test.
   Handlers are now installed only when running on the main thread; otherwise
   shutdown is cooperative. Found by writing the integration test.

4. **Child pipes are closed explicitly.** `serve()` left `stdin`/`stdout` to the
   garbage collector, leaking descriptors in any process serving more than one
   role.

5. **The global orchestrator-sequence lock is declared, not derived.** The source
   anchored it to a fixed path. An earlier draft of this extraction derived it
   from the lexically first inbound pointer, which would silently move the lock
   when a role was added — briefly leaving two locks live and reopening the race
   the lock exists to close. It is now the explicit `global_sequence_lock`
   topology field, defaulting to `<lease_dir parent>/orchestrator-global-sequence`.

## New in the extraction

**`integrity.py`** — the vocabulary-independent half of the metrics tool,
rewritten as a reusable engine with its own tests: strict YAML loading
(duplicate keys rejected), reference-intent classification, hash-binding
verification, and duplicate `(writer, sequence)` detection. `fail_closed` is
true only for parse failures, immutable-reference failures, and duplicate
identities — never for expected snapshot drift.

Run against the source project's live 1,017-event ledger it reports **3
actionable immutable failures and 43 correctly-classified expected drifts**,
where the original undifferentiated check reported 64 mismatches with no way to
tell tampering from routine revision.

## Not extracted

- `tools/gitlab_project_api.py` — project-specific API client, not substrate.
- The governance corpus (charter, protocols, ADRs, `.agents.md`). The mechanisms
  are here; the prose that motivated them is summarized in `docs/DESIGN.md`.
- The event vocabulary embedded in `metrics_reference.py`. See
  `docs/PORTING.md`.

## Verification

```
221 tests, all passing in ~16s (Python 3.9, PyYAML)
  test_publisher.py           70   includes a real two-thread lock race
  test_consumption.py         29   atomic disposable-run consumption
  test_topology.py            28
  test_orphan_recovery.py     24
  test_listener.py            21
  test_metrics_reference.py   17
  test_integrity.py           12
  test_lease.py               11
  test_integration.py          9   real subprocess, real serve() loop
```

The source project's own suites were not copied verbatim — they were written
against the hard-coded topology. The ported tests cover the same invariants plus
new coverage for the topology layer, the integrity engine, the concurrent
publisher race, and the previously untested `serve()` supervision loop.

Two testing notes, in the spirit of the source project's own validity-receipt
rule:

- `test_the_race_harness_actually_detects_a_missing_lock` is a **negative
  control**. It re-runs the race with the lock patched out and asserts the
  harness notices. A concurrency test that cannot fail is not evidence.
- `test_integration.py` uses a real child process (`fake_agent_cli.py`), and
  `FakeAgentCliTests` verifies the double itself replays only when asked —
  otherwise every test built on it would prove nothing.

## Known limitations carried forward

- Single POSIX filesystem only; `flock`/`mkdir`/`link` atomicity is assumed. Not
  NFS-safe.
- Delivery is at-least-once by design; idempotency is the caller's job.
- Ledger freshness is not mechanically enforced. See the closing section of
  `docs/DESIGN.md`.

## Addendum 2026-08-09 — orient

| Extracted | Source | Note |
|---|---|---|
| `coordination_substrate/orient.py` | `coordination/tools/orient_gate.py` | distillation: pure `decide(registry, scope, now)`, no clock/CLI |

`orient` is the execution-phase assumption/CCIR watch (OODA-Orient / RDSP /
JP 5-0 assumption-validation). The authoritative firing tool is the source
project's `orient_gate.py`; this module is the vocabulary-independent harvest
and decides identically (verified against the same registry, 2026-08-09).
Fail-closed: never CONTINUE by default; empty subject => NOT_ESTABLISHED_VACUOUS.
