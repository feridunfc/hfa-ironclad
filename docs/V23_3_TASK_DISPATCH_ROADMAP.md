# V23.3 — Task-Level Dispatch Commit

## What this sprint adds

V23.1 introduced DAG schema and task completion / unlock semantics.
V23.2 introduced task admission and root-task READY seeding.
V23.3 closes the next critical gap: **task-level scheduling commit**.

This patch introduces:

1. `task_dispatch_commit.lua`
2. `DagLua.task_dispatch_commit(...)`
3. task running / event schema helpers
4. a DAG-ready queue bridge for SchedulerLoop integration
5. core and integration tests for task dispatch commit

## Why this matters

Without a task-level atomic dispatch boundary, DAG execution would regress into
non-deterministic Python orchestration. That is explicitly forbidden by the
IRONCLAD architecture.

The task dispatch commit is the DAG analogue of the V22 run dispatch commit:

- preconditioned state transition
- deterministic side effects
- stream emission inside the same atomic boundary
- no silent duplicate dispatch

## Atomic contract

`task_dispatch_commit.lua` guarantees all-or-nothing behavior for a READY task:

- `ready -> scheduled`
- task meta gets dispatch fields
- task enters tenant running ZSET
- control stream receives `TaskScheduled`
- shard stream receives `TaskRequested`

If the task is already `scheduled`/`running`, the result is `already_running`
and no duplicate events are emitted.

If the task is terminal (`done`, `failed`, `blocked`, `skipped`), the result is
`illegal_transition`.

If the task is missing or not READY, the result is `state_conflict`.

## Scheduler authority remains unchanged

This patch does **not** create a second dispatch authority.

The intended integration is:

- SchedulerLoop stays the only dispatch authority
- DAG-ready queue is only a source of candidates
- SchedulerLoop calls `DagLua.task_dispatch_commit(...)`

## Roadmap continuation

### V23.4 — DAG-aware SchedulerLoop
- add DAG candidate selection to SchedulerLoop
- preserve single scheduling authority
- fairness still tenant-scoped

### V23.5 — Task completion / child unlock integration
- wire `task_complete.lua` into worker completion path
- emit `TaskCompleted` / `TaskFailed`
- lazy failure/block semantics

### V23.6 — Task recovery
- stale task running reclaim
- task heartbeats
- duplicate worker replay suppression

### V23.7 — Multi-agent DAG routing
- task-level agent pools
- secondary fairness per agent_type
- worker capability filtering

### V23.8 — DAG observability and chaos
- ready/running/blocked metrics
- unlock fanout metrics
- script flush mid-chain
- reconnect / replay / duplicate completion chaos

## Operational notes

- `task_running_zset` is tenant-scoped in this patch to keep fairness and
  recovery accounting local.
- `payload_json` remains an opaque string at the Lua boundary to avoid JSON
  reserialization drift.
- stream maxlen is inherited from existing run-level constants for operational
  consistency.
