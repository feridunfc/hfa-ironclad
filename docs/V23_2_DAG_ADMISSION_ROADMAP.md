
# V23.2 DAG Admission

## What this patch adds

This patch introduces the first atomic admission path for DAG tasks.

Added components:
- `hfa-core/src/hfa/dag/schema.py`
- `hfa-core/src/hfa/lua/task_admit.lua`
- `hfa-control/src/hfa_control/dag_lua.py`
- core + integration tests for DAG task seeding

## Design

Task admission is now normalized and atomic:
- create task meta
- create task state
- store `remaining_deps`
- optionally seed `children`
- if `dependency_count == 0`, emit to tenant DAG-ready queue

## Reasons for this design

We do **not** push DAG semantics into the scheduler loop yet.
The scheduler should only see READY tasks. Graph mutation remains in Lua.

## Invariants

1. A task is emitted to the READY queue exactly once.
2. A task with `dependency_count > 0` starts in `pending` and is not queued.
3. Re-admitting the same task returns `already_exists` and does not duplicate queue entries.
4. READY queue emission is tenant-scoped and deterministic (`admitted_at` score).

## Sprint 23.3 plan

- `task_dispatch_commit.lua`
- task-level running zset
- task event schema (`TaskReady`, `TaskScheduled`, `TaskCompleted`)
- scheduler-loop bridge from DAG-ready queue to dispatch

## Sprint 23.4 plan

- child unlock on task completion
- lazy failure propagation
- `blocked_by_failure` state
- descendant visibility APIs

## Sprint 23.5 plan

- DAG stale-task recovery
- task heartbeat and task claims
- duplicate completion protection
- operator APIs for graph inspection
