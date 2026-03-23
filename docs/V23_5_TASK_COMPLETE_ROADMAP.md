# V23.5 Task Completion + DAG Unlock

This overwrite fixes the V23.5 completion path end-to-end.

## What changed
- Added `task_complete.lua` at the correct path.
- Fixed `DagLua` path resolution and added `task_complete()` result parsing.
- Restored V23.2/V23.3 methods so admission and task dispatch keep working.
- Corrected DAG key schema helpers and compatibility aliases used by tests.
- Made successful completion unlock direct children exactly once.
- Preserved idempotency: a second completion returns `already_terminal` without duplicate queue emission.

## Verified behaviour
- `done` unlocks direct children whose `remaining_deps` reaches zero.
- `failed` does not unlock children.
- duplicate complete calls do not duplicate ready queue entries.

## Next sprint
- V23.6 stale task recovery + heartbeat
- V23.7 multi-agent DAG routing
- V23.8 DAG observability and chaos soak tests
