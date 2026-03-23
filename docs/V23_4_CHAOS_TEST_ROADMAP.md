# V23.4 Chaos Test Package

## Scope

This package hardens the V23.3 DAG task dispatch layer with chaos-oriented
integration tests on real Redis.

## Included tests

- `test_duplicate_task_dispatch_race`
  - two concurrent dispatch attempts
  - exactly one commit allowed
  - no duplicate stream emission
- `test_script_flush_recovery_for_dispatch_commit`
  - verifies NOSCRIPT recovery path
- `test_partial_commit_impossible_on_rejected_state`
  - proves no partial side effects on rejected dispatch
- `test_double_dispatch_idempotent_after_success`
  - sequential duplicate dispatch stays non-duplicating
- `test_dispatch_commit_preserves_trace_fields_exactly`
  - verifies trace fields are propagated without silent mutation

## Expected command

```bash
pytest tests/integration/test_v23_4_dag_dispatch_chaos_integration.py -m integration -vv
```

## Success criteria

- no duplicate control/shard events
- rejected dispatch never mutates meta/running zset
- script flush does not break strict Lua execution
- trace fields survive exactly
