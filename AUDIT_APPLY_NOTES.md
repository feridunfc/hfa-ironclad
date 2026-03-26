
# Phase 7C-B + 7C-C apply notes

## Files
- hfa-control/src/hfa_control/scheduler_reservation_dispatch.py
- hfa-core/src/hfa/runtime/state_store.py
- tests/core/test_exactly_once_dispatch_core.py
- tests/core/test_exactly_once_completion_core.py

## Verify
```powershell
.\.venv\Scripts\python -m pytest tests/core/test_exactly_once_dispatch_core.py -vv
.\.venv\Scripts\python -m pytest tests/core/test_exactly_once_completion_core.py -vv
```
