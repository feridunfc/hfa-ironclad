
# Apply & verify

Overwrite:
- hfa-core/src/hfa/state/__init__.py
- hfa-control/src/hfa_control/scheduler_reservation_dispatch.py
- hfa-core/src/hfa/dag/schema.py

Run:
```powershell
.\.venv\Scripts\python -m pytest -k "not integration"
```
