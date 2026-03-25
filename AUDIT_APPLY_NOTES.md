
# Apply Notes

## Primary files
- hfa-control/src/hfa_control/auth.py
- hfa-control/src/hfa_control/backpressure.py
- hfa-control/src/hfa_control/scheduler_loop.py
- hfa-core/src/hfa/state/__init__.py
- hfa-core/src/hfa/lua/state_transition.lua
- tests/chaos/conftest.py

## Core verification
```bash
pytest tests/core/test_final_auth_runtime_resolution.py -vv
pytest tests/core/test_final_backpressure_persistence.py -vv
pytest tests/core/test_final_strict_cas_prod.py -vv
pytest tests/core/test_final_scheduler_hygiene.py -vv
```

## Chaos verification
```bash
docker compose -f docker-compose.integration.yml up -d --wait
pytest tests/chaos/ -vv
```
