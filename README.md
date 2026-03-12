# IRONCLAD Sprint 9 — Guardian Refactored Package

This package is a standalone **Sprint 9 Distributed Runtime Foundation** implementation skeleton.
It is intended as a **branch seed / reference package**, not a blind overwrite of an existing repo.

## Included
- `hfa-core/src/hfa/events/schema.py` — pure event dataclasses
- `hfa-core/src/hfa/events/codec.py` — event serialization/deserialization
- `hfa-core/src/hfa/runtime/shard.py` — shard claim / renew / release
- `hfa-core/src/hfa/runtime/state_store.py` — partial-update-safe run state/meta/result store
- `hfa-worker/src/hfa_worker/heartbeat.py` — worker heartbeat publisher
- `hfa-worker/src/hfa_worker/consumer.py` — XREADGROUP worker consumer with pull-side backpressure
- `hfa-worker/src/hfa_worker/main.py` — worker lifecycle wiring
- `tests/` — pytest skeletons for Sprint 9

## Guardian rules enforced
- No `print()`
- Logging only
- `cost_cents` always integer
- `state` updated in both state key and meta hash
- XACK only on terminal success / terminal failure
- Internal crashes leave the message in the PEL
- Shard ownership is **worker_id-based** in Sprint 9
- Pull-side backpressure enforced before `XREADGROUP`

## Integration note
This package intentionally does **not** include a Sprint 10 control plane.
