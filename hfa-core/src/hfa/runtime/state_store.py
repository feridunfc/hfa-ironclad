"""IRONCLAD Sprint 9 — partial-update-safe runtime state store."""
from __future__ import annotations

import json
import time
from typing import Any, Dict, Optional


class StateStore:
    """Redis-backed run state/meta/result helper.

    Contract:
    - `state` is written to both the string state key and the meta hash
    - meta updates are partial (`HSET`), never full overwrites
    - result payload uses the agreed JSON shape
    """

    RESULT_TTL_SECONDS = 86_400
    META_TTL_SECONDS = 86_400
    STATE_TTL_SECONDS = 86_400

    def __init__(self, redis: object) -> None:
        self._redis = redis

    async def create_run_meta(self, run_id: str, mapping: Dict[str, str]) -> None:
        key = f"hfa:run:meta:{run_id}"
        existing = await self._redis.exists(key)
        if existing:
            # Contract is create-once; callers that need updates must use patch_run_meta.
            return
        pipe = self._redis.pipeline()
        pipe.hset(key, mapping=mapping)
        pipe.expire(key, self.META_TTL_SECONDS)
        await pipe.execute()

    async def get_run_state(self, run_id: str) -> Optional[str]:
        value = await self._redis.get(f"hfa:run:state:{run_id}")
        if value is None:
            return None
        return value.decode() if isinstance(value, bytes) else str(value)

    async def get_run_meta(self, run_id: str) -> Dict[str, str]:
        raw = await self._redis.hgetall(f"hfa:run:meta:{run_id}")
        out: Dict[str, str] = {}
        for key, value in raw.items():
            key_text = key.decode() if isinstance(key, bytes) else str(key)
            value_text = value.decode() if isinstance(value, bytes) else str(value)
            out[key_text] = value_text
        return out

    async def patch_run_meta(self, run_id: str, mapping: Dict[str, str]) -> None:
        if not mapping:
            return
        key = f"hfa:run:meta:{run_id}"
        pipe = self._redis.pipeline()
        pipe.hset(key, mapping=mapping)
        pipe.expire(key, self.META_TTL_SECONDS)
        await pipe.execute()

    async def transition_state(self, run_id: str, new_state: str) -> None:
        pipe = self._redis.pipeline()
        pipe.set(f"hfa:run:state:{run_id}", new_state, ex=self.STATE_TTL_SECONDS)
        pipe.hset(f"hfa:run:meta:{run_id}", "state", new_state)
        pipe.expire(f"hfa:run:meta:{run_id}", self.META_TTL_SECONDS)
        await pipe.execute()

    async def store_result(
        self,
        run_id: str,
        tenant_id: str,
        status: str,
        payload: Any,
        cost_cents: int,
        tokens_used: int,
        error: Optional[str] = None,
        completed_at: Optional[float] = None,
    ) -> None:
        result = {
            "run_id": run_id,
            "tenant_id": tenant_id,
            "status": status,
            "payload": payload,
            "error": error,
            "completed_at": completed_at if completed_at is not None else time.time(),
            "cost_cents": int(cost_cents),
            "tokens_used": int(tokens_used),
        }
        await self._redis.set(
            f"hfa:run:result:{run_id}",
            json.dumps(result, separators=(",", ":")),
            ex=self.RESULT_TTL_SECONDS,
        )
