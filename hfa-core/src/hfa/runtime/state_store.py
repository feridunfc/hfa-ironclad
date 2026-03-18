"""
hfa-core/src/hfa/runtime/state_store.py
IRONCLAD Sprint 11 --- State Management Helpers (FINAL)
"""
from __future__ import annotations

import json
import logging
import time
from typing import Any, Dict, Optional

from hfa.config.keys import RedisTTL

logger = logging.getLogger(__name__)


class StateStore:
    """
    Centralized state management for runs.
    Enforces consistent Redis contracts.

    All key patterns and TTL values are sourced from hfa.config.keys.
    """

    # Legacy class-level constants preserved for backward compatibility.
    STATE_KEY = "hfa:run:state:{}"
    META_KEY = "hfa:run:meta:{}"
    RESULT_KEY = "hfa:run:result:{}"
    RUNNING_ZSET = "hfa:cp:running"
    EXECUTION_TOKEN_KEY = "hfa:run:claim:{}"

    # Backward-compatible aliases
    TTL = RedisTTL.RUN_STATE
    CLAIM_TTL = RedisTTL.RUN_CLAIM

    # Preferred names for new code
    STATE_TTL = RedisTTL.RUN_STATE
    META_TTL = RedisTTL.RUN_META
    RESULT_TTL = RedisTTL.RUN_RESULT

    def __init__(self, redis):
        self._redis = redis

    async def create_run_meta(self, run_id: str, mapping: Dict[str, str]) -> None:
        key = self.META_KEY.format(run_id)
        await self._redis.hset(key, mapping=mapping)
        await self._redis.expire(key, self.TTL)

    async def get_run_state(self, run_id: str) -> Optional[str]:
        val = await self._redis.get(self.STATE_KEY.format(run_id))
        if val is None:
            return None
        return val.decode() if isinstance(val, bytes) else val

    async def get_run_meta(self, run_id: str) -> Dict[str, str]:
        raw = await self._redis.hgetall(self.META_KEY.format(run_id))
        if not raw:
            return {}

        out: Dict[str, str] = {}
        for k, v in raw.items():
            key = k.decode() if isinstance(k, bytes) else k
            value = v.decode() if isinstance(v, bytes) else v
            out[key] = value
        return out

    async def patch_run_meta(self, run_id: str, mapping: Dict[str, str]) -> None:
        if not mapping:
            return
        key = self.META_KEY.format(run_id)
        await self._redis.hset(key, mapping=mapping)
        await self._redis.expire(key, self.TTL)

    async def transition_state(self, run_id: str, new_state: str) -> None:
        await self._redis.set(self.STATE_KEY.format(run_id), new_state, ex=self.TTL)
        await self.patch_run_meta(run_id, {"state": new_state})
        logger.debug("State transition: run=%s -> %s", run_id, new_state)

    async def store_result(
        self,
        run_id: str,
        tenant_id: str,
        status: str,
        payload: Dict[str, Any],
        cost_cents: int = 0,
        tokens_used: int = 0,
        error: Optional[str] = None,
    ) -> None:
        key = self.RESULT_KEY.format(run_id)
        result_data: Dict[str, Any] = {
            "run_id": run_id,
            "tenant_id": tenant_id,
            "status": status,
            "payload": payload,
            "cost_cents": cost_cents,
            "tokens_used": tokens_used,
            "completed_at": time.time(),
        }
        if error:
            result_data["error"] = error

        await self._redis.set(key, json.dumps(result_data), ex=self.TTL)
        logger.debug("Result stored: run=%s status=%s", run_id, status)

    async def get_result(self, run_id: str) -> Optional[Dict[str, Any]]:
        val = await self._redis.get(self.RESULT_KEY.format(run_id))
        if not val:
            return None
        try:
            raw = val.decode() if isinstance(val, bytes) else val
            return json.loads(raw)
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            logger.error("Failed to decode result for run=%s: %s", run_id, exc)
            return None

    async def is_terminal(self, run_id: str) -> bool:
        state = await self.get_run_state(run_id)
        return state in ("done", "failed", "dead_lettered")

    async def claim_execution(self, run_id: str, worker_id: str) -> bool:
        key = self.EXECUTION_TOKEN_KEY.format(run_id)
        claimed = await self._redis.set(key, worker_id, nx=True, ex=self.CLAIM_TTL)
        return bool(claimed)

    async def renew_claim(self, run_id: str) -> bool:
        key = self.EXECUTION_TOKEN_KEY.format(run_id)
        return bool(await self._redis.expire(key, self.CLAIM_TTL))

    async def release_claim(self, run_id: str) -> None:
        await self._redis.delete(self.EXECUTION_TOKEN_KEY.format(run_id))

    async def mark_running(
        self,
        run_id: str,
        worker_id: str,
        worker_group: str,
        shard: int,
    ) -> bool:
        if not await self.claim_execution(run_id, worker_id):
            logger.warning(
                "Failed to claim execution for run=%s (already claimed)", run_id
            )
            return False

        if await self.is_terminal(run_id):
            await self.release_claim(run_id)
            return False

        now = time.time()
        await self.patch_run_meta(
            run_id,
            {
                "worker_id": worker_id,
                "worker_group": worker_group,
                "shard": str(shard),
                "started_at": str(now),
            },
        )
        await self.transition_state(run_id, "running")
        await self._redis.zadd(self.RUNNING_ZSET, {run_id: now})

        logger.info(
            "Run marked running: %s worker=%s shard=%d", run_id, worker_id, shard
        )
        return True

    async def mark_completed(self, run_id: str) -> None:
        await self._redis.zrem(self.RUNNING_ZSET, run_id)
        await self.release_claim(run_id)

    # ------------------------------------------------------------------
    # Sprint 12 — diagnostic / introspection helpers
    # ------------------------------------------------------------------

    async def get_running_runs(self, limit: int = 100) -> list[dict]:
        """
        Return up to `limit` runs currently in the running ZSET.
        Each entry contains run_id, score (started_at), and current state.
        """
        try:
            raw = await self._redis.zrange(
                self.RUNNING_ZSET, 0, limit - 1, withscores=True
            )
        except Exception:
            return []

        result: list[dict] = []
        for item in raw:
            if isinstance(item, (list, tuple)) and len(item) == 2:
                run_id_raw, score = item
            else:
                continue
            run_id = (
                run_id_raw.decode() if isinstance(run_id_raw, bytes) else run_id_raw
            )
            state = await self.get_run_state(run_id)
            result.append(
                {
                    "run_id": run_id,
                    "started_at": float(score),
                    "state": state or "unknown",
                }
            )
        return result

    async def get_claim_owner(self, run_id: str) -> str | None:
        """Return the worker_id that holds the execution claim, or None."""
        key = self.EXECUTION_TOKEN_KEY.format(run_id)
        val = await self._redis.get(key)
        if val is None:
            return None
        return val.decode() if isinstance(val, bytes) else val

    async def get_claim_ttl(self, run_id: str) -> int:
        """Return TTL seconds remaining on the execution claim, or -2 if absent."""
        key = self.EXECUTION_TOKEN_KEY.format(run_id)
        try:
            return await self._redis.ttl(key)
        except Exception:
            return -2
