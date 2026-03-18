from __future__ import annotations

import logging
import time
from typing import Optional

from hfa.config.keys import RedisKey

logger = logging.getLogger(__name__)

MAX_PRIORITY = 100
_PRIORITY_BAND = 1_000_000_000_000
QUEUE_IDLE_TTL = 86_400


class TenantQueue:
    """
    Per-tenant run queue backed by Redis ZSET.

    Score = (priority - 1) * 1e12 + admitted_at_micros
    Lower score = higher urgency.

    Args:
        redis: aioredis.Redis client.
    """

    def __init__(self, redis) -> None:
        self._redis = redis

    def _score(self, priority: int, ts: float) -> float:
        """
        Lower score = higher priority.

        Priority 1 gets band 0, priority 2 gets band 1e12, etc.
        Within the same priority, earlier timestamps win.
        """
        priority = max(1, min(int(priority), MAX_PRIORITY))
        ts_micros = int(ts * 1_000_000) % _PRIORITY_BAND
        return float((priority - 1) * _PRIORITY_BAND + ts_micros)

    async def enqueue(
        self,
        tenant_id: str,
        run_id: str,
        priority: int = 5,
        *,
        now: Optional[float] = None,
    ) -> None:
        priority = max(1, min(int(priority), MAX_PRIORITY))
        ts = now if now is not None else time.time()
        score = self._score(priority, ts)

        key = RedisKey.tenant_queue(tenant_id)
        active_key = RedisKey.tenant_active_set()

        pipe = self._redis.pipeline()
        pipe.zadd(key, {run_id: score}, nx=True)
        pipe.sadd(active_key, tenant_id)
        pipe.expire(key, QUEUE_IDLE_TTL)
        await pipe.execute()

        logger.debug(
            "TenantQueue.enqueue: tenant=%s run=%s priority=%d score=%.0f",
            tenant_id,
            run_id,
            priority,
            score,
        )

    async def dequeue(self, tenant_id: str) -> Optional[str]:
        key = RedisKey.tenant_queue(tenant_id)
        active_key = RedisKey.tenant_active_set()

        result = await self._redis.zpopmin(key, count=1)
        if not result:
            await self._redis.srem(active_key, tenant_id)
            return None

        run_id_raw, score = result[0]
        run_id = run_id_raw.decode() if isinstance(run_id_raw, bytes) else run_id_raw

        remaining = await self._redis.zcard(key)
        if remaining == 0:
            await self._redis.srem(active_key, tenant_id)

        logger.debug(
            "TenantQueue.dequeue: tenant=%s run=%s score=%.0f remaining=%d",
            tenant_id,
            run_id,
            score,
            remaining,
        )
        return run_id

    async def remove(self, tenant_id: str, run_id: str) -> bool:
        key = RedisKey.tenant_queue(tenant_id)
        removed = await self._redis.zrem(key, run_id)
        if removed:
            remaining = await self._redis.zcard(key)
            if remaining == 0:
                await self._redis.srem(RedisKey.tenant_active_set(), tenant_id)
        return bool(removed)

    async def depth(self, tenant_id: str) -> int:
        return await self._redis.zcard(RedisKey.tenant_queue(tenant_id))

    async def peek(self, tenant_id: str) -> Optional[str]:
        key = RedisKey.tenant_queue(tenant_id)
        result = await self._redis.zrange(key, 0, 0)
        if not result:
            return None
        raw = result[0]
        return raw.decode() if isinstance(raw, bytes) else raw

    async def active_tenants(self) -> list[str]:
        raw = await self._redis.smembers(RedisKey.tenant_active_set())
        return [r.decode() if isinstance(r, bytes) else r for r in raw]

    async def all_depths(self) -> dict[str, int]:
        tenants = await self.active_tenants()
        result: dict[str, int] = {}
        for tid in tenants:
            d = await self.depth(tid)
            if d > 0:
                result[tid] = d
            else:
                await self._redis.srem(RedisKey.tenant_active_set(), tid)
        return result