"""
hfa-control/src/hfa_control/rate_limit.py
IRONCLAD Sprint 14B --- Tenant rate limiting
"""

from __future__ import annotations

import logging
import time
from typing import Optional

logger = logging.getLogger(__name__)

RATE_LIMIT_KEY = "hfa:tenant:{tenant_id}:rate"


class TenantRateLimiter:
    """
    Sliding-window tenant rate limiter.

    Stores request timestamps in Redis ZSET and enforces max_runs_per_second.
    If max_runs_per_second is None or <= 0, limiter is effectively disabled.

    Note: check_and_consume is intentionally non-atomic for Sprint 14B.
    This is acceptable as a coarse admission guard.
    """

    def __init__(self, redis) -> None:
        self._redis = redis

    def _key(self, tenant_id: str) -> str:
        return RATE_LIMIT_KEY.format(tenant_id=tenant_id)

    async def is_allowed(
        self,
        tenant_id: str,
        max_runs_per_second: Optional[float],
        *,
        now: Optional[float] = None,
    ) -> bool:
        """Check whether a request would be allowed without consuming."""
        if max_runs_per_second is None or max_runs_per_second <= 0:
            return True

        current = now if now is not None else time.time()
        window_start = current - 1.0
        key = self._key(tenant_id)

        await self._redis.zremrangebyscore(key, 0, window_start)
        current_count = await self._redis.zcard(key)

        return int(current_count) < int(max_runs_per_second)

    async def consume(
        self,
        tenant_id: str,
        *,
        now: Optional[float] = None,
    ) -> None:
        """Record one request for rate limiting."""
        current = now if now is not None else time.time()
        key = self._key(tenant_id)
        member = f"{current:.6f}:{id(self)}:{tenant_id}"

        await self._redis.zadd(key, {member: current})
        await self._redis.expire(key, 3600)

    async def check_and_consume(
        self,
        tenant_id: str,
        max_runs_per_second: Optional[float],
        *,
        now: Optional[float] = None,
    ) -> bool:
        """
        Check-and-consume operation.
        Returns True if allowed and consumption recorded.
        """
        allowed = await self.is_allowed(
            tenant_id,
            max_runs_per_second,
            now=now,
        )
        if not allowed:
            return False

        await self.consume(tenant_id, now=now)
        return True