"""
hfa-control/src/hfa_control/rate_limit.py
IRONCLAD Sprint 15 — Atomic tenant rate limiting via Redis Lua

Migration from Sprint 14B
-------------------------
Sprint 14B used a non-atomic check-then-consume pattern:
  1. ZREMRANGEBYSCORE (trim)
  2. ZCARD (read count)
  3. ZADD (write if allowed)

This creates a TOCTOU window: under high concurrency (e.g. 50 simultaneous
admission requests) the count can be read as "under limit" by multiple
goroutines before any of them write, allowing limit * N over-admission.

Sprint 15 fix
-------------
All three operations are collapsed into a single Lua script executed
atomically on the Redis server. There is no TOCTOU window.

Algorithm: sliding-window with ZSET (unchanged from 14B semantics)
  - Window is 1 second (now - 1.0 to now)
  - Expired entries are pruned inside the same Lua transaction
  - Each entry is a unique member to avoid ZADD score collision

Backward compatibility
----------------------
* check_and_consume() signature unchanged
* is_allowed() preserved as read-only helper (still two-step, for inspection only)
* consume() preserved for explicit recording
* Disabled behavior (None / <= 0) unchanged

Fakeredis fallback
------------------
fakeredis does not support redis.eval(). A non-atomic fallback identical to
the Sprint 14B behavior is provided for unit tests. Real Redis (via
Testcontainers in integration tests) exercises the Lua path.
"""

from __future__ import annotations

import logging
import time
from typing import Optional

from hfa.config.keys import RedisKey, RedisTTL

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Lua script — atomic sliding-window rate limit
# ---------------------------------------------------------------------------
# KEYS[1]  = tenant rate ZSET key
# ARGV[1]  = max_runs_per_second  (int)
# ARGV[2]  = now                  (float, microsecond precision string)
# ARGV[3]  = unique member string
# ARGV[4]  = TTL seconds          (int)
#
# Returns: 1 = admitted, 0 = rejected
#
_ATOMIC_RATE_LIMIT_LUA = """
local key          = KEYS[1]
local limit        = tonumber(ARGV[1])
local now          = tonumber(ARGV[2])
local member       = ARGV[3]
local ttl          = tonumber(ARGV[4])
local window_start = now - 1.0

redis.call('ZREMRANGEBYSCORE', key, 0, window_start)

local current = redis.call('ZCARD', key)
if current >= limit then
    return 0
end

redis.call('ZADD', key, now, member)
redis.call('EXPIRE', key, ttl)
return 1
"""


class TenantRateLimiter:
    """
    Atomic sliding-window tenant rate limiter.

    Sprint 15: check-and-consume is now a single atomic Lua operation.

    Args:
        redis: aioredis.Redis (or fakeredis for unit tests).
    """

    def __init__(self, redis) -> None:
        self._redis = redis

    def _key(self, tenant_id: str) -> str:
        return RedisKey.tenant_rate(tenant_id)

    # ------------------------------------------------------------------
    # Public API (backward-compatible)
    # ------------------------------------------------------------------

    async def check_and_consume(
        self,
        tenant_id: str,
        max_runs_per_second: Optional[float],
        *,
        now: Optional[float] = None,
    ) -> bool:
        """
        Atomically check the rate limit and record the request if allowed.

        Returns True if admitted, False if rejected.
        Disabled when max_runs_per_second is None or <= 0.

        Uses Lua on real Redis; falls back to non-atomic path on fakeredis.
        """
        if max_runs_per_second is None or max_runs_per_second <= 0:
            return True

        current = now if now is not None else time.time()
        key = self._key(tenant_id)
        member = f"{tenant_id}:{current:.6f}:{id(self)}"

        try:
            result = await self._redis.eval(
                _ATOMIC_RATE_LIMIT_LUA,
                1,          # number of KEYS
                key,        # KEYS[1]
                int(max_runs_per_second),   # ARGV[1]
                current,                    # ARGV[2]
                member,                     # ARGV[3]
                RedisTTL.TENANT_RATE,       # ARGV[4]
            )
            return bool(result)

        except Exception as exc:
            # fakeredis does not support eval — fall back to non-atomic path
            if "unknown command" in str(exc).lower() or "eval" in str(exc).lower():
                logger.debug(
                    "TenantRateLimiter: eval not supported, using non-atomic fallback "
                    "(acceptable for unit tests with fakeredis)"
                )
                return await self._check_and_consume_non_atomic(
                    tenant_id, max_runs_per_second, current=current
                )
            logger.error(
                "TenantRateLimiter.check_and_consume error: tenant=%s %s",
                tenant_id,
                exc,
            )
            # Fail-open on unexpected Redis error — do not block admission
            return True

    async def is_allowed(
        self,
        tenant_id: str,
        max_runs_per_second: Optional[float],
        *,
        now: Optional[float] = None,
    ) -> bool:
        """
        Read-only check — does NOT consume a slot.
        Used for inspection/metrics; not for gating admission.
        """
        if max_runs_per_second is None or max_runs_per_second <= 0:
            return True

        current = now if now is not None else time.time()
        window_start = current - 1.0
        key = self._key(tenant_id)

        await self._redis.zremrangebyscore(key, 0, window_start)
        count = await self._redis.zcard(key)
        return int(count) < int(max_runs_per_second)

    async def consume(
        self,
        tenant_id: str,
        *,
        now: Optional[float] = None,
    ) -> None:
        """
        Explicitly record one request (for testing / manual replay).
        Not used in the normal admission path (use check_and_consume).
        """
        current = now if now is not None else time.time()
        key = self._key(tenant_id)
        member = f"{current:.6f}:{id(self)}:{tenant_id}"
        await self._redis.zadd(key, {member: current})
        await self._redis.expire(key, RedisTTL.TENANT_RATE)

    # ------------------------------------------------------------------
    # Non-atomic fallback (fakeredis / unit tests)
    # ------------------------------------------------------------------

    async def _check_and_consume_non_atomic(
        self,
        tenant_id: str,
        max_runs_per_second: float,
        *,
        current: float,
    ) -> bool:
        """
        Sprint 14B-equivalent check-then-consume.
        Used ONLY when Redis eval is unavailable (fakeredis in unit tests).
        Production always takes the Lua path.
        """
        window_start = current - 1.0
        key = self._key(tenant_id)

        await self._redis.zremrangebyscore(key, 0, window_start)
        count = await self._redis.zcard(key)

        if int(count) >= int(max_runs_per_second):
            return False

        member = f"{tenant_id}:{current:.6f}:{id(self)}"
        await self._redis.zadd(key, {member: current})
        await self._redis.expire(key, RedisTTL.TENANT_RATE)
        return True