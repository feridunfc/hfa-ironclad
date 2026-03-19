"""
hfa-control/src/hfa_control/redis_resilience.py
IRONCLAD Sprint 17.4 --- Redis resilience: backoff + health monitoring
"""

from __future__ import annotations

import asyncio
import logging
import random
import time
from typing import Any, Awaitable, Callable, TypeVar

logger = logging.getLogger(__name__)

T = TypeVar("T")


async def with_redis_retry(
    operation: Callable[..., Awaitable[T]],
    *args: Any,
    max_retries: int = 3,
    base_delay: float = 0.1,
    max_delay: float = 2.0,
    **kwargs: Any,
) -> T:
    last_exception: Exception | None = None

    for attempt in range(max_retries + 1):
        try:
            return await operation(*args, **kwargs)
        except Exception as exc:  # pragma: no cover - exact Redis exception types vary
            last_exception = exc
            if attempt == max_retries:
                logger.error("Redis operation failed after %d retries: %s", max_retries, exc)
                break
            delay = min(base_delay * (2 ** attempt), max_delay)
            jitter = delay * 0.1 * random.random()
            await asyncio.sleep(delay + jitter)

    assert last_exception is not None
    raise last_exception


class RedisHealthMonitor:
    def __init__(self, redis, check_interval: float = 5.0) -> None:
        self._redis = redis
        self._check_interval = check_interval
        self._healthy = True
        self._last_check = 0.0
        self._task: asyncio.Task | None = None
        self._running = False

    async def start(self) -> None:
        if self._task is not None:
            return
        self._running = True
        self._task = asyncio.create_task(self._health_loop(), name="redis.health")
        logger.info("RedisHealthMonitor started")

    async def close(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        logger.info("RedisHealthMonitor closed")

    async def _health_loop(self) -> None:
        while self._running:
            try:
                await self._check_health()
                await asyncio.sleep(self._check_interval)
            except asyncio.CancelledError:
                break
            except Exception as exc:  # pragma: no cover
                logger.error("Health check error: %s", exc, exc_info=True)
                await asyncio.sleep(1.0)

    async def _check_health(self) -> None:
        try:
            await self._redis.ping()
            self._healthy = True
            self._last_check = time.time()
        except Exception as exc:  # pragma: no cover
            self._healthy = False
            logger.warning("Redis health check failed: %s", exc)

    @property
    def is_healthy(self) -> bool:
        return self._healthy

    @property
    def time_since_last_check(self) -> float:
        if self._last_check == 0:
            return float("inf")
        return time.time() - self._last_check
