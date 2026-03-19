"""
hfa-core/src/hfa/healing/circuit_breaker.py
IRONCLAD Sprint 17.3 --- Async Circuit Breaker
"""

from __future__ import annotations

import asyncio
import logging
import time
from enum import Enum
from typing import Any, Awaitable, Callable, TypeVar

logger = logging.getLogger(__name__)

T = TypeVar("T")


class CircuitState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitOpenError(Exception):
    """Raised when circuit is OPEN."""


class CircuitBreaker:
    def __init__(
        self,
        name: str,
        failure_threshold: int = 5,
        recovery_timeout: float = 30.0,
        half_open_max_calls: int = 1,
    ) -> None:
        self.name = name
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.half_open_max_calls = half_open_max_calls

        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._last_failure_time = 0.0
        self._half_open_calls = 0
        self._lock = asyncio.Lock()

    @property
    def state(self) -> CircuitState:
        return self._state

    async def call(self, func: Callable[..., Awaitable[T]], *args: Any, **kwargs: Any) -> T:
        async with self._lock:
            if self._state == CircuitState.OPEN:
                if time.time() - self._last_failure_time > self.recovery_timeout:
                    logger.info("Circuit %s transitioning OPEN → HALF_OPEN", self.name)
                    self._state = CircuitState.HALF_OPEN
                    self._half_open_calls = 0
                else:
                    raise CircuitOpenError(f"Circuit {self.name} is open")

            if self._state == CircuitState.HALF_OPEN:
                if self._half_open_calls >= self.half_open_max_calls:
                    raise CircuitOpenError(f"Circuit {self.name} is half-open (busy)")
                self._half_open_calls += 1

        try:
            result = await func(*args, **kwargs)
        except Exception:
            await self._record_failure()
            raise

        await self._record_success()
        return result

    async def _record_failure(self) -> None:
        async with self._lock:
            self._failure_count += 1
            self._last_failure_time = time.time()

            if self._state == CircuitState.HALF_OPEN:
                logger.warning("Circuit %s HALF_OPEN test call failed → OPEN", self.name)
                self._state = CircuitState.OPEN
                self._half_open_calls = 0
            elif self._state == CircuitState.CLOSED and self._failure_count >= self.failure_threshold:
                logger.warning(
                    "Circuit %s CLOSED → OPEN (failures=%d/%d)",
                    self.name,
                    self._failure_count,
                    self.failure_threshold,
                )
                self._state = CircuitState.OPEN

    async def _record_success(self) -> None:
        async with self._lock:
            if self._state == CircuitState.HALF_OPEN:
                logger.info("Circuit %s HALF_OPEN test call succeeded → CLOSED", self.name)
                self._state = CircuitState.CLOSED
                self._failure_count = 0
                self._half_open_calls = 0
            elif self._state == CircuitState.CLOSED:
                self._failure_count = 0
