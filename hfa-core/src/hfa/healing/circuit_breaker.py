"""
hfa-core/src/hfa/healing/circuit_breaker.py
IRONCLAD Sprint 1 STUB — Full state-machine implemented in Sprint 4.
This stub keeps Sprint 1 fully runnable without Sprint 4 code.
"""
import logging
import time
from enum import Enum
from threading import Lock

logger = logging.getLogger(__name__)


class CircuitState(str, Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitOpenError(Exception):
    """Raised when circuit breaker is open and requests are blocked."""


class CircuitBreaker:
    """
    Sprint-1 stub: thread-safe circuit breaker.

    States:
      CLOSED     → normal operation
      OPEN       → fail-fast, no calls allowed
      HALF_OPEN  → one probe call allowed after recovery_timeout

    Sprint 4 adds: fingerprint-based deduplication, Redis persistence,
    Prometheus metrics, and full self-healing integration.
    """

    def __init__(
        self,
        name: str,
        failure_threshold: int = 5,
        recovery_timeout: int = 60,
    ) -> None:
        self.name = name
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout

        self._state: CircuitState = CircuitState.CLOSED
        self._failure_count: int = 0
        self._last_failure_time: float = 0.0
        self._lock = Lock()

        logger.info(
            "CircuitBreaker(%s) initialised: threshold=%d, recovery=%ds",
            name, failure_threshold, recovery_timeout,
        )

    def is_open(self) -> bool:
        """Return True when the circuit is fully open (fail-fast mode)."""
        with self._lock:
            if self._state == CircuitState.OPEN:
                if time.monotonic() - self._last_failure_time >= self.recovery_timeout:
                    self._state = CircuitState.HALF_OPEN
                    logger.info("CircuitBreaker(%s) → HALF_OPEN (probe window)", self.name)
                    return False
                return True
            return False

    def record_success(self) -> None:
        """Call after a successful operation to reset the breaker."""
        with self._lock:
            if self._state != CircuitState.CLOSED:
                logger.info("CircuitBreaker(%s) → CLOSED (success)", self.name)
            self._state = CircuitState.CLOSED
            self._failure_count = 0

    def record_failure(self) -> None:
        """Call after a failed operation; trips the breaker at threshold."""
        with self._lock:
            self._failure_count += 1
            self._last_failure_time = time.monotonic()

            if self._failure_count >= self.failure_threshold:
                if self._state != CircuitState.OPEN:
                    logger.warning(
                        "CircuitBreaker(%s) → OPEN after %d failures",
                        self.name, self._failure_count,
                    )
                self._state = CircuitState.OPEN

    @property
    def state(self) -> CircuitState:
        return self._state

    @property
    def failure_count(self) -> int:
        return self._failure_count
