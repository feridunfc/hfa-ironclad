import asyncio

import pytest

from hfa.healing.circuit_breaker import CircuitBreaker, CircuitOpenError, CircuitState


@pytest.mark.asyncio
class TestSprint17CircuitBreaker:
    async def test_closed_state_passes_calls(self):
        cb = CircuitBreaker("test", failure_threshold=2)

        async def success():
            return "ok"

        result = await cb.call(success)
        assert result == "ok"
        assert cb.state == CircuitState.CLOSED

    async def test_opens_after_threshold_failures(self):
        cb = CircuitBreaker("test", failure_threshold=2, recovery_timeout=0.01)

        async def fail():
            raise ValueError("test error")

        with pytest.raises(ValueError):
            await cb.call(fail)
        assert cb.state == CircuitState.CLOSED

        with pytest.raises(ValueError):
            await cb.call(fail)
        assert cb.state == CircuitState.OPEN

    async def test_open_fails_fast(self):
        cb = CircuitBreaker("test", failure_threshold=1)

        async def fail():
            raise ValueError("test error")

        with pytest.raises(ValueError):
            await cb.call(fail)
        with pytest.raises(CircuitOpenError):
            await cb.call(fail)

    async def test_half_open_recovers_after_timeout(self):
        cb = CircuitBreaker("test", failure_threshold=1, recovery_timeout=0.01)

        async def fail():
            raise ValueError("test error")

        async def success():
            return "ok"

        with pytest.raises(ValueError):
            await cb.call(fail)
        await asyncio.sleep(0.02)
        assert await cb.call(success) == "ok"
        assert cb.state == CircuitState.CLOSED
