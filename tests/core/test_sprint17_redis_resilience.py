import asyncio
from unittest.mock import AsyncMock

import fakeredis.aioredis as faredis
import pytest

from hfa_control.redis_resilience import RedisHealthMonitor, with_redis_retry


@pytest.mark.asyncio
class TestSprint17RedisResilience:
    async def test_retry_success_first_try(self):
        mock_op = AsyncMock(return_value="success")
        result = await with_redis_retry(mock_op, max_retries=3)
        assert result == "success"
        mock_op.assert_called_once()

    async def test_retry_after_failures(self):
        mock_op = AsyncMock()
        mock_op.side_effect = [Exception("fail"), Exception("fail"), "success"]
        result = await with_redis_retry(mock_op, max_retries=3, base_delay=0.001)
        assert result == "success"
        assert mock_op.call_count == 3

    async def test_health_monitor(self):
        redis = faredis.FakeRedis()
        monitor = RedisHealthMonitor(redis, check_interval=0.05)
        await monitor.start()
        await asyncio.sleep(0.12)
        assert monitor.is_healthy is True
        await monitor.close()
