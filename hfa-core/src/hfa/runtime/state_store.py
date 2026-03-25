
from __future__ import annotations

from typing import Any

from hfa.runtime.redis_policy import RedisCallPolicy


class StateStore:
    def __init__(self, redis_client: Any, lua_executor: Any, *, policy: RedisCallPolicy | None = None) -> None:
        self._redis = redis_client
        self._lua = lua_executor
        self._policy = policy or RedisCallPolicy(max_retries=3, base_delay_ms=100)

    async def reserve_worker(
        self,
        *,
        worker_id: str,
        run_id: str,
        ttl_ms: int = 5000,
    ) -> Any:
        key = f"hfa:worker:{worker_id}:reservation"
        return await self._policy.execute_with_policy(
            "reserve_worker_lua",
            self._lua.execute,
            script_name="reserve_worker",
            keys=[key],
            args=[run_id, ttl_ms],
        )

    async def update_vruntime(
        self,
        *,
        tenant_id: str,
        delta: float,
    ) -> Any:
        key = f"hfa:tenant:{tenant_id}:vruntime"
        return await self._policy.execute_with_policy(
            "update_vruntime_redis",
            self._redis.incrbyfloat,
            key,
            delta,
        )
