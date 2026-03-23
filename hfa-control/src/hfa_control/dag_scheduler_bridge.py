from __future__ import annotations

"""V23.3 bridge helpers.

These helpers let SchedulerLoop remain the single dispatch authority while DAG
execution evolves in parallel. The loop only needs to know how to consume READY
DAG tasks and pass them into DagLua.task_dispatch_commit(...).
"""

from dataclasses import dataclass
import json
import time
from typing import Optional

from hfa.dag.schema import DagRedisKey, DagTaskDispatchInput
from hfa.config.keys import RedisKey


@dataclass(frozen=True)
class DagReadyTask:
    task_id: str
    run_id: str
    tenant_id: str
    agent_type: str
    priority: int
    admitted_at: float
    payload_json: str
    preferred_region: str
    preferred_placement: str
    trace_parent: str
    trace_state: str


class DagReadyQueue:
    def __init__(self, redis) -> None:
        self._redis = redis

    async def peek(self, tenant_id: str) -> Optional[str]:
        rows = await self._redis.zrange(DagRedisKey.task_ready_queue(tenant_id), 0, 0)
        if not rows:
            return None
        task_id = rows[0]
        return task_id.decode() if isinstance(task_id, bytes) else task_id

    async def dequeue(self, tenant_id: str) -> Optional[str]:
        key = DagRedisKey.task_ready_queue(tenant_id)
        rows = await self._redis.zrange(key, 0, 0)
        if not rows:
            return None
        task_id = rows[0]
        task_id = task_id.decode() if isinstance(task_id, bytes) else task_id
        await self._redis.zrem(key, task_id)
        return task_id

    async def rebuild_dispatch_input(
        self,
        task_id: str,
        *,
        worker_group: str,
        shard: int,
        running_zset: str,
        control_stream: str,
        shard_stream: str,
        region: str = '',
    ) -> Optional[DagTaskDispatchInput]:
        raw = await self._redis.hgetall(DagRedisKey.task_meta(task_id))
        if not raw:
            return None

        def _s(k: str) -> str:
            v = raw.get(k.encode()) or raw.get(k)
            return (v.decode() if isinstance(v, bytes) else v) or ''

        def _i(k: str, default: int = 0) -> int:
            try:
                return int(_s(k) or str(default))
            except Exception:
                return default

        def _f(k: str, default: float = 0.0) -> float:
            try:
                return float(_s(k) or str(default))
            except Exception:
                return default

        return DagTaskDispatchInput(
            task_id=task_id,
            run_id=_s('run_id'),
            tenant_id=_s('tenant_id'),
            agent_type=_s('agent_type'),
            worker_group=worker_group,
            shard=shard,
            priority=_i('priority', 5),
            admitted_at=_f('admitted_at', time.time()),
            scheduled_at=time.time(),
            running_zset=running_zset,
            control_stream=control_stream,
            shard_stream=shard_stream,
            region=region,
            policy=_s('preferred_placement') or 'LEAST_LOADED',
            trace_parent=_s('trace_parent'),
            trace_state=_s('trace_state'),
            payload_json=_s('payload_json') or '{}',
        )
