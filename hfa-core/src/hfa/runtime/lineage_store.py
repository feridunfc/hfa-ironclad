
from __future__ import annotations

import inspect
import json
import time
from typing import Any, Optional

from hfa.runtime.lineage_config import get_lineage_ttl_seconds, is_lineage_enabled


async def _maybe_await(result):
    if inspect.isawaitable(result):
        return await result
    return result


def _produced_key(run_id: str, task_id: str) -> str:
    return f"hfa:lineage:run:{run_id}:task:{task_id}:produced"


def _consumers_key(run_id: str, task_id: str) -> str:
    return f"hfa:lineage:run:{run_id}:task:{task_id}:consumers"


def _edges_key(run_id: str) -> str:
    return f"hfa:lineage:run:{run_id}:edges"


class LineageStore:
    def __init__(self, redis_client: Any):
        self._redis = redis_client

    async def record_produced_output(
        self,
        *,
        run_id: str,
        task_id: str,
        output_ref: str | None,
        payload_mode: str,
        payload_size: int,
        checksum: str,
        payload_type: str,
        producer_worker_id: str,
        producer_attempt: int,
    ) -> bool:
        if not is_lineage_enabled():
            return False
        key = _produced_key(run_id, task_id)
        value = {
            "run_id": run_id,
            "task_id": task_id,
            "output_ref": output_ref,
            "payload_mode": payload_mode,
            "payload_size": payload_size,
            "checksum": checksum,
            "payload_type": payload_type,
            "producer_worker_id": producer_worker_id,
            "producer_attempt": producer_attempt,
            "produced_at_ms": int(time.time() * 1000),
        }
        ok = await _maybe_await(self._redis.set(key, json.dumps(value), nx=True, ex=get_lineage_ttl_seconds()))
        return bool(ok)

    async def record_consumed_input(
        self,
        *,
        run_id: str,
        parent_task_id: str,
        consumer_task_id: str,
        consumed_output_ref: str | None,
        checksum: str,
        consumer_worker_id: str,
        consumer_attempt: int,
    ) -> bool:
        if not is_lineage_enabled():
            return False
        key = _consumers_key(run_id, parent_task_id)
        field = f"{consumer_task_id}:{consumer_attempt}"
        value = json.dumps(
            {
                "run_id": run_id,
                "parent_task_id": parent_task_id,
                "consumer_task_id": consumer_task_id,
                "consumed_output_ref": consumed_output_ref,
                "checksum": checksum,
                "consumer_worker_id": consumer_worker_id,
                "consumer_attempt": consumer_attempt,
                "consumed_at_ms": int(time.time() * 1000),
            }
        )
        created = await _maybe_await(self._redis.hsetnx(key, field, value))
        await _maybe_await(self._redis.expire(key, get_lineage_ttl_seconds()))
        return bool(created)

    async def record_lineage_edge(
        self,
        *,
        run_id: str,
        parent_task_id: str,
        child_task_id: str,
    ) -> bool:
        if not is_lineage_enabled():
            return False
        key = _edges_key(run_id)
        field = f"{parent_task_id}->{child_task_id}"
        value = json.dumps(
            {
                "run_id": run_id,
                "parent_task_id": parent_task_id,
                "child_task_id": child_task_id,
                "recorded_at_ms": int(time.time() * 1000),
            }
        )
        created = await _maybe_await(self._redis.hsetnx(key, field, value))
        await _maybe_await(self._redis.expire(key, get_lineage_ttl_seconds()))
        return bool(created)

    async def get_produced_output(self, *, run_id: str, task_id: str) -> Optional[dict[str, Any]]:
        raw = await _maybe_await(self._redis.get(_produced_key(run_id, task_id)))
        if raw is None:
            return None
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        return json.loads(raw)

    async def get_consumers(self, *, run_id: str, task_id: str) -> list[dict[str, Any]]:
        raw = await _maybe_await(self._redis.hgetall(_consumers_key(run_id, task_id)))
        items = []
        for value in raw.values():
            if isinstance(value, bytes):
                value = value.decode("utf-8")
            items.append(json.loads(value))
        items.sort(key=lambda x: (x["consumer_task_id"], x["consumer_attempt"]))
        return items

    async def get_run_edges(self, *, run_id: str) -> list[dict[str, Any]]:
        raw = await _maybe_await(self._redis.hgetall(_edges_key(run_id)))
        items = []
        for value in raw.values():
            if isinstance(value, bytes):
                value = value.decode("utf-8")
            items.append(json.loads(value))
        items.sort(key=lambda x: (x["parent_task_id"], x["child_task_id"]))
        return items

    async def get_lineage_summary(self, *, run_id: str, task_id: str) -> dict[str, Any]:
        return {
            "produced": await self.get_produced_output(run_id=run_id, task_id=task_id),
            "consumers": await self.get_consumers(run_id=run_id, task_id=task_id),
            "edges": await self.get_run_edges(run_id=run_id),
        }
