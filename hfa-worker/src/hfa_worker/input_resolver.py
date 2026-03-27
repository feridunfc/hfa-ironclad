
from __future__ import annotations

import inspect
import json
from dataclasses import dataclass
from typing import Any

from hfa.runtime.lineage_store import LineageStore
from hfa.runtime.payload_store import PayloadStore


async def _maybe_await(result):
    if inspect.isawaitable(result):
        return await result
    return result


@dataclass(frozen=True)
class ResolvedOutput:
    data: Any
    payload_ref: str | None
    checksum: str | None


class InputResolver:
    def __init__(
        self,
        redis_client,
        payload_store: PayloadStore | None = None,
        lineage_store: LineageStore | None = None,
    ) -> None:
        self._redis = redis_client
        self._payload_store = payload_store
        self._lineage_store = lineage_store

    async def resolve_input(
        self,
        task_id: str,
        *,
        run_id: str = "",
        consumer_task_id: str = "",
        consumer_worker_id: str = "",
        consumer_attempt: int = 0,
    ) -> ResolvedOutput:
        raw = await _maybe_await(self._redis.get(f"hfa:task:{task_id}:output"))
        if raw is None:
            raise KeyError(f"Missing output for task_id={task_id}")
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        record = json.loads(raw)

        mode = record["payload_mode"]
        if mode == "inline":
            resolved = ResolvedOutput(
                data=record["payload_inline"],
                payload_ref=record.get("payload_ref"),
                checksum=record.get("checksum"),
            )
        elif mode == "ref":
            if self._payload_store is None:
                raise RuntimeError("Payload ref found but no payload_store configured")
            data = await self._payload_store.get(
                record["payload_ref"],
                expected_checksum=record.get("checksum"),
            )
            if record.get("payload_type") == "text":
                data = data.decode("utf-8")
            resolved = ResolvedOutput(
                data=data,
                payload_ref=record.get("payload_ref"),
                checksum=record.get("checksum"),
            )
        else:
            raise ValueError(f"Unknown payload_mode={mode}")

        if self._lineage_store is not None and run_id and consumer_task_id and consumer_worker_id and consumer_attempt:
            await self._lineage_store.record_consumed_input(
                run_id=run_id,
                parent_task_id=task_id,
                consumer_task_id=consumer_task_id,
                consumed_output_ref=resolved.payload_ref,
                checksum=resolved.checksum or "",
                consumer_worker_id=consumer_worker_id,
                consumer_attempt=consumer_attempt,
            )
            await self._lineage_store.record_lineage_edge(
                run_id=run_id,
                parent_task_id=task_id,
                child_task_id=consumer_task_id,
            )

        return resolved
