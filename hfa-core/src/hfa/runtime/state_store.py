
from __future__ import annotations

import hashlib
import inspect
import json
import logging
from dataclasses import dataclass
from typing import Any, Optional

from hfa.state import transition_state
from hfa.runtime.lineage_store import LineageStore
from hfa.runtime.payload_config import get_inline_threshold_bytes
from hfa.runtime.payload_metrics import add_bytes, inc_errors, inc_inline, inc_ref
from hfa.runtime.payload_store import PayloadStore
from hfa.runtime.redis_policy import RedisCallPolicy
from hfa_control.effect_config import get_completion_effect_ttl
from hfa_control.effect_ledger import EffectLedger
from hfa_control.effect_metrics import (
    DUPLICATE_COMPLETION_SUPPRESSED,
    STALE_COMPLETION_FENCED,
    increment,
)
from hfa_control.event_hooks import emit_event_background
from hfa_control.event_store import EventStore

logger = logging.getLogger(__name__)


async def _maybe_await(result):
    if inspect.isawaitable(result):
        return await result
    return result


@dataclass(frozen=True)
class CompletionResult:
    ok: bool
    status: str
    run_id: str
    worker_id: str = ""
    duplicate: bool = False
    reason: Optional[str] = None
    committed_state: Optional[str] = None


class StateStore:
    def __init__(
        self,
        redis_client: Any,
        lua_executor: Any,
        *,
        policy: RedisCallPolicy | None = None,
        event_store: EventStore | None = None,
        effect_ledger: EffectLedger | None = None,
        payload_store: PayloadStore | None = None,
        lineage_store: LineageStore | None = None,
    ) -> None:
        self._redis = redis_client
        self._lua = lua_executor
        self._policy = policy or RedisCallPolicy(max_retries=3, base_delay_ms=100)
        self._event_store = event_store
        self._effect_ledger = effect_ledger
        self._payload_store = payload_store
        self._lineage_store = lineage_store

    async def reserve_worker(self, *, worker_id: str, run_id: str, ttl_ms: int = 5000) -> Any:
        key = f"hfa:worker:{worker_id}:reservation"
        return await self._policy.execute_with_policy(
            "reserve_worker_lua",
            self._lua.execute,
            script_name="reserve_worker",
            keys=[key],
            args=[run_id, ttl_ms],
        )

    async def update_vruntime(self, *, tenant_id: str, delta: float) -> Any:
        key = f"hfa:tenant:{tenant_id}:vruntime"
        return await self._policy.execute_with_policy("update_vruntime_redis", self._redis.incrbyfloat, key, delta)

    @staticmethod
    def _completion_token(*, run_id: str, task_id: str, worker_id: str, attempt: int) -> str:
        return f"complete:{run_id}:{task_id}:worker:{worker_id}:attempt:{attempt}"

    async def store_task_output(
        self,
        *,
        task_id: str,
        run_id: str,
        payload: bytes,
        worker_id: str,
        attempt: int,
        duplicate_guard: bool = False,
    ) -> dict[str, Any]:
        if duplicate_guard:
            return {"skipped": True, "reason": "duplicate_completion_suppressed"}

        checksum = hashlib.sha256(payload).hexdigest()
        payload_size = len(payload)
        add_bytes(payload_size)

        if payload_size <= get_inline_threshold_bytes():
            record = {
                "payload_mode": "inline",
                "payload_inline": payload.decode("utf-8"),
                "payload_ref": None,
                "payload_type": "text",
                "payload_size": payload_size,
                "checksum": checksum,
                "produced_by": {"worker_id": worker_id, "attempt": attempt},
                "run_id": run_id,
            }
            inc_inline()
        else:
            if self._payload_store is None:
                raise RuntimeError("Large payload requires payload_store")
            envelope = await self._payload_store.put(payload)
            record = {
                "payload_mode": "ref",
                "payload_inline": None,
                "payload_ref": envelope.payload_ref,
                "payload_type": envelope.payload_type,
                "payload_size": envelope.payload_size,
                "checksum": envelope.checksum,
                "produced_by": {"worker_id": worker_id, "attempt": attempt},
                "run_id": run_id,
            }
            inc_ref()

        try:
            await _maybe_await(self._redis.set(f"hfa:task:{task_id}:output", json.dumps(record)))
        except Exception:
            if record.get("payload_ref"):
                logger.warning("Payload orphan risk: payload_ref=%s task_id=%s", record["payload_ref"], task_id)
            inc_errors()
            raise

        if self._lineage_store is not None:
            try:
                await self._lineage_store.record_produced_output(
                    run_id=run_id,
                    task_id=task_id,
                    output_ref=record.get("payload_ref"),
                    payload_mode=record["payload_mode"],
                    payload_size=record["payload_size"],
                    checksum=record["checksum"],
                    payload_type=record["payload_type"],
                    producer_worker_id=worker_id,
                    producer_attempt=attempt,
                )
            except Exception as exc:
                logger.warning("Lineage produced-output write failed: task_id=%s err=%s", task_id, exc)

        return record

    async def complete_once(
        self,
        *,
        run_id: str,
        task_id: str,
        worker_id: str,
        attempt: int = 1,
        status: str = "done",
        details: dict | None = None,
    ) -> CompletionResult:
        if self._effect_ledger is not None:
            token = self._completion_token(run_id=run_id, task_id=task_id, worker_id=worker_id, attempt=attempt)
            receipt = await self._effect_ledger.acquire_effect(
                run_id=run_id,
                token=token,
                effect_type="complete",
                owner_id=worker_id,
                ttl_seconds=get_completion_effect_ttl(),
            )
            if receipt.duplicate:
                receipt.reason = "duplicate_completion_suppressed"
                increment(DUPLICATE_COMPLETION_SUPPRESSED)
                return CompletionResult(
                    ok=False,
                    status="duplicate_completion_suppressed",
                    run_id=run_id,
                    worker_id=receipt.owner_id or worker_id,
                    duplicate=True,
                    reason=receipt.reason,
                    committed_state=receipt.committed_state,
                )

        owner_key = f"hfa:task:{task_id}:owner"
        current_owner = await _maybe_await(self._redis.get(owner_key))
        if isinstance(current_owner, bytes):
            current_owner = current_owner.decode("utf-8")
        if current_owner and current_owner != worker_id:
            increment(STALE_COMPLETION_FENCED)
            return CompletionResult(
                ok=False,
                status="stale_owner_fenced",
                run_id=run_id,
                worker_id=worker_id,
                duplicate=False,
                reason="stale_owner_fenced",
            )

        state_key = f"hfa:dag:task:{task_id}:state"
        current_state = await _maybe_await(self._redis.get(state_key))
        if isinstance(current_state, bytes):
            current_state = current_state.decode("utf-8")

        if current_state is None:
            await _maybe_await(self._redis.set(state_key, status))
            committed_state = status
        else:
            tr = await transition_state(
                self._redis,
                run_id=run_id,
                target_state=status,
                state_key=state_key,
                expected_state=current_state,
                lua_loader=None,
            )
            if not tr.ok:
                return CompletionResult(
                    ok=False,
                    status=tr.reason,
                    run_id=run_id,
                    worker_id=worker_id,
                    duplicate=False,
                    reason=tr.reason,
                    committed_state=tr.to_state if tr.ok else tr.from_state,
                )
            committed_state = tr.to_state

        await _maybe_await(self._redis.set(owner_key, worker_id))

        if status == "done":
            emit_event_background(
                self._event_store,
                run_id=run_id,
                event_type=EventStore.EVENT_TASK_COMPLETED,
                worker_id=worker_id,
                details=details,
            )
        else:
            emit_event_background(
                self._event_store,
                run_id=run_id,
                event_type=EventStore.EVENT_TASK_FAILED,
                worker_id=worker_id,
                details=details,
            )

        return CompletionResult(
            ok=True,
            status=status,
            run_id=run_id,
            worker_id=worker_id,
            duplicate=False,
            committed_state=committed_state,
        )
