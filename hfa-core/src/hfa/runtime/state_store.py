
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from hfa.runtime.redis_policy import RedisCallPolicy
from hfa_control.effect_ledger import EffectLedger
from hfa_control.event_hooks import emit_event_background
from hfa_control.event_store import EventStore


@dataclass(frozen=True)
class CompletionResult:
    ok: bool
    status: str
    run_id: str
    worker_id: str = ""
    duplicate: bool = False


class StateStore:
    def __init__(
        self,
        redis_client: Any,
        lua_executor: Any,
        *,
        policy: RedisCallPolicy | None = None,
        event_store: EventStore | None = None,
        effect_ledger: EffectLedger | None = None,
    ) -> None:
        self._redis = redis_client
        self._lua = lua_executor
        self._policy = policy or RedisCallPolicy(max_retries=3, base_delay_ms=100)
        self._event_store = event_store
        self._effect_ledger = effect_ledger

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
            )
            if receipt.duplicate:
                return CompletionResult(
                    ok=False,
                    status="duplicate_completion_suppressed",
                    run_id=run_id,
                    worker_id=receipt.owner_id or worker_id,
                    duplicate=True,
                )

        owner_key = f"hfa:task:{task_id}:owner"
        current_owner = await self._redis.get(owner_key)
        if isinstance(current_owner, bytes):
            current_owner = current_owner.decode("utf-8")
        if current_owner and current_owner != worker_id:
            return CompletionResult(
                ok=False,
                status="stale_owner_fenced",
                run_id=run_id,
                worker_id=worker_id,
                duplicate=False,
            )

        state_key = f"hfa:dag:task:{task_id}:state"
        await self._redis.set(state_key, status)
        await self._redis.set(owner_key, worker_id)

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
        )

    async def mark_completed(self, *, run_id: str, worker_id: str | None = None, details: dict | None = None) -> bool:
        emit_event_background(self._event_store, run_id=run_id, event_type=EventStore.EVENT_TASK_COMPLETED, worker_id=worker_id, details=details)
        return True

    async def mark_failed(self, *, run_id: str, worker_id: str | None = None, details: dict | None = None) -> bool:
        emit_event_background(self._event_store, run_id=run_id, event_type=EventStore.EVENT_TASK_FAILED, worker_id=worker_id, details=details)
        return True

    async def mark_requeued(self, *, run_id: str, worker_id: str | None = None, details: dict | None = None) -> bool:
        emit_event_background(self._event_store, run_id=run_id, event_type=EventStore.EVENT_TASK_REQUEUED, worker_id=worker_id, details=details)
        return True
