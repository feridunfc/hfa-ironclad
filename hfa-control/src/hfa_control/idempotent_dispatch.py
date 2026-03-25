
from __future__ import annotations

from dataclasses import dataclass

from hfa.runtime.idempotency_store import IdempotencyStore


@dataclass(frozen=True)
class IdempotentDispatchResult:
    ok: bool
    status: str
    token_key: str = ""
    existing_value: str = ""


class IdempotentDispatchGuard:
    def __init__(self, store: IdempotencyStore) -> None:
        self._store = store

    async def guard(
        self,
        *,
        task_id: str,
        run_id: str,
        worker_id: str,
    ) -> IdempotentDispatchResult:
        token_value = f"{run_id}:{worker_id}"
        result = await self._store.acquire_dispatch_token(
            task_id=task_id,
            token_value=token_value,
        )
        return IdempotentDispatchResult(
            ok=result.accepted,
            status=result.reason,
            token_key=result.token_key,
            existing_value=result.existing_value,
        )
