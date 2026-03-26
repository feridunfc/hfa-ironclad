
from __future__ import annotations

from dataclasses import dataclass

from hfa_control.event_hooks import emit_event_background
from hfa_control.event_store import EventStore


@dataclass(frozen=True)
class TaskClaimResult:
    ok: bool
    status: str
    task_id: str
    worker_id: str
    tenant_id: str = ""


class TaskClaimService:
    def __init__(self, dag_lua, event_store: EventStore | None = None) -> None:
        self._dag_lua = dag_lua
        self._event_store = event_store

    async def claim(self, *, task_id: str, worker_id: str, tenant_id: str = "", now_ms: int = 0) -> TaskClaimResult:
        res = await self._dag_lua.claim_task(
            task_id=task_id,
            worker_instance_id=worker_id,
            claimed_at_ms=now_ms,
        )
        ok = bool(getattr(res, "ok", False) if not isinstance(res, dict) else res.get("ok"))
        status = getattr(res, "status", "") if not isinstance(res, dict) else str(res.get("status", ""))
        result = TaskClaimResult(ok=ok, status=status, task_id=task_id, worker_id=worker_id, tenant_id=tenant_id)
        if result.ok:
            emit_event_background(
                self._event_store,
                run_id=task_id,
                event_type=EventStore.EVENT_TASK_CLAIMED,
                worker_id=worker_id,
                details={"tenant_id": tenant_id} if tenant_id else None,
            )
        return result


class TaskClaimManager(TaskClaimService):
    """Backward-compatible alias for legacy tests/imports."""
    pass
