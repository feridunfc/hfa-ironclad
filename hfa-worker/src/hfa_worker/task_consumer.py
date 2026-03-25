
from __future__ import annotations

from dataclasses import dataclass

from hfa.dag.capabilities import TaskCapabilitySpec, WorkerCapabilitySpec
from hfa_control.capability_router import CapabilityRouter
from hfa_control.task_claim import TaskClaimManager, TaskClaimResult
from hfa_control.task_recovery import TaskHeartbeatManager
from hfa_worker.task_context import TaskContext
from hfa_worker.task_executor import TaskExecutor, TaskExecutionResult
from hfa_worker.task_heartbeat import HeartbeatLoop


@dataclass(frozen=True)
class ConsumedTaskResult:
    claimed: TaskClaimResult | None = None
    executed: TaskExecutionResult | None = None
    rejected_reason: str = ""


class TaskConsumer:
    def __init__(
        self,
        claim_manager: TaskClaimManager,
        executor: TaskExecutor,
        *,
        worker_capabilities: list[str] | None = None,
        heartbeat_manager: TaskHeartbeatManager | None = None,
        heartbeat_interval_ms: int = 5_000,
    ) -> None:
        self._claim_manager = claim_manager
        self._executor = executor
        self._worker_capabilities = worker_capabilities or []
        self._heartbeat_manager = heartbeat_manager
        self._heartbeat_interval_ms = heartbeat_interval_ms

    async def consume_once(self, ctx: TaskContext, *, claimed_at_ms: int) -> ConsumedTaskResult:
        match = CapabilityRouter.matches(
            TaskCapabilitySpec(required_capabilities=ctx.required_capabilities or []),
            WorkerCapabilitySpec(worker_id=ctx.worker_instance_id, capabilities=self._worker_capabilities),
        )
        if not match.ok:
            return ConsumedTaskResult(
                claimed=None,
                executed=None,
                rejected_reason="missing_capabilities:" + ",".join(match.missing_capabilities),
            )

        claim = await self._claim_manager.claim_start(
            task_id=ctx.task_id,
            tenant_id=ctx.tenant_id,
            worker_instance_id=ctx.worker_instance_id,
            claimed_at_ms=claimed_at_ms,
        )
        if not claim.ok:
            return ConsumedTaskResult(claimed=claim, executed=None)

        loop = None
        if self._heartbeat_manager is not None:
            loop = HeartbeatLoop(
                heartbeat_manager=self._heartbeat_manager,
                task_id=ctx.task_id,
                tenant_id=ctx.tenant_id,
                worker_instance_id=ctx.worker_instance_id,
                interval_ms=self._heartbeat_interval_ms,
            )
            await loop.start()

        try:
            executed = await self._executor.execute(ctx)
            return ConsumedTaskResult(claimed=claim, executed=executed)
        finally:
            if loop is not None:
                await loop.stop()
