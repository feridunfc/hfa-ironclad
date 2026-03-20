from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Optional

from hfa.config.keys import RedisKey
from hfa_control.models import WorkerStatus


@dataclass(frozen=True)
class TenantSchedulingSnapshot:
    tenant_id: str
    queued_depth: int
    inflight: int
    vruntime: float
    head_run_id: Optional[str]
    head_age_seconds: float
    dispatchable: bool
    blocked_reason: Optional[str] = None
    weight: int = 1


@dataclass(frozen=True)
class WorkerSchedulingSnapshot:
    worker_id: str
    worker_group: str
    region: str
    capacity: int
    inflight: int
    available_slots: int
    load_factor: float
    status: str
    status_enum: WorkerStatus
    last_seen: float
    capabilities: tuple[str, ...] = ()
    latency_ewma_ms: float = 0.0
    failure_penalty: float = 0.0
    dispatch_reject_penalty: float = 0.0
    drained: bool = False
    schedulable: bool = True
    blocked_reason: Optional[str] = None


@dataclass(frozen=True)
class CapacitySnapshot:
    workers: tuple[WorkerSchedulingSnapshot, ...]
    total_available_slots: int
    dispatch_allowed: bool
    blocked_reason: Optional[str]
    redis_degraded: bool
    max_dispatches_this_cycle: int


class SchedulerSnapshotBuilder:
    def __init__(self, redis, registry, tenant_queue, tenant_fairness, config) -> None:
        self._redis = redis
        self._registry = registry
        self._tenant_queue = tenant_queue
        self._tenant_fairness = tenant_fairness
        self._config = config

    async def list_candidate_tenants(self) -> list[TenantSchedulingSnapshot]:
        result: list[TenantSchedulingSnapshot] = []
        tenant_ids = await self._tenant_queue.active_tenants()
        now = time.time()

        for tenant_id in tenant_ids:
            depth = await self._tenant_queue.depth(tenant_id)
            if depth <= 0:
                try:
                    await self._redis.srem(RedisKey.tenant_active_set(), tenant_id)
                except Exception:
                    pass
                continue

            head_run_id = await self._tenant_queue.peek(tenant_id)
            head_age = 0.0
            inflight_val = await self._redis.get(RedisKey.tenant_inflight(tenant_id))
            inflight = int(inflight_val) if inflight_val else 0

            if head_run_id:
                admitted = await self._redis.hget(RedisKey.run_meta(head_run_id), "admitted_at")
                if admitted:
                    if isinstance(admitted, bytes):
                        admitted = admitted.decode("utf-8")
                    try:
                        head_age = max(0.0, now - float(admitted))
                    except (ValueError, TypeError):
                        pass

            result.append(
                TenantSchedulingSnapshot(
                    tenant_id=tenant_id,
                    queued_depth=depth,
                    inflight=inflight,
                    vruntime=self._tenant_fairness.get(tenant_id),
                    head_run_id=head_run_id,
                    head_age_seconds=head_age,
                    dispatchable=head_run_id is not None,
                    blocked_reason=None if head_run_id else "empty_head",
                    weight=1,
                )
            )
        return result

    async def build_capacity_snapshot(self) -> CapacitySnapshot:
        workers = await self._registry.list_schedulable_workers(region=None)
        snapshots: list[WorkerSchedulingSnapshot] = []
        for w in workers:
            worker_id = getattr(w, "worker_id", "")
            worker_group = getattr(w, "worker_group", "")
            region = getattr(w, "region", "") or ""
            capacity = int(getattr(w, "capacity", 1) or 1)
            inflight = int(getattr(w, "inflight", 0) or 0)
            status_enum = getattr(w, "status", WorkerStatus.HEALTHY)
            last_seen = float(getattr(w, "last_seen", 0.0) or 0.0)
            capabilities = tuple(getattr(w, "capabilities", ()) or ())
            status_str = status_enum.value if hasattr(status_enum, "value") else str(status_enum)
            available = max(capacity - inflight, 0)
            load_factor = inflight / capacity if capacity > 0 else 1.0
            is_draining = status_enum == WorkerStatus.DRAINING
            schedulable = status_enum == WorkerStatus.HEALTHY and not is_draining and available > 0
            snapshots.append(
                WorkerSchedulingSnapshot(
                    worker_id=worker_id,
                    worker_group=worker_group,
                    region=region,
                    capacity=capacity,
                    inflight=inflight,
                    available_slots=available,
                    load_factor=load_factor,
                    status=status_str,
                    status_enum=status_enum,
                    last_seen=last_seen,
                    capabilities=capabilities,
                    drained=is_draining,
                    schedulable=schedulable,
                    blocked_reason="draining" if is_draining else None,
                )
            )

        total_slots = sum(w.available_slots for w in snapshots if w.schedulable)
        return CapacitySnapshot(
            workers=tuple(snapshots),
            total_available_slots=total_slots,
            dispatch_allowed=total_slots > 0,
            blocked_reason=None if total_slots > 0 else "worker_pool_empty",
            redis_degraded=False,
            max_dispatches_this_cycle=min(total_slots, self._config.scheduler_loop_max_dispatches),
        )
