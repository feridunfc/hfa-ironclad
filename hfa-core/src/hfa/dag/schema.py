
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


class DagRedisKey:
    # Legacy contract retained for active tests
    @staticmethod
    def task_state(task_id: str) -> str:
        return f"hfa:dag:task:{task_id}:state"

    @staticmethod
    def task_meta(task_id: str) -> str:
        return f"hfa:dag:task:{task_id}:meta"

    @staticmethod
    def task_output(task_id: str) -> str:
        return f"hfa:dag:task:{task_id}:output"

    @staticmethod
    def task_lineage(task_id: str) -> str:
        return f"hfa:dag:task:{task_id}:lineage"

    @staticmethod
    def task_children(task_id: str) -> str:
        return f"hfa:dag:task:{task_id}:children"

    @staticmethod
    def task_remaining_deps(task_id: str) -> str:
        return f"hfa:dag:task:{task_id}:remaining_deps"

    @staticmethod
    def tenant_inflight(tenant_id: str) -> str:
        return f"hfa:dag:tenant:{tenant_id}:inflight"

    @staticmethod
    def task_running_zset(tenant_id: str) -> str:
        return f"hfa:dag:tenant:{tenant_id}:running"

    @staticmethod
    def tenant_ready_zset(tenant_id: str) -> str:
        return f"hfa:dag:tenant:{tenant_id}:ready"

    @staticmethod
    def task_ready_queue(tenant_id: str) -> str:
        return DagRedisKey.tenant_ready_zset(tenant_id)

    @staticmethod
    def tenant_ready_queue(tenant_id: str) -> str:
        return DagRedisKey.tenant_ready_zset(tenant_id)

    @staticmethod
    def scheduler_metric_counter(metric_name: str) -> str:
        return f"hfa:dag:scheduler:metrics:{metric_name}"

    @staticmethod
    def scheduler_decision_stream() -> str:
        return "hfa:dag:scheduler:decisions"

    @staticmethod
    def worker_reservation(worker_id: str) -> str:
        return f"hfa:dag:worker:{worker_id}:reservation"

    @staticmethod
    def worker_reservation_pattern() -> str:
        return "hfa:dag:worker:*:reservation"

    @staticmethod
    def worker_load(worker_id: str) -> str:
        return f"hfa:dag:worker:{worker_id}:load"

    @staticmethod
    def worker_capacity(worker_id: str) -> str:
        return f"hfa:dag:worker:{worker_id}:capacity"

    @staticmethod
    def worker_heartbeat(worker_id: str) -> str:
        return f"hfa:dag:worker:{worker_id}:heartbeat"


@dataclass(frozen=True)
class DagTaskSeed:
    task_id: str
    run_id: str
    tenant_id: str
    agent_type: str = ""
    worker_group: str = ""
    priority: int = 0
    admitted_at: float = 0.0
    dependency_count: int = 0
    child_task_ids: tuple[str, ...] = ()
    input_payload: dict[str, Any] = field(default_factory=dict)
    required_capabilities: list[str] = field(default_factory=list)
    parent_task_ids: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class DagTaskDispatchInput:
    task_id: str
    run_id: str
    tenant_id: str
    worker_id: str = ""
    worker_group: str = ""
    agent_type: str = ""
    shard: int = 0
    priority: int = 0
    admitted_at: float = 0.0
    scheduled_at: float = 0.0
    running_zset: str = ""
    control_stream: str = ""
    shard_stream: str = ""
    payload: dict[str, Any] = field(default_factory=dict)
    required_capabilities: list[str] = field(default_factory=list)
