from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable


@dataclass(frozen=True)
class DagTTL:
    GRAPH_META: int = 86_400
    TASK_META: int = 86_400
    TASK_STATE: int = 86_400
    TASK_READY: int = 86_400
    TASK_RUNNING: int = 86_400


class DagRedisKey:
    PREFIX = "hfa:dag"
    TASK_PREFIX = PREFIX + ":task:"

    @classmethod
    def run_graph(cls, run_id: str) -> str:
        return f"{cls.PREFIX}:run:{run_id}:graph"

    @classmethod
    def run_tasks(cls, run_id: str) -> str:
        return f"{cls.PREFIX}:run:{run_id}:tasks"

    @classmethod
    def task_meta(cls, task_id: str) -> str:
        return f"{cls.TASK_PREFIX}{task_id}:meta"

    @classmethod
    def task_state(cls, task_id: str) -> str:
        return f"{cls.TASK_PREFIX}{task_id}:state"

    @classmethod
    def task_children(cls, task_id: str) -> str:
        return f"{cls.TASK_PREFIX}{task_id}:children"

    @classmethod
    def task_parents(cls, task_id: str) -> str:
        return f"{cls.TASK_PREFIX}{task_id}:parents"

    @classmethod
    def task_remaining_deps(cls, task_id: str) -> str:
        return f"{cls.TASK_PREFIX}{task_id}:remaining_deps"

    @classmethod
    def task_key_prefix(cls) -> str:
        return cls.TASK_PREFIX

    @classmethod
    def task_ready_queue(cls, tenant_id: str) -> str:
        return f"{cls.PREFIX}:tenant:{tenant_id}:ready"

    @classmethod
    def tenant_ready_queue(cls, tenant_id: str) -> str:
        return cls.task_ready_queue(tenant_id)

    @classmethod
    def task_ready_emitted(cls, task_id: str) -> str:
        return f"{cls.TASK_PREFIX}{task_id}:ready_emitted"

    @classmethod
    def ready_emitted(cls, task_id: str) -> str:
        return cls.task_ready_emitted(task_id)

    @classmethod
    def ready_emitted_prefix(cls) -> str:
        return f"{cls.TASK_PREFIX}"

    @classmethod
    def completion_stream(cls, tenant_id: str) -> str:
        return f"{cls.PREFIX}:tenant:{tenant_id}:task_completion"

    @classmethod
    def task_running_zset(cls, tenant_id: str) -> str:
        return f"{cls.PREFIX}:tenant:{tenant_id}:running"

    @classmethod
    def task_result(cls, task_id: str) -> str:
        return f"{cls.TASK_PREFIX}{task_id}:result"


@dataclass(frozen=True)
class DagTaskSeed:
    task_id: str
    tenant_id: str
    run_id: str
    agent_type: str
    priority: int
    admitted_at: float
    estimated_cost_cents: int = 0
    preferred_region: str = ""
    preferred_placement: str = "LEAST_LOADED"
    payload_json: str = "{}"
    trace_parent: str = ""
    trace_state: str = ""
    dependency_count: int = 0
    child_task_ids: tuple[str, ...] = ()


@dataclass(frozen=True)
class DagTaskDispatchInput:
    task_id: str
    run_id: str
    tenant_id: str
    agent_type: str
    worker_group: str
    shard: int
    priority: int
    admitted_at: float
    scheduled_at: float
    running_zset: str
    control_stream: str
    shard_stream: str
    region: str = ""
    policy: str = "LEAST_LOADED"
    trace_parent: str = ""
    trace_state: str = ""
    payload_json: str = "{}"


def validate_topology(tasks: Iterable[DagTaskSeed]) -> None:
    ids = set()
    for task in tasks:
        if task.task_id in ids:
            raise ValueError(f"duplicate task_id: {task.task_id}")
        ids.add(task.task_id)
