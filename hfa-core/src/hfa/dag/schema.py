
from __future__ import annotations

class DagRedisKey:
    PREFIX = "hfa:dag"

    @classmethod
    def task_key_prefix(cls) -> str:
        return f"{cls.PREFIX}:task:"

    @classmethod
    def task_state(cls, task_id: str) -> str:
        return f"{cls.PREFIX}:task:{task_id}:state"

    @classmethod
    def task_meta(cls, task_id: str) -> str:
        return f"{cls.PREFIX}:task:{task_id}:meta"

    @classmethod
    def task_output(cls, task_id: str) -> str:
        return f"{cls.PREFIX}:task:{task_id}:output"

    @classmethod
    def task_lineage(cls, task_id: str) -> str:
        return f"{cls.PREFIX}:task:{task_id}:lineage"

    @classmethod
    def task_children(cls, task_id: str) -> str:
        return f"{cls.PREFIX}:task:{task_id}:children"

    @classmethod
    def task_parents(cls, task_id: str) -> str:
        return f"{cls.PREFIX}:task:{task_id}:parents"

    @classmethod
    def task_remaining_deps(cls, task_id: str) -> str:
        return f"{cls.PREFIX}:task:{task_id}:deps_remaining"

    @classmethod
    def task_ready_queue(cls, tenant_id: str) -> str:
        return f"{cls.PREFIX}:tenant:{tenant_id}:ready"

    @classmethod
    def tenant_ready_queue(cls, tenant_id: str) -> str:
        return cls.task_ready_queue(tenant_id)

    @classmethod
    def ready_emitted_prefix(cls) -> str:
        return f"{cls.PREFIX}:task:"

    @classmethod
    def task_running_zset(cls, tenant_id: str) -> str:
        return f"{cls.PREFIX}:tenant:{tenant_id}:running"

    @classmethod
    def completion_stream(cls, tenant_id: str) -> str:
        return f"{cls.PREFIX}:tenant:{tenant_id}:completion_stream"

    @classmethod
    def run_graph(cls, run_id: str) -> str:
        return f"{cls.PREFIX}:run:{run_id}:graph"
