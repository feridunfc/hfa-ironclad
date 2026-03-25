
from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class TaskContext:
    task_id: str
    run_id: str
    tenant_id: str
    agent_type: str
    worker_group: str
    worker_instance_id: str
    payload: dict[str, Any]
    trace_parent: str = ""
    trace_state: str = ""
    required_capabilities: list[str] | None = None
