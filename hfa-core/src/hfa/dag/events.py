from __future__ import annotations

from dataclasses import dataclass, field
import time
from typing import Any, Dict, Optional

from hfa.events.schema import HFAEvent


@dataclass
class TaskReadyEvent(HFAEvent):
    event_type: str = "TaskReady"
    task_id: str = ""
    run_id: str = ""
    tenant_id: str = ""
    agent_type: str = ""
    priority: int = 5
    payload: Dict[str, Any] = field(default_factory=dict)
    admitted_at: float = field(default_factory=time.time)


@dataclass
class TaskScheduledEvent(HFAEvent):
    event_type: str = "TaskScheduled"
    task_id: str = ""
    run_id: str = ""
    tenant_id: str = ""
    agent_type: str = ""
    worker_group: str = ""
    shard: int = 0
    region: str = ""
    policy: str = "LEAST_LOADED"
    scheduled_at: float = field(default_factory=time.time)


@dataclass
class TaskRequestedEvent(HFAEvent):
    event_type: str = "TaskRequested"
    task_id: str = ""
    run_id: str = ""
    tenant_id: str = ""
    agent_type: str = ""
    worker_group: str = ""
    shard: int = 0
    payload: Dict[str, Any] = field(default_factory=dict)
    requested_at: float = field(default_factory=time.time)
