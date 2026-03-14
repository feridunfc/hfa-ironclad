"""
hfa-worker/src/hfa_worker/models.py
IRONCLAD Sprint 11 --- Worker Execution Models
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Literal, Optional


@dataclass
class ExecutionResult:
    status: Literal["done", "failed"]
    payload: Dict[str, Any] = field(default_factory=dict)
    error: Optional[str] = None
    cost_cents: int = 0
    tokens_used: int = 0

    @property
    def is_success(self) -> bool:
        return self.status == "done"

    @property
    def is_terminal_failure(self) -> bool:
        return self.status == "failed"


class ExecutionError(Exception):
    pass


class TerminalExecutionError(ExecutionError):
    def __init__(self, message: str, cost_cents: int = 0, tokens_used: int = 0):
        super().__init__(message)
        self.cost_cents = cost_cents
        self.tokens_used = tokens_used


class InfrastructureError(ExecutionError):
    pass
