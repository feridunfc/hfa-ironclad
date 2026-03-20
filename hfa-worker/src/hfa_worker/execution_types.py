from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional


@dataclass(frozen=True)
class ExecutionRequest:
    run_id: str
    tenant_id: str
    agent_type: str
    payload: dict[str, Any]
    trace_parent: Optional[str] = None
    trace_state: Optional[str] = None


@dataclass(frozen=True)
class ExecutionUsage:
    prompt_tokens: int = 0
    completion_tokens: int = 0
    total_tokens: int = 0
    estimated_cost_cents: int = 0


@dataclass(frozen=True)
class ExecutionResult:
    output_text: str
    output_json: Optional[dict[str, Any]] = None
    model: Optional[str] = None
    provider: Optional[str] = None
    latency_ms: Optional[float] = None
    usage: ExecutionUsage = field(default_factory=ExecutionUsage)
    raw_response: Optional[dict[str, Any]] = None
    error: Optional[str] = None

    @property
    def status(self) -> str:
        return "failed" if self.error else "done"

    @property
    def payload(self) -> dict[str, Any]:
        return {
            "output_text": self.output_text,
            "output_json": self.output_json,
        }

    @property
    def cost_cents(self) -> int:
        return self.usage.estimated_cost_cents

    @property
    def tokens_used(self) -> int:
        return self.usage.total_tokens


class ExecutionError(Exception):
    pass


class ExecutionTransientError(ExecutionError):
    pass


class ExecutionPermanentError(ExecutionError):
    pass


class ExecutionTimeoutError(ExecutionTransientError):
    pass


class ExecutionRateLimitError(ExecutionTransientError):
    pass


class ExecutionProviderError(ExecutionPermanentError):
    pass
