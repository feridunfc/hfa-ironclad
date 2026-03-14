"""
hfa-worker/src/hfa_worker/executor.py
IRONCLAD Sprint 11 --- Executor Abstraction
"""
from __future__ import annotations

import abc
import logging
from typing import Optional

from hfa.events.schema import RunRequestedEvent
from hfa_worker.models import ExecutionResult

logger = logging.getLogger(__name__)


class BaseExecutor(abc.ABC):
    @abc.abstractmethod
    async def execute(self, run_event: RunRequestedEvent) -> ExecutionResult:
        raise NotImplementedError


class FakeExecutor(BaseExecutor):
    def __init__(
        self,
        should_succeed: bool = True,
        fail_with: Optional[Exception] = None,
        cost_cents: int = 42,
        tokens_used: int = 100,
    ):
        self.should_succeed = should_succeed
        self.fail_with = fail_with
        self.cost_cents = cost_cents
        self.tokens_used = tokens_used

    async def execute(self, run_event: RunRequestedEvent) -> ExecutionResult:
        if self.fail_with:
            raise self.fail_with

        if self.should_succeed:
            return ExecutionResult(
                status="done",
                payload={"result": "success", "input": run_event.payload},
                cost_cents=self.cost_cents,
                tokens_used=self.tokens_used,
            )

        return ExecutionResult(
            status="failed",
            payload={},
            error="Business logic failure",
            cost_cents=self.cost_cents,
            tokens_used=self.tokens_used,
        )
