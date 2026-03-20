from __future__ import annotations

from typing import Any, Protocol
from hfa_worker.execution_types import ExecutionResult


class Executor(Protocol):
    async def execute(self, request: Any) -> ExecutionResult:
        ...
