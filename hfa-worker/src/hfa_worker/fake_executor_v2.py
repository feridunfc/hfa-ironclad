from __future__ import annotations

from typing import Any

from hfa_worker.execution_types import ExecutionResult, ExecutionUsage


class FakeExecutorV2:
    async def execute(self, request: Any) -> ExecutionResult:
        payload = getattr(request, "payload", {}) or {}
        if not isinstance(payload, dict):
            payload = {}

        prompt = str(payload.get("prompt", ""))
        prompt_tokens = len(prompt.split()) if prompt else 0

        return ExecutionResult(
            output_text=f"FAKE_RESPONSE: {prompt[:50]}...",
            provider="fake",
            model="fake-v1",
            latency_ms=10.0,
            usage=ExecutionUsage(
                prompt_tokens=prompt_tokens,
                completion_tokens=10,
                total_tokens=prompt_tokens + 10,
                estimated_cost_cents=0,
            ),
        )
