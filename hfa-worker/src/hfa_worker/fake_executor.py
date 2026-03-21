from hfa_worker.execution_types import ExecutionResult, ExecutionUsage


class FakeExecutor:
    async def execute(self, request) -> ExecutionResult:
        payload = getattr(request, "payload", {}) or {}
        prompt = str(payload.get("prompt", ""))

        fake_output = (
            f"FAKE_RESPONSE: {prompt[:50]}..." if prompt else "FAKE_RESPONSE: no_prompt"
        )
        prompt_tokens = len(prompt.split()) if prompt else 0
        completion_tokens = 10

        return ExecutionResult(
            output_text=fake_output,
            provider="fake",
            model="fake-v1",
            latency_ms=10.0,
            usage=ExecutionUsage(
                prompt_tokens=prompt_tokens,
                completion_tokens=completion_tokens,
                total_tokens=prompt_tokens + completion_tokens,
                estimated_cost_cents=0,
            ),
        )
