from hfa_worker.execution_types import ExecutionResult, ExecutionUsage


def test_execution_result_backward_compat_success():
    result = ExecutionResult(
        output_text="hello",
        usage=ExecutionUsage(estimated_cost_cents=42, total_tokens=100),
    )
    assert result.status == "done"
    assert result.cost_cents == 42
    assert result.tokens_used == 100
    assert result.payload == {"output_text": "hello", "output_json": None}


def test_execution_result_backward_compat_failure():
    result = ExecutionResult(output_text="", error="boom", usage=ExecutionUsage(total_tokens=3))
    assert result.status == "failed"
    assert result.tokens_used == 3
