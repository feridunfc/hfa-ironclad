from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from hfa_worker.execution_types import (
    ExecutionProviderError,
    ExecutionRateLimitError,
    ExecutionRequest,
    ExecutionTransientError,
)
from hfa_worker.openai_executor import OpenAIExecutor


@pytest.fixture
def valid_request():
    return ExecutionRequest(
        run_id="run-1",
        tenant_id="tenant-1",
        agent_type="bot",
        payload={"prompt": "Say hello"},
    )


@pytest.fixture
def mock_openai_client():
    with patch("hfa_worker.openai_executor.AsyncOpenAI") as mock_client_class:
        mock_client = AsyncMock()
        mock_client_class.return_value = mock_client
        yield mock_client


@pytest.mark.asyncio
async def test_success(mock_openai_client, valid_request):
    mock_response = MagicMock()
    mock_response.choices = [MagicMock()]
    mock_response.choices[0].message.content = "Hello there!"
    mock_response.usage = MagicMock()
    mock_response.usage.prompt_tokens = 10
    mock_response.usage.completion_tokens = 5
    mock_response.usage.total_tokens = 15
    mock_response.model = "gpt-4"
    mock_response.model_dump.return_value = {"id": "resp-1"}

    mock_openai_client.chat.completions.create.return_value = mock_response

    executor = OpenAIExecutor(api_key="sk-test", model="gpt-4", timeout_seconds=30.0)
    result = await executor.execute(valid_request)

    assert result.output_text == "Hello there!"
    assert result.provider == "openai"
    assert result.model == "gpt-4"
    assert result.usage.prompt_tokens == 10
    assert result.usage.completion_tokens == 5
    assert result.usage.total_tokens == 15
    assert result.raw_response == {"id": "resp-1"}


@pytest.mark.asyncio
async def test_missing_prompt_is_permanent():
    executor = OpenAIExecutor(api_key="sk-test", model="gpt-4")
    request = ExecutionRequest(
        run_id="run-1",
        tenant_id="tenant-1",
        agent_type="bot",
        payload={},
    )

    with pytest.raises(ExecutionProviderError, match="Missing 'prompt'"):
        await executor.execute(request)


@pytest.mark.asyncio
async def test_empty_choices_is_transient(mock_openai_client, valid_request):
    mock_response = MagicMock()
    mock_response.choices = []
    mock_openai_client.chat.completions.create.return_value = mock_response

    executor = OpenAIExecutor(api_key="sk-test", model="gpt-4")

    with pytest.raises(ExecutionTransientError, match="Empty choices array"):
        await executor.execute(valid_request)


@pytest.mark.asyncio
async def test_rate_limit_maps_to_rate_limit_error(mock_openai_client, valid_request):
    from openai import RateLimitError

    mock_openai_client.chat.completions.create.side_effect = RateLimitError(
        message="Too many requests",
        response=MagicMock(),
        body=None,
    )

    executor = OpenAIExecutor(api_key="sk-test", model="gpt-4")

    with pytest.raises(ExecutionRateLimitError):
        await executor.execute(valid_request)


@pytest.mark.asyncio
async def test_bad_request_maps_to_provider_error(mock_openai_client, valid_request):
    from openai import BadRequestError

    mock_openai_client.chat.completions.create.side_effect = BadRequestError(
        message="Bad request",
        response=MagicMock(),
        body=None,
    )

    executor = OpenAIExecutor(api_key="sk-test", model="gpt-4")

    with pytest.raises(ExecutionProviderError):
        await executor.execute(valid_request)