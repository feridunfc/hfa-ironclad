import time
import logging

from openai import (
    APIConnectionError, APIError, APITimeoutError, AsyncOpenAI,
    AuthenticationError, BadRequestError, NotFoundError,
    PermissionDeniedError, RateLimitError, UnprocessableEntityError
)

from hfa_worker.execution_types import (
    ExecutionProviderError, ExecutionRateLimitError,
    ExecutionRequest, ExecutionResult, ExecutionTimeoutError,
    ExecutionTransientError, ExecutionUsage
)

logger = logging.getLogger(__name__)

class OpenAIExecutor:
    def __init__(self, api_key: str, model: str, timeout_seconds: float = 60.0):
        if not api_key:
            raise ValueError("OpenAI API key is missing.")
        self._model = model
        self._timeout_seconds = float(timeout_seconds)
        self._client = AsyncOpenAI(api_key=api_key, timeout=self._timeout_seconds)

    async def execute(self, request: ExecutionRequest) -> ExecutionResult:
        start_time = time.monotonic()
        prompt = request.payload.get("prompt")
        if not prompt:
            raise ExecutionProviderError("Missing 'prompt' in request payload.")

        try:
            response = await self._client.chat.completions.create(
                model=self._model,
                messages=[{"role": "user", "content": str(prompt)}],
            )

            latency_ms = (time.monotonic() - start_time) * 1000.0

            if not getattr(response, "choices", None):
                raise ExecutionTransientError("Empty choices array")

            output_text = getattr(response.choices[0].message, "content", "") or ""

            usage = getattr(response, "usage", None)
            prompt_tokens = getattr(usage, "prompt_tokens", 0) if usage else 0
            completion_tokens = getattr(usage, "completion_tokens", 0) if usage else 0
            total_tokens = getattr(usage, "total_tokens", 0) if usage else 0

            raw = response.model_dump() if hasattr(response, "model_dump") else None

            return ExecutionResult(
                output_text=output_text,
                provider="openai",
                model=getattr(response, "model", self._model),
                latency_ms=latency_ms,
                usage=ExecutionUsage(
                    prompt_tokens=prompt_tokens,
                    completion_tokens=completion_tokens,
                    total_tokens=total_tokens,
                    estimated_cost_cents=0,
                ),
                raw_response=raw,
            )

        except APITimeoutError as exc:

            raise ExecutionTimeoutError(str(exc))

        except RateLimitError as exc:

            raise ExecutionRateLimitError(str(exc))

        except APIConnectionError as exc:

            raise ExecutionTransientError(str(exc))

        except (BadRequestError, AuthenticationError, PermissionDeniedError,

                NotFoundError, UnprocessableEntityError) as exc:

            raise ExecutionProviderError(str(exc))

        except APIError as exc:

            raise ExecutionTransientError(str(exc))

        except ExecutionTransientError:

            raise

        except Exception as exc:

            raise ExecutionTransientError(str(exc))
