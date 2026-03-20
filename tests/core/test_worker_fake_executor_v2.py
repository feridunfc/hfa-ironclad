import pytest

from hfa_worker.fake_executor_v2 import FakeExecutorV2


class LegacyLikeEvent:
    def __init__(self):
        self.payload = {"prompt": "hello world from legacy event"}


@pytest.mark.asyncio
async def test_fake_executor_v2_accepts_legacy_like_request():
    executor = FakeExecutorV2()
    result = await executor.execute(LegacyLikeEvent())
    assert result.status == "done"
    assert result.provider == "fake"
    assert result.model == "fake-v1"
    assert result.payload["output_text"].startswith("FAKE_RESPONSE:")
