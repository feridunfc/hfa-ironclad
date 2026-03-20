import pytest

from hfa_worker.executor_factory import build_executor
from hfa_worker.fake_executor import FakeExecutor


class ObjConfig:
    executor_mode = "fake"


def test_factory_returns_fake_executor_for_dict_config():
    assert isinstance(build_executor({"executor_mode": "fake"}), FakeExecutor)


def test_factory_returns_fake_executor_for_object_config():
    assert isinstance(build_executor(ObjConfig()), FakeExecutor)


def test_factory_raises_on_invalid_mode():
    with pytest.raises(ValueError, match="Unsupported executor_mode"):
        build_executor({"executor_mode": "openai"})
