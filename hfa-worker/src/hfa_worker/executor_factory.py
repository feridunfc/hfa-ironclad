from typing import Any

from hfa_worker.fake_executor import FakeExecutor


def _cfg(config: Any, key: str, default=None):
    if isinstance(config, dict):
        return config.get(key, default)
    return getattr(config, key, default)


def build_executor(config: Any):
    mode = str(_cfg(config, "executor_mode", "fake")).lower()

    if mode == "fake":
        return FakeExecutor()

    raise ValueError(f"Unsupported executor_mode: {mode}. Supported: fake")
