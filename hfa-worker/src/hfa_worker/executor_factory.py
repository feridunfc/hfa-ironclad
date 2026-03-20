from __future__ import annotations

import logging
from typing import Any

from hfa_worker.executor_base import Executor
from hfa_worker.fake_executor_v2 import FakeExecutorV2

logger = logging.getLogger(__name__)


def _cfg(config: Any, key: str, default: Any) -> Any:
    if isinstance(config, dict):
        return config.get(key, default)
    return getattr(config, key, default)


def build_executor(config: Any) -> Executor:
    mode = str(_cfg(config, "executor_mode", "fake")).lower()
    logger.info("Building executor: mode=%s", mode)

    if mode == "fake":
        return FakeExecutorV2()

    raise ValueError(
        f"Unsupported executor_mode: {mode}. Currently implemented: fake"
    )
