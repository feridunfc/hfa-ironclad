"""
hfa-worker/src/hfa_worker/executor_factory.py
IRONCLAD Sprint 20 — Executor Factory
"""
from __future__ import annotations

import logging
import os
from typing import Any

from hfa_worker.executor_base import Executor
from hfa_worker.fake_executor import FakeExecutor

logger = logging.getLogger(__name__)


def _cfg(config: Any, key: str, default: Any = None) -> Any:
    if isinstance(config, dict):
        return config.get(key, default)
    return getattr(config, key, default)


def build_executor(config: Any) -> Executor:
    mode = str(_cfg(config, "executor_mode", "fake")).lower()
    logger.info("Building executor: mode=%s", mode)

    if mode == "fake":
        return FakeExecutor()

    if mode == "openai":
        from hfa_worker.openai_executor import OpenAIExecutor

        api_key = _cfg(config, "openai_api_key") or os.getenv("OPENAI_API_KEY")
        if not api_key:
            logger.error("OPENAI_API_KEY is missing. Cannot boot OpenAI executor.")
            raise ValueError("OPENAI_API_KEY is missing in config and env (Fail-Fast).")

        model = str(_cfg(config, "openai_model", "gpt-4o-mini"))
        timeout = float(_cfg(config, "executor_timeout_seconds", 60.0))

        return OpenAIExecutor(
            api_key=api_key,
            model=model,
            timeout_seconds=timeout,
        )

    raise ValueError(f"Unsupported executor_mode: {mode}. Supported: fake, openai")