"""
hfa-worker/src/hfa_worker/main.py
IRONCLAD Sprint 12 — Worker Lifecycle Wiring

Wires WorkerConsumer, WorkerHeartbeatPublisher, and DrainManager
into a clean WorkerService with explicit lifecycle:

    1. init config
    2. start consumer  (registers consumer group, starts background tasks)
    3. start heartbeat (publishes heartbeat every WORKER_HEARTBEAT_INTERVAL)
    4. graceful_shutdown -> drain -> close consumer -> close heartbeat

Sprint 9 main.py referenced ShardConsumer / HeartbeatPublisher (old names).
This replaces it with the Sprint 11 class names without changing any
external contracts.
"""
from __future__ import annotations

import logging
import os
import uuid
from typing import Dict

from hfa_worker.consumer import WorkerConsumer
from hfa_worker.drain import DrainManager
from hfa_worker.executor import BaseExecutor, FakeExecutor
from hfa_worker.heartbeat import WorkerHeartbeatPublisher

logger = logging.getLogger(__name__)


def _require_env(name: str) -> str:
    val = os.environ.get(name, "")
    if not val:
        raise RuntimeError(f"Required environment variable not set: {name}")
    return val


class WorkerService:
    """
    Standalone worker service.

    config keys (all optional with sane defaults):
        worker_id       str     unique worker identifier
        worker_group    str     scheduling group name
        region          str     deployment region
        version         str     build version
        capabilities    list    agent_type capabilities
        shards          list    shard numbers to consume
        capacity        int     max concurrent runs
        executor        BaseExecutor  injected executor (for tests)
    """

    def __init__(self, redis, config: Dict) -> None:
        self._redis = redis

        self._worker_id = str(
            config.get("worker_id") or f"worker-{uuid.uuid4().hex[:8]}"
        )
        self._worker_group = str(config.get("worker_group") or "default-group")
        self._region = str(config.get("region") or "us-east-1")
        self._version = str(config.get("version") or "0.0.0")
        self._capabilities: list[str] = list(config.get("capabilities") or ["base"])
        self._shards: list[int] = list(config.get("shards") or [0])
        self._capacity = int(config.get("capacity") or 10)

        executor: BaseExecutor = config.get("executor") or FakeExecutor()

        self._consumer = WorkerConsumer(
            redis=redis,
            worker_id=self._worker_id,
            worker_group=self._worker_group,
            shards=self._shards,
            executor=executor,
        )

        self._heartbeat = WorkerHeartbeatPublisher(
            redis=redis,
            worker_id=self._worker_id,
            worker_group=self._worker_group,
            region=self._region,
            shards=self._shards,
            capacity=self._capacity,
            inflight_fn=lambda: self._consumer.inflight_count,
            is_draining_fn=lambda: self._consumer.is_draining,
            version=self._version,
            capabilities=self._capabilities,
        )

        self._drain_manager = DrainManager(
            redis=redis,
            worker_id=self._worker_id,
            worker_group=self._worker_group,
            shards=self._shards,
            consumer=self._consumer,
        )

    @property
    def worker_id(self) -> str:
        return self._worker_id

    async def start(self) -> None:
        logger.info(
            "WorkerService starting: worker_id=%s group=%s shards=%s capacity=%d",
            self._worker_id, self._worker_group, self._shards, self._capacity,
        )
        await self._consumer.start()
        await self._heartbeat.start()
        logger.info("WorkerService started: worker_id=%s", self._worker_id)

    async def graceful_shutdown(
        self, drain_timeout: float = 30.0
    ) -> None:
        logger.info(
            "WorkerService graceful_shutdown: worker_id=%s", self._worker_id
        )
        await self._drain_manager.start_drain(
            reason="SIGTERM", timeout=drain_timeout
        )
        await self._consumer.close()
        await self._heartbeat.close()
        logger.info(
            "WorkerService shutdown complete: worker_id=%s", self._worker_id
        )
