"""
hfa-control/src/hfa_control/service.py
IRONCLAD Sprint 10 — Control Plane Service

Wires all components and manages lifecycle.
Leader-gated: Scheduler and RecoveryService only run on the leader instance.
Standby instances run WorkerRegistry (heartbeat consumption) continuously.

IRONCLAD rules
--------------
* No print() — logging only.
* No asyncio.get_event_loop() — get_running_loop().
* close() always safe (idempotent, each component guards its own close).
"""
from __future__ import annotations

import asyncio
import logging
import os
import uuid
from typing import Optional

from hfa_control.models     import ControlPlaneConfig
from hfa_control.leader     import LeaderElection
from hfa_control.registry   import WorkerRegistry
from hfa_control.shard      import ShardOwnershipManager
from hfa_control.admission  import AdmissionController
from hfa_control.scheduler  import Scheduler
from hfa_control.recovery   import RecoveryService

logger = logging.getLogger(__name__)

_LEADER_CHECK_INTERVAL = 5.0  # seconds


class ControlPlaneService:

    def __init__(self, redis, config: Optional[ControlPlaneConfig] = None) -> None:
        self._redis  = redis
        self._config = config or _config_from_env()
        # Assign a unique instance_id if not set
        if not self._config.instance_id:
            self._config.instance_id = os.environ.get(
                "CP_INSTANCE_ID", uuid.uuid4().hex[:8]
            )

        self._leader    = LeaderElection(redis, self._config.instance_id, self._config)
        self._registry  = WorkerRegistry(redis, self._config)
        self._shards    = ShardOwnershipManager(redis, self._config)
        self._admitter  = AdmissionController(redis, self._config)
        self._scheduler = Scheduler(redis, self._registry, self._shards, self._config)
        self._recovery  = RecoveryService(redis, self._config)

        self._leader_task: Optional[asyncio.Task] = None
        self._sched_started    = False
        self._recovery_started = False

    # ------------------------------------------------------------------
    # Public properties
    # ------------------------------------------------------------------

    @property
    def admission(self) -> AdmissionController:
        return self._admitter

    @property
    def registry(self) -> WorkerRegistry:
        return self._registry

    @property
    def recovery(self) -> RecoveryService:
        return self._recovery

    @property
    def shards(self) -> ShardOwnershipManager:
        return self._shards

    @property
    def is_leader(self) -> bool:
        return self._leader.is_leader

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        # All instances run registry and shard monitor
        await self._registry.start()
        await self._shards.start()
        await self._leader.start()

        # Leader-gated components started via watchdog loop
        loop = asyncio.get_running_loop()
        self._leader_task = loop.create_task(
            self._leader_watchdog(), name="cp.leader_watchdog"
        )
        logger.info(
            "ControlPlaneService started: instance=%s region=%s",
            self._config.instance_id, self._config.region,
        )

    async def close(self) -> None:
        if self._leader_task:
            self._leader_task.cancel()
            try:
                await self._leader_task
            except asyncio.CancelledError:
                pass

        # Close leader-gated components
        if self._sched_started:
            await self._scheduler.close()
        if self._recovery_started:
            await self._recovery.close()

        # Close always-running components
        await self._shards.close()
        await self._registry.close()
        await self._leader.close()

        logger.info("ControlPlaneService closed: instance=%s", self._config.instance_id)

    # ------------------------------------------------------------------
    # Leader watchdog — starts Scheduler+Recovery when leadership acquired
    # ------------------------------------------------------------------

    async def _leader_watchdog(self) -> None:
        """
        Monitor leadership state.
        Start leader-only components when leadership is acquired.
        Stop them gracefully when leadership is lost.
        (Leadership loss is rare but possible during rolling redeploys.)
        """
        while True:
            try:
                if self._leader.is_leader:
                    if not self._sched_started:
                        await self._scheduler.start()
                        self._sched_started = True
                        logger.info(
                            "Leader components started: instance=%s",
                            self._config.instance_id,
                        )
                    if not self._recovery_started:
                        await self._recovery.start()
                        self._recovery_started = True
                else:
                    if self._sched_started:
                        await self._scheduler.close()
                        self._sched_started = False
                        logger.info(
                            "Scheduler stopped (leadership lost): instance=%s",
                            self._config.instance_id,
                        )
                    if self._recovery_started:
                        await self._recovery.close()
                        self._recovery_started = False

            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error("_leader_watchdog error: %s", exc)

            try:
                await asyncio.sleep(_LEADER_CHECK_INTERVAL)
            except asyncio.CancelledError:
                break


def _config_from_env() -> ControlPlaneConfig:
    return ControlPlaneConfig(
        region=os.environ.get("CP_REGION", "us-east-1"),
        instance_id=os.environ.get("CP_INSTANCE_ID", ""),
        worker_heartbeat_ttl=float(
            os.environ.get("WORKER_HEARTBEAT_TTL", "30")
        ),
        stale_run_timeout=float(
            os.environ.get("STALE_RUN_TIMEOUT", "600")
        ),
        recovery_sweep_interval=float(
            os.environ.get("RECOVERY_SWEEP_INTERVAL", "30")
        ),
        max_reschedule_attempts=int(
            os.environ.get("MAX_RESCHEDULE_ATTEMPTS", "3")
        ),
    )
