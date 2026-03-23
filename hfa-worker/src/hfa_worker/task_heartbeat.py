
from __future__ import annotations

import asyncio
import contextlib
from dataclasses import dataclass

from hfa_control.task_recovery import TaskHeartbeatManager


@dataclass
class HeartbeatLoop:
    heartbeat_manager: TaskHeartbeatManager
    task_id: str
    tenant_id: str
    worker_instance_id: str
    interval_ms: int
    _task: asyncio.Task | None = None
    _stopped: asyncio.Event | None = None

    async def _run(self) -> None:
        assert self._stopped is not None
        try:
            while not self._stopped.is_set():
                await self.heartbeat_manager.record_heartbeat(
                    task_id=self.task_id,
                    tenant_id=self.tenant_id,
                    worker_id=self.worker_instance_id,
                )
                await asyncio.wait_for(
                    self._stopped.wait(),
                    timeout=max(self.interval_ms, 1) / 1000.0,
                )
        except asyncio.TimeoutError:
            await self._run()
        except asyncio.CancelledError:
            raise

    async def start(self) -> None:
        if self._task is not None and not self._task.done():
            return
        self._stopped = asyncio.Event()
        self._task = asyncio.create_task(self._run(), name=f"heartbeat:{self.task_id}")

    async def stop(self) -> None:
        if self._stopped is not None:
            self._stopped.set()
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._task = None
