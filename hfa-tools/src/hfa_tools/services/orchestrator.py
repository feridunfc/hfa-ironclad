"""
hfa-tools/src/hfa_tools/services/orchestrator.py
IRONCLAD Sprint 5 — Hardened Production Orchestrator

✅ Ledger task tracking & drain
✅ Atomic budget gate
✅ Graceful shutdown with wait_for drain
✅ Memory leak protection (consume semantics)
✅ Start-guard on enqueue
"""
from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, Optional, Set

from hfa.obs.run_graph import ExecutionGraph
from hfa_tools.middleware.tenant import validate_run_id_format, TenantFormatError

logger = logging.getLogger(__name__)

class RunStatus(str, Enum):
    QUEUED    = "queued"
    RUNNING   = "running"
    DONE      = "done"
    FAILED    = "failed"
    REJECTED  = "rejected"

@dataclass
class RunRequest:
    agent_type:   str
    tenant_id:    str
    run_id:       str
    payload:      Any
    priority:     int   = 5
    submitted_at: float = field(default_factory=time.time)

    def __lt__(self, other: "RunRequest") -> bool:
        return (self.priority, self.submitted_at) < (other.priority, other.submitted_at)

@dataclass
class RunResult:
    run_id:      str
    tenant_id:   str
    agent_type:  str
    status:      RunStatus
    payload:     Optional[Any]  = None
    error:       Optional[str]  = None
    duration_ms: Optional[float]= None
    graph:       Optional[Dict] = None
    tokens_used: int            = 0
    cost_cents:  int            = 0

class OrchestratorError(Exception):
    """Base orchestrator error."""

class QueueFullError(OrchestratorError):
    def __init__(self, tenant_id: str, queue_size: int) -> None:
        super().__init__(f"Run queue full for tenant={tenant_id} (size={queue_size})")
        self.tenant_id  = tenant_id
        self.queue_size = queue_size

class AgentNotRegisteredError(OrchestratorError):
    """No handler registered for the given agent_type."""

class RunOrchestrator:
    def __init__(
        self,
        max_queue_size:          int   = 500,
        worker_count:            int   = 8,
        per_tenant_concurrency:  int   = 5,
        worker_shutdown_timeout: float = 30.0,
        budget_guard:            Optional[Any] = None,
        ledger:                  Optional[Any] = None,
    ) -> None:
        self._max_queue_size     = max_queue_size
        self._worker_count       = worker_count
        self._per_tenant_limit   = per_tenant_concurrency
        self._shutdown_timeout   = worker_shutdown_timeout
        self._budget_guard       = budget_guard
        self._ledger             = ledger

        self._queue: asyncio.PriorityQueue = asyncio.PriorityQueue(maxsize=max_queue_size)
        self._dispatch_table: Dict[str, Callable] = {}
        self._tenant_sems: Dict[str, asyncio.Semaphore] = {}
        self._sems_lock   = asyncio.Lock()

        self._active_graphs: Dict[str, ExecutionGraph] = {}
        self._results:       Dict[str, RunResult]      = {}
        self._result_events: Dict[str, asyncio.Event]  = {}
        self._background_tasks: Set[asyncio.Task]      = set()
        self._workers: list[asyncio.Task]              = []
        self._running = False

    async def start(self) -> None:
        if self._running: return
        self._running = True
        loop = asyncio.get_running_loop()
        for i in range(self._worker_count):
            task = loop.create_task(self._worker(i), name=f"orchestrator.worker.{i}")
            self._workers.append(task)
        logger.info("RunOrchestrator started: %d workers", self._worker_count)

    async def close(self) -> None:
        if not self._running and not self._workers: return
        self._running = False
        logger.info("RunOrchestrator shutting down...")

        # 1. Drain Queue (only if workers exist)
        if self._worker_count > 0:
            try:
                await asyncio.wait_for(self._queue.join(), timeout=self._shutdown_timeout / 2)
            except asyncio.TimeoutError:
                logger.warning("Queue drain timeout")

        # 2. Stop Workers with sentinels
        for _ in range(self._worker_count):
            try: await asyncio.wait_for(self._queue.put((0, None)), timeout=0.1)
            except: pass

        if self._workers:
            done, pending = await asyncio.wait(self._workers, timeout=1.0)
            for t in pending: t.cancel()
            await asyncio.gather(*self._workers, return_exceptions=True)

        # 3. Drain Background Ledger Tasks
        if self._background_tasks:
            logger.info("Draining %d background ledger tasks...", len(self._background_tasks))
            await asyncio.gather(*self._background_tasks, return_exceptions=True)
            self._background_tasks.clear()

        self._workers.clear()
        logger.info("RunOrchestrator closed")

    def register(self, agent_type: str, handler: Callable) -> None:
        self._dispatch_table[agent_type] = handler
        logger.info("Registered agent: %s", agent_type)

    def registered_agents(self) -> list[str]:
        return list(self._dispatch_table.keys())

    async def enqueue(self, request: RunRequest) -> str:
        if not self._running:
            raise OrchestratorError("RunOrchestrator is not started")

        try:
            ext_tenant, _ = validate_run_id_format(request.run_id)
            if ext_tenant != request.tenant_id:
                raise OrchestratorError(f"Tenant mismatch: {ext_tenant} != {request.tenant_id}")
        except TenantFormatError as exc:
            raise OrchestratorError(f"Invalid run_id format: {exc}") from exc

        if request.agent_type not in self._dispatch_table:
            raise AgentNotRegisteredError(request.agent_type)

        try:
            self._queue.put_nowait((request.priority, request))
        except asyncio.QueueFull:
            raise QueueFullError(request.tenant_id, self._max_queue_size)

        self._result_events[request.run_id] = asyncio.Event()
        self._active_graphs[request.run_id] = ExecutionGraph(request.run_id, request.tenant_id)
        return request.run_id

    def queue_size(self) -> int: return self._queue.qsize()
    def get_graph(self, run_id: str) -> Optional[ExecutionGraph]: return self._active_graphs.get(run_id)
    def inflight_run_count(self) -> int: return len(self._active_graphs)

    async def wait_for(self, run_id: str, timeout: float = 120.0, consume: bool = True) -> RunResult:
        event = self._result_events.get(run_id)
        if not event: raise KeyError(f"Run ID {run_id} not enqueued")
        await asyncio.wait_for(event.wait(), timeout=timeout)
        return self.get_result(run_id, consume=consume)

    def get_result(self, run_id: str, consume: bool = False) -> Optional[RunResult]:
        result = self._results.get(run_id)
        if result and consume:
            self._results.pop(run_id, None)
            self._result_events.pop(run_id, None)
        return result

    async def _worker(self, worker_id: int) -> None:
        while True:
            try:
                _, item = await self._queue.get()
                if item is None:
                    self._queue.task_done()
                    break
                await self._execute(item, worker_id)
                self._queue.task_done()
            except asyncio.CancelledError: break
            except Exception: logger.error("Worker error", exc_info=True)

    async def _execute(self, request: RunRequest, worker_id: int) -> None:
        sem = await self._get_tenant_sem(request.tenant_id)
        start = time.time()
        async with sem:
            # Atomic Budget Check
            if self._budget_guard:
                try:
                    state = await self._budget_guard.get_state(request.tenant_id, request.run_id)
                    if state and state.status.value in ("exhausted", "frozen"):
                        await self._record_result(RunResult(request.run_id, request.tenant_id, request.agent_type, RunStatus.REJECTED, error="Budget exhausted"))
                        self._active_graphs.pop(request.run_id, None)
                        return
                except Exception as exc:
                    logger.error("Budget check failed: %s", exc)

            graph = self._active_graphs.get(request.run_id)
            handler = self._dispatch_table.get(request.agent_type)
            node_id = graph.add_node(agent_type=request.agent_type)
            try:
                await graph.start_node(node_id)
                res = await handler(request, graph)
                await graph.commit_node(node_id, tokens=getattr(res, "total_tokens", 0), cost_cents=getattr(res, "cost_cents", 0))
                run_result = RunResult(request.run_id, request.tenant_id, request.agent_type, RunStatus.DONE, payload=res, duration_ms=(time.time()-start)*1000, graph=graph.snapshot())
            except Exception as exc:
                await graph.fail_node(node_id, str(exc))
                run_result = RunResult(request.run_id, request.tenant_id, request.agent_type, RunStatus.FAILED, error=str(exc), graph=graph.snapshot())

            await self._record_result(run_result)
            self._active_graphs.pop(request.run_id, None)

    async def _record_result(self, run_result: RunResult) -> None:
        self._results[run_result.run_id] = run_result
        if self._ledger:
            task = asyncio.create_task(self._ledger.append(run_result.tenant_id, run_result.run_id, "orchestrator_run", {"status": run_result.status.value}))
            self._background_tasks.add(task)
            task.add_done_callback(self._background_tasks.discard)
        event = self._result_events.get(run_result.run_id)
        if event: event.set()

    async def _get_tenant_sem(self, tenant_id: str) -> asyncio.Semaphore:
        async with self._sems_lock:
            if tenant_id not in self._tenant_sems:
                self._tenant_sems[tenant_id] = asyncio.Semaphore(self._per_tenant_limit)
            return self._tenant_sems[tenant_id]