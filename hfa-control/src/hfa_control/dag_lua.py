from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

from hfa.config.keys import RedisKey, RedisTTL
from hfa.dag.schema import DagRedisKey, DagTTL, DagTaskSeed, DagTaskDispatchInput
from hfa.lua.loader import LuaScriptLoader

logger = logging.getLogger(__name__)

_LUA_DIR = (
    Path(__file__).resolve().parent.parent.parent.parent
    / "hfa-core"
    / "src"
    / "hfa"
    / "lua"
)
_LUA_DIR_INSTALLED = Path(__file__).resolve().parent.parent.parent / "hfa" / "lua"


def _lua_path(filename: str) -> Path:
    for base in (_LUA_DIR, _LUA_DIR_INSTALLED):
        candidate = base / filename
        if candidate.exists():
            return candidate

    here = Path(__file__).resolve()
    for parent in here.parents:
        for subdir in ("hfa-core/src/hfa/lua", "hfa/lua"):
            candidate = parent / subdir / filename
            if candidate.exists():
                return candidate

    raise FileNotFoundError(f"{filename} not found in known lua directories")


class TaskAdmitResult:
    def __init__(self, status: str, task_id: str) -> None:
        self.status = status
        self.task_id = task_id

    @property
    def admitted(self) -> bool:
        return self.status in {"seeded_root", "seeded_waiting"}

    @property
    def ready(self) -> bool:
        return self.status == "seeded_root"


class TaskDispatchCommitResult:
    SUCCESS = "committed"
    STATE_CONFLICT = "state_conflict"
    ALREADY_RUNNING = "already_running"
    ILLEGAL_TRANSITION = "illegal_transition"
    INTERNAL_ERROR = "internal_error"

    def __init__(self, status: str, task_id: str, details: str = "") -> None:
        self.status = status
        self.task_id = task_id
        self.details = details

    @property
    def committed(self) -> bool:
        return self.status == self.SUCCESS

    @property
    def should_requeue(self) -> bool:
        return self.status == self.INTERNAL_ERROR


class TaskCompleteResult:
    SUCCESS = "completed"
    STATE_CONFLICT = "state_conflict"
    ILLEGAL_TERMINAL_STATE = "illegal_terminal_state"
    ALREADY_TERMINAL = "already_terminal"

    def __init__(self, status: str, task_id: str, detail: str = "", unlocked_count: int = 0) -> None:
        self.status = status
        self.task_id = task_id
        self.detail = detail
        self.unlocked_count = unlocked_count

    @property
    def completed(self) -> bool:
        return self.status == self.SUCCESS


class DagLua:
    def __init__(self, redis) -> None:
        self._redis = redis
        self._task_admit_loader: Optional[LuaScriptLoader] = None
        self._task_dispatch_loader: Optional[LuaScriptLoader] = None
        self._task_complete_loader: Optional[LuaScriptLoader] = None

    async def initialise(self) -> None:
        admit_path = _lua_path("task_admit.lua")
        self._task_admit_loader = LuaScriptLoader(self._redis, admit_path)
        await self._task_admit_loader.load()

        dispatch_path = _lua_path("task_dispatch_commit.lua")
        self._task_dispatch_loader = LuaScriptLoader(self._redis, dispatch_path)
        await self._task_dispatch_loader.load()

        complete_path = _lua_path("task_complete.lua")
        self._task_complete_loader = LuaScriptLoader(self._redis, complete_path)
        await self._task_complete_loader.load()

        logger.info(
            "DagLua initialised: task_admit=%s… task_dispatch=%s… task_complete=%s…",
            self._task_admit_loader.sha[:8] if self._task_admit_loader and self._task_admit_loader.sha else "fallback",
            self._task_dispatch_loader.sha[:8] if self._task_dispatch_loader and self._task_dispatch_loader.sha else "fallback",
            self._task_complete_loader.sha[:8] if self._task_complete_loader and self._task_complete_loader.sha else "fallback",
        )

    async def task_admit(self, task: DagTaskSeed) -> TaskAdmitResult:
        if self._task_admit_loader is None:
            await self.initialise()
        keys = [
            DagRedisKey.run_graph(task.run_id),
            DagRedisKey.run_tasks(task.run_id),
            DagRedisKey.task_meta(task.task_id),
            DagRedisKey.task_state(task.task_id),
            DagRedisKey.task_remaining_deps(task.task_id),
            DagRedisKey.task_ready_queue(task.tenant_id),
            DagRedisKey.task_ready_emitted(task.task_id),
            DagRedisKey.task_children(task.task_id),
            RedisKey.tenant_active_set(),
        ]
        args = [
            task.task_id,
            task.run_id,
            task.tenant_id,
            task.agent_type,
            str(task.priority),
            str(task.admitted_at),
            str(task.estimated_cost_cents),
            task.preferred_region,
            task.preferred_placement,
            task.payload_json,
            task.trace_parent,
            task.trace_state,
            str(task.dependency_count),
            str(DagTTL.GRAPH_META),
            str(DagTTL.TASK_META),
            str(DagTTL.TASK_STATE),
            str(DagTTL.TASK_READY),
            *task.child_task_ids,
        ]
        result = await self._task_admit_loader.run(num_keys=len(keys), keys=keys, args=args)
        if isinstance(result, (list, tuple)) and result:
            status = result[0].decode() if isinstance(result[0], bytes) else str(result[0])
        else:
            status = str(result)
        return TaskAdmitResult(status, task.task_id)

    async def task_dispatch_commit(self, dispatch: DagTaskDispatchInput) -> TaskDispatchCommitResult:
        if self._task_dispatch_loader is None:
            await self.initialise()

        keys = [
            DagRedisKey.task_state(dispatch.task_id),
            DagRedisKey.task_meta(dispatch.task_id),
            dispatch.running_zset,
            dispatch.control_stream,
            dispatch.shard_stream,
        ]
        args = [
            dispatch.task_id,
            dispatch.run_id,
            dispatch.tenant_id,
            dispatch.agent_type,
            dispatch.worker_group,
            str(dispatch.shard),
            str(dispatch.priority),
            str(dispatch.admitted_at),
            str(dispatch.scheduled_at),
            str(DagTTL.TASK_STATE),
            str(DagTTL.TASK_META),
            str(RedisTTL.STREAM_MAXLEN),
            str(RedisTTL.SHARD_MAXLEN),
            dispatch.trace_parent,
            dispatch.trace_state,
            dispatch.policy,
            dispatch.region,
            dispatch.payload_json,
        ]
        try:
            result = await self._task_dispatch_loader.run(num_keys=len(keys), keys=keys, args=args)
        except Exception as exc:
            logger.warning("DagLua.task_dispatch_commit Lua failed: %s", exc)
            return TaskDispatchCommitResult(TaskDispatchCommitResult.INTERNAL_ERROR, dispatch.task_id, str(exc))

        if isinstance(result, (list, tuple)) and result:
            status = result[0].decode() if isinstance(result[0], bytes) else str(result[0])
            details = result[1].decode() if len(result) > 1 and isinstance(result[1], bytes) else (str(result[1]) if len(result) > 1 else "")
        else:
            status = str(result)
            details = ""
        return TaskDispatchCommitResult(status, dispatch.task_id, details)

    async def task_complete(
        self,
        *,
        task_id: str,
        run_id: str | None = None,
        tenant_id: str,
        terminal_state: str,
        finished_at_ms: int,
        expected_state: str = "running",
        reason_code: str = "completed",
        worker_group: str = "",
        shard: str = "",
        trace_parent: str = "",
        trace_state: str = "",
    ) -> TaskCompleteResult:
        if self._task_complete_loader is None:
            await self.initialise()

        result = await self._task_complete_loader.run(
            num_keys=9,
            keys=[
                DagRedisKey.task_state(task_id),
                DagRedisKey.task_meta(task_id),
                DagRedisKey.run_graph(run_id or ""),
                DagRedisKey.task_children(task_id),
                DagRedisKey.task_key_prefix(),
                DagRedisKey.task_ready_queue(tenant_id),
                DagRedisKey.ready_emitted_prefix(),
                DagRedisKey.completion_stream(tenant_id),
                DagRedisKey.task_running_zset(tenant_id),
            ],
            args=[
                task_id,
                run_id or "",
                tenant_id,
                terminal_state,
                expected_state,
                str(finished_at_ms),
                str(DagTTL.TASK_STATE),
                str(DagTTL.TASK_META),
                str(finished_at_ms),
                reason_code,
                worker_group,
                shard,
                trace_parent,
                trace_state,
                str(getattr(RedisTTL, "STREAM_MAXLEN", 10000)),
            ],
        )

        if isinstance(result, (list, tuple)) and result:
            status = result[0].decode() if isinstance(result[0], bytes) else str(result[0])
            detail = result[1].decode() if len(result) > 1 and isinstance(result[1], bytes) else (str(result[1]) if len(result) > 1 else "")
            raw_unlocked = result[2] if len(result) > 2 else 0
            raw_unlocked = raw_unlocked.decode() if isinstance(raw_unlocked, bytes) else raw_unlocked
            try:
                unlocked = int(raw_unlocked)
            except Exception:
                unlocked = 0
            return TaskCompleteResult(status, task_id, detail, unlocked)

        return TaskCompleteResult("state_conflict", task_id, "invalid_result", 0)
