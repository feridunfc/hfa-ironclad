
from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from hfa.config.keys import RedisTTL
from hfa.dag.schema import DagRedisKey
from hfa.lua.loader import LuaScriptLoader

logger = logging.getLogger(__name__)

_LUA_DIR = (
    Path(__file__).resolve().parent.parent.parent.parent
    / "hfa-core" / "src" / "hfa" / "lua"
)
_LUA_DIR_INSTALLED = Path(__file__).resolve().parent.parent.parent / "hfa" / "lua"

def _lua_path(filename: str) -> Path:
    for base in (_LUA_DIR, _LUA_DIR_INSTALLED):
        p = base / filename
        if p.exists():
            return p
    here = Path(__file__).resolve()
    for parent in here.parents:
        for subdir in ("hfa-core/src/hfa/lua", "hfa/lua"):
            p = parent / subdir / filename
            if p.exists():
                return p
    raise FileNotFoundError(f"{filename} not found in known lua directories")

@dataclass(frozen=True)
class TaskCompleteResult:
    completed: bool
    status: str
    unlocked_count: int = 0
    already_terminal: bool = False

class DagLua:
    def __init__(self, redis) -> None:
        self._redis = redis
        self._task_complete_loader: Optional[LuaScriptLoader] = None

    async def initialise(self) -> None:
        complete_path = _lua_path("task_complete.lua")
        self._task_complete_loader = LuaScriptLoader(self._redis, complete_path)
        await self._task_complete_loader.load()
        logger.info("DagLua initialised: task_complete=%s…", self._task_complete_loader.sha[:8] if self._task_complete_loader and self._task_complete_loader.sha else "fallback")

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
        worker_instance_id: str,
        output_data: str | None = None,
    ) -> TaskCompleteResult:
        if self._task_complete_loader is None:
            await self.initialise()
        assert self._task_complete_loader is not None

        result = await self._task_complete_loader.run(
            num_keys=10,
            keys=[
                DagRedisKey.task_state(task_id),
                DagRedisKey.task_meta(task_id),
                DagRedisKey.run_graph(run_id or ""),
                DagRedisKey.task_children(task_id),
                DagRedisKey.task_output(task_id),
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
                str(RedisTTL.RUN_STATE),
                str(RedisTTL.RUN_META),
                str(RedisTTL.RUN_META),
                str(finished_at_ms),
                reason_code,
                worker_group,
                shard,
                trace_parent,
                trace_state,
                str(getattr(RedisTTL, "STREAM_MAXLEN", 10000)),
                worker_instance_id,
                output_data or "",
            ],
        )

        ok = bool(int(result[0]))
        status = result[1].decode() if isinstance(result[1], bytes) else result[1]
        unlocked = int(result[2])
        already_terminal = bool(int(result[3]))
        return TaskCompleteResult(completed=ok, status=status, unlocked_count=unlocked, already_terminal=already_terminal)
