"""
hfa-control/src/hfa_control/scheduler_lua.py
IRONCLAD Sprint 18 — Lua script loader for scheduler atomic operations

Exposes two atomic operations:

  enqueue_admitted(...)    — Sprint 18.1: atomic queue+meta+state+index write
  dispatch_commit(...)     — Sprint 18.2: preconditioned state transition + meta + ZADD

Both use LuaScriptLoader (EVALSHA + NOSCRIPT recovery).
fakeredis fallback path preserved for unit tests.
"""

from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Optional

from hfa.config.keys import RedisKey, RedisTTL
from hfa.lua.loader import LuaScriptLoader

logger = logging.getLogger(__name__)

# Paths relative to this package's repo location
_LUA_DIR = Path(__file__).parent.parent.parent.parent.parent / "hfa-core" / "src" / "hfa" / "lua"

# Fallback: installed package path
_LUA_DIR_INSTALLED = Path(__file__).parent.parent.parent.parent / "hfa" / "lua"


def _lua_path(filename: str) -> Path:
    for base in (_LUA_DIR, _LUA_DIR_INSTALLED):
        p = base / filename
        if p.exists():
            return p
    # Final fallback: search upward
    here = Path(__file__).resolve()
    for parent in here.parents:
        for subdir in ("hfa-core/src/hfa/lua", "hfa/lua"):
            p = parent / subdir / filename
            if p.exists():
                return p
    raise FileNotFoundError(f"{filename} not found in known lua directories")


class SchedulerLua:
    """
    Loads and executes scheduler Lua scripts via EVALSHA.

    Lifecycle: call await initialise() once at scheduler startup.
    Then use enqueue_admitted() and dispatch_commit() in hot paths.
    """

    def __init__(self, redis) -> None:
        self._redis = redis
        self._enqueue_loader: Optional[LuaScriptLoader] = None
        self._commit_loader:  Optional[LuaScriptLoader] = None

    async def initialise(self) -> None:
        """Load both Lua scripts into Redis. Safe to call multiple times."""
        try:
            enqueue_path = _lua_path("enqueue_admitted.lua")
            self._enqueue_loader = LuaScriptLoader(self._redis, enqueue_path)
            await self._enqueue_loader.load()
        except FileNotFoundError as exc:
            logger.warning("SchedulerLua: enqueue_admitted.lua not found: %s — fallback active", exc)
            self._enqueue_loader = None

        try:
            commit_path = _lua_path("dispatch_commit.lua")
            self._commit_loader = LuaScriptLoader(self._redis, commit_path)
            await self._commit_loader.load()
        except FileNotFoundError as exc:
            logger.warning("SchedulerLua: dispatch_commit.lua not found: %s — fallback active", exc)
            self._commit_loader = None

        enqueue_sha = self._enqueue_loader.sha[:8] if (self._enqueue_loader and self._enqueue_loader.sha) else "fallback"
        commit_sha  = self._commit_loader.sha[:8]  if (self._commit_loader  and self._commit_loader.sha)  else "fallback"
        logger.info("SchedulerLua initialised: enqueue=%s… commit=%s…", enqueue_sha, commit_sha)

    # ------------------------------------------------------------------
    # 18.1 Atomic enqueue
    # ------------------------------------------------------------------

    async def enqueue_admitted(
        self,
        *,
        run_id: str,
        tenant_id: str,
        agent_type: str,
        priority: int,
        preferred_region: str,
        preferred_placement: str,
        admitted_at: float,
        score: float,
    ) -> bool:
        """
        Atomically write:
          - tenant queue ZSET entry (NX)
          - run meta HASH
          - run state STRING = "queued"
          - active tenant SET membership

        Returns True if newly enqueued, False if already present (idempotent).
        Falls back to multi-step write if Lua not available.
        """
        queue_key  = RedisKey.tenant_queue(tenant_id)
        meta_key   = RedisKey.run_meta(run_id)
        active_key = RedisKey.tenant_active_set()
        state_key  = RedisKey.run_state(run_id)

        if self._enqueue_loader is not None:
            try:
                result = await self._enqueue_loader.run(
                    num_keys=4,
                    keys=[queue_key, meta_key, active_key, state_key],
                    args=[
                        score,
                        run_id,
                        tenant_id,
                        agent_type,
                        str(priority),
                        preferred_region or "",
                        preferred_placement or "LEAST_LOADED",
                        str(admitted_at),
                        RedisTTL.RUN_META,      # queue_ttl
                        RedisTTL.RUN_META,      # meta_ttl
                        RedisTTL.RUN_STATE,     # state_ttl
                    ],
                    fallback=lambda: self._enqueue_fallback(
                        queue_key, meta_key, active_key, state_key,
                        run_id, tenant_id, agent_type, priority,
                        preferred_region, preferred_placement, admitted_at, score,
                    ),
                )
                return bool(result)
            except Exception as exc:
                logger.warning("SchedulerLua.enqueue_admitted Lua failed: %s — using fallback", exc)

        # Direct fallback (fakeredis / Lua unavailable)
        return await self._enqueue_fallback(
            queue_key, meta_key, active_key, state_key,
            run_id, tenant_id, agent_type, priority,
            preferred_region, preferred_placement, admitted_at, score,
        )

    async def _enqueue_fallback(
        self,
        queue_key: str, meta_key: str, active_key: str, state_key: str,
        run_id: str, tenant_id: str, agent_type: str, priority: int,
        preferred_region: str, preferred_placement: str,
        admitted_at: float, score: float,
    ) -> bool:
        """Non-atomic fallback for unit tests / Lua-unavailable environments."""
        pipe = self._redis.pipeline()
        pipe.zadd(queue_key, {run_id: score}, nx=True)
        pipe.hset(meta_key, mapping={
            "run_id": run_id,
            "tenant_id": tenant_id,
            "agent_type": agent_type,
            "priority": str(priority),
            "preferred_region": preferred_region or "",
            "preferred_placement": preferred_placement or "LEAST_LOADED",
            "admitted_at": str(admitted_at),
            "queue_state": "queued",
        })
        pipe.set(state_key, "queued", ex=RedisTTL.RUN_STATE)
        pipe.sadd(active_key, tenant_id)
        pipe.expire(queue_key, RedisTTL.RUN_META)
        pipe.expire(meta_key, RedisTTL.RUN_META)
        results = await pipe.execute()
        # results[0] = ZADD return: 1 if new, 0 if already existed
        return bool(results[0])

    # ------------------------------------------------------------------
    # 18.2 Atomic dispatch commit
    # ------------------------------------------------------------------

    async def dispatch_commit(
        self,
        *,
        run_id: str,
        tenant_id: str,
        agent_type: str,
        worker_group: str,
        shard: int,
        reschedule_count: int = 0,
        admitted_at: float,
        running_zset: str,
    ) -> bool:
        """
        Atomically:
          1. Check run_state is "admitted" or "queued" (precondition)
          2. Transition run_state → "scheduled"
          3. Update run_meta with placement decision
          4. Add to running ZSET

        Returns True if committed (proceed with XADD stream writes).
        Returns False if precondition failed (double-dispatch or stale state).
        Falls back to non-atomic writes if Lua not available.
        """
        state_key = RedisKey.run_state(run_id)
        meta_key  = RedisKey.run_meta(run_id)
        now       = time.time()

        if self._commit_loader is not None:
            try:
                result = await self._commit_loader.run(
                    num_keys=3,
                    keys=[state_key, meta_key, running_zset],
                    args=[
                        run_id,
                        tenant_id,
                        agent_type,
                        worker_group,
                        str(shard),
                        str(reschedule_count),
                        str(admitted_at),
                        str(now),
                        RedisTTL.RUN_STATE,
                        RedisTTL.RUN_META,
                    ],
                    fallback=lambda: self._commit_fallback(
                        state_key, meta_key, running_zset,
                        run_id, tenant_id, agent_type, worker_group,
                        shard, reschedule_count, admitted_at, now,
                    ),
                )
                return bool(result)
            except Exception as exc:
                logger.warning("SchedulerLua.dispatch_commit Lua failed: %s — using fallback", exc)

        return await self._commit_fallback(
            state_key, meta_key, running_zset,
            run_id, tenant_id, agent_type, worker_group,
            shard, reschedule_count, admitted_at, now,
        )

    async def _commit_fallback(
        self, state_key: str, meta_key: str, running_zset: str, run_id: str, tenant_id: str, agent_type: str, worker_group: str,
        shard: int, reschedule_count: int, admitted_at: float, scheduled_at: float,
    ) -> bool:
        curr = await self._redis.get(state_key)
        if curr and (curr.decode() if isinstance(curr, bytes) else curr) not in ("admitted", "queued"): return False
        pipe = self._redis.pipeline()
        pipe.set(state_key, "scheduled", ex=RedisTTL.RUN_STATE)
        pipe.hset(meta_key, mapping={
            "run_id": run_id,
            "tenant_id": tenant_id,
            "agent_type": agent_type,
            "worker_group": worker_group,
            "shard": str(shard),
            "reschedule_count": str(reschedule_count),
            "admitted_at": str(admitted_at),
            "scheduled_at": str(scheduled_at),
            "queue_state": "scheduled",
        })
        pipe.expire(meta_key, RedisTTL.RUN_META)
        pipe.zadd(running_zset, {run_id: scheduled_at})
        await pipe.execute()
        return True
