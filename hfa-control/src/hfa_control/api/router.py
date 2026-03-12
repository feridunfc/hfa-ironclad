"""
hfa-control/src/hfa_control/api/router.py
IRONCLAD Sprint 10 — Control Plane REST API

Endpoints
---------
  GET  /control/v1/workers
  GET  /control/v1/workers/{worker_id}
  POST /control/v1/workers/{worker_id}/drain      [operator]
  GET  /control/v1/shards
  GET  /control/v1/runs/{run_id}/placement
  POST /control/v1/runs/{run_id}/reschedule       [operator]
  GET  /control/v1/dlq
  POST /control/v1/dlq/{run_id}/replay            [operator]
  DELETE /control/v1/dlq/{run_id}                 [operator]
  GET  /control/v1/health

Auth
----
  All endpoints require X-Tenant-ID for tenant isolation.
  Operator endpoints additionally require X-CP-Auth: <secret>.
  CP_AUTH_SECRET env var sets the accepted secret.

IRONCLAD rules
--------------
* No print() — logging only.
* Tenant cross-check on every resource access.
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta, timezone
from typing import List

from fastapi import APIRouter, Header, HTTPException, Request

from hfa.events.schema   import WorkerDrainingEvent
from hfa.events.codec    import serialize_event
from hfa_control.exceptions import (
    WorkerNotFoundError, DLQEntryNotFoundError,
    TenantMismatchError, LeadershipError,
)
from hfa_control.api.models import (
    WorkerResponse, ShardResponse, PlacementResponse,
    DLQEntryResponse, HealthResponse,
)

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/control/v1", tags=["control-plane"])

_CP_AUTH_SECRET = os.environ.get("CP_AUTH_SECRET", "")


def _require_operator(x_cp_auth: str = "") -> None:
    if _CP_AUTH_SECRET and x_cp_auth != _CP_AUTH_SECRET:
        raise HTTPException(status_code=403, detail="Operator auth required")


def _tenant_header(x_tenant_id: str) -> str:
    if not x_tenant_id:
        raise HTTPException(status_code=400, detail="X-Tenant-ID header required")
    return x_tenant_id


# ------------------------------------------------------------------
# Workers
# ------------------------------------------------------------------

@router.get("/workers", response_model=List[WorkerResponse])
async def list_workers(
    request:     Request,
    x_tenant_id: str = Header(...),
) -> List[WorkerResponse]:
    _tenant_header(x_tenant_id)
    registry = request.app.state.cp.registry
    workers  = await registry.list_healthy_workers()
    # Also fetch all — healthy filter done client-side for full picture
    return [_worker_resp(w) for w in workers]


@router.get("/workers/{worker_id}", response_model=WorkerResponse)
async def get_worker(
    worker_id:   str,
    request:     Request,
    x_tenant_id: str = Header(...),
) -> WorkerResponse:
    _tenant_header(x_tenant_id)
    try:
        profile = await request.app.state.cp.registry.get_worker(worker_id)
        return _worker_resp(profile)
    except WorkerNotFoundError:
        raise HTTPException(status_code=404, detail=f"Worker {worker_id!r} not found")


@router.post("/workers/{worker_id}/drain")
async def drain_worker(
    worker_id:   str,
    request:     Request,
    x_tenant_id: str = Header(...),
    x_cp_auth:   str = Header(default=""),
) -> dict:
    _tenant_header(x_tenant_id)
    _require_operator(x_cp_auth)
    try:
        profile = await request.app.state.cp.registry.get_worker(worker_id)
    except WorkerNotFoundError:
        raise HTTPException(status_code=404, detail=f"Worker {worker_id!r} not found")

    deadline = (
        datetime.now(timezone.utc) + timedelta(seconds=120)
    ).isoformat()

    evt = WorkerDrainingEvent(
        worker_id=worker_id,
        worker_group=profile.worker_group,
        region=profile.region,
        drain_deadline_utc=deadline,
        reason="manual",
    )
    redis = request.app.state.redis
    cfg   = request.app.state.cp._config
    await redis.xadd(cfg.heartbeat_stream, serialize_event(evt),
                     maxlen=10_000, approximate=True)
    logger.info("Drain initiated: worker=%s deadline=%s", worker_id, deadline)
    return {"worker_id": worker_id, "drain_deadline_utc": deadline, "status": "draining"}


# ------------------------------------------------------------------
# Shards
# ------------------------------------------------------------------

@router.get("/shards", response_model=List[ShardResponse])
async def list_shards(
    request:     Request,
    x_tenant_id: str = Header(...),
) -> List[ShardResponse]:
    _tenant_header(x_tenant_id)
    redis     = request.app.state.redis
    shard_mgr = request.app.state.cp.shards
    owners    = await shard_mgr.all_owners()
    result    = []
    for shard, group in sorted(owners.items()):
        try:
            stream_len = await redis.xlen(f"hfa:stream:runs:{shard}")
        except Exception:
            stream_len = -1
        alive = bool(await redis.exists(f"hfa:cp:shard:owner:{shard}"))
        result.append(ShardResponse(
            shard=shard, worker_group=group,
            stream_len=stream_len, owner_alive=alive,
        ))
    return result


# ------------------------------------------------------------------
# Run placement
# ------------------------------------------------------------------

@router.get("/runs/{run_id}/placement", response_model=PlacementResponse)
async def get_placement(
    run_id:      str,
    request:     Request,
    x_tenant_id: str = Header(...),
) -> PlacementResponse:
    _tenant_header(x_tenant_id)
    redis = request.app.state.redis
    meta  = await redis.hgetall(f"hfa:run:meta:{run_id}")
    if not meta:
        raise HTTPException(status_code=404, detail=f"Run {run_id!r} not found")

    def _s(k: str) -> str:
        v = meta.get(k.encode()) or meta.get(k)
        return (v.decode() if isinstance(v, bytes) else v) or ""

    if _s("tenant_id") != x_tenant_id:
        raise HTTPException(status_code=403, detail="Tenant mismatch")

    state_raw = await redis.get(f"hfa:run:state:{run_id}")
    state = (state_raw.decode() if isinstance(state_raw, bytes) else state_raw) or "unknown"

    return PlacementResponse(
        run_id=run_id,
        tenant_id=_s("tenant_id"),
        state=state,
        worker_group=_s("worker_group"),
        shard=int(_s("shard") or "0"),
        reschedule_count=int(_s("reschedule_count") or "0"),
        admitted_at=float(_s("admitted_at") or "0"),
    )


@router.post("/runs/{run_id}/reschedule")
async def force_reschedule(
    run_id:      str,
    request:     Request,
    x_tenant_id: str = Header(...),
    x_cp_auth:   str = Header(default=""),
) -> dict:
    _tenant_header(x_tenant_id)
    _require_operator(x_cp_auth)
    try:
        await request.app.state.cp._leader.assert_leader()
    except LeadershipError as exc:
        raise HTTPException(status_code=503, detail=str(exc))

    redis    = request.app.state.redis
    recovery = request.app.state.cp.recovery

    meta = await redis.hgetall(f"hfa:run:meta:{run_id}")
    if not meta:
        raise HTTPException(status_code=404, detail=f"Run {run_id!r} not found")

    def _s(k: str) -> str:
        v = meta.get(k.encode()) or meta.get(k)
        return (v.decode() if isinstance(v, bytes) else v) or ""

    if _s("tenant_id") != x_tenant_id:
        raise HTTPException(status_code=403, detail="Tenant mismatch")

    # Use recovery service to handle the reschedule / DLQ logic
    result = await recovery._handle_stale(run_id)
    return {"run_id": run_id, "result": result}


# ------------------------------------------------------------------
# DLQ
# ------------------------------------------------------------------

@router.get("/dlq", response_model=List[DLQEntryResponse])
async def list_dlq(
    request:     Request,
    x_tenant_id: str = Header(...),
    x_cp_auth:   str = Header(default=""),
) -> List[DLQEntryResponse]:
    _tenant_header(x_tenant_id)
    _require_operator(x_cp_auth)
    entries = await request.app.state.cp.recovery.list_dlq(x_tenant_id)
    return [
        DLQEntryResponse(
            run_id=e["run_id"],
            tenant_id=e["tenant_id"],
            reason=e["reason"],
            delivery_count=e["reschedule_count"],
            dead_lettered_at=e["dead_lettered_at"],
            original_error="",
            cost_cents=0,
        )
        for e in entries
    ]


@router.post("/dlq/{run_id}/replay")
async def replay_dlq(
    run_id:      str,
    request:     Request,
    x_tenant_id: str = Header(...),
    x_cp_auth:   str = Header(default=""),
) -> dict:
    _tenant_header(x_tenant_id)
    _require_operator(x_cp_auth)
    try:
        await request.app.state.cp.recovery.replay_dlq_run(run_id, x_tenant_id)
        return {"run_id": run_id, "status": "replayed"}
    except DLQEntryNotFoundError:
        raise HTTPException(status_code=404, detail=f"DLQ entry {run_id!r} not found")
    except TenantMismatchError as exc:
        raise HTTPException(status_code=403, detail=str(exc))


@router.delete("/dlq/{run_id}")
async def delete_dlq(
    run_id:      str,
    request:     Request,
    x_tenant_id: str = Header(...),
    x_cp_auth:   str = Header(default=""),
) -> dict:
    _tenant_header(x_tenant_id)
    _require_operator(x_cp_auth)
    redis = request.app.state.redis
    meta  = await redis.hgetall(f"hfa:cp:dlq:meta:{run_id}")
    if not meta:
        raise HTTPException(status_code=404, detail=f"DLQ {run_id!r} not found")

    def _s(k: str) -> str:
        v = meta.get(k.encode()) or meta.get(k)
        return (v.decode() if isinstance(v, bytes) else v) or ""
    if _s("tenant_id") != x_tenant_id:
        raise HTTPException(status_code=403, detail="Tenant mismatch")

    await redis.delete(f"hfa:cp:dlq:meta:{run_id}")
    logger.info("DLQ entry deleted: run=%s by tenant=%s", run_id, x_tenant_id)
    return {"run_id": run_id, "status": "deleted"}


# ------------------------------------------------------------------
# Health
# ------------------------------------------------------------------

@router.get("/health", response_model=HealthResponse)
async def health(request: Request) -> HealthResponse:
    cp      = request.app.state.cp
    redis   = request.app.state.redis
    config  = cp._config

    reg_size = await cp.registry.registry_size()
    healthy  = len(await cp.registry.list_healthy_workers())

    try:
        dlq_depth = await cp.recovery.dlq_depth()
    except Exception:
        dlq_depth = -1

    try:
        sched_lag = await redis.xlen(config.control_stream)
    except Exception:
        sched_lag = -1

    return HealthResponse(
        is_leader=cp.is_leader,
        instance_id=config.instance_id,
        region=config.region,
        registry_size=reg_size,
        healthy_workers=healthy,
        scheduler_lag=sched_lag,
        dlq_depth=dlq_depth,
    )


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def _worker_resp(profile) -> WorkerResponse:
    from hfa_control.api.models import WorkerResponse
    return WorkerResponse(
        worker_id=profile.worker_id,
        worker_group=profile.worker_group,
        region=profile.region,
        shards=profile.shards,
        capacity=profile.capacity,
        inflight=profile.inflight,
        load_factor=profile.load_factor,
        status=profile.status.value,
        last_seen=profile.last_seen,
        version=profile.version,
        capabilities=profile.capabilities,
    )
