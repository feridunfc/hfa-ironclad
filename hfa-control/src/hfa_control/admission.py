"""
hfa-control/src/hfa_control/admission.py
IRONCLAD Sprint 10 — Admission Controller

Gate order (Sprint 5/9 contracts preserved, now centralised here)
-----------------------------------------------------------------
  1. validate_run_id_format        (Sprint 5)
  2. tenant cross-check            (Sprint 5)
  3. QuotaManager.check_and_increment_runs  (Sprint 9)
  4. QuotaManager.check_rate_limit          (Sprint 9)
  5. QuotaManager.check_and_reserve_budget  (Sprint 9 v2 — atomic Lua)
  6. Emit RunAdmittedEvent → hfa:stream:control
  7. Set hfa:run:state:{run_id} = 'admitted'

On any gate failure the quota rollback is performed before raising.

IRONCLAD rules
--------------
* No print() — logging only.
* cost_cents: int — no float USD.
* close() not needed (no background tasks).
"""
from __future__ import annotations

import logging

from hfa.events.schema import RunAdmittedEvent
from hfa.events.codec  import serialize_event

try:
    from hfa.governance.quota_manager import QuotaManager           # type: ignore
    from hfa_tools.middleware.tenant  import validate_run_id_format  # type: ignore
    from hfa_tools.middleware.tenant  import TenantFormatError       # type: ignore
except ImportError:
    # Allow isolated testing without full hfa-tools dependency
    QuotaManager            = None   # type: ignore
    validate_run_id_format  = None   # type: ignore
    TenantFormatError       = Exception

try:
    from hfa.obs.tracing import get_tracer, HFATracing               # type: ignore
    _tracer = get_tracer("hfa.admission")
except Exception:
    _tracer = None

from hfa_control.exceptions import (
    AdmissionError, QuotaExceededError,
    RateLimitedError, BudgetExceededError,
)

logger = logging.getLogger(__name__)


def _noop_span():
    """Minimal no-op context manager when OTel is not available."""
    class _Span:
        def __enter__(self): return self
        def __exit__(self, *_): pass
        def set_attribute(self, *_): pass
    return _Span()


class AdmissionController:

    def __init__(self, redis, config) -> None:
        self._redis  = redis
        self._config = config
        self._quota  = QuotaManager(redis) if QuotaManager else None

    async def admit(self, request) -> str:
        """
        Admit a RunRequest.
        Returns run_id on success.
        Raises AdmissionError subclass on rejection.

        request must expose:
          .run_id, .tenant_id, .agent_type, .priority,
          .payload (dict), .estimated_cost_cents (int),
          .preferred_region (str, optional),
          .preferred_placement (str, optional)
        """
        span = (
            _tracer.start_as_current_span("hfa.admission.admit")
            if _tracer else _noop_span()
        )
        with span as sp:
            _set_attr(sp, "hfa.run_id",     request.run_id)
            _set_attr(sp, "hfa.tenant_id",  request.tenant_id)
            _set_attr(sp, "hfa.agent_type", request.agent_type)

            try:
                # Gate 1+2: format + tenant
                if validate_run_id_format:
                    try:
                        ext_tenant, _ = validate_run_id_format(request.run_id)
                        if ext_tenant != request.tenant_id:
                            raise AdmissionError(
                                f"run_id tenant mismatch: "
                                f"run_id encodes {ext_tenant!r}, "
                                f"header says {request.tenant_id!r}"
                            )
                    except TenantFormatError as exc:
                        raise AdmissionError(str(exc)) from exc

                # Gate 3: concurrent run quota
                if self._quota:
                    if not await self._quota.check_and_increment_runs(
                        request.tenant_id
                    ):
                        raise QuotaExceededError(
                            f"Concurrent run limit exceeded: {request.tenant_id!r}"
                        )

                # Gate 4: rate limit
                if self._quota:
                    if not await self._quota.check_rate_limit(request.tenant_id):
                        # rollback concurrent counter
                        await self._quota.decrement_runs(request.tenant_id)
                        raise RateLimitedError(
                            f"RPM limit exceeded: {request.tenant_id!r}"
                        )

                # Gate 5: atomic budget reserve
                estimated = int(getattr(request, "estimated_cost_cents", 0))
                if self._quota and estimated > 0:
                    if not await self._quota.check_and_reserve_budget(
                        request.tenant_id, estimated
                    ):
                        await self._quota.decrement_runs(request.tenant_id)
                        raise BudgetExceededError(
                            f"Budget exceeded: {request.tenant_id!r}"
                        )

                # Emit RunAdmittedEvent
                evt = RunAdmittedEvent(
                    run_id=request.run_id,
                    tenant_id=request.tenant_id,
                    agent_type=request.agent_type,
                    priority=request.priority,
                    payload=(
                        request.payload
                        if isinstance(request.payload, dict)
                        else vars(request.payload)
                    ),
                    estimated_cost_cents=estimated,
                    preferred_region=getattr(request, "preferred_region", ""),
                    preferred_placement=getattr(
                        request, "preferred_placement", "LEAST_LOADED"
                    ),
                )

                # Inject W3C trace context if OTel available
                try:
                    if HFATracing:
                        HFATracing.inject_trace_context(evt)  # type: ignore
                except Exception:
                    pass

                await self._redis.xadd(
                    self._config.control_stream,
                    serialize_event(evt),
                    maxlen=100_000,
                    approximate=True,
                )
                await self._redis.set(
                    f"hfa:run:state:{request.run_id}", "admitted", ex=86400
                )

                logger.info(
                    "Admitted: run=%s tenant=%s agent=%s priority=%d",
                    request.run_id, request.tenant_id,
                    request.agent_type, request.priority,
                )
                _set_attr(sp, "hfa.admitted", "true")
                return request.run_id

            except AdmissionError:
                _set_attr(sp, "hfa.admitted", "false")
                raise
            except Exception as exc:
                _set_attr(sp, "hfa.admitted", "false")
                logger.error(
                    "AdmissionController.admit unexpected error: run=%s %s",
                    request.run_id, exc, exc_info=True,
                )
                raise


def _set_attr(span, key: str, value: str) -> None:
    try:
        span.set_attribute(key, value)
    except Exception:
        pass
