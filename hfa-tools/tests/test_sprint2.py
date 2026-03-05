"""
hfa-tools/tests/test_sprint2.py
IRONCLAD — Sprint 2 test suite.

Coverage:
  * BudgetGuard   — debit, exhaustion, freeze, recovery, evalsha unpack
  * SignedLedger  — append, chain verify, tamper detection
  * supervisor.py — BudgetPolicy, ComplianceRule, PolicyInjectionResult
  * tenant.py     — extract, validate, assert_owns, LRU cache, middleware
  * ledger.py     — middleware fire-and-forget, skip paths
"""
from __future__ import annotations

import asyncio
import json
import time
import uuid
from unittest.mock import AsyncMock, MagicMock, call, patch

import pytest

# ---------------------------------------------------------------------------
# BudgetGuard tests
# ---------------------------------------------------------------------------

class TestBudgetGuard:
    """Unit tests using a mock Redis client."""

    @pytest.fixture
    def redis(self):
        r = AsyncMock()
        pipe = AsyncMock()
        pipe.execute = AsyncMock(return_value=[None, None, None])
        r.pipeline = MagicMock(return_value=pipe)
        r.script_load = AsyncMock(side_effect=["sha_debit", "sha_check", "sha_freeze", "sha_reset"])
        return r

    @pytest.mark.asyncio
    async def test_initialise_loads_four_scripts(self, redis):
        from hfa.governance.budget_guard import BudgetGuard

        guard = BudgetGuard(redis)
        await guard.initialise()

        assert redis.script_load.call_count == 4
        assert guard._sha_debit == "sha_debit"
        assert guard._sha_check == "sha_check"

    @pytest.mark.asyncio
    async def test_usd_to_cents_rounds_up(self):
        from hfa.governance.budget_guard import usd_to_cents

        assert usd_to_cents(1.00)  == 100
        assert usd_to_cents(0.003) == 1     # ceil($0.003 * 100 = 0.3) → 1
        assert usd_to_cents(0.01)  == 1
        assert usd_to_cents(5.50)  == 550

    @pytest.mark.asyncio
    async def test_debit_rejects_float(self, redis):
        from hfa.governance.budget_guard import BudgetGuard

        guard = BudgetGuard(redis)
        await guard.initialise()
        redis.get = AsyncMock(return_value="500")

        with pytest.raises(TypeError, match="must be int"):
            await guard.debit("acme", "run-acme-abc", 0.003)  # float → rejected

    @pytest.mark.asyncio
    async def test_debit_evalsha_called_with_unpacked_args(self, redis):
        """IRONCLAD rule: evalsha must receive unpacked positional args, not lists."""
        from hfa.governance.budget_guard import BudgetGuard, BudgetStatus

        guard = BudgetGuard(redis)
        await guard.initialise()

        redis.get = AsyncMock(return_value="500")           # limit_cents = 500
        redis.evalsha = AsyncMock(
            side_effect=[1, ["1", "active"]]                # check=1 (allowed), debit result
        )

        state = await guard.debit("acme_corp", "run-acme_corp-abc123", 1)

        # Verify CHECK call: evalsha(sha, 2, key1, key2, arg1, arg2) — positional
        check_call = redis.evalsha.call_args_list[0]
        args = check_call.args
        assert args[0] == "sha_check"
        assert args[1] == 2             # num_keys — int, not list
        assert isinstance(args[2], str) # spent_key
        assert isinstance(args[3], str) # status_key
        assert args[4] == "1"           # amount_cents — str, unpacked
        assert args[5] == "500"         # limit_cents  — str, unpacked

        assert state.status == BudgetStatus.ACTIVE
        assert state.spent_cents == 1
        assert state.spent_usd == pytest.approx(0.01)

    @pytest.mark.asyncio
    async def test_debit_raises_on_exhausted(self, redis):
        from hfa.governance.budget_guard import BudgetGuard, BudgetExhaustedError

        guard = BudgetGuard(redis)
        await guard.initialise()

        redis.get = AsyncMock(return_value="500")
        redis.evalsha = AsyncMock(return_value=0)  # check denied

        with pytest.raises(BudgetExhaustedError) as exc_info:
            await guard.debit("acme_corp", "run-acme_corp-abc", 300)

        assert exc_info.value.tenant_id == "acme_corp"
        assert exc_info.value.attempted_cents == 300

    @pytest.mark.asyncio
    async def test_debit_fails_closed_on_redis_error(self, redis):
        from hfa.governance.budget_guard import BudgetGuard, BudgetGuardError

        guard = BudgetGuard(redis, fail_open=False)
        await guard.initialise()
        redis.get = AsyncMock(side_effect=ConnectionError("Redis down"))

        with pytest.raises(BudgetGuardError):
            await guard.debit("acme_corp", "run-acme_corp-abc", 1)

    @pytest.mark.asyncio
    async def test_debit_fail_open_permits_on_redis_error(self, redis):
        from hfa.governance.budget_guard import BudgetGuard, BudgetStatus

        guard = BudgetGuard(redis, fail_open=True)
        await guard.initialise()
        redis.get = AsyncMock(side_effect=ConnectionError("Redis down"))

        state = await guard.debit("acme_corp", "run-acme_corp-abc", 1)
        assert state.status == BudgetStatus.ACTIVE

    @pytest.mark.asyncio
    async def test_redis_keys_use_cents_suffix(self, redis):
        """Key names must end in _cents, not _usd."""
        from hfa.governance.budget_guard import BudgetGuard

        guard = BudgetGuard(redis)
        limit_key, spent_key, _ = guard._keys("acme", "run-acme-x")
        assert limit_key.endswith(":limit_cents")
        assert spent_key.endswith(":spent_cents")

    @pytest.mark.asyncio
    async def test_freeze_uses_evalsha_unpack(self, redis):
        from hfa.governance.budget_guard import BudgetGuard

        guard = BudgetGuard(redis)
        await guard.initialise()
        redis.evalsha = AsyncMock(return_value=1)

        await guard.freeze("acme_corp", "run-acme_corp-abc")

        args = redis.evalsha.call_args.args
        assert args[0] == "sha_freeze"
        assert args[1] == 1              # num_keys int
        assert "status" in args[2]       # status_key

    @pytest.mark.asyncio
    async def test_recover_run_exhausted_when_spent_equals_limit(self, redis):
        from hfa.governance.budget_guard import BudgetGuard

        guard = BudgetGuard(redis)
        pipe = AsyncMock()
        pipe.execute = AsyncMock(return_value=[None, None, None])
        redis.pipeline = MagicMock(return_value=pipe)

        await guard.recover_run("acme_corp", "run-x", 500, 500)

        set_calls = pipe.set.call_args_list
        status_call = [c for c in set_calls if c.args[1] in ("active", "exhausted")]
        assert status_call[0].args[1] == "exhausted"

    @pytest.mark.asyncio
    async def test_recover_run_rejects_float(self, redis):
        from hfa.governance.budget_guard import BudgetGuard

        guard = BudgetGuard(redis)
        with pytest.raises(TypeError, match="must be int"):
            await guard.recover_run("acme", "run-x", 500.0, 100)

    @pytest.mark.asyncio
    async def test_raises_without_initialise(self, redis):
        from hfa.governance.budget_guard import BudgetGuard, BudgetGuardError

        guard = BudgetGuard(redis)  # NOT initialised
        with pytest.raises(BudgetGuardError, match="initialised"):
            await guard.debit("t", "r", 1)


# ---------------------------------------------------------------------------
# SignedLedger tests
# ---------------------------------------------------------------------------

class TestSignedLedger:
    @pytest.fixture
    def key_provider(self, tmp_path):
        """Create real Ed25519 key pair for tests."""
        from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
        from cryptography.hazmat.primitives.serialization import (
            Encoding, NoEncryption, PrivateFormat, PublicFormat,
        )
        from hfa.governance.signed_ledger_v1 import Ed25519EnvKeyProvider

        priv = Ed25519PrivateKey.generate()
        priv_pem = priv.private_bytes(Encoding.PEM, PrivateFormat.PKCS8, NoEncryption())
        pub_pem = priv.public_key().public_bytes(Encoding.PEM, PublicFormat.SubjectPublicKeyInfo)

        return Ed25519EnvKeyProvider(
            private_pem=priv_pem,
            public_pem=pub_pem,
            key_id="test-key-001",
        )

    @pytest.fixture
    def ledger(self, key_provider):
        from hfa.governance.signed_ledger_v1 import SignedLedger, InMemoryLedgerStore

        return SignedLedger(key_provider=key_provider, store=InMemoryLedgerStore())

    @pytest.mark.asyncio
    async def test_append_and_verify_single_entry(self, ledger):
        entry = await ledger.append(
            tenant_id="acme_corp",
            run_id="run-acme_corp-001",
            event_type="llm_call",
            payload={"model": "gpt-4o", "tokens": 312},
        )

        assert entry.sequence == 1
        assert entry.prev_hash == ""
        assert len(entry.signature) == 128  # 64 bytes hex = 128 chars
        assert entry.key_id == "test-key-001"

        ok = await ledger.verify_chain("acme_corp", "run-acme_corp-001")
        assert ok is True

    @pytest.mark.asyncio
    async def test_chain_links_entries(self, ledger):
        e1 = await ledger.append("acme", "run-acme-001", "evt", {"n": 1})
        e2 = await ledger.append("acme", "run-acme-001", "evt", {"n": 2})
        e3 = await ledger.append("acme", "run-acme-001", "evt", {"n": 3})

        assert e2.prev_hash == e1.content_hash()
        assert e3.prev_hash == e2.content_hash()
        assert e1.sequence == 1
        assert e2.sequence == 2
        assert e3.sequence == 3

    @pytest.mark.asyncio
    async def test_verify_empty_ledger(self, ledger):
        ok = await ledger.verify_chain("nobody", "run-nobody-xyz")
        assert ok is True

    @pytest.mark.asyncio
    async def test_tamper_detection_signature(self, ledger):
        from hfa.governance.signed_ledger_v1 import LedgerIntegrityError

        await ledger.append("acme", "run-acme-t1", "evt", {"v": 1})

        # Tamper: replace the entry with a bad signature
        store = ledger._store
        entries = store._store["acme:run-acme-t1"]
        original = entries[0]
        tampered = original.__class__(
            **{**original.__dict__, "signature": "a" * 128}
        )
        store._store["acme:run-acme-t1"] = [tampered]

        with pytest.raises(LedgerIntegrityError, match="Invalid signature"):
            await ledger.verify_chain("acme", "run-acme-t1")

    @pytest.mark.asyncio
    async def test_tamper_detection_prev_hash(self, ledger):
        from hfa.governance.signed_ledger_v1 import LedgerIntegrityError

        e1 = await ledger.append("acme", "run-acme-t2", "evt", {"v": 1})
        e2 = await ledger.append("acme", "run-acme-t2", "evt", {"v": 2})

        # Tamper: corrupt prev_hash of second entry
        store = ledger._store
        entries = store._store["acme:run-acme-t2"]
        # replace e2 with wrong prev_hash (not re-signing, so also fails sig check)
        tampered = entries[1].__class__(
            **{**entries[1].__dict__, "prev_hash": "0" * 64}
        )
        store._store["acme:run-acme-t2"] = [entries[0], tampered]

        with pytest.raises(LedgerIntegrityError):
            await ledger.verify_chain("acme", "run-acme-t2")

    @pytest.mark.asyncio
    async def test_content_hash_is_deterministic(self, ledger):
        e = await ledger.append("acme", "run-acme-h1", "test", {"x": 1})
        assert e.content_hash() == e.content_hash()

    @pytest.mark.asyncio
    async def test_idempotent_append(self, ledger):
        e = await ledger.append("acme", "run-acme-idem", "evt", {"v": 1})
        await ledger._store.append(e)  # duplicate

        entries = await ledger.get_entries("acme", "run-acme-idem")
        assert len(entries) == 1

    @pytest.mark.asyncio
    async def test_close_drains_empty_in_flight(self, ledger):
        """close() with no pending tasks completes immediately."""
        await ledger.close()  # must not raise

    @pytest.mark.asyncio
    async def test_close_drains_pending_tasks(self, key_provider):
        """close() awaits all in-flight tasks before returning."""
        from hfa.governance.signed_ledger_v1 import SignedLedger, InMemoryLedgerStore

        ledger = SignedLedger(key_provider=key_provider, store=InMemoryLedgerStore())

        drained = []

        async def slow_append():
            await asyncio.sleep(0.02)
            drained.append(True)

        loop = asyncio.get_running_loop()
        task = loop.create_task(slow_append())
        ledger._in_flight.add(task)
        task.add_done_callback(ledger._in_flight.discard)

        await ledger.close()
        assert len(drained) == 1   # task completed before close() returned

    @pytest.mark.asyncio
    async def test_async_context_manager_calls_close(self, key_provider):
        """async with SignedLedger(...) as ledger: must call close() on exit."""
        from hfa.governance.signed_ledger_v1 import SignedLedger, InMemoryLedgerStore

        async with SignedLedger(
            key_provider=key_provider, store=InMemoryLedgerStore()
        ) as ledger:
            await ledger.append("acme", "run-acme-ctx", "test", {"x": 1})

        # After __aexit__ the in_flight set should be empty
        assert len(ledger._in_flight) == 0


# ---------------------------------------------------------------------------
# Supervisor schema tests
# ---------------------------------------------------------------------------

class TestSupervisorSchemas:
    def test_budget_policy_valid(self):
        from hfa.schemas.supervisor import BudgetPolicy

        bp = BudgetPolicy(limit_usd=10.0, alert_at_fraction=0.7, hard_stop_at_fraction=0.95)
        assert bp.limit_usd == pytest.approx(10.0)
        assert bp.alert_at_fraction == pytest.approx(0.7)

    def test_budget_policy_alert_after_hard_stop_raises(self):
        from hfa.schemas.supervisor import BudgetPolicy
        from pydantic import ValidationError

        with pytest.raises(ValidationError, match="alert_at_fraction"):
            BudgetPolicy(limit_usd=10.0, alert_at_fraction=0.9, hard_stop_at_fraction=0.8)

    def test_compliance_rule_and_logic_model(self):
        from hfa.schemas.supervisor import ComplianceRule, ComplianceAction, ComplianceCondition

        rule = ComplianceRule(
            rule_id="no-pii",
            description="Deny requests containing PII keywords",
            action=ComplianceAction.DENY,
            conditions=[
                ComplianceCondition(
                    field_path="requirement",
                    operator="contains",
                    value="social_security",
                )
            ],
        )
        assert rule.action == ComplianceAction.DENY
        assert len(rule.conditions) == 1

    def test_supervisor_policy_sorted_rules(self):
        from hfa.schemas.supervisor import (
            SupervisorPolicy, BudgetPolicy, RoutingPolicy,
            ComplianceRule, ComplianceAction, ComplianceCondition,
        )

        make_rule = lambda rid, prio: ComplianceRule(
            rule_id=rid,
            description="Test rule description long enough",
            action=ComplianceAction.ALLOW,
            conditions=[ComplianceCondition(
                field_path="req", operator="equals", value="x"
            )],
            priority=prio,
        )

        policy = SupervisorPolicy(
            policy_id="test-policy-01",
            budget=BudgetPolicy(limit_usd=5.0),
            routing=RoutingPolicy(default_agent="architect"),
            rules=[make_rule("r3", 30), make_rule("r1", 10), make_rule("r2", 20)],
        )

        sorted_ids = [r.rule_id for r in policy.sorted_rules()]
        assert sorted_ids == ["r1", "r2", "r3"]

    def test_policy_injection_result_hitl_sets_flag(self):
        from hfa.schemas.supervisor import PolicyInjectionResult, ComplianceAction

        result = PolicyInjectionResult(
            action=ComplianceAction.HITL,
            policy_id="test-policy-01",
            reasoning="Triggered PII detection rule",
        )
        assert result.requires_human_review is True

    def test_policy_injection_result_allow_no_flag(self):
        from hfa.schemas.supervisor import PolicyInjectionResult, ComplianceAction

        result = PolicyInjectionResult(
            action=ComplianceAction.ALLOW,
            policy_id="test-policy-01",
            reasoning="No rules matched, default allow",
        )
        assert result.requires_human_review is False


# ---------------------------------------------------------------------------
# Tenant middleware tests
# ---------------------------------------------------------------------------

class TestTenantExtraction:
    def test_extract_tenant_from_resource_id(self):
        from hfa_tools.middleware.tenant import extract_tenant_from_resource_id

        assert extract_tenant_from_resource_id("run-acme_corp-550e8400-e29b-41d4") == "acme_corp"
        assert extract_tenant_from_resource_id("plan-my.tenant-abc123") == "my.tenant"
        assert extract_tenant_from_resource_id("nodash") is None
        assert extract_tenant_from_resource_id("only-one") is None

    def test_exact_segment_match_not_startswith(self):
        """IRONCLAD: tenant must match exact segment, not prefix."""
        from hfa_tools.middleware.tenant import assert_tenant_owns_resource, TenantMismatchError

        # Evil corp tries to spoof acme_corp via crafted resource_id
        with pytest.raises(TenantMismatchError):
            assert_tenant_owns_resource(
                "evilcorp",
                "run-acme_corp-uuid-goes-here",
            )

    def test_tenant_owns_resource_passes(self):
        from hfa_tools.middleware.tenant import assert_tenant_owns_resource

        # Should not raise
        assert_tenant_owns_resource("acme_corp", "run-acme_corp-550e8400-e29b-41d4")

    def test_is_valid_tenant_id(self):
        from hfa_tools.middleware.tenant import is_valid_tenant_id

        assert is_valid_tenant_id("acme_corp") is True
        assert is_valid_tenant_id("my.company") is True
        assert is_valid_tenant_id("tenant-123") is True
        assert is_valid_tenant_id("ab") is False          # too short
        assert is_valid_tenant_id("a" * 101) is False     # too long
        assert is_valid_tenant_id("has space") is False
        assert is_valid_tenant_id("_starts_underscore_x") is False

    def test_lru_cache_hit(self):
        """Validate that LRU cache is being used (call twice, miss only once)."""
        from hfa_tools.middleware.tenant import is_valid_tenant_id

        is_valid_tenant_id.cache_clear()
        is_valid_tenant_id("cached-tenant-01")
        is_valid_tenant_id("cached-tenant-01")
        info = is_valid_tenant_id.cache_info()
        assert info.hits >= 1

    @pytest.mark.asyncio
    async def test_middleware_injects_context_from_header(self):
        from hfa_tools.middleware.tenant import TenantMiddleware, TenantContext

        captured_state = {}

        async def dummy_app(scope, receive, send):
            pass

        middleware = TenantMiddleware(dummy_app)

        mock_request = MagicMock()
        mock_request.url.path = "/api/v1/plans"
        mock_request.headers = {
            "x-tenant-id": "acme_corp",
            "x-run-id": "run-acme_corp-00000001",
        }
        mock_request.path_params = {}
        mock_request.state = MagicMock()
        mock_request.client = None

        async def call_next(req):
            resp = MagicMock()
            resp.status_code = 200
            resp.headers = {}
            return resp

        response = await middleware.dispatch(mock_request, call_next)

        ctx: TenantContext = mock_request.state.tenant
        assert ctx.tenant_id == "acme_corp"
        assert ctx.run_id == "run-acme_corp-00000001"
        assert ctx.source == "header"

    @pytest.mark.asyncio
    async def test_middleware_returns_400_on_missing_tenant(self):
        from hfa_tools.middleware.tenant import TenantMiddleware

        async def dummy_app(scope, receive, send):
            pass

        middleware = TenantMiddleware(dummy_app)

        mock_request = MagicMock()
        mock_request.url.path = "/api/v1/plans"
        mock_request.headers = {}
        mock_request.path_params = {}
        mock_request.client = None

        async def call_next(req):
            return MagicMock(status_code=200, headers={})

        response = await middleware.dispatch(mock_request, call_next)
        assert response.status_code == 400

    @pytest.mark.asyncio
    async def test_middleware_skips_health_path(self):
        from hfa_tools.middleware.tenant import TenantMiddleware

        called = []

        async def dummy_app(scope, receive, send):
            pass

        middleware = TenantMiddleware(dummy_app)

        mock_request = MagicMock()
        mock_request.url.path = "/health"
        mock_request.headers = {}

        async def call_next(req):
            called.append(True)
            return MagicMock(status_code=200, headers={})

        response = await middleware.dispatch(mock_request, call_next)
        assert response.status_code == 200
        assert called  # call_next was invoked without tenant check


# ---------------------------------------------------------------------------
# LedgerMiddleware tests
# ---------------------------------------------------------------------------

class TestLedgerMiddleware:
    @pytest.mark.asyncio
    async def test_middleware_appends_entry(self):
        from hfa_tools.middleware.ledger import LedgerMiddleware
        from hfa_tools.middleware.tenant import TenantContext

        async def dummy_app(scope, receive, send):
            pass

        middleware = LedgerMiddleware(dummy_app)

        mock_ledger = AsyncMock()
        mock_app = MagicMock()
        mock_app.state.ledger = mock_ledger

        mock_request = MagicMock()
        mock_request.app = mock_app
        mock_request.url.path = "/api/v1/plans"
        mock_request.method = "POST"
        mock_request.client = MagicMock(host="127.0.0.1")
        mock_request.state.tenant = TenantContext(
            tenant_id="acme_corp",
            run_id="run-acme_corp-abc",
            source="header",
        )

        async def call_next(req):
            resp = MagicMock()
            resp.status_code = 200
            resp.headers = {}
            return resp

        response = await middleware.dispatch(mock_request, call_next)
        assert response.status_code == 200

        # Allow the background task to run
        await asyncio.sleep(0.05)

        mock_ledger.append.assert_awaited_once()
        call_kwargs = mock_ledger.append.call_args.kwargs
        assert call_kwargs["tenant_id"] == "acme_corp"
        assert call_kwargs["run_id"] == "run-acme_corp-abc"
        assert call_kwargs["event_type"] == "http_request"
        assert call_kwargs["payload"]["status_code"] == 200

    @pytest.mark.asyncio
    async def test_middleware_skips_health_path(self):
        from hfa_tools.middleware.ledger import LedgerMiddleware

        async def dummy_app(scope, receive, send):
            pass

        middleware = LedgerMiddleware(dummy_app)

        mock_ledger = AsyncMock()
        mock_request = MagicMock()
        mock_request.url.path = "/health"
        mock_request.app = MagicMock()
        mock_request.app.state.ledger = mock_ledger

        called = []

        async def call_next(req):
            called.append(True)
            return MagicMock(status_code=200, headers={})

        response = await middleware.dispatch(mock_request, call_next)
        assert called
        await asyncio.sleep(0.05)
        mock_ledger.append.assert_not_called()

    @pytest.mark.asyncio
    async def test_ledger_failure_does_not_affect_response(self):
        """Ledger errors must never propagate to the HTTP client."""
        from hfa_tools.middleware.ledger import LedgerMiddleware
        from hfa_tools.middleware.tenant import TenantContext

        async def dummy_app(scope, receive, send):
            pass

        middleware = LedgerMiddleware(dummy_app)

        mock_ledger = AsyncMock()
        mock_ledger.append = AsyncMock(side_effect=RuntimeError("Ledger crashed"))
        mock_app = MagicMock()
        mock_app.state.ledger = mock_ledger

        mock_request = MagicMock()
        mock_request.app = mock_app
        mock_request.url.path = "/api/v1/plans"
        mock_request.method = "GET"
        mock_request.client = None
        mock_request.state.tenant = TenantContext("acme", "run-acme-x1", "header")

        async def call_next(req):
            return MagicMock(status_code=200, headers={})

        # Should NOT raise despite ledger crash
        response = await middleware.dispatch(mock_request, call_next)
        assert response.status_code == 200
        await asyncio.sleep(0.05)  # let fire-and-forget settle


# ===========================================================================
# New tests: Run-ID entropy + Atomic BudgetGuard + Rate Limiting
# ===========================================================================

class TestRunIdEntropy:
    """validate_run_id_format — UUID tail enforcement."""

    def test_valid_run_id(self):
        from hfa_tools.middleware.tenant import validate_run_id_format
        tenant, uid = validate_run_id_format(
            "run-acme_corp-550e8400-e29b-41d4-a716-446655440000"
        )
        assert tenant == "acme_corp"
        assert uid == "550e8400-e29b-41d4-a716-446655440000"

    def test_rejects_missing_uuid(self):
        from hfa_tools.middleware.tenant import validate_run_id_format, TenantFormatError
        with pytest.raises(TenantFormatError, match="UUID tail"):
            validate_run_id_format("run-acme_corp-notauuid")

    def test_rejects_wrong_prefix(self):
        from hfa_tools.middleware.tenant import validate_run_id_format, TenantFormatError
        with pytest.raises(TenantFormatError, match="expected"):
            validate_run_id_format("job-acme_corp-550e8400-e29b-41d4-a716-446655440000")

    def test_rejects_only_two_parts(self):
        from hfa_tools.middleware.tenant import validate_run_id_format, TenantFormatError
        with pytest.raises(TenantFormatError):
            validate_run_id_format("run-acme_corp")

    def test_normalises_uuid(self):
        """UUID should be normalised to lowercase hyphenated form."""
        from hfa_tools.middleware.tenant import validate_run_id_format
        _, uid = validate_run_id_format(
            "run-acme_corp-550E8400-E29B-41D4-A716-446655440000"
        )
        assert uid == "550e8400-e29b-41d4-a716-446655440000"

    def test_invalid_tenant_segment(self):
        from hfa_tools.middleware.tenant import validate_run_id_format, TenantFormatError
        with pytest.raises(TenantFormatError, match="tenant segment"):
            validate_run_id_format("run--550e8400-e29b-41d4-a716-446655440000")

    @pytest.mark.asyncio
    async def test_middleware_rejects_non_uuid_run_id(self):
        """Full middleware path: X-Run-Id with non-UUID tail → 403."""
        from unittest.mock import AsyncMock, MagicMock
        from hfa_tools.middleware.tenant import TenantMiddleware

        mock_app = AsyncMock()
        mw = TenantMiddleware(mock_app)

        request = MagicMock()
        request.url.path = "/runs/run-acme_corp-BADTAIL"
        request.headers = {
            "X-Run-Id": "run-acme_corp-BADTAIL"
        }
        request.path_params = {}

        async def call_next(req):
            return MagicMock(status_code=200, headers={})

        resp = await mw.dispatch(request, call_next)
        assert resp.status_code == 403


class TestAtomicBudgetGuard:
    """Verify single-script atomic debit — no _sha_check, result[2] for status."""

    @pytest.fixture
    def redis(self):
        from unittest.mock import AsyncMock, MagicMock
        r = AsyncMock()
        pipe = AsyncMock()
        pipe.execute = AsyncMock(return_value=[None, None, None])
        r.pipeline = MagicMock(return_value=pipe)
        r.script_load = AsyncMock(side_effect=["sha_debit", "sha_freeze", "sha_reset"])
        return r

    @pytest.mark.asyncio
    async def test_initialise_loads_three_scripts_not_four(self, redis):
        """Atomic refactor: 3 scripts loaded (debit, freeze, reset) — check removed."""
        from hfa.governance.budget_guard import BudgetGuard
        guard = BudgetGuard(redis)
        await guard.initialise()
        assert redis.script_load.call_count == 3
        assert not hasattr(guard, '_sha_check') or guard._sha_check is None  # type: ignore

    @pytest.mark.asyncio
    async def test_debit_single_evalsha_call(self, redis):
        """debit() must issue exactly ONE evalsha call (atomic)."""
        from hfa.governance.budget_guard import BudgetGuard, BudgetStatus

        guard = BudgetGuard(redis)
        await guard.initialise()

        redis.get = AsyncMock(return_value="500")
        # atomic script returns [allowed, new_spent, status]
        redis.evalsha = AsyncMock(return_value=["1", "50", "active"])

        state = await guard.debit("acme_corp", "run-acme_corp-abc", 50)

        # Must be exactly 1 evalsha call (not 2)
        assert redis.evalsha.call_count == 1
        assert state.status == BudgetStatus.ACTIVE
        assert state.spent_cents == 50

    @pytest.mark.asyncio
    async def test_debit_denied_when_allowed_zero(self, redis):
        """Lua returns allowed=0 → BudgetExhaustedError raised."""
        from hfa.governance.budget_guard import BudgetGuard, BudgetExhaustedError

        guard = BudgetGuard(redis)
        await guard.initialise()

        redis.get = AsyncMock(return_value="500")
        redis.evalsha = AsyncMock(return_value=["0", "500", "exhausted"])

        with pytest.raises(BudgetExhaustedError) as exc:
            await guard.debit("acme_corp", "run-acme_corp-abc", 100)
        assert exc.value.attempted_cents == 100

    @pytest.mark.asyncio
    async def test_evalsha_unpacked_args(self, redis):
        """IRONCLAD rule: evalsha must receive unpacked positional args, not lists."""
        from hfa.governance.budget_guard import BudgetGuard

        guard = BudgetGuard(redis)
        await guard.initialise()

        redis.get = AsyncMock(return_value="500")
        redis.evalsha = AsyncMock(return_value=["1", "10", "active"])

        await guard.debit("acme_corp", "run-acme_corp-abc", 10)

        call_args = redis.evalsha.call_args.args
        # evalsha(sha, num_keys, key1, key2, arg1, arg2)
        assert call_args[1] == 2          # num_keys as int
        assert isinstance(call_args[2], str)  # key1 unpacked
        assert isinstance(call_args[3], str)  # key2 unpacked
        assert call_args[4] == "10"       # amount_cents
        assert call_args[5] == "500"      # limit_cents


class TestTenantRateLimitMiddleware:
    """TenantRateLimitMiddleware — per-tenant fixed-window rate limiting."""

    def _make_middleware(self, rpm=10, burst=5, fail_open=True):
        from unittest.mock import AsyncMock, MagicMock
        from hfa_tools.middleware.rate_limit import TenantRateLimitMiddleware

        mock_app = AsyncMock()
        redis = AsyncMock()
        pipe = AsyncMock()
        redis.pipeline = MagicMock(return_value=pipe)
        # Default: count=1, expire=True
        pipe.execute = AsyncMock(return_value=[1, True])

        mw = TenantRateLimitMiddleware(
            mock_app, redis=redis, rpm=rpm, burst=burst, fail_open=fail_open
        )
        return mw, redis, pipe

    def _make_request(self, path="/api/plans", tenant_id="acme_corp"):
        from unittest.mock import MagicMock
        from hfa_tools.middleware.tenant import TenantContext

        request = MagicMock()
        request.url.path = path
        request.state.tenant = TenantContext(
            tenant_id=tenant_id,
            run_id=None,
            source="header",
        )
        return request

    @pytest.mark.asyncio
    async def test_allows_request_under_limit(self):
        mw, redis, pipe = self._make_middleware(rpm=10, burst=5)
        request = self._make_request()
        pipe.execute = AsyncMock(return_value=[1, True])  # count=1

        async def call_next(req):
            from unittest.mock import MagicMock
            resp = MagicMock()
            resp.headers = {}
            resp.status_code = 200
            return resp

        resp = await mw.dispatch(request, call_next)
        assert resp.status_code == 200
        assert "X-RateLimit-Limit" in resp.headers

    @pytest.mark.asyncio
    async def test_rejects_request_over_limit(self):
        mw, redis, pipe = self._make_middleware(rpm=10, burst=5)
        request = self._make_request()
        pipe.execute = AsyncMock(return_value=[16, True])  # count=16 > 15

        resp = await mw.dispatch(request, AsyncMock())
        assert resp.status_code == 429
        assert "Retry-After" in resp.headers
        assert resp.headers["X-RateLimit-Remaining"] == "0"

    @pytest.mark.asyncio
    async def test_skips_health_path(self):
        mw, redis, pipe = self._make_middleware()
        request = self._make_request(path="/health")

        called = []
        async def call_next(req):
            called.append(True)
            from unittest.mock import MagicMock
            resp = MagicMock()
            resp.headers = {}
            resp.status_code = 200
            return resp

        resp = await mw.dispatch(request, call_next)
        assert resp.status_code == 200
        assert len(called) == 1
        # Redis must NOT have been touched for skip paths
        redis.pipeline.assert_not_called()

    @pytest.mark.asyncio
    async def test_fail_open_allows_on_redis_error(self):
        mw, redis, pipe = self._make_middleware(fail_open=True)
        request = self._make_request()
        pipe.execute = AsyncMock(side_effect=ConnectionError("Redis down"))

        called = []
        async def call_next(req):
            called.append(True)
            from unittest.mock import MagicMock
            resp = MagicMock()
            resp.headers = {}
            resp.status_code = 200
            return resp

        resp = await mw.dispatch(request, call_next)
        assert len(called) == 1  # request was allowed through

    @pytest.mark.asyncio
    async def test_fail_closed_returns_503_on_redis_error(self):
        mw, redis, pipe = self._make_middleware(fail_open=False)
        request = self._make_request()
        pipe.execute = AsyncMock(side_effect=ConnectionError("Redis down"))

        resp = await mw.dispatch(request, AsyncMock())
        assert resp.status_code == 503

    @pytest.mark.asyncio
    async def test_no_tenant_context_passes_through(self):
        """If TenantMiddleware didn't run, rate limiter is skipped."""
        from unittest.mock import MagicMock
        mw, redis, pipe = self._make_middleware()

        request = MagicMock()
        request.url.path = "/api/plans"
        del request.state.tenant  # no tenant context
        request.state = MagicMock(spec=[])  # empty state

        called = []
        async def call_next(req):
            called.append(True)
            resp = MagicMock()
            resp.headers = {}
            resp.status_code = 200
            return resp

        resp = await mw.dispatch(request, call_next)
        assert len(called) == 1
        redis.pipeline.assert_not_called()

    @pytest.mark.asyncio
    async def test_redis_key_format(self):
        """Rate limit key must be rl:<tenant_id>:<minute_bucket>."""
        import time
        mw, redis, pipe = self._make_middleware()
        request = self._make_request(tenant_id="test_tenant")
        pipe.execute = AsyncMock(return_value=[1, True])

        async def call_next(req):
            from unittest.mock import MagicMock
            resp = MagicMock()
            resp.headers = {}
            return resp

        await mw.dispatch(request, call_next)

        # Check that INCR was called with the right key
        incr_call = pipe.incr.call_args
        key = incr_call.args[0]
        expected_bucket = int(time.time()) // 60
        assert key == f"rl:test_tenant:{expected_bucket}"
