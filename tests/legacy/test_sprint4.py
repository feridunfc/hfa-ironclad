"""
hfa-tools/tests/test_sprint4.py
IRONCLAD Sprint 4 — Comprehensive test suite

Coverage:
  * StateStore ABC         — cannot instantiate, InMemory CRUD + TTL + cleanup
  * InMemoryStateStore     — get/set/delete, expiry, close(), background task
  * RedisStateStore        — get/set/delete, cleanup_expired contract
  * LoopState              — circuit breaker helpers
  * SelfHealingEngine      — success, retry, max retry, circuit open, reset
  * CompliancePolicy       — init validation, AND logic, catch-all forbidden,
                             single evaluate, evaluate_all, priority order
  * DebuggerService        — no failures, fix generation, compliance gate,
                             apply_suggestion, LLM error propagation
"""
from __future__ import annotations

import time
from unittest.mock import AsyncMock

import pytest


# ===========================================================================
# StateStore & LoopState
# ===========================================================================

class TestLoopState:
    def test_circuit_open_false_by_default(self):
        from hfa.healing.store import LoopState
        s = LoopState()
        assert s.is_circuit_open() is False

    def test_open_circuit_sets_future_timestamp(self):
        from hfa.healing.store import LoopState
        s = LoopState()
        s.open_circuit(60.0)
        assert s.circuit_open_until is not None
        assert s.circuit_open_until > time.time()
        assert s.is_circuit_open() is True

    def test_close_circuit_clears_timestamp(self):
        from hfa.healing.store import LoopState
        s = LoopState()
        s.open_circuit(60.0)
        s.close_circuit()
        assert s.circuit_open_until is None
        assert s.is_circuit_open() is False

    def test_expired_circuit_reports_closed(self):
        from hfa.healing.store import LoopState
        s = LoopState()
        s.circuit_open_until = time.time() - 1.0  # already expired
        assert s.is_circuit_open() is False

    def test_total_cost_cents_is_int(self):
        from hfa.healing.store import LoopState
        s = LoopState(total_cost_cents=500)
        assert isinstance(s.total_cost_cents, int)
        assert s.total_cost_cents == 500

    def test_last_updated_auto_set(self):
        from hfa.healing.store import LoopState
        before = time.time()
        s = LoopState()
        after = time.time()
        assert before <= s.last_updated <= after


class TestStateStoreABC:
    def test_cannot_instantiate_abstract(self):
        from hfa.healing.store import StateStore
        with pytest.raises(TypeError):
            StateStore()  # type: ignore

    def test_partial_impl_cannot_instantiate(self):
        from hfa.healing.store import StateStore

        class Partial(StateStore):
            async def get(self, key): return None
            async def set(self, key, state, ttl=None): pass
            # missing delete and cleanup_expired

        with pytest.raises(TypeError):
            Partial()


class TestInMemoryStateStore:
    async def test_set_and_get(self):
        from hfa.healing.store import InMemoryStateStore, LoopState
        store = InMemoryStateStore()
        state = LoopState(attempt=2, last_error="timeout")
        await store.set("acme:run-1", state)
        got = await store.get("acme:run-1")
        assert got is not None
        assert got.attempt == 2
        assert got.last_error == "timeout"
        await store.close()

    async def test_get_missing_returns_none(self):
        from hfa.healing.store import InMemoryStateStore
        store = InMemoryStateStore()
        result = await store.get("no:such:key")
        assert result is None
        await store.close()

    async def test_delete_removes_entry(self):
        from hfa.healing.store import InMemoryStateStore, LoopState
        store = InMemoryStateStore()
        await store.set("k", LoopState())
        await store.delete("k")
        assert await store.get("k") is None
        await store.close()

    async def test_delete_nonexistent_is_noop(self):
        from hfa.healing.store import InMemoryStateStore
        store = InMemoryStateStore()
        await store.delete("ghost")  # must not raise
        await store.close()

    async def test_set_updates_last_updated(self):
        from hfa.healing.store import InMemoryStateStore, LoopState
        store = InMemoryStateStore()
        state = LoopState(last_updated=0.0)  # old timestamp
        await store.set("k", state)
        got = await store.get("k")
        assert got.last_updated > 1_000_000  # Unix-epoch-like
        await store.close()

    async def test_cleanup_expired_returns_deleted_count(self):
        from hfa.healing.store import InMemoryStateStore, LoopState
        store = InMemoryStateStore(default_ttl=1)  # 1-second TTL
        s = LoopState()
        s.last_updated = time.time() - 5  # already expired
        async with store._lock:
            store._states["expired-key"] = s
        count = await store.cleanup_expired()
        assert count == 1
        assert await store.get("expired-key") is None
        await store.close()

    async def test_cleanup_expired_live_entry_not_deleted(self):
        from hfa.healing.store import InMemoryStateStore, LoopState
        store = InMemoryStateStore(default_ttl=3600)
        await store.set("live", LoopState())
        count = await store.cleanup_expired()
        assert count == 0
        assert await store.get("live") is not None
        await store.close()

    async def test_close_cancels_background_task(self):
        from hfa.healing.store import InMemoryStateStore, LoopState
        store = InMemoryStateStore()
        await store.set("k", LoopState())  # trigger task creation
        await store.close()
        assert store._cleanup_task.cancelled() or store._cleanup_task.done()

    async def test_close_clears_all_state(self):
        from hfa.healing.store import InMemoryStateStore, LoopState
        store = InMemoryStateStore()
        await store.set("a", LoopState())
        await store.set("b", LoopState())
        await store.close()
        # After close, internal dict should be empty
        assert len(store._states) == 0

    async def test_multiple_keys_independent(self):
        from hfa.healing.store import InMemoryStateStore, LoopState
        store = InMemoryStateStore()
        await store.set("k1", LoopState(attempt=1))
        await store.set("k2", LoopState(attempt=5))
        k1 = await store.get("k1")
        k2 = await store.get("k2")
        assert k1.attempt == 1
        assert k2.attempt == 5
        await store.close()


class TestRedisStateStore:
    @pytest.fixture
    def redis(self):
        r = AsyncMock()
        r.setex  = AsyncMock(return_value=True)
        r.get    = AsyncMock(return_value=None)
        r.delete = AsyncMock(return_value=1)
        return r

    async def test_set_calls_setex(self, redis):
        from hfa.healing.store import RedisStateStore, LoopState
        store = RedisStateStore(redis, default_ttl=600)
        state = LoopState(attempt=1)
        await store.set("acme:run-x", state)
        redis.setex.assert_awaited_once()
        call_args = redis.setex.call_args
        assert call_args.args[1] == 600  # TTL
        assert "healing:acme:run-x" in call_args.args[0]

    async def test_get_returns_none_for_missing(self, redis):
        from hfa.healing.store import RedisStateStore
        redis.get = AsyncMock(return_value=None)
        store = RedisStateStore(redis)
        assert await store.get("missing") is None

    async def test_get_deserialises_json(self, redis):
        import json
        from hfa.healing.store import RedisStateStore, LoopState
        state = LoopState(attempt=3, last_error="boom")
        import dataclasses
        redis.get = AsyncMock(return_value=json.dumps(dataclasses.asdict(state)).encode())
        store = RedisStateStore(redis)
        got = await store.get("k")
        assert got.attempt == 3
        assert got.last_error == "boom"

    async def test_cleanup_expired_returns_active_count(self, redis):
        from hfa.healing.store import RedisStateStore

        async def _mock_scan_iter(pattern):
            for k in [b"healing:a", b"healing:b", b"healing:c"]:
                yield k

        redis.scan_iter = _mock_scan_iter
        store = RedisStateStore(redis)
        count = await store.cleanup_expired()
        # Contract: returns ACTIVE key count (not deleted count)
        assert count == 3

    async def test_delete_calls_redis_delete(self, redis):
        from hfa.healing.store import RedisStateStore
        store = RedisStateStore(redis)
        await store.delete("k")
        redis.delete.assert_awaited_once_with("healing:k")


# ===========================================================================
# SelfHealingEngine
# ===========================================================================

class TestSelfHealingEngine:
    @pytest.fixture
    def store(self):
        from hfa.healing.store import InMemoryStateStore
        return InMemoryStateStore()

    def _make_engine(self, store, max_attempts=3, cooldown=5.0):
        from hfa.healing.loop import SelfHealingEngine
        return SelfHealingEngine(
            store=store,
            max_attempts=max_attempts,
            cooldown_seconds=cooldown,
            base_backoff_seconds=0.01,  # fast tests
            jitter=False,
        )

    async def test_success_on_first_attempt(self, store):
        from hfa.healing.loop import AttemptOutcome, HealingResult

        engine = self._make_engine(store)

        async def ok(ctx):
            return AttemptOutcome(payload="result", tokens_used=50, cost_cents=3)

        result = await engine.run(ok, "acme", "run-acme-01")
        assert isinstance(result, HealingResult)
        assert result.attempts == 1
        assert result.payload == "result"
        assert result.tokens_used == 50
        assert result.cost_cents == 3
        assert result.recovered is False
        await engine.close()

    async def test_success_after_one_retry(self, store):
        from hfa.healing.loop import AttemptOutcome

        engine = self._make_engine(store, max_attempts=3)
        call_count = [0]

        async def flaky(ctx):
            call_count[0] += 1
            if call_count[0] == 1:
                raise RuntimeError("transient failure")
            return AttemptOutcome(payload="ok", tokens_used=10, cost_cents=1)

        result = await engine.run(flaky, "acme", "run-acme-02")
        assert result.attempts == 2
        assert result.recovered is True
        assert call_count[0] == 2
        await engine.close()

    async def test_max_retries_raises_error(self, store):
        from hfa.healing.loop import HealingMaxRetriesError

        engine = self._make_engine(store, max_attempts=2)

        async def always_fail(ctx):
            raise ValueError("always fails")

        with pytest.raises(HealingMaxRetriesError) as exc_info:
            await engine.run(always_fail, "acme", "run-acme-03")

        assert exc_info.value.run_id == "run-acme-03"
        assert exc_info.value.attempts == 2
        assert "always fails" in exc_info.value.last_error
        await engine.close()

    async def test_circuit_opens_after_max_retries(self, store):
        from hfa.healing.loop import HealingMaxRetriesError, HealingCircuitOpenError

        engine = self._make_engine(store, max_attempts=2, cooldown=60.0)

        async def always_fail(ctx):
            raise RuntimeError("boom")

        with pytest.raises(HealingMaxRetriesError):
            await engine.run(always_fail, "acme", "run-acme-04")

        # Second attempt: circuit should be open
        with pytest.raises(HealingCircuitOpenError) as exc_info:
            await engine.run(always_fail, "acme", "run-acme-04")

        assert exc_info.value.run_id == "run-acme-04"
        assert exc_info.value.open_until > time.time()
        await engine.close()

    async def test_reset_clears_circuit(self, store):
        from hfa.healing.loop import HealingMaxRetriesError, HealingCircuitOpenError, AttemptOutcome

        engine = self._make_engine(store, max_attempts=1, cooldown=60.0)

        async def always_fail(ctx): raise RuntimeError("fail")
        async def always_ok(ctx):   return AttemptOutcome(payload="ok")

        with pytest.raises(HealingMaxRetriesError):
            await engine.run(always_fail, "acme", "run-acme-05")

        # Confirm circuit is open
        with pytest.raises(HealingCircuitOpenError):
            await engine.run(always_fail, "acme", "run-acme-05")

        # Human override: reset
        await engine.reset("acme", "run-acme-05")

        # Now it should work again
        result = await engine.run(always_ok, "acme", "run-acme-05")
        assert result.payload == "ok"
        await engine.close()

    async def test_get_state_returns_none_for_clean_run(self, store):
        engine = self._make_engine(store)
        state = await engine.get_state("acme", "run-new")
        assert state is None
        await engine.close()

    async def test_context_passed_to_callable(self, store):
        from hfa.healing.loop import AttemptOutcome, AttemptContext

        engine = self._make_engine(store)
        received = []

        async def capture(ctx):
            received.append(ctx)
            return AttemptOutcome(payload="ok")

        await engine.run(capture, "acme_corp", "run-acme_corp-test")
        assert len(received) == 1
        ctx = received[0]
        assert isinstance(ctx, AttemptContext)
        assert ctx.tenant_id == "acme_corp"
        assert ctx.run_id == "run-acme_corp-test"
        assert ctx.attempt == 0
        assert ctx.prior_error is None
        await engine.close()

    async def test_fingerprint_populated_on_retry(self, store):
        from hfa.healing.loop import AttemptOutcome

        engine = self._make_engine(store, max_attempts=3)
        ctxs = []

        async def capture_then_ok(ctx):
            ctxs.append(ctx)
            if ctx.attempt == 0:
                raise ValueError("first failure")
            return AttemptOutcome(payload="ok")

        await engine.run(capture_then_ok, "acme", "run-fp-01")
        assert ctxs[1].prior_error == "first failure"
        assert ctxs[1].fingerprint is not None
        assert len(ctxs[1].fingerprint) == 64  # SHA-256 hex
        await engine.close()

    async def test_cost_cents_accumulated(self, store):
        from hfa.healing.loop import AttemptOutcome

        engine = self._make_engine(store, max_attempts=3)
        call_count = [0]

        async def two_attempts(ctx):
            call_count[0] += 1
            if call_count[0] == 1:
                raise RuntimeError("first fail")
            return AttemptOutcome(payload="ok", tokens_used=20, cost_cents=7)

        result = await engine.run(two_attempts, "acme", "run-cost-01")
        assert isinstance(result.cost_cents, int)
        assert result.cost_cents == 7   # only success outcome adds cost
        await engine.close()

    async def test_state_deleted_on_success(self, store):
        from hfa.healing.loop import AttemptOutcome

        engine = self._make_engine(store)

        async def ok(ctx): return AttemptOutcome(payload="ok")
        await engine.run(ok, "acme", "run-clean-01")

        # State must be cleaned up after success
        state = await engine.get_state("acme", "run-clean-01")
        assert state is None
        await engine.close()


# ===========================================================================
# CompliancePolicy
# ===========================================================================

class TestCompliancePolicyInit:
    def test_empty_rules_allowed(self):
        from hfa.governance.compliance_policy import CompliancePolicy
        p = CompliancePolicy([])  # valid — no rules means ALLOW-all
        assert p.rules == []

    def test_valid_rule_with_pattern(self):
        from hfa.governance.compliance_policy import CompliancePolicy
        p = CompliancePolicy([{"action": "deny", "pattern": "pii"}])
        assert len(p.rules) == 1

    def test_valid_rule_with_severity(self):
        from hfa.governance.compliance_policy import CompliancePolicy
        CompliancePolicy([{"action": "hitl", "severity": "high"}])

    def test_valid_rule_with_field_value(self):
        from hfa.governance.compliance_policy import CompliancePolicy
        CompliancePolicy([{"action": "allow", "field": "src", "value": "internal"}])

    def test_missing_action_raises(self):
        from hfa.governance.compliance_policy import CompliancePolicy
        with pytest.raises(ValueError, match="action"):
            CompliancePolicy([{"pattern": "pii"}])

    def test_invalid_action_raises(self):
        from hfa.governance.compliance_policy import CompliancePolicy
        with pytest.raises(ValueError, match="invalid action"):
            CompliancePolicy([{"action": "block", "pattern": "x"}])

    def test_catch_all_rule_forbidden(self):
        """Rule with no conditions must raise ValueError at init."""
        from hfa.governance.compliance_policy import CompliancePolicy
        with pytest.raises(ValueError, match="[Cc]atch-all"):
            CompliancePolicy([{"action": "deny"}])

    def test_field_without_value_raises(self):
        from hfa.governance.compliance_policy import CompliancePolicy
        with pytest.raises(ValueError, match="'value'"):
            CompliancePolicy([{"action": "deny", "field": "src"}])

    def test_value_without_field_raises(self):
        from hfa.governance.compliance_policy import CompliancePolicy
        with pytest.raises(ValueError, match="'field'"):
            CompliancePolicy([{"action": "deny", "value": "x"}])

    def test_non_list_rules_raises_type_error(self):
        from hfa.governance.compliance_policy import CompliancePolicy
        with pytest.raises(TypeError, match="list"):
            CompliancePolicy({"action": "deny", "pattern": "x"})  # type: ignore

    def test_non_dict_rule_raises(self):
        from hfa.governance.compliance_policy import CompliancePolicy
        with pytest.raises(ValueError, match="dict"):
            CompliancePolicy(["deny everything"])  # type: ignore


class TestCompliancePolicyEvaluate:
    def _make_policy(self):
        from hfa.governance.compliance_policy import CompliancePolicy
        return CompliancePolicy([
            {"action": "deny",  "pattern": "ssn",     "severity": "critical"},
            {"action": "deny",  "pattern": "password"},
            {"action": "hitl",  "severity": "high"},
            {"action": "allow", "field": "source",    "value": "internal"},
        ])

    def test_pattern_match_returns_deny(self):
        from hfa.governance.compliance_policy import PolicyAction
        p = self._make_policy()
        action = p.evaluate({"message": "SSN exposed", "severity": "critical"})
        assert action == PolicyAction.DENY

    def test_no_match_returns_allow(self):
        from hfa.governance.compliance_policy import PolicyAction
        p = self._make_policy()
        action = p.evaluate({"message": "all clear", "severity": "low"})
        assert action == PolicyAction.ALLOW

    def test_severity_match_returns_hitl(self):
        from hfa.governance.compliance_policy import PolicyAction
        p = self._make_policy()
        action = p.evaluate({"message": "something", "severity": "high"})
        assert action == PolicyAction.HITL

    def test_field_value_match(self):
        from hfa.governance.compliance_policy import PolicyAction
        p = self._make_policy()
        action = p.evaluate({"message": "whatever", "source": "internal"})
        assert action == PolicyAction.ALLOW

    def test_and_logic_requires_all_conditions(self):
        """Rule: deny when pattern=ssn AND severity=critical.
        Finding has pattern but NOT severity=critical → should NOT match deny rule."""
        from hfa.governance.compliance_policy import CompliancePolicy, PolicyAction
        p = CompliancePolicy([
            {"action": "deny", "pattern": "ssn", "severity": "critical"},
        ])
        # Has "ssn" but severity is "low" — AND fails → ALLOW
        action = p.evaluate({"message": "ssn found", "severity": "low"})
        assert action == PolicyAction.ALLOW

    def test_case_insensitive_pattern(self):
        from hfa.governance.compliance_policy import CompliancePolicy, PolicyAction
        p = CompliancePolicy([{"action": "deny", "pattern": "SSN"}])
        action = p.evaluate({"message": "contains ssn data", "severity": "low"})
        assert action == PolicyAction.DENY

    def test_priority_deny_over_hitl(self):
        """Multiple matching rules — most restrictive wins."""
        from hfa.governance.compliance_policy import CompliancePolicy, PolicyAction
        p = CompliancePolicy([
            {"action": "hitl",  "severity": "high"},
            {"action": "deny",  "pattern": "ssn"},
        ])
        action = p.evaluate({"message": "SSN found", "severity": "high"})
        assert action == PolicyAction.DENY  # DENY wins over HITL


class TestCompliancePolicyEvaluateAll:
    def _make_policy(self):
        from hfa.governance.compliance_policy import CompliancePolicy
        return CompliancePolicy([
            {"action": "deny", "pattern": "ssn"},
            {"action": "hitl", "severity": "medium"},
        ])

    def test_empty_findings_returns_allow(self):
        from hfa.governance.compliance_policy import PolicyAction
        p = self._make_policy()
        result = p.evaluate_all([])
        assert result.decision == PolicyAction.ALLOW

    def test_deny_finding_sets_decision_deny(self):
        from hfa.governance.compliance_policy import PolicyAction
        p = self._make_policy()
        result = p.evaluate_all([{"message": "SSN exposed"}])
        assert result.decision == PolicyAction.DENY
        assert len(result.denials) == 1
        assert len(result.hitls) == 0

    def test_hitl_finding_sets_decision_hitl(self):
        from hfa.governance.compliance_policy import PolicyAction
        p = self._make_policy()
        result = p.evaluate_all([{"message": "warning", "severity": "medium"}])
        assert result.decision == PolicyAction.HITL
        assert len(result.hitls) == 1

    def test_mixed_deny_overrides_hitl(self):
        from hfa.governance.compliance_policy import PolicyAction
        p = self._make_policy()
        result = p.evaluate_all([
            {"message": "ssn leak"},           # → deny
            {"message": "warn", "severity": "medium"},  # → hitl
        ])
        assert result.decision == PolicyAction.DENY
        assert len(result.denials) == 1
        assert len(result.hitls) == 1

    def test_details_length_matches_findings(self):
        p = self._make_policy()
        findings = [
            {"message": "ssn"},
            {"message": "ok"},
            {"message": "check", "severity": "medium"},
        ]
        result = p.evaluate_all(findings)
        assert len(result.details) == 3

    def test_summary_string_present(self):
        p = self._make_policy()
        result = p.evaluate_all([{"message": "ssn"}])
        assert isinstance(result.summary, str)
        assert len(result.summary) > 0

    def _make_policy(self):
        from hfa.governance.compliance_policy import CompliancePolicy
        return CompliancePolicy([
            {"action": "deny", "pattern": "ssn"},
            {"action": "hitl", "severity": "medium"},
        ])


# ===========================================================================
# DebuggerService
# ===========================================================================

class TestDebuggerService:
    @pytest.fixture
    def code_set(self):
        from hfa.schemas.agent import CodeChangeSet, CodeChange
        return CodeChangeSet(
            change_set_id="cs-test-01",
            plan_id="plan-test-01",
            language="python",
            changes=[
                CodeChange(
                    file_path="main.py",
                    new_content="def add(a, b): return a - b  # bug: wrong op",
                    change_type="create",
                )
            ],
            total_tokens=100,
        )

    @pytest.fixture
    def passing_test_results(self):
        from hfa.schemas.agent import TestSuiteResult, TestResult
        return TestSuiteResult(
            suite_id="suite-01",
            code_set_id="cs-test-01",
            results=[
                TestResult(
                    test_id="t-01",
                    name="test_add",
                    status="passed",
                    duration_ms=10,
                )
            ],
            total_tokens=50,
        )

    @pytest.fixture
    def failing_test_results(self):
        from hfa.schemas.agent import TestSuiteResult, TestResult
        return TestSuiteResult(
            suite_id="suite-02",
            code_set_id="cs-test-01",
            results=[
                TestResult(
                    test_id="t-01",
                    name="test_add",
                    status="failed",
                    duration_ms=5,
                    error_message="AssertionError: 3 != 5",
                )
            ],
            total_tokens=50,
        )

    @pytest.fixture
    def mock_llm(self):
        from hfa_tools.services.debugger_service import DebugFixSuggestion
        client = AsyncMock()
        client.generate_structured = AsyncMock(return_value=DebugFixSuggestion(
            explanation="Fixed subtraction to addition",
            confidence=0.95,
            fixed_files=[{
                "file_path": "main.py",
                "new_content": "def add(a, b): return a + b",
                "change_type": "modify",
            }],
        ))
        client.close = AsyncMock()
        return client

    async def test_no_failures_returns_unchanged(self, code_set, passing_test_results, mock_llm):
        from hfa_tools.services.debugger_service import DebuggerService
        svc = DebuggerService(mock_llm)
        result = await svc.fix(code_set, passing_test_results, "run-acme-1", "acme")
        # LLM must NOT be called when there are no failures
        mock_llm.generate_structured.assert_not_called()
        assert result.fixed_code_set == code_set
        assert result.confidence == 1.0

    async def test_fix_calls_llm_for_failures(self, code_set, failing_test_results, mock_llm):
        from hfa_tools.services.debugger_service import DebuggerService
        from hfa.schemas.agent import DebuggerOutput
        svc = DebuggerService(mock_llm)
        result = await svc.fix(code_set, failing_test_results, "run-acme-2", "acme")
        mock_llm.generate_structured.assert_awaited_once()
        assert isinstance(result, DebuggerOutput)
        assert result.confidence == pytest.approx(0.95)
        assert "Fixed" in result.explanation

    async def test_fix_applies_changed_file(self, code_set, failing_test_results, mock_llm):
        from hfa_tools.services.debugger_service import DebuggerService
        svc = DebuggerService(mock_llm)
        result = await svc.fix(code_set, failing_test_results, "run-acme-3", "acme")
        fixed_contents = {c.file_path: c.new_content for c in result.fixed_code_set.changes}
        assert fixed_contents["main.py"] == "def add(a, b): return a + b"

    async def test_fix_change_set_id_prefixed_with_fix(self, code_set, failing_test_results, mock_llm):
        from hfa_tools.services.debugger_service import DebuggerService
        svc = DebuggerService(mock_llm)
        result = await svc.fix(code_set, failing_test_results, "run-acme-4", "acme")
        assert result.fixed_code_set.change_set_id.startswith("fix-")

    async def test_llm_error_raises_debugger_llm_error(self, code_set, failing_test_results):
        from hfa_tools.services.debugger_service import DebuggerService, DebuggerLLMError
        bad_llm = AsyncMock()
        bad_llm.generate_structured = AsyncMock(side_effect=RuntimeError("LLM down"))
        bad_llm.close = AsyncMock()
        svc = DebuggerService(bad_llm)
        with pytest.raises(DebuggerLLMError):
            await svc.fix(code_set, failing_test_results, "run-err", "acme")

    async def test_compliance_deny_raises_error(self, code_set, failing_test_results, mock_llm):
        from hfa.governance.compliance_policy import CompliancePolicy
        from hfa_tools.services.debugger_service import DebuggerService, DebuggerComplianceError

        # Policy that denies ANY finding (pattern always matches)
        policy = CompliancePolicy([{"action": "deny", "pattern": "fixed"}])

        svc = DebuggerService(mock_llm, compliance_policy=policy)
        with pytest.raises(DebuggerComplianceError) as exc_info:
            await svc.fix(code_set, failing_test_results, "run-comp", "acme")
        assert exc_info.value.decision == "deny"

    async def test_compliance_allow_passes_through(self, code_set, failing_test_results, mock_llm):
        from hfa.governance.compliance_policy import CompliancePolicy
        from hfa_tools.services.debugger_service import DebuggerService

        # Policy that allows everything with source=llm_fix
        policy = CompliancePolicy([{"action": "allow", "field": "source", "value": "llm_fix"}])

        svc = DebuggerService(mock_llm, compliance_policy=policy)
        result = await svc.fix(code_set, failing_test_results, "run-ok", "acme")
        assert result.confidence == pytest.approx(0.95)

    async def test_prompt_contains_failing_test_info(self, code_set, failing_test_results, mock_llm):
        from hfa_tools.services.debugger_service import DebuggerService
        svc = DebuggerService(mock_llm)
        await svc.fix(code_set, failing_test_results, "run-p", "acme")
        call_kwargs = mock_llm.generate_structured.call_args.kwargs
        prompt = call_kwargs["prompt"]
        assert "test_add" in prompt
        assert "AssertionError" in prompt

    async def test_prompt_contains_source_code(self, code_set, failing_test_results, mock_llm):
        from hfa_tools.services.debugger_service import DebuggerService
        svc = DebuggerService(mock_llm)
        await svc.fix(code_set, failing_test_results, "run-src", "acme")
        prompt = mock_llm.generate_structured.call_args.kwargs["prompt"]
        assert "main.py" in prompt

    async def test_low_temperature_used(self, code_set, failing_test_results, mock_llm):
        from hfa_tools.services.debugger_service import DebuggerService
        svc = DebuggerService(mock_llm)
        await svc.fix(code_set, failing_test_results, "run-temp", "acme")
        temp = mock_llm.generate_structured.call_args.kwargs["temperature"]
        assert temp <= 0.2  # deterministic fix

    async def test_close_propagates_to_llm(self, mock_llm):
        from hfa_tools.services.debugger_service import DebuggerService
        svc = DebuggerService(mock_llm)
        await svc.close()
        mock_llm.close.assert_awaited_once()

    async def test_apply_suggestion_empty_fixed_files_keeps_original(
        self, code_set
    ):
        from hfa_tools.services.debugger_service import DebuggerService, DebugFixSuggestion
        suggestion = DebugFixSuggestion(
            explanation="no changes needed",
            confidence=0.5,
            fixed_files=[],
        )
        result = DebuggerService._apply_suggestion(code_set, suggestion)
        assert len(result) == len(code_set.changes)
        assert result[0].file_path == "main.py"
