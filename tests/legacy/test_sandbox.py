"""
hfa-tools/tests/test_sandbox.py
IRONCLAD Sprint 3 — Sandbox pool + runner unit tests.
All Docker calls are mocked; no daemon required.
"""
from __future__ import annotations

import asyncio
import base64
import json
import tarfile
import io
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# CodeRunner tests
# ---------------------------------------------------------------------------

class TestCodeRunner:
    def _make_runner(self, language="python"):
        """Return a CodeRunner with a fully mocked docker client."""
        from hfa_tools.sandbox.runner import CodeRunner

        docker_client = MagicMock()
        container = MagicMock()
        container.labels = {"hfa.language": language}
        container.id     = "abc123" * 5  # 30-char fake ID
        docker_client.containers.get.return_value = container

        # exec_create returns an exec id dict
        container.client.api.exec_create.return_value = {"Id": "exec-111"}
        # exec_start returns bytes (stream=False)
        container.client.api.exec_start.return_value = b"Hello, World!\n"
        # exec_inspect returns ExitCode=0
        container.client.api.exec_inspect.return_value = {"ExitCode": 0}

        runner = CodeRunner(docker_client, container.id)
        return runner, docker_client, container

    # ── fix #1: import json present ────────────────────────────────────
    def test_import_json_present(self):
        """Regression: runner.py must import json at module level."""
        import hfa_tools.sandbox.runner as mod
        assert hasattr(mod, "json"), "import json missing from runner.py"

    # ── fix #2: template includes import base64 ─────────────────────────
    def test_python_template_has_import_base64(self):
        from hfa_tools.sandbox.runner import _PYTHON_TEMPLATE
        assert "import base64" in _PYTHON_TEMPLATE, (
            "Python template must have 'import base64'"
        )

    # ── fix #3: input_b64 appended to cmd list ──────────────────────────
    def test_input_b64_passed_as_argv(self):
        from hfa_tools.sandbox.runner import CodeRunner

        runner = CodeRunner(MagicMock(), "fake-id")
        _, cmd = runner._build_cmd("python", "x=1", input_data={"key": "val"})

        # cmd must be: ["python3", "-c", script, input_b64]
        assert len(cmd) == 4, f"Expected 4 elements in cmd, got {len(cmd)}: {cmd}"
        assert cmd[0] == "python3"
        assert cmd[1] == "-c"
        # 4th element must be non-empty base64
        decoded = json.loads(base64.b64decode(cmd[3]).decode())
        assert decoded == {"key": "val"}

    def test_input_b64_empty_when_no_input(self):
        from hfa_tools.sandbox.runner import CodeRunner

        runner = CodeRunner(MagicMock(), "fake-id")
        _, cmd = runner._build_cmd("python", "x=1", input_data=None)
        # 4th element should be empty string
        assert cmd[3] == ""

    # ── fix #4: timeout wraps full executor call (stream=False) ─────────
    @pytest.mark.asyncio
    async def test_exec_start_called_with_stream_false(self):
        runner, docker_client, container = self._make_runner()

        loop = asyncio.get_running_loop()
        with patch("asyncio.wait_for", wraps=asyncio.wait_for) as mock_wf:
            await runner.run("x = 1 + 1", timeout=5)

        # exec_start must have been called with stream=False
        exec_start_call = container.client.api.exec_start.call_args
        assert exec_start_call.kwargs.get("stream") is False or \
               exec_start_call.args[1:] == (False,) or \
               "stream=False" in str(exec_start_call), \
               "exec_start must be called with stream=False"

    # ── fix #5: timeout triggers container.kill() ───────────────────────
    @pytest.mark.asyncio
    async def test_timeout_kills_container(self):
        from hfa_tools.sandbox.runner import CodeRunner

        docker_client = MagicMock()
        container     = MagicMock()
        container.labels = {"hfa.language": "python"}
        container.id     = "abc" * 10
        docker_client.containers.get.return_value = container
        container.client.api.exec_create.return_value = {"Id": "exec-timeout"}

        # Make exec_start hang → TimeoutError
        async def hang(*a, **kw):
            await asyncio.sleep(999)

        runner = CodeRunner(docker_client, container.id)

        with patch("asyncio.wait_for", side_effect=asyncio.TimeoutError):
            result = await runner.run("import time; time.sleep(999)", timeout=1)

        assert result["success"] is False
        assert "timeout" in result["error"].lower()
        assert result["exit_code"] == -1
        container.kill.assert_called_once()   # ✅ fix #5

    # ── run_command ──────────────────────────────────────────────────────
    @pytest.mark.asyncio
    async def test_run_command_uses_cmd_directly(self):
        runner, docker_client, container = self._make_runner()
        container.client.api.exec_start.return_value = b"collected\n"
        container.client.api.exec_inspect.return_value = {"ExitCode": 0}

        cmd = ["python3", "-m", "pytest", "/workspace", "-v"]
        result = await runner.run_command(cmd, timeout=10)

        assert result["success"] is True
        # Verify the exact cmd was passed to exec_create
        exec_create_call = container.client.api.exec_create.call_args
        assert exec_create_call.kwargs.get("cmd") == cmd or \
               exec_create_call.args[1] == cmd, \
               "run_command must pass cmd list directly to exec_create"

    # ── write_files ──────────────────────────────────────────────────────
    @pytest.mark.asyncio
    async def test_write_files_uses_put_archive(self):
        runner, docker_client, container = self._make_runner()
        container.put_archive = MagicMock(return_value=True)

        files = {
            "src/main.py": "def hello(): return 42",
            "tests/test_main.py": "def test_hello(): assert True",
        }
        await runner.write_files(files)

        container.put_archive.assert_called_once()
        # First arg is path, second is tar data
        path_arg = container.put_archive.call_args.args[0]
        assert path_arg == "/workspace"

    @pytest.mark.asyncio
    async def test_write_files_tar_contains_all_files(self):
        runner, docker_client, container = self._make_runner()

        captured_tar = []

        def capture_put_archive(path, data):
            captured_tar.append(data.read())
            return True

        container.put_archive.side_effect = capture_put_archive

        files = {"a.py": "x=1", "b/c.py": "y=2"}
        await runner.write_files(files)

        # Verify tar contents
        tar_bytes = captured_tar[0]
        with tarfile.open(fileobj=io.BytesIO(tar_bytes)) as tar:
            names = tar.getnames()

        assert "a.py"   in names
        assert "b/c.py" in names


# ---------------------------------------------------------------------------
# SandboxPool tests (mocked docker)
# ---------------------------------------------------------------------------

class TestSandboxPool:

    def _make_pool(self):
        from hfa_tools.sandbox.pool import SandboxPool

        with patch("docker.from_env") as mock_docker:
            container = MagicMock()
            container.id     = "pool-cid-" + "x" * 20
            container.status = "running"
            container.labels = {"hfa.language": "python", "hfa.created": "0"}
            container.reload = MagicMock()   # ✅ reload() present

            # ✅ Guardian fix: Proper Mocking of Docker API to prevent _wipe_workspace fallback from failing
            container.client.api.exec_create.return_value = {"Id": "exec-111"}
            container.client.api.exec_start.return_value = b""
            container.client.api.exec_inspect.return_value = {"ExitCode": 0}

            mock_docker.return_value.containers.run.return_value   = container
            mock_docker.return_value.containers.get.return_value   = container

            pool = SandboxPool(pool_size=2, languages=["python"])
            pool._docker = mock_docker.return_value
            return pool, container

    @pytest.mark.asyncio
    async def test_is_container_healthy_calls_reload(self):
        """Guardian fix A: _is_container_healthy must call container.reload()."""
        pool, container = self._make_pool()
        container.status = "running"

        result = await pool._is_container_healthy(container.id)

        assert result is True
        container.reload.assert_called()   # ✅

    @pytest.mark.asyncio
    async def test_checkout_removes_stale_container(self):
        """Guardian fix B: stale container in pool → removed and replaced."""
        pool, container = self._make_pool()

        # First call returns "stale" (not running), second call returns fresh one
        call_count = {"n": 0}
        original_healthy = pool._is_container_healthy

        async def patched_healthy(cid):
            call_count["n"] += 1
            if call_count["n"] == 1:
                return False   # first candidate is stale
            return True        # replacement is healthy

        pool._is_container_healthy = patched_healthy

        # Pre-fill pool with one stale id + one good id
        await pool._pool["python"].put("stale-id-000")
        await pool._pool["python"].put(container.id)

        ids_seen = []
        async with pool.get_container("python") as cid:
            ids_seen.append(cid)

        # The stale one should have been skipped
        assert ids_seen[0] == container.id

    @pytest.mark.asyncio
    async def test_create_container_uses_read_only_and_tmpfs(self):
        """IRONCLAD: both read_only=True AND tmpfs must be present."""
        pool, _ = self._make_pool()

        await pool._create_container("python")

        run_call = pool._docker.containers.run.call_args
        kwargs   = run_call.kwargs if run_call.kwargs else {}

        assert kwargs.get("read_only") is True,  "read_only=True required"
        assert kwargs.get("tmpfs"),               "tmpfs required with read_only"
        assert kwargs.get("network_disabled") is True

    @pytest.mark.asyncio
    async def test_start_creates_background_tasks(self):
        pool, _ = self._make_pool()
        await pool.start()

        assert pool._warmup_task  is not None
        assert pool._cleanup_task is not None

        # Cleanup
        pool._running = False
        pool._warmup_task.cancel()
        pool._cleanup_task.cancel()


# ---------------------------------------------------------------------------
# ResearcherService tests
# ---------------------------------------------------------------------------

class TestResearcherService:

    def _make_service(self):
        from hfa_tools.services.researcher_service import ResearcherService
        from hfa.schemas.research import ResearchSummary

        llm = AsyncMock()
        llm.generate_structured = AsyncMock(return_value=ResearchSummary(
            summary="Test summary about the query.",
            citations=["https://example.com/result-0"],
        ))
        return ResearcherService(llm_client=llm, search_enabled=True), llm

    @pytest.mark.asyncio
    async def test_research_returns_result(self):
        from hfa.schemas.agent import ResearchRequest

        svc, _ = self._make_service()
        req = ResearchRequest(
            agent_type="researcher",
            tenant_id="acme-corp",
            run_id="run-acme-corp-001",
            query="What is FastAPI?",
            max_results=3,
        )
        result = await svc.research(req)
        assert result.summary.startswith("Test summary")
        assert result.total_tokens > 0

    @pytest.mark.asyncio
    async def test_generate_structured_called_with_model(self):
        """Fix #7: response_model must be ResearchSummary, never None."""
        from hfa.schemas.agent import ResearchRequest
        from hfa.schemas.research import ResearchSummary

        svc, llm = self._make_service()
        req = ResearchRequest(
            agent_type="researcher",
            tenant_id="acme-corp",
            run_id="run-acme-corp-002",
            query="Test query for model check",
            max_results=2,
        )
        await svc.research(req)

        call_kwargs = llm.generate_structured.call_args.kwargs
        assert call_kwargs["response_model"] is ResearchSummary, (
            "response_model must be ResearchSummary, not None"
        )

    @pytest.mark.asyncio
    async def test_cache_stores_research_result(self):
        """Fix #8: cache must store ResearchResult, not list of traces."""
        from hfa.schemas.agent import ResearchRequest
        from hfa.schemas.research import ResearchResult

        svc, _ = self._make_service()
        req = ResearchRequest(
            agent_type="researcher",
            tenant_id="acme-corp",
            run_id="run-acme-corp-003",
            query="Cache type test query",
            max_results=2,
        )
        await svc.research(req)

        # Check cache: values must be (float, ResearchResult)
        assert len(svc._cache) == 1
        ts, cached = next(iter(svc._cache.values()))
        assert isinstance(ts, float)
        assert isinstance(cached, ResearchResult), (
            f"Cache must store ResearchResult, got {type(cached)}"
        )

    @pytest.mark.asyncio
    async def test_cache_hit_skips_llm(self):
        from hfa.schemas.agent import ResearchRequest

        svc, llm = self._make_service()
        req = ResearchRequest(
            agent_type="researcher",
            tenant_id="acme-corp",
            run_id="run-acme-corp-004",
            query="Repeated query",
            max_results=2,
        )
        await svc.research(req)
        await svc.research(req)   # second call → cache hit

        # LLM should have been called only once
        assert llm.generate_structured.call_count == 1


# ---------------------------------------------------------------------------
# TesterService tests
# ---------------------------------------------------------------------------

class TestTesterService:

    @pytest.mark.asyncio
    async def test_write_files_called_not_sleep(self):
        """Fix #6a: _write_files must use runner.write_files(), not sleep()."""
        from hfa_tools.services.tester_service import TesterService
        from hfa.schemas.agent import CodeChangeSet, CodeChange

        pool = MagicMock()
        pool._docker = MagicMock()

        # Mock get_container context manager
        from contextlib import asynccontextmanager
        @asynccontextmanager
        async def fake_get_container(lang):
            yield "container-id-test"

        pool.get_container = fake_get_container

        runner_mock = AsyncMock()
        runner_mock.write_files = AsyncMock()
        runner_mock.run_command = AsyncMock(return_value={
            "success": True,
            "output": "test_basic PASSED",
            "error": "",
            "exit_code": 0,
            "duration_ms": 100,
        })

        svc = TesterService(pool, test_timeout=5)

        code_set = CodeChangeSet(
            change_set_id="cs-test-001",
            plan_id="plan-test-001",
            language="python",
            total_tokens=10,
            changes=[
                CodeChange(
                    file_path="src/main.py",
                    new_content="def hello(): return 42",
                    change_type="create",
                )
            ],
        )

        with patch("hfa_tools.services.tester_service.CodeRunner", return_value=runner_mock):
            result = await svc.run_tests(code_set)

        # write_files must have been called (real copy)
        runner_mock.write_files.assert_awaited_once()

        # run_command must have been called (not execute_code injection)
        runner_mock.run_command.assert_awaited_once()

        assert result.summary["passed"] == 1