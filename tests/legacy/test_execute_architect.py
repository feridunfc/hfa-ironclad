"""
hfa-tools/tests/test_execute_architect.py
IRONCLAD — Integration tests for architect service
"""
import asyncio
import pytest
import uuid
from unittest.mock import AsyncMock

from hfa.schemas.agent import ArchitectRequest, PlanManifest
from hfa_tools.services.architect_service import ArchitectService


@pytest.fixture
def mock_llm_client():
    client = AsyncMock()

    mock_manifest = PlanManifest(
        plan_id="test-plan-123",
        title="Test Plan",
        description="A test plan",
        requirements=["Req1", "Req2"],
        steps=[{"step": 1, "action": "test"}],
        estimated_tokens=100,
        created_at="2024-01-01T00:00:00Z",
    )

    client.generate_structured.return_value = mock_manifest
    return client


@pytest.fixture
def architect_service(mock_llm_client):
    return ArchitectService(llm_client=mock_llm_client)


@pytest.mark.asyncio
async def test_create_plan_success(architect_service, mock_llm_client):
    request = ArchitectRequest(
        agent_type="architect",
        tenant_id="test-tenant",
        run_id=f"run-{uuid.uuid4().hex[:8]}",
        requirement="Create a REST API with FastAPI",
        context={"tech_stack": "python"},
    )

    result = await architect_service.create_plan(request)

    assert isinstance(result, PlanManifest)
    assert result.plan_id.startswith("plan-test-tenant")
    assert result.title == "Test Plan"
    assert len(result.requirements) == 2
    assert result.estimated_tokens == 100

    mock_llm_client.generate_structured.assert_called_once()
    call_kwargs = mock_llm_client.generate_structured.call_args.kwargs
    assert call_kwargs["response_model"] == PlanManifest
    assert call_kwargs["temperature"] == 0.2
    assert request.requirement in call_kwargs["prompt"]


@pytest.mark.asyncio
async def test_create_plan_with_existing_id(architect_service):
    custom_plan_id = f"custom-{uuid.uuid4().hex[:8]}"
    request = ArchitectRequest(
        agent_type="architect",
        tenant_id="test-tenant",
        run_id=f"run-{uuid.uuid4().hex[:8]}",
        plan_id=custom_plan_id,
        requirement="Build a web app",
    )

    result = await architect_service.create_plan(request)
    assert result.plan_id == custom_plan_id


@pytest.mark.asyncio
async def test_create_plan_llm_error(architect_service, mock_llm_client):
    from hfa.llm.robust_client import LLMCallError
    from fastapi import HTTPException

    mock_llm_client.generate_structured.side_effect = LLMCallError("API down")

    request = ArchitectRequest(
        agent_type="architect",
        tenant_id="test-tenant",
        run_id=f"run-{uuid.uuid4().hex[:8]}",
        requirement="Test requirement",
    )

    with pytest.raises(HTTPException) as exc_info:
        await architect_service.create_plan(request)

    assert exc_info.value.status_code == 503


@pytest.mark.asyncio
async def test_modify_plan(architect_service, mock_llm_client):
    existing = PlanManifest(
        plan_id="plan-123",
        title="Original",
        description="Original desc",
        requirements=["Old"],
        steps=[{"step": 1}],
        estimated_tokens=50,
        created_at="2024-01-01T00:00:00Z",
    )

    mock_llm_client.generate_structured.return_value = PlanManifest(
        plan_id="plan-123",
        title="Updated Plan",
        description="Updated desc",
        requirements=["Old", "New"],
        steps=[{"step": 1}, {"step": 2}],
        estimated_tokens=100,
        created_at="2024-01-02T00:00:00Z",
    )

    result = await architect_service.modify_plan(existing_plan=existing, new_requirement="Add authentication")

    assert result.plan_id == "plan-123"
    assert len(result.requirements) == 2
    assert result.title == "Updated Plan"

    call_args = mock_llm_client.generate_structured.call_args
    assert "Existing Plan" in call_args.kwargs["prompt"]
    assert "Add authentication" in call_args.kwargs["prompt"]


@pytest.mark.asyncio
async def test_validate_plan(architect_service):
    valid = PlanManifest(
        plan_id="valid-123",
        title="Valid Plan",
        description="This is a valid test plan",
        requirements=["Req1", "Req2"],
        steps=[{"step": 1, "action": "test"}],
        estimated_tokens=100,
        created_at="2024-01-01T00:00:00Z",
    )
    assert await architect_service.validate_plan(valid) is True

    with pytest.raises(ValueError, match="plan_id"):
        await architect_service.validate_plan(
            PlanManifest(
                plan_id="a",
                title="Valid",
                description="Valid description",
                requirements=["Req"],
                steps=[{"step": 1}],
                estimated_tokens=100,
                created_at="2024-01-01T00:00:00Z",
            )
        )

    with pytest.raises(ValueError, match="requirement"):
        await architect_service.validate_plan(
            PlanManifest(
                plan_id="valid-123",
                title="Valid",
                description="Valid description",
                requirements=[],
                steps=[{"step": 1}],
                estimated_tokens=100,
                created_at="2024-01-01T00:00:00Z",
            )
        )


@pytest.mark.asyncio
async def test_concurrent_requests(architect_service, mock_llm_client):
    async def make_request(i: int):
        request = ArchitectRequest(
            agent_type="architect",
            tenant_id="test-tenant",
            run_id=f"run-{i}",
            requirement=f"Requirement {i}",
        )
        return await architect_service.create_plan(request)

    tasks = [make_request(i) for i in range(5)]
    results = await asyncio.gather(*tasks)

    assert len(results) == 5
    assert all(isinstance(r, PlanManifest) for r in results)
    assert mock_llm_client.generate_structured.call_count == 5


@pytest.mark.asyncio
async def test_close(architect_service, mock_llm_client):
    await architect_service.close()
    mock_llm_client.close.assert_awaited_once()
