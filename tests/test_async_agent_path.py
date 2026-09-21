from __future__ import annotations

import json
from unittest.mock import AsyncMock

import pytest

from agents.contracts import (
    PreviousResultReference,
    WorkerOutcome,
    WorkerRequestParts,
)


class _AsyncRouterModel:
    def __init__(self, *results):
        self.results = list(results)
        self.calls = []
        self.schema = None
        self.method = None

    def with_structured_output(self, schema, method=None):
        self.schema = schema
        self.method = method
        return self

    async def ainvoke(self, messages, config=None):
        self.calls.append((messages, config))
        result = self.results.pop(0)
        if isinstance(result, BaseException):
            raise result
        return result


@pytest.mark.asyncio
async def test_supervisor_dispatches_to_async_coordinator(monkeypatch):
    from agents import supervisor
    from agents.coordinator import CoordinatorAnswer

    coordinator = AsyncMock(
        return_value=CoordinatorAnswer(answer="done", display_refs=[])
    )
    monkeypatch.setattr(supervisor, "coordinator_chat_async", coordinator)

    result = await supervisor._call_coordinator_chat("task", context="ctx")

    assert result.answer == "done"
    coordinator.assert_awaited_once_with("task", context="ctx")


@pytest.mark.asyncio
async def test_coordinator_dispatches_to_async_worker(monkeypatch):
    from agents import coordinator

    worker = AsyncMock(
        return_value=WorkerOutcome(summary="done", status="complete")
    )
    monkeypatch.setattr(coordinator, "worker_chat_async", worker)
    request = WorkerRequestParts(current_task="read data")

    result = await coordinator._call_worker_chat(request)

    assert result.summary == "done"
    worker.assert_awaited_once_with(request)


@pytest.mark.asyncio
async def test_worker_dispatches_to_async_graph(monkeypatch):
    from agents import worker

    graph = AsyncMock(return_value="graph result")
    monkeypatch.setattr(worker, "run_worker_graph_async", graph)

    result = await worker._run_worker_graph_compat(task="read data")

    assert result == "graph result"
    graph.assert_awaited_once_with(task="read data")


@pytest.mark.asyncio
async def test_async_router_uses_native_ainvoke_and_typed_context():
    from agents.tools import get_tools
    from agents.tools.routing import ToolRoute, select_chat_route_async

    model = _AsyncRouterModel(
        ToolRoute(
            tools=[
                "list_s2t_transformations",
                "trace_transformation_path",
            ],
            skills=[],
            schemas=[],
        )
    )
    request = WorkerRequestParts(
        current_task="list files",
        operation_execution_context="read-only context",
        previous_results=[
            PreviousResultReference(
                result_id="result-1",
                description="previous rows",
            )
        ],
    )

    route = await select_chat_route_async(
        request,
        history=[{"role": "assistant", "content": "previous answer"}],
        model=model,
        available_tools=get_tools(),
        callbacks=["async-callback"],
        reroute_context={
            "gap": "need graph lineage",
            "reason": "missing_capability",
            "required_capabilities": ["graph_read"],
            "previous_tool_palettes": [["list_s2t_transformations"]],
            "attempt": 1,
        },
        catalog_stage="specialized_only",
    )

    assert route.tools == [
        "list_s2t_transformations",
        "trace_transformation_path",
    ]
    assert model.schema is ToolRoute
    assert model.method == "function_calling"
    messages, config = model.calls[0]
    payload = json.loads(messages[1].content)
    assert payload["current_task"] == "list files"
    assert payload["operation_context"] == "read-only context"
    assert payload["previous_results"][0]["result_id"] == "result-1"
    assert payload["reroute_context"]["required_capabilities"] == [
        "graph_read"
    ]
    assert config == {"callbacks": ["async-callback"]}


@pytest.mark.asyncio
async def test_async_router_repairs_invalid_structured_output():
    from agents.tools import get_tools
    from agents.tools.routing import ToolRoute, select_chat_route_async

    model = _AsyncRouterModel(
        {"tools": ["missing_tool"], "skills": [], "schemas": []},
        ToolRoute(tools=["list_files"], skills=[], schemas=[]),
    )

    route = await select_chat_route_async(
        "list files",
        model=model,
        available_tools=get_tools(),
    )

    assert route.tools == ["list_files"]
    assert len(model.calls) == 2
    repair_prompt = model.calls[1][0][-1].content
    assert "structured-маршрута" in repair_prompt
    assert "missing_tool" in repair_prompt


@pytest.mark.asyncio
async def test_async_router_uses_bounded_fallback_after_two_failures():
    from agents.tools import get_tools
    from agents.tools.routing import select_chat_route_async

    model = _AsyncRouterModel(RuntimeError("offline"), RuntimeError("offline"))

    route = await select_chat_route_async(
        "read data",
        model=model,
        available_tools=get_tools(),
        catalog_stage="general_fallback",
    )

    assert route.tools
    assert len(model.calls) == 2


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("query", "tools", "stage", "message"),
    [
        ("", [object()], "unrestricted", "пустой запрос"),
        ("query", [], "unrestricted", "не получил каталог tools"),
        ("query", [object()], "unknown", "неизвестный catalog_stage"),
    ],
)
async def test_async_router_rejects_invalid_ingress(query, tools, stage, message):
    from agents.tools.routing import ToolRoutingError, select_chat_route_async

    with pytest.raises(ToolRoutingError, match=message):
        await select_chat_route_async(
            query,
            model=_AsyncRouterModel(),
            available_tools=tools,
            catalog_stage=stage,
        )
