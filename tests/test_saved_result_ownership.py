import asyncio
import json

import pytest
from langchain_core.messages import ToolMessage

from agents.chat_graph import WorkerDisplayItem, WorkerRunResult
from agents.contracts import WorkerRequestParts
from agents.tools.routing import ToolRoute
from agents.tools.saved_results import (
    get_active_saved_result_store,
    persist_sqlite_tool_message,
    query_saved_result,
    read_previous_result,
    saved_result_store_scope,
    worker_result_access_scope,
)
from agents.worker import worker_chat_async


@pytest.mark.asyncio
async def test_parallel_workers_with_same_tool_call_id_keep_own_dataset(
    monkeypatch,
):
    """Provider-local call ids must not join results across workers."""
    route = ToolRoute(tools=["run_sql"], skills=[], schemas=[])
    route_ready = asyncio.Event()
    route_count = 0

    async def select_route(*args, **kwargs):
        nonlocal route_count
        del args, kwargs
        route_count += 1
        if route_count == 2:
            route_ready.set()
        await route_ready.wait()
        return route

    saved_ready = asyncio.Event()
    saved_count = 0

    async def run_graph(*, task, **kwargs):
        nonlocal saved_count
        del kwargs
        store = get_active_saved_result_store()
        assert store is not None
        owner = task.current_task
        enriched = persist_sqlite_tool_message(
            ToolMessage(
                content=json.dumps({"rows": [{"owner": owner}]}),
                tool_call_id="call_1",
                name="run_sql",
            )
        )
        saved_result_ref = json.loads(str(enriched.content))["saved_result"][
            "result_ref"
        ]
        saved_count += 1
        if saved_count == 2:
            saved_ready.set()
        await saved_ready.wait()
        return WorkerRunResult(
            answer=f"Получен результат {owner}.",
            display_items=[
                WorkerDisplayItem(
                    name="run_sql",
                    content=str(enriched.content),
                    evidence_id=f"evidence-{owner}",
                    tool_call_id="call_1",
                    result_ref=saved_result_ref,
                    arguments={"owner": owner},
                    preview=owner,
                )
            ],
            accepted_tool_call_ids=["call_1"],
        )

    monkeypatch.setattr(
        "agents.worker._select_chat_route_compat",
        select_route,
    )
    monkeypatch.setattr(
        "agents.worker._run_worker_graph_compat",
        run_graph,
    )

    with saved_result_store_scope() as store:
        left, right = await asyncio.gather(
            worker_chat_async(WorkerRequestParts(current_task="left")),
            worker_chat_async(WorkerRequestParts(current_task="right")),
        )

        for owner, outcome in (("left", left), ("right", right)):
            assert len(outcome.datasets) == 1
            assert outcome.evidence[0].dataset_ref == outcome.datasets[0].result_ref
            result = store.query(
                result_ref=outcome.datasets[0].result_ref,
                query="SELECT owner FROM result",
                preview_limit=10,
            )
            assert result["rows"] == [{"owner": owner}]
            assert "worker_execution_id" not in outcome.datasets[0].model_dump()


def test_worker_result_access_denies_known_sibling_references():
    with saved_result_store_scope() as store:
        own_dataset = store.save_payload(
            source_tool="run_sql",
            source_tool_call_id="call_1",
            payload={"rows": [{"owner": "own"}]},
        )
        sibling_dataset = store.save_payload(
            source_tool="run_sql",
            source_tool_call_id="call_1",
            payload={"rows": [{"owner": "sibling"}]},
        )
        assert own_dataset is not None
        assert sibling_dataset is not None
        own_result = store.register_previous_result(
            source_tool="run_sql",
            source_tool_call_id="call_1",
            content=json.dumps({"rows": [{"owner": "own"}]}),
            description="own",
            dataset_ref=own_dataset.result_ref,
        )
        sibling_result = store.register_previous_result(
            source_tool="run_sql",
            source_tool_call_id="call_1",
            content=json.dumps({"rows": [{"owner": "sibling"}]}),
            description="sibling",
            dataset_ref=sibling_dataset.result_ref,
        )

        with worker_result_access_scope([own_result]):
            own_read = read_previous_result.invoke(
                {"result_id": own_result.result_id}
            )
            denied_read = read_previous_result.invoke(
                {"result_id": sibling_result.result_id}
            )
            own_query = query_saved_result.invoke(
                {
                    "result_ref": own_dataset.result_ref,
                    "query": "SELECT owner FROM result",
                }
            )
            denied_query = query_saved_result.invoke(
                {
                    "result_ref": sibling_dataset.result_ref,
                    "query": "SELECT owner FROM result",
                }
            )

        assert own_read["result"]["rows"] == [{"owner": "own"}]
        assert "not allowed" in denied_read["error"]
        assert own_query["rows"] == [{"owner": "own"}]
        assert "not allowed" in denied_query["error"]


def test_repeated_tool_call_id_creates_distinct_saved_results():
    with saved_result_store_scope() as store:
        first = store.save_payload(
            source_tool="run_sql",
            source_tool_call_id="call_1",
            payload={"rows": [{"value": 1}]},
        )
        second = store.save_payload(
            source_tool="run_sql",
            source_tool_call_id="call_1",
            payload={"rows": [{"value": 2}]},
        )

        assert first is not None
        assert second is not None
        assert first.result_ref != second.result_ref
        assert {item.result_ref for item in store.descriptors()} == {
            first.result_ref,
            second.result_ref,
        }
