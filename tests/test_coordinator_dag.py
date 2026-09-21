import asyncio
from uuid import uuid4

import pytest

from agents.contracts import (
    PreviousResultReference,
    WorkerOutcome,
    WorkerPlan,
)
from agents.coordinator import _execute_worker_plan_dag
from agents.run_metrics import capture_agent_run, consume_agent_run_metrics


def _result_outcome(name: str) -> WorkerOutcome:
    return WorkerOutcome(
        summary=f"done:{name}",
        previous_results=[
            PreviousResultReference(
                result_id=f"result_{name}",
                description=f"result from {name}",
            )
        ],
    )


def test_worker_plan_validates_and_layers_a_mixed_dag():
    plan = WorkerPlan.model_validate(
        {
            "steps": [
                {"id": "a", "task": "A", "depends_on": []},
                {"id": "b", "task": "B", "depends_on": []},
                {"id": "c", "task": "C", "depends_on": ["a"]},
                {"id": "d", "task": "D", "depends_on": ["a", "b"]},
                {"id": "e", "task": "E", "depends_on": ["c", "d"]},
            ]
        }
    )

    assert [
        [step.id for step in group] for group in plan.ready_groups()
    ] == [["a", "b"], ["c", "d"], ["e"]]


@pytest.mark.parametrize(
    ("steps", "message"),
    [
        (
            [
                {"id": "a", "task": "A", "depends_on": []},
                {"id": "a", "task": "B", "depends_on": []},
            ],
            "duplicate DAG step ids",
        ),
        (
            [{"id": "a", "task": "A", "depends_on": ["missing"]}],
            "missing dependencies",
        ),
        (
            [{"id": "a", "task": "A", "depends_on": ["a"]}],
            "cannot depend on itself",
        ),
        (
            [
                {"id": "a", "task": "A", "depends_on": ["b"]},
                {"id": "b", "task": "B", "depends_on": ["a"]},
            ],
            "dependency cycle",
        ),
        (
            [
                {"id": "a", "task": "A", "depends_on": []},
                {"task": "B"},
            ],
            "either provide id or omit it",
        ),
        (
            [{"task": "A", "depends_on": ["legacy"]}],
            "legacy steps without id",
        ),
    ],
)
def test_worker_plan_rejects_invalid_dags(steps, message):
    with pytest.raises(ValueError, match=message):
        WorkerPlan.model_validate({"steps": steps})


@pytest.mark.asyncio
async def test_dag_executes_roots_concurrently_and_passes_only_dependencies(
    monkeypatch,
):
    monkeypatch.setenv("AGENT_RUN_METRICS_ENABLED", "1")
    plan = WorkerPlan.model_validate(
        {
            "steps": [
                {"id": "a", "task": "A", "depends_on": []},
                {"id": "b", "task": "B", "depends_on": []},
                {"id": "c", "task": "C", "depends_on": []},
                {"id": "d", "task": "D", "depends_on": ["a", "b", "c"]},
                {"id": "e", "task": "E", "depends_on": []},
                {"id": "f", "task": "F", "depends_on": ["d"]},
            ]
        }
    )
    root_tasks = {"A", "B", "C", "E"}
    started_roots: set[str] = set()
    roots_ready = asyncio.Event()
    requests = {}

    async def runner(request):
        requests[request.current_task] = request
        if request.current_task in root_tasks:
            started_roots.add(request.current_task)
            if started_roots == root_tasks:
                roots_ready.set()
            await asyncio.wait_for(roots_ready.wait(), timeout=1)
        await asyncio.sleep(0)
        return _result_outcome(request.current_task.lower())

    session_id = f"dag-{uuid4()}"
    with capture_agent_run(session_id):
        runs = await _execute_worker_plan_dag(
            plan,
            cycle=1,
            original_task="mixed dag",
            planner_context="planner",
            observer_context="observer",
            worker_runner=runner,
            max_concurrency=4,
        )

    assert [run["step_id"] for run in runs] == ["a", "b", "c", "d", "e", "f"]
    assert started_roots == root_tasks
    assert requests["A"].previous_results is None
    assert requests["E"].previous_results is None
    assert {
        item.result_id for item in requests["D"].previous_results or []
    } == {"result_a", "result_b", "result_c"}
    assert [
        item.result_id for item in requests["F"].previous_results or []
    ] == ["result_d"]

    metrics = consume_agent_run_metrics(session_id)
    assert metrics is not None
    assert len(metrics.coordinator_dag) == 1
    dag = metrics.coordinator_dag[0]
    assert dag["plan_size"] == 6
    assert dag["dag_depth"] == 3
    assert dag["max_parallel_width"] == 4
    assert dag["max_observed_concurrency"] == 4
    assert len(dag["workers"]) == 6


@pytest.mark.asyncio
async def test_legacy_plan_remains_linear_and_keeps_all_previous_results():
    plan = WorkerPlan.model_validate(
        {"steps": [{"task": "A"}, {"task": "B"}, {"task": "C"}]}
    )
    active = 0
    max_active = 0
    previous_ids = {}

    async def runner(request):
        nonlocal active, max_active
        previous_ids[request.current_task] = [
            item.result_id for item in request.previous_results or []
        ]
        active += 1
        max_active = max(max_active, active)
        await asyncio.sleep(0)
        active -= 1
        return _result_outcome(request.current_task.lower())

    await _execute_worker_plan_dag(
        plan,
        cycle=1,
        original_task="legacy",
        planner_context="",
        observer_context="",
        worker_runner=runner,
        max_concurrency=3,
    )

    assert max_active == 1
    assert previous_ids == {
        "A": [],
        "B": ["result_a"],
        "C": ["result_a", "result_b"],
    }


@pytest.mark.asyncio
async def test_dag_respects_worker_concurrency_limit():
    plan = WorkerPlan.model_validate(
        {
            "steps": [
                {"id": name, "task": name.upper(), "depends_on": []}
                for name in ("a", "b", "c", "d")
            ]
        }
    )
    active = 0
    max_active = 0

    async def runner(request):
        nonlocal active, max_active
        active += 1
        max_active = max(max_active, active)
        await asyncio.sleep(0.01)
        active -= 1
        return _result_outcome(request.current_task.lower())

    await _execute_worker_plan_dag(
        plan,
        cycle=1,
        original_task="bounded",
        planner_context="",
        observer_context="",
        worker_runner=runner,
        max_concurrency=2,
    )

    assert max_active == 2

    with pytest.raises(ValueError, match="max_concurrency must be positive"):
        await _execute_worker_plan_dag(
            plan,
            cycle=1,
            original_task="invalid limit",
            planner_context="",
            observer_context="",
            worker_runner=runner,
            max_concurrency=0,
        )


@pytest.mark.asyncio
async def test_dag_worker_failure_cancels_running_sibling():
    plan = WorkerPlan.model_validate(
        {
            "steps": [
                {"id": "fail", "task": "fail", "depends_on": []},
                {"id": "slow", "task": "slow", "depends_on": []},
            ]
        }
    )
    slow_started = asyncio.Event()
    slow_cancelled = asyncio.Event()

    async def runner(request):
        if request.current_task == "fail":
            await slow_started.wait()
            raise RuntimeError("worker failed")
        slow_started.set()
        try:
            await asyncio.sleep(60)
        except asyncio.CancelledError:
            slow_cancelled.set()
            raise
        raise AssertionError("slow worker must be cancelled")

    with pytest.raises(ExceptionGroup) as captured:
        await _execute_worker_plan_dag(
            plan,
            cycle=1,
            original_task="fail fast",
            planner_context="",
            observer_context="",
            worker_runner=runner,
            max_concurrency=2,
        )

    assert any(
        isinstance(error, RuntimeError) and str(error) == "worker failed"
        for error in captured.value.exceptions
    )
    assert slow_cancelled.is_set()
