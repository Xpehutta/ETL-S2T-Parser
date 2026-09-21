import asyncio
from uuid import uuid4

import pytest

from agents.contracts import (
    SubmittedWorkerPlan,
    PreviousResultReference,
    WorkerOutcome,
    WorkerPlan,
    adapt_legacy_worker_plan,
)
from agents.coordinator import _execute_worker_plan_dag, _worker_run_manifest
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


def test_submitted_plan_requires_explicit_id_and_dependencies():
    with pytest.raises(ValueError, match="required submitted plan fields"):
        SubmittedWorkerPlan.model_validate(
            {"steps": [{"task": "A", "depends_on": []}]}
        )
    with pytest.raises(ValueError, match="required submitted plan fields"):
        SubmittedWorkerPlan.model_validate(
            {"steps": [{"id": "a", "task": "A"}]}
        )


def test_legacy_plan_requires_explicit_adapter():
    with pytest.raises(ValueError):
        WorkerPlan.model_validate({"steps": [{"task": "A"}]})

    plan = adapt_legacy_worker_plan([{"task": "A"}, {"task": "B"}])

    assert plan.plan_origin == "legacy_adapter"
    assert [step.id for step in plan.steps] == ["step_1", "step_2"]
    assert plan.steps[1].depends_on == ["step_1"]


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
            "required runtime plan fields",
        ),
        (
            [{"task": "A", "depends_on": ["legacy"]}],
            "required runtime plan fields",
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
    assert all(item["status"] == "complete" for item in dag["workers"])
    assert all(item["ready_at_seconds"] is not None for item in dag["workers"])
    workers_by_id = {item["step_id"]: item for item in dag["workers"]}
    assert workers_by_id["a"]["input_result_ids"] == []
    assert workers_by_id["a"]["output_result_ids"] == ["result_a"]
    assert workers_by_id["d"]["input_result_ids"] == [
        "result_a",
        "result_b",
        "result_c",
    ]
    assert workers_by_id["d"]["output_result_ids"] == ["result_d"]
    assert workers_by_id["f"]["input_result_ids"] == ["result_d"]


@pytest.mark.asyncio
async def test_legacy_plan_remains_linear_and_keeps_all_previous_results():
    plan = adapt_legacy_worker_plan(
        [{"task": "A"}, {"task": "B"}, {"task": "C"}]
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
async def test_dag_worker_failure_cancels_running_sibling(monkeypatch):
    monkeypatch.setenv("AGENT_RUN_METRICS_ENABLED", "1")
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

    session_id = f"dag-cancel-running-{uuid4()}"
    with capture_agent_run(session_id):
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
    metrics = consume_agent_run_metrics(session_id)
    assert metrics is not None
    statuses = {
        item["step_id"]: item["status"]
        for item in metrics.coordinator_dag[0]["workers"]
    }
    assert statuses == {"fail": "failed", "slow": "cancelled_running"}


@pytest.mark.asyncio
async def test_structured_failed_parent_blocks_descendant_only():
    plan = WorkerPlan.model_validate(
        {
            "steps": [
                {"id": "a", "task": "A", "depends_on": []},
                {"id": "b", "task": "B", "depends_on": []},
                {"id": "c", "task": "C", "depends_on": ["a"]},
            ]
        }
    )
    called = []

    async def runner(request):
        called.append(request.current_task)
        if request.current_task == "A":
            return WorkerOutcome(
                summary="A failed",
                status="failed",
                stop_reason="tool_error",
                unmet_requirements=["source unavailable"],
            )
        return _result_outcome(request.current_task.lower())

    runs = await _execute_worker_plan_dag(
        plan,
        cycle=1,
        original_task="failure policy",
        planner_context="",
        observer_context="",
        worker_runner=runner,
        max_concurrency=2,
    )

    assert called == ["A", "B"]
    assert [run["terminal_status"] for run in runs] == [
        "failed",
        "complete",
        "blocked_by_dependency",
    ]
    assert runs[2]["outcome"] is None
    manifest = _worker_run_manifest(runs)
    assert manifest == [
        {
            "step_id": "a",
            "depends_on": [],
            "status": "failed",
            "stop_reason": "tool_error",
            "evidence_count": 0,
            "result_count": 0,
        },
        {
            "step_id": "b",
            "depends_on": [],
            "status": "complete",
            "stop_reason": None,
            "evidence_count": 0,
            "result_count": 1,
        },
        {
            "step_id": "c",
            "depends_on": ["a"],
            "status": "blocked_by_dependency",
            "stop_reason": "blocked_by_dependency",
            "evidence_count": 0,
            "result_count": 0,
        },
    ]
    assert all("summary" not in item for item in manifest)


@pytest.mark.asyncio
@pytest.mark.parametrize("has_result", [False, True])
async def test_partial_parent_requires_usable_result_for_child(has_result):
    plan = WorkerPlan.model_validate(
        {
            "steps": [
                {"id": "a", "task": "A", "depends_on": []},
                {"id": "c", "task": "C", "depends_on": ["a"]},
            ]
        }
    )
    child_request = None

    async def runner(request):
        nonlocal child_request
        if request.current_task == "A":
            return WorkerOutcome(
                summary="A partial",
                status="partial",
                stop_reason="truncated_source",
                unmet_requirements=["remaining rows"],
                previous_results=(
                    [
                        PreviousResultReference(
                            result_id="result_a",
                            description="partial result from A",
                        )
                    ]
                    if has_result
                    else []
                ),
            )
        child_request = request
        return _result_outcome("c")

    runs = await _execute_worker_plan_dag(
        plan,
        cycle=1,
        original_task="partial policy",
        planner_context="",
        observer_context="",
        worker_runner=runner,
        max_concurrency=2,
    )

    if not has_result:
        assert child_request is None
        assert runs[1]["terminal_status"] == "blocked_by_dependency"
        return

    assert child_request is not None
    assert [item.result_id for item in child_request.previous_results] == [
        "result_a"
    ]
    assert len(child_request.dependency_bundles) == 1
    bundle = child_request.dependency_bundles[0]
    assert bundle.step_id == "a"
    assert bundle.status == "partial"
    assert bundle.stop_reason == "truncated_source"
    assert [item.result_id for item in bundle.previous_results] == [
        "result_a"
    ]


@pytest.mark.asyncio
async def test_ready_child_starts_before_independent_slow_root_finishes():
    plan = WorkerPlan.model_validate(
        {
            "steps": [
                {"id": "a", "task": "A", "depends_on": []},
                {"id": "b", "task": "B", "depends_on": []},
                {"id": "c", "task": "C", "depends_on": ["a"]},
            ]
        }
    )
    a_finished = asyncio.Event()
    c_started = asyncio.Event()
    b_finished = asyncio.Event()

    async def runner(request):
        if request.current_task == "A":
            a_finished.set()
            return _result_outcome("a")
        if request.current_task == "B":
            await asyncio.wait_for(c_started.wait(), timeout=1)
            b_finished.set()
            return _result_outcome("b")
        assert a_finished.is_set()
        assert not b_finished.is_set()
        c_started.set()
        return _result_outcome("c")

    runs = await _execute_worker_plan_dag(
        plan,
        cycle=1,
        original_task="readiness",
        planner_context="",
        observer_context="",
        worker_runner=runner,
        max_concurrency=3,
    )

    assert [run["step_id"] for run in runs] == ["a", "b", "c"]
    assert c_started.is_set()


@pytest.mark.asyncio
async def test_infrastructure_failure_terminalizes_steps_before_semaphore(
    monkeypatch,
):
    monkeypatch.setenv("AGENT_RUN_METRICS_ENABLED", "1")
    plan = WorkerPlan.model_validate(
        {
            "steps": [
                {"id": name, "task": name, "depends_on": []}
                for name in ("fail", "queued_1", "queued_2")
            ]
        }
    )

    async def runner(request):
        if request.current_task == "fail":
            await asyncio.sleep(0)
            raise RuntimeError("infrastructure failure")
        await asyncio.sleep(60)
        raise AssertionError("queued worker must not finish")

    session_id = f"dag-cancel-before-{uuid4()}"
    with capture_agent_run(session_id):
        with pytest.raises(ExceptionGroup):
            await _execute_worker_plan_dag(
                plan,
                cycle=1,
                original_task="cancel queued",
                planner_context="",
                observer_context="",
                worker_runner=runner,
                max_concurrency=1,
            )

    metrics = consume_agent_run_metrics(session_id)
    assert metrics is not None
    workers = metrics.coordinator_dag[0]["workers"]
    assert len(workers) == 3
    assert workers[0]["status"] == "failed"
    assert any(
        item["status"] == "cancelled_before_start"
        for item in workers[1:]
    )
    assert all(item["status"] is not None for item in workers)


@pytest.mark.asyncio
async def test_ready_queue_preserves_declared_order_under_one_permit():
    plan = WorkerPlan.model_validate(
        {
            "steps": [
                {"id": name, "task": name.upper(), "depends_on": []}
                for name in ("a", "b", "c", "d")
            ]
        }
    )
    started = []

    async def runner(request):
        started.append(request.current_task)
        await asyncio.sleep(0)
        return _result_outcome(request.current_task.lower())

    await _execute_worker_plan_dag(
        plan,
        cycle=1,
        original_task="stable order",
        planner_context="",
        observer_context="",
        worker_runner=runner,
        max_concurrency=1,
    )

    assert started == ["A", "B", "C", "D"]


@pytest.mark.asyncio
async def test_server_worker_limit_is_separate_from_per_run_limit(
    monkeypatch,
):
    monkeypatch.setenv("SERVER_WORKER_MAX_CONCURRENCY", "1")
    monkeypatch.setenv("AGENT_RUN_METRICS_ENABLED", "1")
    first_plan = WorkerPlan.model_validate(
        {"steps": [{"id": "one", "task": "one", "depends_on": []}]}
    )
    second_plan = WorkerPlan.model_validate(
        {"steps": [{"id": "two", "task": "two", "depends_on": []}]}
    )
    first_started = asyncio.Event()
    release_first = asyncio.Event()
    active = 0
    peak = 0

    async def runner(request):
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        if request.current_task == "one":
            first_started.set()
            await release_first.wait()
        active -= 1
        return _result_outcome(request.current_task)

    session_id = f"dag-global-limit-{uuid4()}"
    with capture_agent_run(session_id):
        first = asyncio.create_task(
            _execute_worker_plan_dag(
                first_plan,
                cycle=1,
                original_task="first run",
                planner_context="",
                observer_context="",
                worker_runner=runner,
                max_concurrency=2,
            )
        )
        await asyncio.wait_for(first_started.wait(), timeout=1)
        second = asyncio.create_task(
            _execute_worker_plan_dag(
                second_plan,
                cycle=1,
                original_task="second run",
                planner_context="",
                observer_context="",
                worker_runner=runner,
                max_concurrency=2,
            )
        )
        await asyncio.sleep(0)
        assert not second.done()
        release_first.set()
        await asyncio.gather(first, second)

    assert peak == 1
    metrics = consume_agent_run_metrics(session_id)
    assert metrics is not None
    assert len(metrics.coordinator_dag) == 2
    assert all(
        item["worker_max_concurrency"] == 2
        and item["server_worker_max_concurrency"] == 1
        for item in metrics.coordinator_dag
    )
    waits = [
        worker["server_semaphore_wait_seconds"]
        for item in metrics.coordinator_dag
        for worker in item["workers"]
    ]
    assert all(value is not None and value >= 0 for value in waits)
    assert any(value > 0 for value in waits)
