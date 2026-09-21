"""LLM-driven workers with separate upstream and downstream coordination."""

from __future__ import annotations

import asyncio
import json
import logging
import os
from contextlib import asynccontextmanager
from time import perf_counter
from typing import (
    Any,
    Awaitable,
    Callable,
    Dict,
    List,
    Literal,
    Mapping,
    Optional,
    Sequence,
    TypedDict,
)

from langchain_core.messages import (
    AIMessage,
    BaseMessage,
    HumanMessage,
    SystemMessage,
    ToolMessage,
)
from langgraph.graph import END, START, StateGraph
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationError,
    model_validator,
)

from .agent import chat_model
from .async_runtime import (
    ainvoke_compat,
    ainvoke_graph_compat,
    concurrency_slot,
    offloaded_job_scope,
    run_sync_compat,
    run_coroutine_sync,
    server_worker_max_concurrency,
    worker_max_concurrency,
)
from .contracts import (
    DependencyBundle,
    EvidenceArtifact,
    MAX_PLAN_STEPS,
    OperationPipeline,
    PlanStep,
    SqlRiskAspect,
    SubmittedWorkerPlan,
    UpstreamDecision,
    UpstreamOutput,
    WorkerOutcome,
    WorkerPlan,
    WorkerRequestParts,
)
from .experiment_flags import experiment_flag_enabled
from .chat_graph import WorkerDisplayItem
from .native_call_adapter import recover_required_tool_call
from .observability import get_callback_handler, langfuse_trace_context
from .operation_protocols import (
    OPERATION_SQL_RISK_PROTOCOL_EXPERIMENT_ENV,
    protocol_variant_sha256,
    selected_sql_risk_protocol,
)
from .run_metrics import (
    get_run_metrics_callback,
    llm_stage,
    record_coordinator_dag,
    record_coordinator_plan,
    record_entity_resolution,
    record_sql_risk_operation,
    record_upstream_output,
    record_validation_protocol,
    record_worker_outcome,
)
from .sql_risk_operation_pipeline import (
    SqlRiskOperationIssue,
    SqlRiskOperationPipelineResult,
    run_sql_risk_operation_pipeline,
)
from .sql_risk_assessment import (
    MAX_SQL_RISK_ASSESSMENT_ATTEMPTS,
    SQL_RISK_ASSESSMENT_PROMPT,
    SQL_RISK_ASSESSMENT_TOOL_NAME,
    SqlRiskAssessment,
    render_sql_risk_assessment_repair,
    render_sql_risk_assessment_request,
    validate_sql_risk_assessment,
)
from .sql_risk_scope_contract import (
    build_sql_risk_scope_contract,
    sql_risk_scope_evidence_enabled,
)
from .sql_risk_scope_extraction import (
    MAX_SQL_RISK_SCOPE_EXTRACTION_ATTEMPTS,
    SQL_RISK_SCOPE_EXECUTION_MODES,
    SQL_RISK_SCOPE_EXTRACTION_PROMPT,
    SQL_RISK_SCOPE_EXTRACTION_TOOL_NAME,
    SqlRiskScopeExtraction,
    render_sql_risk_scope_extraction_repair,
    validate_sql_risk_scope_extraction,
)
from .tools.context import (
    OPERATION_SQL_RISK_ASPECTS_EXPERIMENT_ENV,
    OPERATION_SKILL_CATALOG,
    get_downstream_capability_context,
    get_downstream_table_context,
    load_operation_skills,
    load_upstream_analysis_context,
)
from .worker import (
    discard_worker_display_refs,
    register_worker_display_items,
    worker_chat,
    worker_chat_async,
)
from .tools.saved_results import (
    get_active_saved_result_store,
    saved_result_store_scope,
)
from .test_protocol import (
    MAX_PROTOCOL_OBJECTS,
    PROTOCOL_CHECKS,
    RawTestProtocolContract,
    build_test_protocol_display_payloads,
    compile_test_protocol,
    render_test_protocol_answer,
)
from .test_protocol_contract_review import (
    VALIDATION_CONTRACT_REVIEW_PROMPT,
    VALIDATION_CONTRACT_REVIEW_TOOL_NAME,
    ValidationContractReview,
    render_validation_contract_review_request,
    validate_validation_contract_review,
    validation_contract_review_tool_schema,
)
from .test_protocol_resolution import (
    resolve_test_protocol_contract,
    validate_raw_contract_origin,
)
from .validation_protocol import read_test_protocol_inputs
logger = logging.getLogger(__name__)
_DEFAULT_SYNC_WORKER_CHAT = worker_chat


async def _call_worker_chat(task: WorkerRequestParts) -> WorkerOutcome:
    """Keep legacy injected workers compatible without blocking ASGI."""

    if worker_chat is not _DEFAULT_SYNC_WORKER_CHAT:
        return await run_sync_compat(worker_chat, task)
    return await worker_chat_async(task)


@asynccontextmanager
async def _coordinator_runtime_scope() -> Any:
    """Keep callbacks and saved results alive through offloaded cleanup."""
    with (
        saved_result_store_scope(),
        langfuse_trace_context(
            trace_name="worker_coordinator",
            metadata={
                "max_workers": COORDINATOR_MAX_WORKERS,
                "max_cycles": COORDINATOR_MAX_CYCLES,
            },
            tags=["coordinator", "worker", "experiment"],
        ),
    ):
        async with offloaded_job_scope():
            yield

COORDINATOR_MAX_WORKERS = MAX_PLAN_STEPS
COORDINATOR_MAX_CYCLES = 2
COORDINATOR_CONTEXT_MAX_CHARS = 4000
UPSTREAM_EVIDENCE_PREVIEW_MAX_CHARS = 2000
UPSTREAM_EVIDENCE_SERIALIZED_MAX_CHARS = 12000
_PLAN_TOOL_NAME = "submit_worker_plan"
_SQL_RISK_OPERATION_SKILL = "Анализ SQL-рисков"
_DEFAULT_SQL_RISK_PROTOCOL = "default/current"
_OPERATION_SKILL_TOOL_NAME = "select_operation_skills"
_VALIDATION_PROTOCOL_CONTRACT_TOOL_NAME = "submit_validation_protocol_contract"
_UPSTREAM_ANSWER_TOOL_NAME = "submit_upstream_answer"
_UPSTREAM_DATA_DECISION_TOOL_NAME = "submit_upstream_data_decision"
_UPSTREAM_ANALYSIS_CONTEXT = load_upstream_analysis_context()
_DOWNSTREAM_CAPABILITY_CONTEXT = get_downstream_capability_context()
_DOWNSTREAM_TABLE_CONTEXT = get_downstream_table_context()


def _typed_sql_risk_aspects_enabled() -> bool:
    """Return whether E2 routes only explicitly selected SQL-risk aspects."""
    return experiment_flag_enabled(
        OPERATION_SQL_RISK_ASPECTS_EXPERIMENT_ENV,
    )


def _sql_risk_operation_scope_enabled() -> bool:
    """Expose the separate operation-scope route only when opted in."""

    return sql_risk_scope_evidence_enabled()


def _sql_risk_protocol_attestation(
    operation_skills: Sequence[str],
    sql_risk_aspects: Sequence[SqlRiskAspect],
) -> Dict[str, Any]:
    """Describe the exact opt-in SQL-risk protocol without changing it."""
    if _SQL_RISK_OPERATION_SKILL not in operation_skills:
        return {}

    variant = selected_sql_risk_protocol(
        os.getenv(OPERATION_SQL_RISK_PROTOCOL_EXPERIMENT_ENV)
    )
    if variant is None:
        return {
            "operation_sql_risk_protocol": _DEFAULT_SQL_RISK_PROTOCOL,
            "operation_sql_risk_protocol_sha256": None,
        }

    selected_aspects = tuple(sql_risk_aspects)
    if selected_aspects != (variant.aspect,):
        raise ValueError(
            f"SQL-risk protocol candidate {variant.name!r} is for aspect "
            f"{variant.aspect!r}, but selected aspects are "
            f"{selected_aspects!r}"
        )
    return {
        "operation_sql_risk_protocol": variant.name,
        "operation_sql_risk_protocol_sha256": (
            protocol_variant_sha256(variant)
        ),
    }


class CoordinatorAnswer(BaseModel):
    """Coordinator output consumed by the top-level supervisor."""

    model_config = ConfigDict(extra="forbid")

    answer: str
    display_refs: List[str] = Field(default_factory=list)


class OperationSkillSelection(BaseModel):
    """Execution route and prompt profiles selected for one operation."""

    model_config = ConfigDict(extra="forbid")

    pipeline: OperationPipeline
    skills: List[str]
    sql_risk_aspects: List[SqlRiskAspect] = Field(default_factory=list)

    @model_validator(mode="after")
    def _aspects_match_selected_skill(self) -> "OperationSkillSelection":
        self.skills = list(dict.fromkeys(self.skills))
        self.sql_risk_aspects = list(dict.fromkeys(self.sql_risk_aspects))
        if self.pipeline in {"validation_protocol", "sql_risk_scope"} and (
            self.skills or self.sql_risk_aspects
        ):
            raise ValueError(
                f"pipeline={self.pipeline} requires no operation skills and "
                "no sql_risk_aspects"
            )
        if self.sql_risk_aspects and "Анализ SQL-рисков" not in self.skills:
            raise ValueError(
                "sql_risk_aspects require Анализ SQL-рисков skill"
            )
        if not _typed_sql_risk_aspects_enabled():
            # E2 baseline deliberately loads the complete legacy profile.
            self.sql_risk_aspects = []
            return self
        if (
            "Анализ SQL-рисков" in self.skills
            and not self.sql_risk_aspects
        ):
            raise ValueError(
                "Анализ SQL-рисков requires at least one sql_risk_aspect"
            )
        return self


class CoordinatorWorkerRun(TypedDict):
    cycle: int
    step: int
    step_id: str
    depends_on: List[str]
    terminal_status: Literal[
        "complete",
        "partial",
        "failed",
        "blocked_by_dependency",
    ]
    stop_reason: Optional[str]
    outcome: Optional[WorkerOutcome]


class CoordinatorGraphState(TypedDict):
    task: str
    context: str
    operation_skills: Optional[List[str]]
    operation_sql_risk_aspects: Optional[List[SqlRiskAspect]]
    operation_pipeline: Optional[OperationPipeline]
    cycle: int
    plan: List[Dict[str, Any]]
    next_step: int
    worker_runs: List[CoordinatorWorkerRun]
    upstream_problem: Optional[str]
    upstream_output: Optional[Dict[str, Any]]
    final_answer: Optional[str]
    selected_display_refs: List[str]


def _worker_run_manifest(
    runs: Sequence[CoordinatorWorkerRun],
) -> List[Dict[str, Any]]:
    """Project terminal DAG state without summaries, rows or runtime refs."""
    manifest: List[Dict[str, Any]] = []
    for run in runs:
        outcome = run["outcome"]
        manifest.append(
            {
                "step_id": run["step_id"],
                "depends_on": list(run["depends_on"]),
                "status": run["terminal_status"],
                "stop_reason": run["stop_reason"],
                "evidence_count": (
                    sum(
                        not item.lineage_evidence_ids
                        for item in outcome.evidence
                    )
                    if outcome is not None
                    else 0
                ),
                "result_count": (
                    len(outcome.previous_results)
                    if outcome is not None
                    else 0
                ),
            }
        )
    return manifest


def _compact_json_chars(value: Any) -> int:
    return len(
        json.dumps(
            value,
            ensure_ascii=False,
            default=str,
            separators=(",", ":"),
        )
    )


def _bounded_upstream_preview(value: str) -> tuple[str, bool]:
    preview = str(value or "")
    if len(preview) <= UPSTREAM_EVIDENCE_PREVIEW_MAX_CHARS:
        return preview, False
    marker = "… [upstream preview truncated]"
    prefix_length = max(
        0,
        UPSTREAM_EVIDENCE_PREVIEW_MAX_CHARS - len(marker),
    )
    return preview[:prefix_length].rstrip() + marker, True


def _build_upstream_evidence_bundle(
    runs: Sequence[CoordinatorWorkerRun],
) -> Dict[str, Any]:
    """Build a bounded, explicit evidence envelope in stable plan order."""
    evidence: List[Dict[str, Any]] = []
    evidence_lineage: List[Dict[str, Any]] = []
    omitted_evidence_ids: List[str] = []
    preview_truncated_evidence_ids: List[str] = []
    seen_evidence_ids: set[str] = set()

    for run in runs:
        outcome = run["outcome"]
        if outcome is None:
            continue
        for artifact in outcome.evidence:
            if artifact.evidence_id in seen_evidence_ids:
                raise CoordinatorResponseError(
                    "Workers вернули дублирующий evidence_id: "
                    + artifact.evidence_id
                )
            seen_evidence_ids.add(artifact.evidence_id)
            if artifact.lineage_evidence_ids:
                evidence_lineage.append(
                    {
                        "producer_step_id": run["step_id"],
                        "evidence_id": artifact.evidence_id,
                        "source_evidence_ids": list(
                            artifact.lineage_evidence_ids
                        ),
                    }
                )
                continue

            preview, preview_truncated = _bounded_upstream_preview(
                artifact.preview
            )
            envelope = {
                "evidence_id": artifact.evidence_id,
                "producer_step_id": run["step_id"],
                "tool_name": artifact.tool_name,
                "args": artifact.compact_args,
                "preview": preview,
                "truncated": artifact.truncated or preview_truncated,
                "preview_truncated": preview_truncated,
                "displayable": artifact.display_ref is not None,
            }
            if (
                _compact_json_chars([*evidence, envelope])
                > UPSTREAM_EVIDENCE_SERIALIZED_MAX_CHARS
            ):
                omitted_evidence_ids.append(artifact.evidence_id)
                continue
            evidence.append(envelope)
            if preview_truncated:
                preview_truncated_evidence_ids.append(
                    artifact.evidence_id
                )

    serialized_chars = _compact_json_chars(evidence)
    return {
        "evidence": evidence,
        "evidence_budget": {
            "max_preview_chars": UPSTREAM_EVIDENCE_PREVIEW_MAX_CHARS,
            "max_serialized_chars": (
                UPSTREAM_EVIDENCE_SERIALIZED_MAX_CHARS
            ),
            "serialized_chars": serialized_chars,
            "overflow": bool(omitted_evidence_ids),
            "omitted_evidence_ids": omitted_evidence_ids,
            "preview_truncated_evidence_ids": (
                preview_truncated_evidence_ids
            ),
        },
        "evidence_lineage": evidence_lineage,
    }


async def _execute_worker_plan_dag(
    plan: WorkerPlan,
    *,
    cycle: int,
    original_task: str,
    planner_context: str,
    observer_context: str,
    collected_display_refs: Optional[List[str]] = None,
    worker_runner: Optional[
        Callable[[WorkerRequestParts], Awaitable[WorkerOutcome]]
    ] = None,
    max_concurrency: Optional[int] = None,
) -> List[CoordinatorWorkerRun]:
    """Execute a validated DAG from a stable readiness queue."""

    runner = worker_runner or _call_worker_chat
    concurrency_limit = (
        worker_max_concurrency()
        if max_concurrency is None
        else max_concurrency
    )
    if concurrency_limit < 1:
        raise ValueError("worker DAG max_concurrency must be positive")
    server_concurrency_limit = server_worker_max_concurrency()

    groups = plan.ready_groups()
    step_numbers = {
        str(step.id): index
        for index, step in enumerate(plan.steps, start=1)
    }
    steps_by_id = {str(step.id): step for step in plan.steps}
    layer_by_id = {
        str(step.id): layer
        for layer, group in enumerate(groups, start=1)
        for step in group
    }
    children: Dict[str, List[str]] = {
        step_id: [] for step_id in step_numbers
    }
    remaining_dependencies = {
        str(step.id): len(step.depends_on) for step in plan.steps
    }
    for step in plan.steps:
        for dependency_id in step.depends_on:
            children[dependency_id].append(str(step.id))
    completed: Dict[str, CoordinatorWorkerRun] = {}
    semaphore = asyncio.Semaphore(concurrency_limit)
    activity_lock = asyncio.Lock()
    active_workers = 0
    max_observed_concurrency = 0
    scheduler_started = perf_counter()
    timings: Dict[str, Dict[str, Any]] = {
        step_id: {
            "step_id": step_id,
            "depends_on": list(steps_by_id[step_id].depends_on),
            "layer": layer_by_id[step_id],
            "ready_at_seconds": None,
            "queued_at_seconds": None,
            "started_at_seconds": None,
            "ended_at_seconds": None,
            "dependency_wait_seconds": None,
            "semaphore_wait_seconds": None,
            "server_semaphore_wait_seconds": None,
            "input_result_ids": [],
            "output_result_ids": [],
            "status": None,
        }
        for step_id in step_numbers
    }

    def dependency_bundles(step: PlanStep) -> List[DependencyBundle]:
        bundles: List[DependencyBundle] = []
        for dependency_id in step.depends_on:
            dependency_run = completed[dependency_id]
            outcome = dependency_run["outcome"]
            bundles.append(
                DependencyBundle(
                    step_id=dependency_id,
                    status=dependency_run["terminal_status"],
                    stop_reason=dependency_run["stop_reason"],
                    previous_results=(
                        list(outcome.previous_results)
                        if outcome is not None
                        else []
                    ),
                )
            )
        return bundles

    def dependency_results(
        bundles: Sequence[DependencyBundle],
    ) -> List[Any]:
        references: List[Any] = []
        seen_ids: set[str] = set()
        for bundle in bundles:
            for reference in bundle.previous_results:
                if reference.result_id in seen_ids:
                    continue
                seen_ids.add(reference.result_id)
                references.append(reference)
        return references

    async def run_step(
        step: PlanStep,
    ) -> CoordinatorWorkerRun:
        nonlocal active_workers, max_observed_concurrency
        step_id = str(step.id)
        bundles = dependency_bundles(step)
        previous_results = dependency_results(bundles)
        request = WorkerRequestParts(
            current_task=step.task,
            original_task=original_task,
            operation_execution_context=planner_context,
            operation_completeness_context=observer_context,
            previous_results=(
                previous_results if step.depends_on else None
            ),
            dependency_bundles=(bundles if step.depends_on else None),
        )
        timing = timings[step_id]
        timing["input_result_ids"] = [
            reference.result_id for reference in previous_results
        ]
        queued_at = scheduler_started + float(
            timing["queued_at_seconds"] or 0.0
        )
        acquired = False
        started_at: Optional[float] = None
        active_incremented = False
        try:
            await semaphore.acquire()
            acquired = True
            local_acquired_at = perf_counter()
            timing["semaphore_wait_seconds"] = (
                local_acquired_at - queued_at
            )
            async with concurrency_slot("worker"):
                started_at = perf_counter()
                timing["server_semaphore_wait_seconds"] = (
                    started_at - local_acquired_at
                )
                async with activity_lock:
                    active_workers += 1
                    active_incremented = True
                    max_observed_concurrency = max(
                        max_observed_concurrency,
                        active_workers,
                    )
                    concurrent_at_start = active_workers
                timing.update(
                    {
                        "started_at_seconds": started_at - scheduler_started,
                        "concurrent_at_start": concurrent_at_start,
                    }
                )
                logger.info(
                    "Coordinator dispatches DAG worker step_id=%s step=%s "
                    "depends_on=%s previous_results=%s",
                    step_id,
                    step_numbers[step_id],
                    step.depends_on,
                    len(previous_results),
                )
                outcome = await runner(request)
                timing["status"] = outcome.status
                timing["output_result_ids"] = [
                    reference.result_id
                    for reference in outcome.previous_results
                ]
        except asyncio.CancelledError as exc:
            timing["status"] = (
                "cancelled_running"
                if started_at is not None
                else "cancelled_before_start"
            )
            timing["error_type"] = type(exc).__name__
            raise
        except BaseException as exc:
            timing["status"] = "failed"
            timing["error_type"] = type(exc).__name__
            raise
        finally:
            ended_at = perf_counter()
            timing["ended_at_seconds"] = ended_at - scheduler_started
            timing["elapsed_seconds"] = (
                ended_at - started_at if started_at is not None else 0.0
            )
            if active_incremented:
                async with activity_lock:
                    active_workers -= 1
            if acquired:
                semaphore.release()

        record_worker_outcome(
            cycle=cycle,
            step=step_numbers[step_id],
            status=outcome.status,
            stop_reason=outcome.stop_reason,
            unmet_requirements=list(outcome.unmet_requirements),
            evidence_count=len(outcome.evidence),
            dataset_count=len(outcome.datasets),
        )
        if collected_display_refs is not None:
            collected_display_refs.extend(
                artifact.display_ref
                for artifact in outcome.evidence
                if artifact.display_ref
            )
        return {
            "cycle": cycle,
            "step": step_numbers[step_id],
            "step_id": step_id,
            "depends_on": list(step.depends_on),
            "terminal_status": outcome.status,
            "stop_reason": outcome.stop_reason,
            "outcome": outcome,
        }

    def blocking_dependencies(step: PlanStep) -> List[str]:
        blockers: List[str] = []
        for dependency_id in step.depends_on:
            dependency_run = completed[dependency_id]
            dependency_outcome = dependency_run["outcome"]
            status = dependency_run["terminal_status"]
            if status in {"failed", "blocked_by_dependency"}:
                blockers.append(dependency_id)
            elif (
                status == "partial"
                and (
                    dependency_outcome is None
                    or not dependency_outcome.previous_results
                )
            ):
                blockers.append(dependency_id)
        return blockers

    try:
        active_tasks: Dict[asyncio.Task[CoordinatorWorkerRun], str] = {}
        async with asyncio.TaskGroup() as task_group:
            def unlock_children(step_id: str) -> None:
                for child_id in children[step_id]:
                    remaining_dependencies[child_id] -= 1
                    if remaining_dependencies[child_id] == 0:
                        enqueue_ready(child_id)

            def mark_blocked(step_id: str, blockers: Sequence[str]) -> None:
                now = perf_counter()
                elapsed = now - scheduler_started
                timing = timings[step_id]
                timing.update(
                    {
                        "ready_at_seconds": elapsed,
                        "dependency_wait_seconds": elapsed,
                        "semaphore_wait_seconds": 0.0,
                        "ended_at_seconds": elapsed,
                        "elapsed_seconds": 0.0,
                        "status": "blocked_by_dependency",
                        "blocked_by": list(blockers),
                    }
                )
                step = steps_by_id[step_id]
                completed[step_id] = {
                    "cycle": cycle,
                    "step": step_numbers[step_id],
                    "step_id": step_id,
                    "depends_on": list(step.depends_on),
                    "terminal_status": "blocked_by_dependency",
                    "stop_reason": "blocked_by_dependency",
                    "outcome": None,
                }
                record_worker_outcome(
                    cycle=cycle,
                    step=step_numbers[step_id],
                    status="blocked_by_dependency",
                    stop_reason="blocked_by_dependency",
                    unmet_requirements=[
                        "Blocked by dependency: " + ", ".join(blockers)
                    ],
                    evidence_count=0,
                    dataset_count=0,
                )
                unlock_children(step_id)

            def enqueue_ready(step_id: str) -> None:
                step = steps_by_id[step_id]
                now = perf_counter()
                elapsed = now - scheduler_started
                timing = timings[step_id]
                timing["ready_at_seconds"] = elapsed
                timing["dependency_wait_seconds"] = elapsed
                blockers = blocking_dependencies(step)
                if blockers:
                    mark_blocked(step_id, blockers)
                    return
                timing["queued_at_seconds"] = elapsed
                task = task_group.create_task(
                    run_step(step),
                    name=f"worker-dag:{step_id}",
                )
                active_tasks[task] = step_id

            for step in plan.steps:
                step_id = str(step.id)
                if remaining_dependencies[step_id] == 0:
                    enqueue_ready(step_id)

            while active_tasks:
                done, _ = await asyncio.wait(
                    tuple(active_tasks),
                    return_when=asyncio.FIRST_COMPLETED,
                )
                for task in sorted(
                    done,
                    key=lambda item: step_numbers[active_tasks[item]],
                ):
                    step_id = active_tasks.pop(task)
                    completed[step_id] = task.result()
                    unlock_children(step_id)

            if len(completed) != len(plan.steps):
                raise RuntimeError("DAG scheduler stopped before all steps")
    finally:
        terminalized_at = perf_counter() - scheduler_started
        for timing in timings.values():
            if timing["status"] is not None:
                continue
            timing.update(
                {
                    "ended_at_seconds": terminalized_at,
                    "elapsed_seconds": 0.0,
                    "status": "cancelled_before_start",
                }
            )
        record_coordinator_dag(
            {
                "cycle": cycle,
                "plan_size": len(plan.steps),
                "dag_depth": len(groups),
                "max_parallel_width": max(len(group) for group in groups),
                "worker_max_concurrency": concurrency_limit,
                "server_worker_max_concurrency": server_concurrency_limit,
                "max_observed_concurrency": max_observed_concurrency,
                "blocked_count": sum(
                    item["status"] == "blocked_by_dependency"
                    for item in timings.values()
                ),
                "cancelled_before_start_count": sum(
                    item["status"] == "cancelled_before_start"
                    for item in timings.values()
                ),
                "cancelled_running_count": sum(
                    item["status"] == "cancelled_running"
                    for item in timings.values()
                ),
                "elapsed_seconds": perf_counter() - scheduler_started,
                "workers": [
                    timings[step_id] for step_id in step_numbers
                ],
            }
        )

    return [completed[str(step.id)] for step in plan.steps]


_OPERATION_SKILL_CATALOG_CONTEXT = "\n".join(
    f"- `{name}` — {description}"
    for name, description in OPERATION_SKILL_CATALOG.items()
)

_SQL_RISK_TYPED_ROUTER_GUIDANCE = """
Если выбрана `Анализ SQL-рисков`, заполни `sql_risk_aspects` только аспектами,
которые прямо нужны результату: `row_filtering`, `cardinality`,
`constraint_rejection`, `value_changes`, `write_semantics`. Для остальных
agentic skills верни `sql_risk_aspects=[]`. Не добавляй все аспекты
автоматически.
""".strip()

_SQL_RISK_SCOPE_PIPELINE_GUIDANCE = """
Сначала определи, от каких сохранённых фактов зависит запрошенный вывод.
Если его полностью определяет прямое сравнение catalog-атрибутов двух колонок
— `data_type`, `not_null` или признака ключа — выбери `pipeline="agentic"` и
профиль `Совместимость колонок`; transformation SQL для этого не нужен. Это
правило определяется требуемым evidence, а не лексикой запроса: условная
совместимость nullable source с NOT NULL target остаётся catalog-сравнением.
`sql_risk_scope` нужен только когда вывод зависит от того, может ли именно
сохранённая SQL-проекция, выражение или predicate породить NULL.
The same evidence boundary applies regardless of the query language: a result
determined by source/target catalog attributes (`data_type`, `not_null`, key
flags) is `agentic` column compatibility; select `sql_risk_scope` only when the
result depends on transformation SQL structure.

`pipeline="sql_risk_scope"` выбирай для одной узкой задачи анализа риска
сохранённого SQL по одной явно указанной направленной source → target паре.
Конкретный вид риска, точный scope и optional file_id извлечёт отдельный
внутренний native LLM-вызов этого pipeline; здесь их не выбирай и не извлекай.
Просьба назвать или дословно привести подтверждающий JOIN, predicate,
выражение целевого поля либо корневой SQL-оператор остаётся частью такого
узкого статического анализа сохранённого SQL и сама по себе не делает задачу
составной или agentic.
При `sql_risk_scope` верни `skills=[]`.
Для составной задачи, исполнения/вычисления фактических метрик по данным,
перечня самих строк, нескольких пар либо результата шире одного SQL-risk
аспекта выбери `pipeline="agentic"`.
""".strip()

_SQL_RISK_LEGACY_ROUTER_GUIDANCE = ""

_OPERATION_SKILL_PROMPT = f"""
Ты operation router. Один раз для всей `original_task` выбери исполнительный
`pipeline` и operation-skills. Верни ровно один native call
`{_OPERATION_SKILL_TOOL_NAME}`.

`stable_context` используй только для устойчивой терминологии, intent и границ
результата при выборе pipeline/skills. Не извлекай из него разовые identifiers
или scope и не подменяй им буквальные значения из `original_task`.

`pipeline="validation_protocol"` выбирай для явной просьбы составить explicit,
standard либо exhaustive SQL test protocol внешней Greenplum-проверки
source→target S2T-загрузки. Файл необязателен: без него catalog-dependent checks
будут помечены unavailable, а остальные всё равно компилируются. Сюда относятся
row/key/field/schema/aggregate reconciliation, uniqueness, NULL и статический
preflight. SQL только проектируется и не исполняется. Для этой отдельной ветки
верни `skills=[]`.

{_SQL_RISK_SCOPE_PIPELINE_GUIDANCE}

Во всех остальных случаях выбирай `pipeline="agentic"`; выбранные
operation-skills направят downstream, workers и upstream внутри общего потока.

Operation-skill — профиль результата, которого добивается пользователь, а не
источник данных, тип объекта или retrieval-skill. Выбирай профиль по intent и
однозначно требуемому результату: буквальное название профиля в запросе не
требуется. Не выбирай профиль только из-за связанных терминов. Несколько
профилей допустимы, только если для ответа действительно нужны несколько
разных видов анализа; не добавляй смежный анализ «на всякий случай».

{_SQL_RISK_TYPED_ROUTER_GUIDANCE}

`skills=[]` — нормальный вариант по умолчанию. Оставляй массив пустым для
простого чтения, списка либо объяснения одной сохранённой трансформации, если
пользователь не просит сравнение атрибутов, оценку риска строк, разность покрытия
маппинга или проектирование проверки. Само наличие SQL, S2T, пары source→target,
колонок либо слова «трансформация» не является основанием выбрать профиль.

Доступные operation-skills:
{_OPERATION_SKILL_CATALOG_CONTEXT}

Не отвечай на задачу, не планируй чтение и не придумывай новый профиль.
""".strip()

_OPERATION_SKILL_REPAIR_PROMPT = f"""
Предыдущий native call `{_OPERATION_SKILL_TOOL_NAME}` нарушает схему или содержит
имя вне каталога. Верни ровно один исправленный call с полями `pipeline`,
`skills` и `sql_risk_aspects`. Pipeline — `agentic` либо
`validation_protocol`; массив skills может быть пустым. Аспекты допустимы только
для `Анализ SQL-рисков`; при выборе этого профиля верни хотя бы один нужный
аспект, иначе верни пустой массив. Для `validation_protocol` верни пустые skills
и aspects. Используй только дословные имена из каталогов.
""".strip()

_OPERATION_SKILL_SCOPE_REPAIR_PROMPT = f"""
Предыдущий native call `{_OPERATION_SKILL_TOOL_NAME}` нарушает схему или содержит
имя вне каталога. Верни ровно один исправленный call с полями `pipeline`,
`skills` и `sql_risk_aspects`. Pipeline — `agentic`, `validation_protocol`
либо `sql_risk_scope`. Для `validation_protocol` и `sql_risk_scope` массивы
skills и sql_risk_aspects пусты. Для agentic используй только дословные enum и
имена из каталогов. Заново выбери согласованный pipeline по исходному intent;
не исправляй смешанный route механическим удалением skills/aspects. Если вывод
полностью определяется source/target catalog-атрибутами (`data_type`,
`not_null`, признаки ключа) без анализа transformation SQL, верни `agentic` и
`Совместимость колонок`. Выбирай по требуемому evidence, а не по лексике или
языку запроса. `sql_risk_scope` выбирай только когда вывод зависит от SQL
projection, expression, predicate либо корневого SQL-оператора.
""".strip()

_OPERATION_SKILL_SCOPE_BASELINE_REPAIR_PROMPT = f"""
Предыдущий native call `{_OPERATION_SKILL_TOOL_NAME}` нарушает схему или содержит
имя вне каталога. Верни ровно один исправленный call с полями `pipeline` и
`skills`. Pipeline — `agentic`, `validation_protocol` либо `sql_risk_scope`.
Для `validation_protocol` и `sql_risk_scope` верни `skills=[]`; для agentic
используй только дословные имена профилей из каталога. Заново выбери
согласованный pipeline по исходному intent, а не механически удаляй skills.
Если вывод полностью определяется source/target catalog-атрибутами без анализа
transformation SQL, верни `agentic` и `Совместимость колонок`.
`sql_risk_scope` выбирай только когда вывод зависит от SQL projection,
expression, predicate либо корневого SQL-оператора.
""".strip()


def _operation_skill_prompt() -> str:
    """Build an E2-consistent router prompt for the active variant."""
    prompt = _OPERATION_SKILL_PROMPT
    if not _typed_sql_risk_aspects_enabled():
        prompt = prompt.replace(
            _SQL_RISK_TYPED_ROUTER_GUIDANCE,
            _SQL_RISK_LEGACY_ROUTER_GUIDANCE,
        )
    if not _sql_risk_operation_scope_enabled():
        prompt = prompt.replace(
            _SQL_RISK_SCOPE_PIPELINE_GUIDANCE,
            "",
        )
    return prompt


def _operation_skill_repair_prompt() -> str:
    """Build repair instructions that match the active E2 schema."""
    if _sql_risk_operation_scope_enabled():
        return (
            _OPERATION_SKILL_SCOPE_REPAIR_PROMPT
            if _typed_sql_risk_aspects_enabled()
            else _OPERATION_SKILL_SCOPE_BASELINE_REPAIR_PROMPT
        )
    if _typed_sql_risk_aspects_enabled():
        return _OPERATION_SKILL_REPAIR_PROMPT
    return (
        f"Предыдущий native call `{_OPERATION_SKILL_TOOL_NAME}` нарушает "
        "схему или содержит имя вне каталога. Верни ровно один исправленный "
        "call с полями `pipeline` и `skills`. Pipeline — "
        "`agentic` либо `validation_protocol`; массив skills может быть "
        "пустым; для `validation_protocol` он обязан быть пустым. Используй "
        "только дословные имена из каталога."
    )

_VALIDATION_PROTOCOL_CONTRACT_PROMPT = f"""
Извлеки только явно заданный контракт SQL test protocol из `original_task`.
Верни ровно один native call `{_VALIDATION_PROTOCOL_CONTRACT_TOOL_NAME}`.

- Всегда явно выбери `file_scope_kind`: `file_id`, если в original_task дан
  числовой file_id; `file_mention`, если дано буквальное имя или смысловое
  описание файла; `not_provided` только если original_task вообще не задаёт
  файловый scope. Опциональность файла не разрешает терять явно названный scope.
  Заполни ровно соответствующее поле `file_id` либо `file_mention`; не помещай
  filename в source/target mentions.
- `loads` содержит отдельный элемент для каждой явно заданной загрузки;
  `source_mentions` — все исходные mentions именно этого `target_mention`.
  Явную запись `source → target` разделяй на два атомарных значения правильных
  ролей; не копируй всю запись со стрелкой в один mention.
  Копируй mention дословно, включая опечатку, неполное или смысловое имя:
  канонизацию выполнит код после этого native call.
- `mode="explicit"`, когда пользователь перечислил проверки; сохрани их в
  `requested_checks` всего contract либо конкретного load. `mode="standard"`
  для общей просьбы о тест-протоколе, `mode="exhaustive"` для максимально
  полного протокола. Не добавляй не запрошенные checks в explicit mode.
- Допустимые checks: {', '.join(PROTOCOL_CHECKS)}.
- Явно заданные поля ключа копируй в `explicit_key` contract/load.
- Не придумывай таблицы, колонки, SQL, проверки и scope-поля.
- Перед native call ещё раз сопоставь каждый явно заданный file scope, load,
  source, target, check и key с отдельным полем результата; ничего не опускай.
""".strip()

_VALIDATION_PROTOCOL_CONTRACT_REPAIR_PROMPT = f"""
Предыдущий `{_VALIDATION_PROTOCOL_CONTRACT_TOOL_NAME}` нарушает схему,
полноту либо роли исходной задачи.
Верни один исправленный native call с `mode`, `requested_checks` и непустым
`file_scope_kind` и `loads`; в каждом load нужны непустые literal
`source_mentions`, один
`target_mention`, `requested_checks` и при наличии `explicit_key`. Файл
необязателен только если его нет в original_task: тогда выбери `not_provided`.
Явный file scope обязан сохраниться как `file_id` либо `file_mention`, и filename
нельзя помещать в table mentions. Разделяй literal `source → target` на
атомарные значения соответствующих ролей. Копируй только значения из
`original_task`, не исправляй mentions самостоятельно.
""".strip()

class CoordinatorResponseError(RuntimeError):
    """Raised when an LLM response violates a structural coordinator contract."""


_DOWNSTREAM_PLAN_PROMPT = f"""
Ты downstream planner. Верни native call `{_PLAN_TOOL_NAME}` с 1–{COORDINATOR_MAX_WORKERS}
`steps` чтения. Задай уникальный `id`; `depends_on=[]` для независимых steps,
иначе ID прямых входов. Missing/self dependencies и циклы запрещены.

Каждый step незаменим для ответа. Не добавляй незапрошенные проверки,
обогащение, реализацию и чтение справочника без необходимости.

Сохрани сущность, направление, scope и фильтры. Роль source/target известна,
только если привязана к идентификатору в original_task/context или доказана
S2T-строкой; роль результата не задаёт роль кандидата. Точные идентификаторы
бери только из original_task/context, пиши в обратных кавычках без внешней
пунктуации. Не превращай бизнес-термин в техническое имя или tool.

Если scope задан полным `filename`, а чтению нужен внутренний `file_id`, сначала
запланируй точное разрешение имён, затем зависимые чтения.
`file_id` допустим лишь из original_task либо принятого результата разрешения.
Каждая task должна быть самодостаточной: дословно повторяй в ней все нужные
точные идентификаторы, роли, scope и фильтры из original_task/context. Worker
видит `original_task` только как immutable reference; поручение задаёт task.
Результат предыдущего worker может
добавить ранее неизвестное значение, но не заменить уже заданный идентификатор
другой сущности: `filename` даёт `file_id`, но не определяет и не заменяет
`table_name`.

Сохраняй тип поиска из original_task:
- смысл/бизнес-смысл/описание/назначение/«наиболее вероятный» при неизвестном
  имени — смысловой поиск цельной естественной фразой;
- содержит/подстрока/фрагмент — буквальный поиск, только если фрагмент явно дан.
Не превращай смысловой поиск в «найти содержащие», набор слов, синонимов,
переводов или OR-вариантов.

Смысл поля ищется в каталогах колонок; при неизвестной роли — сразу в обоих.
Смысл таблицы — в каталогах таблиц, правило — в S2T. Семантический кандидат не
имеет S2T-роли: определи её только по найденной S2T-строке.

SQLite-каталоги хранят метаданные. Значения `table_name`, `source_table` и
`target_table` — логические ETL-объекты, а не SQLite-таблицы для физического SQL.
Для target-объекта читай атрибуты из `target_columns`, для source — из
`source_columns`; глобальную `s2t_transformations` не ограничивай `file_id`.
Планируй чтение только тех фактов, без которых нельзя получить запрошенный
результат. Если пользователь просит описать способ будущего действия, не выполняй
это действие вместо описания. Для вывода по сохранённому выражению сначала читай
само выражение; дополнительные данные запрашивай лишь когда они действительно
нужны исходной задаче.

Полный/transitive lineage `table.column` — одна task пути до
конечных endpoint с branches/subqueries/rules, без catalog/hops/assembly.
Сравнение: отдельная task для каждого endpoint с table/column/direction;
чужой result не вход, сравнит upstream.
Не создавай значения для неподтверждённых физических объектов и полей. Передавай
upstream только подтверждённые имена, а нехватку данных опиши явно.

Worker получает lazy-ссылки только прямых `depends_on`; перечисли все нужные
входы и не связывай независимые чтения ради порядка.

Если объект задан только бизнес-смыслом, отдельный worker может сначала получить
технические кандидаты из каталога, а следующий — найти эти кандидаты в S2T.
S2T-поиск по подстроке лексический, не семантический: не передавай ему русский
бизнес-термин, придуманный перевод или предполагаемое имя.

Сравнение, оценку, объяснение, вывод и оформление делает upstream: не создавай
для них tasks. Не выбирай tools/skills и не пиши task как вызов функции. При
reroute построй полный план по original_task и problem; прошлых результатов нет,
а problem не заменяет и не переопределяет явные идентификаторы original_task.

{_DOWNSTREAM_CAPABILITY_CONTEXT}

{_DOWNSTREAM_TABLE_CONTEXT}
""".strip()

_DOWNSTREAM_PLAN_REPAIR_PROMPT = f"""
Предыдущий native call `{_PLAN_TOOL_NAME}` нарушает схему или смысловой контракт.
Верни исправленный native call ровно один раз. Массив `steps` должен содержать
от 1 до {COORDINATOR_MAX_WORKERS} элементов; каждый элемент должен иметь
уникальный `id`, непустую `task` и `depends_on` с существующими ID. Удали
self-dependency, missing dependency и cycles. Независимые чтения не связывай.
Сохрани запрошенные роли, объекты, фильтры и результаты. Не придумывай
идентификаторы, функции, tools или требования. Используй только реальные таблицы
хранилища из system prompt; неизвестные бизнес-объекты оставляй текстом поиска.
Разрешение идентификатора одной сущности не заменяет идентификаторы другой;
явно известные значения повторяй дословно в каждой зависимой task.
Не добавляй анализ и оформление.

Причина отклонения: {{validation_error}}
""".strip()

_UPSTREAM_DATA_DECISION_PROMPT = f"""
Проверь `evidence` и обязательный `execution_manifest` для `original_task`.
Верни один native call `{_UPSTREAM_DATA_DECISION_TOOL_NAME}`. Выбери
`decision="pass"`, если конечный ответ обоснован, иначе `decision="reroute"`.

При `pass` передай в `selected_evidence_ids` только релевантный subset; если
evidence есть, он не пуст. При `reroute` список пуст, а непустой `problem`
описывает только недостающий факт/чтение: не предлагай имена таблиц, колонок или
значения вне входа. Не формируй пользовательский ответ и не выбирай display.

Evidence содержит ID, `producer_step_id`, tool, точные args, preview и признаки
truncation/display. Args подтверждают scope, preview — данные. Truncation и
`evidence_budget.overflow=true` не подтверждают полноту или пустой результат.
Manifest `failed`/`blocked_by_dependency` не является успехом; `partial`
подтверждает лишь имеющиеся evidence/results.

Каждый запрошенный результат сопоставь с прямым evidence. Для сравнения,
разности и производной метрики нужны evidence всех операндов; отсутствие
операнда не доказывает пустое множество или ноль. Значение одной метрики не
заменяет другую. Промежуточный список кандидатов не подтверждает связь, mapping
или lineage. Если нужен следующий источник, верни `reroute`.
""".strip()

_UPSTREAM_ANSWER_PROMPT = f"""
Ты upstream answer coordinator. Предварительная проверка уже вернула `pass`.
Вход содержит `original_task`, обязательный `execution_manifest` и принятые
`evidence`. Сам выполни запрошенный
анализ и верни ровно один native call `{_UPSTREAM_ANSWER_TOOL_NAME}` с готовым
`answer`. Вход уже содержит только выбранный decision subset evidence. Если он
не пуст, передай хотя бы один `used_evidence_id`; используй только этот subset.
Нужные `display_evidence_ids` также должны входить в used.

Evidence содержит `evidence_id`, `producer_step_id`, `tool_name`, точные `args`,
фактический `preview`, `truncated`, `preview_truncated` и `displayable`.
Аргументы подтверждают область чтения, preview — найденные данные. Не додумывай
отсутствующее; при `truncated=true` не утверждай полноту набора.
`evidence_budget.overflow=true` явно сообщает о не вошедших evidence.
`display_evidence_ids` выбирай
как `evidence_id` только у результатов с `displayable=true` и включай также в
`used_evidence_ids`.
Manifest содержит terminal status всех шагов. Не представляй failed/blocked
шаг как выполненный; на последнем цикле явно отрази связанную нехватку данных.

Если `answer` вводит физический идентификатор таблицы, поля или схемы, которого
нет в `original_task`, выбери подтверждающий его displayable evidence также в
`display_evidence_ids`. Если такого displayable evidence нет, не вводи этот
идентификатор как подтверждённый факт.

Перед `answer` сопоставь каждый запрошенный результат и его scope с прямым
подтверждением в evidence. Нельзя повторять значение одной метрики вместо
отсутствующей другой. Если данных всё же недостаточно, явно укажи это в ответе:
на этом линейном этапе возврата к чтению уже нет.

Для сравнения, разности множеств и производной метрики используй evidence всех
операндов; отсутствие одного из них не доказывает пустое множество или ноль.

Соблюдай запрошенный формат. Если буквальный компактный формат не задан, ответ
должен быть самодостаточным: подпиши смысл каждого значения и не возвращай
безымянную CSV-последовательность. Если пользователь потребовал «только»
конкретные элементы, не добавляй вступление и заключение. Для шаблона вида
`имя=<значение>` сохрани имя и знак `=` дословно. Не упоминай coordinators,
workers, tools, previews, result refs и внутреннюю схему.
""".strip()

_UPSTREAM_DATA_DECISION_REPAIR_PROMPT = f"""
Предыдущий native call решения о данных не соответствует схеме. Верни ровно один
`{_UPSTREAM_DATA_DECISION_TOOL_NAME}` с обязательным `decision`: `pass` или
`reroute`. Для `reroute` обязателен непустой bounded `problem`. Не формируй
ответ. Верни `selected_evidence_ids`: релевантный непустой subset доступных ID
для `pass` при наличии evidence либо пустой список для `reroute`.
""".strip()

_UPSTREAM_ANSWER_REPAIR_PROMPT = f"""
Предыдущий upstream answer call не соответствует схеме. Верни ровно один
`{_UPSTREAM_ANSWER_TOOL_NAME}` с обязательным `answer` и опциональными evidence
IDs. Используй только доступные evidence_id и не добавляй факты.
""".strip()


def _operation_skill_tool_schema() -> Dict[str, Any]:
    typed_sql_risk_aspects = _typed_sql_risk_aspects_enabled()
    sql_risk_aspects_schema: Dict[str, Any] = {
        "type": "array",
        "maxItems": 5 if typed_sql_risk_aspects else 0,
        "uniqueItems": True,
        "items": {
            "type": "string",
            "enum": [
                "row_filtering",
                "cardinality",
                "constraint_rejection",
                "value_changes",
                "write_semantics",
            ],
        },
        "description": (
            "Только явно нужные аспекты профиля Анализ SQL-рисков; "
            "при выборе этого профиля нужен хотя бы один аспект."
            if typed_sql_risk_aspects
            else (
                "E2 baseline: массив всегда пуст; полный legacy-профиль "
                "SQL-рисков загружается кодом."
            )
        ),
    }
    pipeline_values = ["agentic", "validation_protocol"]
    if _sql_risk_operation_scope_enabled():
        pipeline_values.append("sql_risk_scope")
    properties: Dict[str, Any] = {
        "pipeline": {
            "type": "string",
            "enum": pipeline_values,
            "description": (
                "Общий агентный поток или компиляция внешнего SQL test "
                "protocol"
                + (
                    "; также отдельный LLM-анализ риска, зависящего от "
                    "структуры transformation SQL. Прямое сравнение catalog "
                    "атрибутов колонок остаётся agentic"
                    if _sql_risk_operation_scope_enabled()
                    else ""
                )
                + "."
            ),
        },
        "skills": {
            "type": "array",
            "maxItems": len(OPERATION_SKILL_CATALOG),
            "items": {
                "type": "string",
                "enum": list(OPERATION_SKILL_CATALOG),
            },
            "description": (
                "Точные имена применимых профилей; пустой массив "
                "означает, что специальный профиль не нужен."
            ),
        },
    }
    required = ["pipeline", "skills"]
    if typed_sql_risk_aspects:
        properties["sql_risk_aspects"] = sql_risk_aspects_schema
        required.append("sql_risk_aspects")
    return {
        "type": "function",
        "function": {
            "name": _OPERATION_SKILL_TOOL_NAME,
            "description": (
                "Выбрать pipeline и применимые prompt-профили для всей операции."
            ),
            "parameters": {
                "type": "object",
                "properties": properties,
                "required": required,
                "additionalProperties": False,
            },
        },
    }


def _required_model_tool_schema(
    *,
    name: str,
    description: str,
    payload_model: type[BaseModel],
) -> Dict[str, Any]:
    """Expose one strict Pydantic payload as a required native tool."""

    def without_titles(value: Any) -> Any:
        if isinstance(value, Mapping):
            return {
                key: without_titles(item)
                for key, item in value.items()
                if key != "title"
            }
        if isinstance(value, list):
            return [without_titles(item) for item in value]
        return value

    parameters = without_titles(payload_model.model_json_schema())
    return {
        "type": "function",
        "function": {
            "name": name,
            "description": description,
            "parameters": parameters,
        },
    }


def _sql_risk_scope_extraction_tool_schema() -> Dict[str, Any]:
    """Return a flat native schema accepted by GigaChat function calling.

    Optional values are represented by absent keys. This avoids JSON Schema
    ``anyOf``/``$ref`` constructs without weakening Pydantic validation of the
    returned payload.
    """

    endpoint = {
        "type": "object",
        "properties": {
            "table_name": {
                "type": "string",
                "minLength": 1,
                "description": "Exact technical table name from original_task.",
            },
            "field_name": {
                "type": "string",
                "minLength": 1,
                "description": (
                    "Exact technical field name; omit for table-level mode."
                ),
            },
        },
        "required": ["table_name"],
        "additionalProperties": False,
    }
    attestation_endpoint = {
        "type": "object",
        "properties": {
            "table_name": {
                "type": "string",
                "minLength": 1,
                "description": (
                    "Exact table-name component copied verbatim from "
                    "original_task."
                ),
            },
            "field_name": {
                "type": "string",
                "minLength": 1,
                "description": (
                    "Exact field-name component copied verbatim from "
                    "original_task; omit for a table-level mode."
                ),
            },
        },
        "required": ["table_name"],
        "additionalProperties": False,
    }
    return {
        "type": "function",
        "function": {
            "name": SQL_RISK_SCOPE_EXTRACTION_TOOL_NAME,
            "description": (
                "Выбрать внутренний SQL-risk mode и дословно извлечь exact scope."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "execution_mode": {
                        "type": "string",
                        "enum": list(SQL_RISK_SCOPE_EXECUTION_MODES),
                    },
                    "source": endpoint,
                    "target": endpoint,
                    "file_id": {
                        "type": "integer",
                        "minimum": 1,
                        "description": (
                            "Literal positive file_id; omit when absent."
                        ),
                    },
                    "origin": {
                        "type": "object",
                        "properties": {
                            "source": attestation_endpoint,
                            "target": attestation_endpoint,
                            "file_id": {
                                "type": "string",
                                "minLength": 1,
                                "description": (
                                    "Verbatim decimal file_id token; omit when "
                                    "file_id is absent."
                                ),
                            },
                        },
                        "required": ["source", "target"],
                        "additionalProperties": False,
                    },
                },
                "required": [
                    "execution_mode",
                    "source",
                    "target",
                    "origin",
                ],
                "additionalProperties": False,
            },
        },
    }


def _sql_risk_assessment_tool_schema() -> Dict[str, Any]:
    return _required_model_tool_schema(
        name=SQL_RISK_ASSESSMENT_TOOL_NAME,
        description=(
            "Проанализировать exact evidence и нейтральную SQLGlot-структуру риска."
        ),
        payload_model=SqlRiskAssessment,
    )


def _validation_protocol_contract_tool_schema() -> Dict[str, Any]:
    key_schema = {
        "type": "array",
        "minItems": 1,
        "maxItems": 32,
        "uniqueItems": True,
        "items": {"type": "string", "minLength": 1, "maxLength": 300},
    }
    checks_schema = {
        "type": "array",
        "maxItems": len(PROTOCOL_CHECKS),
        "uniqueItems": True,
        "items": {"type": "string", "enum": list(PROTOCOL_CHECKS)},
    }
    return {
        "type": "function",
        "function": {
            "name": _VALIDATION_PROTOCOL_CONTRACT_TOOL_NAME,
            "description": "Извлечь строгий scope внешнего SQL test protocol.",
            "parameters": {
                "type": "object",
                "properties": {
                    "file_scope_kind": {
                        "type": "string",
                        "enum": ["not_provided", "file_id", "file_mention"],
                        "description": (
                            "Обязательное LLM-решение о наличии файлового "
                            "scope в original_task; not_provided допустим "
                            "только при его полном отсутствии."
                        ),
                    },
                    "file_id": {
                        "type": "integer",
                        "minimum": 1,
                        "description": "Только явно указанный file_id.",
                    },
                    "file_mention": {
                        "type": "string",
                        "minLength": 1,
                        "maxLength": 500,
                        "description": (
                            "Только дословное имя либо смысловое описание "
                            "файла, не source/target table mention; "
                            "канонизацию выполняет deterministic resolver."
                        ),
                    },
                    "mode": {
                        "type": "string",
                        "enum": ["explicit", "standard", "exhaustive"],
                    },
                    "requested_checks": checks_schema,
                    "explicit_key": key_schema,
                    "loads": {
                        "type": "array",
                        "minItems": 1,
                        "maxItems": MAX_PROTOCOL_OBJECTS,
                        "items": {
                            "type": "object",
                            "properties": {
                                "source_mentions": {
                                    "type": "array",
                                    "minItems": 1,
                                    "maxItems": MAX_PROTOCOL_OBJECTS,
                                    "uniqueItems": True,
                                    "items": {
                                        "type": "string",
                                        "minLength": 1,
                                        "maxLength": 300,
                                    },
                                    "description": (
                                        "Отдельные source-table mentions; "
                                        "не filename и не выражение со "
                                        "стрелкой."
                                    ),
                                },
                                "target_mention": {
                                    "type": "string",
                                    "minLength": 1,
                                    "maxLength": 300,
                                    "description": (
                                        "Один отдельный target-table mention; "
                                        "не filename и не выражение со "
                                        "стрелкой."
                                    ),
                                },
                                "requested_checks": checks_schema,
                                "explicit_key": key_schema,
                            },
                            "required": [
                                "source_mentions",
                                "target_mention",
                                "requested_checks",
                            ],
                            "additionalProperties": False,
                        },
                    },
                },
                "required": [
                    "file_scope_kind",
                    "mode",
                    "requested_checks",
                    "loads",
                ],
                "additionalProperties": False,
            },
        },
    }


def _plan_tool_schema() -> Dict[str, Any]:
    return {
        "type": "function",
        "function": {
            "name": _PLAN_TOOL_NAME,
            "description": (
                "Зафиксировать DAG готовых worker tasks."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "steps": {
                        "type": "array",
                        "minItems": 1,
                        "maxItems": COORDINATOR_MAX_WORKERS,
                        "description": (
                            "Необходимые чтения исходных данных; без отдельных "
                            "шагов производного анализа"
                        ),
                        "items": {
                            "type": "object",
                            "properties": {
                                "id": {
                                    "type": "string",
                                    "minLength": 1,
                                    "maxLength": 64,
                                    "pattern": (
                                        "^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$"
                                    ),
                                    "description": (
                                        "Уникальный стабильный ID шага."
                                    ),
                                },
                                "task": {
                                    "type": "string",
                                    "description": (
                                        "Готовая задача одного worker только "
                                        "на получение необходимых исходных "
                                        "данных; производный анализ выполняется "
                                        "upstream"
                                    ),
                                },
                                "depends_on": {
                                    "type": "array",
                                    "maxItems": COORDINATOR_MAX_WORKERS - 1,
                                    "uniqueItems": True,
                                    "items": {
                                        "type": "string",
                                        "minLength": 1,
                                        "maxLength": 64,
                                    },
                                    "description": (
                                        "ID прямых зависимостей; пусто для "
                                        "независимого шага."
                                    ),
                                },
                            },
                            "required": ["id", "task", "depends_on"],
                            "additionalProperties": False,
                        },
                    }
                },
                "required": ["steps"],
                "additionalProperties": False,
            },
        },
    }


def _upstream_answer_tool_schema() -> Dict[str, Any]:
    return {
        "type": "function",
        "function": {
            "name": _UPSTREAM_ANSWER_TOOL_NAME,
            "description": (
                "Вернуть готовый итоговый ответ по достаточным evidence."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "answer": {
                        "type": "string",
                        "description": "Непустой готовый пользовательский ответ.",
                    },
                    "used_evidence_ids": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Все evidence_id, использованные в ответе.",
                    },
                    "display_evidence_ids": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": (
                            "Подмножество used evidence для display; обязательно "
                            "включает evidence, подтверждающее новые физические "
                            "идентификаторы в answer."
                        ),
                    },
                },
                "required": ["answer"],
                "additionalProperties": False,
            },
        },
    }


def _upstream_data_decision_tool_schema() -> Dict[str, Any]:
    return {
        "type": "function",
        "function": {
            "name": _UPSTREAM_DATA_DECISION_TOOL_NAME,
            "description": (
                "Решить, перейти к upstream answer или повторить чтение данных."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "decision": {
                        "type": "string",
                        "enum": ["pass", "reroute"],
                        "description": (
                            "pass продолжает к ответу; reroute повторяет чтение."
                        ),
                    },
                    "problem": {
                        "type": "string",
                        "description": (
                            "Необязательное уточнение нехватки данных для нового плана."
                        ),
                    },
                    "selected_evidence_ids": {
                        "type": "array",
                        "uniqueItems": True,
                        "items": {"type": "string"},
                        "description": (
                            "Релевантные evidence_id для answer; пусто только "
                            "если evidence нет либо выбран reroute."
                        ),
                    },
                },
                "required": ["decision", "selected_evidence_ids"],
                "additionalProperties": False,
            },
        },
    }


def _native_payload(
    message: Any,
    tool_name: str,
    payload_model: type[BaseModel],
) -> BaseModel:
    message = recover_required_tool_call(message, tool_name)
    if not isinstance(message, AIMessage):
        raise CoordinatorResponseError(
            f"Coordinator ожидал AIMessage с native call {tool_name}."
        )
    if (
        len(message.tool_calls) != 1
        or message.tool_calls[0].get("name") != tool_name
    ):
        raise CoordinatorResponseError(
            f"Coordinator должен вернуть ровно один native call {tool_name}."
        )
    try:
        return payload_model.model_validate(
            message.tool_calls[0].get("args") or {}
        )
    except ValidationError as exc:
        details: List[str] = []
        for item in exc.errors():
            path = ".".join(str(part) for part in item.get("loc") or ())
            message_text = str(item.get("msg") or "validation failed")
            details.append(
                f"{path}: {message_text}" if path else message_text
            )
        detail_text = "; ".join(details)[:1200]
        raise CoordinatorResponseError(
            f"Coordinator вернул невалидную структуру {tool_name}: "
            + detail_text
        ) from exc
    except (TypeError, ValueError) as exc:
        raise CoordinatorResponseError(
            f"Coordinator вернул невалидную структуру {tool_name}: "
            + str(exc)[:1200]
        ) from exc


def _native_call_arguments(message: Any, tool_name: str) -> Mapping[str, Any]:
    """Return one native-call argument mapping without semantic validation."""

    message = recover_required_tool_call(message, tool_name)
    if not isinstance(message, AIMessage):
        raise CoordinatorResponseError(
            f"Coordinator ожидал AIMessage с native call {tool_name}."
        )
    if (
        len(message.tool_calls) != 1
        or message.tool_calls[0].get("name") != tool_name
    ):
        raise CoordinatorResponseError(
            f"Coordinator должен вернуть ровно один native call {tool_name}."
        )
    arguments = message.tool_calls[0].get("args")
    if not isinstance(arguments, Mapping):
        raise CoordinatorResponseError(
            f"Coordinator вернул не-object arguments для {tool_name}."
        )
    return arguments


def _native_upstream_decision(message: Any) -> UpstreamDecision:
    """Parse the data decision made before upstream answer generation."""
    decision = _native_payload(
        message,
        _UPSTREAM_DATA_DECISION_TOOL_NAME,
        UpstreamDecision,
    )
    assert isinstance(decision, UpstreamDecision)
    return decision


def _native_operation_route(message: Any) -> OperationSkillSelection:
    """Parse and validate the once-per-operation execution route."""
    selection = _native_payload(
        message,
        _OPERATION_SKILL_TOOL_NAME,
        OperationSkillSelection,
    )
    assert isinstance(selection, OperationSkillSelection)
    unknown = [
        name for name in selection.skills if name not in OPERATION_SKILL_CATALOG
    ]
    if unknown:
        raise CoordinatorResponseError(
            "Operation router выбрал неизвестные skills: "
            + ", ".join(dict.fromkeys(unknown))
        )
    if (
        selection.pipeline == "sql_risk_scope"
        and not _sql_risk_operation_scope_enabled()
    ):
        raise CoordinatorResponseError(
            "Operation router выбрал отключённый pipeline=sql_risk_scope."
        )
    return selection.model_copy(
        update={"skills": list(dict.fromkeys(selection.skills))}
    )


def _native_validation_protocol_contract(message: Any) -> RawTestProtocolContract:
    contract = _native_payload(
        message,
        _VALIDATION_PROTOCOL_CONTRACT_TOOL_NAME,
        RawTestProtocolContract,
    )
    assert isinstance(contract, RawTestProtocolContract)
    return contract


def _native_validation_contract_review(message: Any) -> ValidationContractReview:
    """Parse one model-owned semantic review without inspecting task text."""

    arguments = _native_call_arguments(
        message,
        VALIDATION_CONTRACT_REVIEW_TOOL_NAME,
    )
    validation = validate_validation_contract_review(arguments)
    if validation.status != "valid" or validation.review is None:
        details = "; ".join(
            f"{item.location}: {item.message}"
            for item in validation.errors
        )[:1200]
        raise CoordinatorResponseError(
            "Coordinator вернул невалидный review validation contract: "
            + (details or "unknown schema error")
        )
    return validation.review


def _validation_review_issue_text(review: ValidationContractReview) -> str:
    """Serialize only closed issue codes; never promote free text to repair."""

    return ", ".join(
        dict.fromkeys(item.code for item in review.issues)
    )


def _validation_contract_issue(error: Exception) -> Dict[str, Any]:
    """Turn a failed extraction into a stable no-fallback public state."""

    detail = str(error).strip()[:1200]
    lowered = detail.casefold()
    code = (
        "unsupported_check"
        if "requested_checks" in lowered
        and ("input should be" in lowered or "literal" in lowered)
        else "missing_parameter"
    )
    return {
        "code": code,
        "message": detail or "Строгий validation contract не сформирован.",
        "candidates": [],
    }


def _validation_contract_unavailable_issue(error: Exception) -> Dict[str, Any]:
    """Represent an unavailable extraction/review provider boundary safely."""

    detail = str(error).strip()[:1200]
    return {
        "code": "unavailable",
        "message": detail or "LLM review validation contract недоступен.",
        "candidates": [],
    }


def _render_validation_failure(
    status: str,
    issues: Sequence[Dict[str, Any]],
) -> str:
    """Render a machine-readable validation failure without an agentic retry."""

    titles = {
        "missing_parameter": "Не хватает обязательных параметров",
        "unsupported_check": "Запрошена неподдерживаемая проверка",
        "unresolved_entity": "Сущность не разрешена",
        "ambiguous_entity": "Сущность неоднозначна",
        "unavailable": "Validation contract недоступен",
    }
    lines = [
        "Тест-протокол не сформирован.",
        f"Статус: {status}.",
        titles.get(status, "Validation contract недоступен") + ".",
    ]
    for issue in issues:
        message = str(issue.get("message") or "").strip()
        candidates = [
            str(value)
            for value in issue.get("candidates", [])
            if str(value).strip()
        ]
        if message:
            lines.append(f"- {message}")
        if candidates:
            lines.append("  Кандидаты: " + ", ".join(candidates))
    if status == "ambiguous_entity":
        lines.append("Уточните один точный вариант из списка кандидатов.")
    return "\n".join(lines)


def _native_upstream_answer(message: Any) -> UpstreamOutput:
    """Parse the final answer after the data decision returned pass."""
    output = _native_payload(
        message,
        _UPSTREAM_ANSWER_TOOL_NAME,
        UpstreamOutput,
    )
    assert isinstance(output, UpstreamOutput)
    return output


def _repair_messages(
    base_messages: Sequence[BaseMessage],
    invalid_result: Any,
    repair_prompt: str,
) -> List[BaseMessage]:
    """Build provider-valid history after rejecting a native tool call."""
    messages = list(base_messages)
    if isinstance(invalid_result, AIMessage):
        tool_calls = list(invalid_result.tool_calls)
        call_ids = [
            str(call.get("id") or "").strip() for call in tool_calls
        ]
        if not tool_calls or all(call_ids):
            messages.append(invalid_result)
            for call, call_id in zip(tool_calls, call_ids):
                messages.append(
                    ToolMessage(
                        content=json.dumps(
                            {
                                "status": "rejected",
                                "reason": "native call failed validation",
                            },
                            ensure_ascii=False,
                        ),
                        tool_call_id=call_id,
                        name=str(call.get("name") or "invalid_call"),
                    )
                )
    messages.append(HumanMessage(content=repair_prompt))
    return messages


def select_operation_route(
    task: str,
    *,
    model: Any,
    callbacks: Optional[Sequence[Any]] = None,
    stable_context: str = "",
) -> OperationSkillSelection:
    """Select the pipeline once from the resolved task and stable context.

    This is the public front door shared by the top-level graph and the
    coordinator fallback. It classifies intent only through the structured LLM
    enum; downstream code never reclassifies natural-language wording. Stable
    context may clarify intent but is never used as identifier provenance.
    """

    callback_list = list(callbacks or [])
    model_config = {"callbacks": callback_list} if callback_list else None
    schema = _operation_skill_tool_schema()
    try:
        try:
            selected_model = model.bind_tools(
                [schema],
                tool_choice=_OPERATION_SKILL_TOOL_NAME,
            )
        except TypeError:
            selected_model = model.bind_tools([schema])
        messages: List[BaseMessage] = [
            SystemMessage(content=_operation_skill_prompt()),
            HumanMessage(
                content=json.dumps(
                    {
                        "original_task": str(task or "").strip(),
                        "stable_context": str(stable_context or "").strip(),
                    },
                    ensure_ascii=False,
                )
            ),
        ]
        with llm_stage("operation_router"):
            result = (
                selected_model.invoke(messages, config=model_config)
                if model_config is not None
                else selected_model.invoke(messages)
            )
    except Exception as exc:
        if isinstance(exc, CoordinatorResponseError):
            raise
        raise CoordinatorResponseError(
            f"Ошибка LLM coordinator: {type(exc).__name__}"
        ) from exc

    try:
        return _native_operation_route(result)
    except CoordinatorResponseError as first_error:
        logger.warning(
            "Operation route violated selection schema; requesting one "
            "LLM repair: %s",
            first_error,
        )
        repair_messages = _repair_messages(
            messages,
            result,
            _operation_skill_repair_prompt() + "\nОшибка: " + str(first_error),
        )
        try:
            with llm_stage("operation_router"):
                repaired = (
                    selected_model.invoke(repair_messages, config=model_config)
                    if model_config is not None
                    else selected_model.invoke(repair_messages)
                )
        except Exception as exc:
            raise CoordinatorResponseError(
                f"Ошибка LLM coordinator: {type(exc).__name__}"
            ) from exc
        return _native_operation_route(repaired)


async def select_operation_route_async(
    task: str,
    *,
    model: Any,
    callbacks: Optional[Sequence[Any]] = None,
    stable_context: str = "",
) -> OperationSkillSelection:
    """Async operation router with the same bounded repair contract."""

    callback_list = list(callbacks or [])
    model_config = {"callbacks": callback_list} if callback_list else None
    schema = _operation_skill_tool_schema()
    try:
        try:
            selected_model = model.bind_tools(
                [schema],
                tool_choice=_OPERATION_SKILL_TOOL_NAME,
            )
        except TypeError:
            selected_model = model.bind_tools([schema])
        messages: List[BaseMessage] = [
            SystemMessage(content=_operation_skill_prompt()),
            HumanMessage(
                content=json.dumps(
                    {
                        "original_task": str(task or "").strip(),
                        "stable_context": str(stable_context or "").strip(),
                    },
                    ensure_ascii=False,
                )
            ),
        ]
        with llm_stage("operation_router"):
            result = await ainvoke_compat(
                selected_model,
                messages,
                config=model_config,
                category="llm",
            )
    except Exception as exc:
        if isinstance(exc, CoordinatorResponseError):
            raise
        raise CoordinatorResponseError(
            f"Ошибка LLM coordinator: {type(exc).__name__}"
        ) from exc

    try:
        return _native_operation_route(result)
    except CoordinatorResponseError as first_error:
        logger.warning(
            "Operation route violated selection schema; requesting one "
            "LLM repair: %s",
            first_error,
        )
        repair_messages = _repair_messages(
            messages,
            result,
            _operation_skill_repair_prompt() + "\nОшибка: " + str(first_error),
        )
        try:
            with llm_stage("operation_router"):
                repaired = await ainvoke_compat(
                    selected_model,
                    repair_messages,
                    config=model_config,
                    category="llm",
                )
        except Exception as exc:
            raise CoordinatorResponseError(
                f"Ошибка LLM coordinator: {type(exc).__name__}"
            ) from exc
        return _native_operation_route(repaired)


def build_coordinator_graph(
    model: Any,
    *,
    callbacks: Optional[Sequence[Any]] = None,
    collected_display_refs: Optional[List[str]] = None,
):
    """Build downstream task flow and upstream result flow around workers."""
    callback_list = list(callbacks or [])
    model_config = {"callbacks": callback_list} if callback_list else None

    def bind_required_tool(schema: Dict[str, Any], tool_name: str) -> Any:
        try:
            return model.bind_tools([schema], tool_choice=tool_name)
        except TypeError:
            return model.bind_tools([schema])

    plan_model = bind_required_tool(_plan_tool_schema(), _PLAN_TOOL_NAME)
    upstream_data_decision_model = bind_required_tool(
        _upstream_data_decision_tool_schema(),
        _UPSTREAM_DATA_DECISION_TOOL_NAME,
    )
    upstream_answer_model = bind_required_tool(
        _upstream_answer_tool_schema(),
        _UPSTREAM_ANSWER_TOOL_NAME,
    )

    async def invoke(
        selected_model: Any,
        messages: Sequence[BaseMessage],
        *,
        stage: str,
    ) -> Any:
        try:
            with llm_stage(stage):
                return await ainvoke_compat(
                    selected_model,
                    messages,
                    config=model_config,
                    category="llm",
                )
        except Exception as exc:
            raise CoordinatorResponseError(
                f"Ошибка LLM coordinator: {type(exc).__name__}"
            ) from exc

    async def extract_sql_risk_scope(
        original_task: str,
        stable_context: str = "",
    ) -> tuple[SqlRiskScopeExtraction | None, List[str]]:
        """Run the internal mode/scope extractor with one bounded repair."""

        messages: List[BaseMessage] = [
            SystemMessage(content=SQL_RISK_SCOPE_EXTRACTION_PROMPT),
            HumanMessage(
                content=json.dumps(
                    {
                        "original_task": original_task,
                        "stable_context": stable_context,
                    },
                    ensure_ascii=False,
                )
            ),
        ]
        try:
            sql_risk_scope_extraction_model = bind_required_tool(
                _sql_risk_scope_extraction_tool_schema(),
                SQL_RISK_SCOPE_EXTRACTION_TOOL_NAME,
            )
        except Exception as exc:
            return None, [
                "scope extraction transport setup failed: "
                f"{type(exc).__name__}: {exc}"
            ]
        result: Any = None
        last_errors: List[str] = []
        for attempt in range(MAX_SQL_RISK_SCOPE_EXTRACTION_ATTEMPTS):
            try:
                result = await invoke(
                    sql_risk_scope_extraction_model,
                    messages,
                    stage="sql_risk_scope_contract",
                )
            except CoordinatorResponseError as exc:
                return None, [str(exc)]
            try:
                arguments = _native_call_arguments(
                    result,
                    SQL_RISK_SCOPE_EXTRACTION_TOOL_NAME,
                )
            except CoordinatorResponseError as error:
                validation = None
                last_errors = [str(error)]
            else:
                validation = validate_sql_risk_scope_extraction(
                    arguments,
                    original_task=original_task,
                )
                if validation.status == "valid":
                    assert validation.contract is not None
                    return validation.contract, []
                last_errors = [
                    f"{item.code}:{item.location}:{item.message}"
                    for item in validation.issues
                ]
            if attempt + 1 >= MAX_SQL_RISK_SCOPE_EXTRACTION_ATTEMPTS:
                break
            repair_prompt = (
                render_sql_risk_scope_extraction_repair(validation.issues)
                if validation is not None
                else (
                    "Предыдущий native call не прошёл fail-closed validation. "
                    "Верни ровно один исправленный call "
                    f"`{SQL_RISK_SCOPE_EXTRACTION_TOOL_NAME}`; не меняй intent, "
                    "не придумывай literals и не предлагай agentic fallback. "
                    "Ошибка: "
                    + "; ".join(last_errors)
                )
            )
            messages = _repair_messages(messages, result, repair_prompt)
        return None, last_errors

    async def assess_sql_risk_scope(context: Any) -> Any:
        """Run the bounded scope-analysis LLM and validate its provenance."""

        messages: List[BaseMessage] = [
            SystemMessage(content=SQL_RISK_ASSESSMENT_PROMPT),
            HumanMessage(content=render_sql_risk_assessment_request(context)),
        ]
        sql_risk_assessment_model = bind_required_tool(
            _sql_risk_assessment_tool_schema(),
            SQL_RISK_ASSESSMENT_TOOL_NAME,
        )
        result: Any = None
        validation: Any = None
        for attempt in range(MAX_SQL_RISK_ASSESSMENT_ATTEMPTS):
            result = await invoke(
                sql_risk_assessment_model,
                messages,
                stage="sql_risk_scope_analysis",
            )
            try:
                arguments = _native_call_arguments(
                    result,
                    SQL_RISK_ASSESSMENT_TOOL_NAME,
                )
            except CoordinatorResponseError as error:
                arguments = {"invalid_native_call": str(error)}
            validation = validate_sql_risk_assessment(
                arguments,
                context=context,
            )
            if validation.status == "valid":
                return validation
            if attempt + 1 >= MAX_SQL_RISK_ASSESSMENT_ATTEMPTS:
                break
            messages = _repair_messages(
                messages,
                result,
                render_sql_risk_assessment_repair(validation.issues),
            )
        return validation

    async def downstream_plan_node(
        state: CoordinatorGraphState,
    ) -> Dict[str, Any]:
        operation_skills = state.get("operation_skills")
        operation_sql_risk_aspects = (
            state.get("operation_sql_risk_aspects") or []
        )
        operation_pipeline = state.get("operation_pipeline")
        if operation_skills is None:
            operation_route = await select_operation_route_async(
                state["task"],
                model=model,
                callbacks=callback_list,
                stable_context=state["context"],
            )
            operation_skills = operation_route.skills
            operation_sql_risk_aspects = (
                operation_route.sql_risk_aspects
            )
            operation_pipeline = operation_route.pipeline
        if operation_pipeline is None:
            operation_pipeline = "agentic"
        if _SQL_RISK_OPERATION_SKILL in operation_skills:
            # Surface a bad experiment setting only for a selected SQL-risk
            # route; unrelated requests keep their default behavior.
            sql_risk_scope_evidence_enabled()

        if operation_pipeline == "sql_risk_scope":
            if not _sql_risk_operation_scope_enabled():
                raise CoordinatorResponseError(
                    "pipeline=sql_risk_scope отключён конфигурацией."
                )
            # This is a separate graph branch. Do not synthesize a worker plan
            # and do not load an operation-skill prompt profile.
            return {
                "operation_skills": [],
                "operation_sql_risk_aspects": [],
                "operation_pipeline": "sql_risk_scope",
                "plan": [],
                "next_step": 0,
            }

        if operation_pipeline == "validation_protocol":
            protocol_display_refs: List[str] = []
            protocol_reader_results: List[Dict[str, Any]] = []
            protocol_trace: Dict[str, Any]
            contract_review_trace: List[Dict[str, Any]] = []
            protocol_payload = {"original_task": state["task"]}
            protocol_messages: List[BaseMessage] = [
                SystemMessage(content=_VALIDATION_PROTOCOL_CONTRACT_PROMPT),
                HumanMessage(
                    content=json.dumps(protocol_payload, ensure_ascii=False)
                ),
            ]
            raw_protocol_contract: Optional[RawTestProtocolContract] = None
            extraction_issue: Optional[Dict[str, Any]] = None
            try:
                protocol_contract_model = bind_required_tool(
                    _validation_protocol_contract_tool_schema(),
                    _VALIDATION_PROTOCOL_CONTRACT_TOOL_NAME,
                )
                protocol_review_model = bind_required_tool(
                    validation_contract_review_tool_schema(),
                    VALIDATION_CONTRACT_REVIEW_TOOL_NAME,
                )
            except Exception as bind_error:
                extraction_issue = _validation_contract_unavailable_issue(
                    bind_error
                )
            else:
                extraction_messages = protocol_messages
                for extraction_attempt in range(2):
                    try:
                        protocol_result = await invoke(
                            protocol_contract_model,
                            extraction_messages,
                            stage="validation_protocol_contract",
                        )
                    except CoordinatorResponseError as invoke_error:
                        extraction_issue = (
                            _validation_contract_unavailable_issue(invoke_error)
                        )
                        break

                    try:
                        candidate_contract = (
                            _native_validation_protocol_contract(
                                protocol_result
                            )
                        )
                        validate_raw_contract_origin(
                            candidate_contract,
                            state["task"],
                        )
                    except (CoordinatorResponseError, ValueError) as error:
                        if extraction_attempt == 0:
                            logger.warning(
                                "Typed test protocol contract was invalid; "
                                "requesting one LLM repair: %s",
                                error,
                            )
                            extraction_messages = _repair_messages(
                                protocol_messages,
                                protocol_result,
                                _VALIDATION_PROTOCOL_CONTRACT_REPAIR_PROMPT
                                + "\nОшибка schema/origin: "
                                + str(error),
                            )
                            continue
                        extraction_issue = _validation_contract_issue(error)
                        break

                    try:
                        review_messages: List[BaseMessage] = [
                            SystemMessage(
                                content=VALIDATION_CONTRACT_REVIEW_PROMPT
                            ),
                            HumanMessage(
                                content=(
                                    render_validation_contract_review_request(
                                        state["task"],
                                        candidate_contract,
                                    )
                                )
                            ),
                        ]
                        review_result = await invoke(
                            protocol_review_model,
                            review_messages,
                            stage="validation_protocol_contract_review",
                        )
                        contract_review = _native_validation_contract_review(
                            review_result
                        )
                    except (CoordinatorResponseError, ValueError) as error:
                        contract_review_trace.append(
                            {
                                "attempt": extraction_attempt + 1,
                                "status": "unavailable",
                                "error": str(error)[:1200],
                            }
                        )
                        extraction_issue = (
                            _validation_contract_unavailable_issue(error)
                        )
                        break

                    review_entry = {
                        "attempt": extraction_attempt + 1,
                        "status": contract_review.decision,
                        "issues": [
                            item.model_dump(mode="json")
                            for item in contract_review.issues
                        ],
                    }
                    contract_review_trace.append(review_entry)
                    if contract_review.decision == "accept":
                        raw_protocol_contract = candidate_contract
                        break

                    review_issue_text = _validation_review_issue_text(
                        contract_review
                    )
                    if extraction_attempt == 0:
                        logger.warning(
                            "Model-owned validation contract review requested "
                            "one extraction repair: %s",
                            review_issue_text,
                        )
                        extraction_messages = _repair_messages(
                            protocol_messages,
                            protocol_result,
                            _VALIDATION_PROTOCOL_CONTRACT_REPAIR_PROMPT
                            + "\nReview полноты/ролей: "
                            + review_issue_text,
                        )
                        continue

                    extraction_issue = _validation_contract_issue(
                        ValueError(
                            "Model-owned review повторно отклонил validation "
                            "contract: "
                            + review_issue_text
                        )
                    )
                    break

            if raw_protocol_contract is None:
                assert extraction_issue is not None
                failure_status = str(extraction_issue["code"])
                failure_issues = [extraction_issue]
                answer = _render_validation_failure(
                    failure_status,
                    failure_issues,
                )
                protocol_trace = {
                    "mode": None,
                    "status": failure_status,
                    "issues": failure_issues,
                    "phases": [],
                    "targets": [],
                    "reader_calls": [],
                    "silent_fallback": False,
                }
            else:
                try:
                    resolution = await run_sync_compat(
                        resolve_test_protocol_contract,
                        raw_protocol_contract,
                        callbacks=callback_list,
                    )
                except Exception as exc:
                    logger.warning(
                        "Test protocol entity resolution failed safely: %s",
                        exc,
                    )
                    failure_status = "unresolved_entity"
                    failure_issues = [
                        {
                            "code": failure_status,
                            "message": (
                                "Entity resolution недоступен: "
                                f"{type(exc).__name__}."
                            ),
                            "candidates": [],
                        }
                    ]
                    answer = _render_validation_failure(
                        failure_status,
                        failure_issues,
                    )
                    protocol_trace = {
                        "mode": raw_protocol_contract.mode,
                        "status": failure_status,
                        "issues": failure_issues,
                        "phases": [],
                        "targets": [],
                        "reader_calls": [],
                        "silent_fallback": False,
                    }
                else:
                    record_entity_resolution(
                        [
                            {
                                **item.model_dump(mode="json"),
                                "resolver_invoked": item.method != "exact",
                            }
                            for item in resolution.resolutions
                        ]
                    )
                    if resolution.status != "resolved":
                        failure_issues = [
                            item.model_dump(mode="json")
                            for item in resolution.issues
                        ]
                        answer = _render_validation_failure(
                            resolution.status,
                            failure_issues,
                        )
                        protocol_trace = {
                            "mode": raw_protocol_contract.mode,
                            "status": resolution.status,
                            "issues": failure_issues,
                            "phases": [],
                            "targets": [],
                            "reader_calls": [],
                            "exact_bypass_count": (
                                resolution.exact_bypass_count
                            ),
                            "silent_fallback": False,
                        }
                    else:
                        protocol_contract = resolution.contract
                        assert protocol_contract is not None
                        protocol_reader_results = await run_sync_compat(
                            read_test_protocol_inputs,
                            protocol_contract,
                            callbacks=callback_list,
                        )
                        compiled_protocol = await run_sync_compat(
                            compile_test_protocol,
                            protocol_contract,
                            reader_results=protocol_reader_results,
                        )
                        answer = render_test_protocol_answer(
                            protocol_contract,
                            compiled_protocol,
                        )
                        protocol_display_refs = register_worker_display_items(
                            [
                                WorkerDisplayItem(**item)
                                for item in build_test_protocol_display_payloads(
                                    compiled_protocol
                                )
                            ]
                        )
                        if collected_display_refs is not None:
                            collected_display_refs.extend(protocol_display_refs)
                        protocol_trace = compiled_protocol.model_dump(mode="json")
                        protocol_trace.update(
                            {
                                "contract": protocol_contract.model_dump(
                                    mode="json"
                                ),
                                "reader_calls": [
                                    {
                                        key: item.get(key)
                                        for key in (
                                            "kind",
                                            "tool_name",
                                            "args",
                                            "error",
                                        )
                                        if item.get(key) is not None
                                    }
                                    for item in protocol_reader_results
                                ],
                                "exact_bypass_count": (
                                    resolution.exact_bypass_count
                                ),
                                "silent_fallback": False,
                            }
                        )

            protocol_trace["contract_reviews"] = contract_review_trace
            record_validation_protocol(protocol_trace)
            direct_plan = [
                {
                    "cycle": state["cycle"],
                    "step": index,
                    "task": task,
                    "operation_skills": list(operation_skills),
                    "sql_risk_aspects": list(
                        operation_sql_risk_aspects
                    ),
                    "pipeline": "validation_protocol",
                }
                for index, task in enumerate(
                    (
                        "Извлечь RawTestProtocolContract из запроса.",
                        "Проверить полноту и роли contract отдельным "
                        "model-owned review.",
                        "Разрешить неподтверждённые сущности и прочитать "
                        "только зависимости checks.",
                        "Выполнить static preflight и скомпилировать Phase 0–3.",
                    ),
                    start=1,
                )
            ]
            record_coordinator_plan(direct_plan)
            direct_output = {
                "answer": answer,
                "pipeline": "validation_protocol",
                "protocol_status": protocol_trace.get("status"),
            }
            record_upstream_output(direct_output)
            return {
                "operation_skills": list(operation_skills),
                "operation_sql_risk_aspects": list(
                    operation_sql_risk_aspects
                ),
                "operation_pipeline": "validation_protocol",
                "plan": [],
                "next_step": 0,
                "upstream_output": direct_output,
                "final_answer": answer,
                "selected_display_refs": protocol_display_refs,
            }

        plan_operation_context = load_operation_skills(
            operation_skills,
            stage="plan",
            sql_risk_aspects=operation_sql_risk_aspects,
        )
        sql_risk_protocol_attestation = _sql_risk_protocol_attestation(
            operation_skills,
            operation_sql_risk_aspects,
        )

        plan_payload: Dict[str, Any] = {
            "original_task": state["task"],
            "context": state["context"],
        }
        if state["upstream_problem"] is not None:
            plan_payload["problem"] = state["upstream_problem"]
        plan_messages: List[BaseMessage] = [
            SystemMessage(
                content="\n\n".join(
                    part
                    for part in (
                        _DOWNSTREAM_PLAN_PROMPT,
                        plan_operation_context,
                    )
                    if part
                )
            ),
            HumanMessage(
                content=json.dumps(
                    plan_payload,
                    ensure_ascii=False,
                )
            ),
        ]
        plan_result = await invoke(
            plan_model,
            plan_messages,
            stage="downstream_plan",
        )
        try:
            submitted_plan = _native_payload(
                plan_result,
                _PLAN_TOOL_NAME,
                SubmittedWorkerPlan,
            )
        except CoordinatorResponseError as first_error:
            logger.warning(
                "Coordinator plan call violated plan schema; requesting "
                "one LLM repair: %s",
                first_error,
            )
            repaired_result = await invoke(
                plan_model,
                _repair_messages(
                    plan_messages,
                    plan_result,
                    _DOWNSTREAM_PLAN_REPAIR_PROMPT.replace(
                        "{validation_error}",
                        str(first_error),
                    ),
                ),
                stage="downstream_plan",
            )
            submitted_plan = _native_payload(
                repaired_result,
                _PLAN_TOOL_NAME,
                SubmittedWorkerPlan,
            )
            assert isinstance(submitted_plan, SubmittedWorkerPlan)
        assert isinstance(submitted_plan, SubmittedWorkerPlan)
        plan = submitted_plan.to_worker_plan()
        recorded_plan = [
            {
                "cycle": state["cycle"],
                "step": index,
                "plan_origin": plan.plan_origin,
                **step.model_dump(mode="json", exclude_none=True),
                "operation_skills": list(operation_skills),
                "sql_risk_aspects": list(operation_sql_risk_aspects),
                "pipeline": operation_pipeline,
                **sql_risk_protocol_attestation,
            }
            for index, step in enumerate(plan.steps, start=1)
        ]
        logger.info(
            "Coordinator planned worker_steps=%s plan=%s",
            len(plan.steps),
            json.dumps(recorded_plan, ensure_ascii=False),
        )
        record_coordinator_plan(recorded_plan)
        return {
            "operation_skills": list(operation_skills),
            "operation_sql_risk_aspects": list(
                operation_sql_risk_aspects
            ),
            "operation_pipeline": operation_pipeline,
            "plan": [step.model_dump() for step in plan.steps],
            "next_step": 0,
        }

    async def sql_risk_scope_node(
        state: CoordinatorGraphState,
    ) -> Dict[str, Any]:
        """Extract, read, structure and assess one closed SQL-risk scope."""

        extraction, extraction_errors = await extract_sql_risk_scope(
            state["task"],
            state["context"],
        )
        if extraction is None:
            result = SqlRiskOperationPipelineResult(
                status="unavailable",
                execution_mode="unresolved",
                scope="unresolved model-owned scope",
                answer=(
                    "Оценка SQL-риска недоступна: внутренний typed LLM-контракт "
                    "режима и exact source → target scope не прошёл проверку. "
                    "Agentic fallback не выполнялся."
                ),
                answer_source="sql_risk_scope_unavailable",
                issues=[
                    SqlRiskOperationIssue(
                        code="invalid_contract",
                        message=(
                            "; ".join(extraction_errors)[:600]
                            or "Model-owned scope extraction remained invalid."
                        ),
                    )
                ],
            )
        else:
            contract = build_sql_risk_scope_contract(extraction)
            event_loop = asyncio.get_running_loop()

            def run_pipeline() -> SqlRiskOperationPipelineResult:
                def assess_from_reader_thread(context: Any) -> Any:
                    future = asyncio.run_coroutine_threadsafe(
                        assess_sql_risk_scope(context),
                        event_loop,
                    )
                    return future.result()

                return run_sql_risk_operation_pipeline(
                    contract,
                    original_task=state["task"],
                    stable_context=state["context"],
                    assessment_runner=assess_from_reader_thread,
                    callbacks=callback_list,
                    evidence_namespace=f"cycle-{state['cycle']}",
                )

            result = await run_sync_compat(run_pipeline)

        operation_trace = {
            **result.metrics_payload(),
            "silent_fallback": False,
        }
        record_sql_risk_operation(operation_trace)
        selected_display_refs = register_worker_display_items(
            result.display_items
        )
        if collected_display_refs is not None:
            collected_display_refs.extend(selected_display_refs)
        upstream_output = {
            "answer": result.answer,
            "used_evidence_ids": list(result.used_evidence_ids),
            "display_evidence_ids": list(result.display_evidence_ids),
            "answer_source": result.answer_source,
            "pipeline": "sql_risk_scope",
            "status": result.status,
        }
        record_upstream_output(upstream_output)
        return {
            "upstream_output": upstream_output,
            "final_answer": result.answer,
            "selected_display_refs": selected_display_refs,
        }

    async def worker_node(
        state: CoordinatorGraphState,
    ) -> Dict[str, Any]:
        plan = WorkerPlan.model_validate({"steps": state["plan"]})
        selected_operation_skills = state.get("operation_skills") or []
        selected_sql_risk_aspects = (
            state.get("operation_sql_risk_aspects") or []
        )
        planner_context = load_operation_skills(
            selected_operation_skills,
            stage="planner",
            sql_risk_aspects=selected_sql_risk_aspects,
        )
        observer_context = load_operation_skills(
            selected_operation_skills,
            stage="observer",
            sql_risk_aspects=selected_sql_risk_aspects,
        )
        runs = await _execute_worker_plan_dag(
            plan,
            cycle=state["cycle"],
            original_task=state["task"],
            planner_context=planner_context,
            observer_context=observer_context,
            collected_display_refs=collected_display_refs,
        )
        return {
            "worker_runs": [*state["worker_runs"], *runs],
            "next_step": len(plan.steps),
        }

    def validate_upstream_decision(
        message: Any,
        *,
        can_reroute: bool,
        available_evidence_ids: set[str],
    ) -> UpstreamDecision:
        decision = _native_upstream_decision(message)
        if decision.decision == "reroute" and not can_reroute:
            raise CoordinatorResponseError(
                "На последнем цикле data decision должен быть pass."
            )
        unknown_ids = sorted(
            set(decision.selected_evidence_ids)
            - available_evidence_ids
        )
        if unknown_ids:
            raise CoordinatorResponseError(
                "Upstream data decision выбрал неизвестные evidence_id: "
                + ", ".join(unknown_ids)
            )
        if (
            decision.decision == "pass"
            and available_evidence_ids
            and not decision.selected_evidence_ids
        ):
            raise CoordinatorResponseError(
                "Data-backed pass обязан выбрать хотя бы один "
                "selected_evidence_id."
            )
        return decision

    def validate_upstream_answer(
        message: Any,
        *,
        available_evidence_ids: set[str],
        available_display_refs: Dict[str, str],
        require_used_evidence: bool = False,
    ) -> UpstreamOutput:
        output = _native_upstream_answer(message)
        unknown_ids = sorted(
            (
                set(output.used_evidence_ids)
                | set(output.display_evidence_ids)
            )
            - available_evidence_ids
        )
        undisplayable_ids = sorted(
            set(output.display_evidence_ids) - set(available_display_refs)
        )
        if unknown_ids or undisplayable_ids:
            raise CoordinatorResponseError(
                "Upstream coordinator выбрал неизвестные evidence_id: "
                + ", ".join([*unknown_ids, *undisplayable_ids])
            )
        if (
            require_used_evidence
            and available_evidence_ids
            and not output.used_evidence_ids
        ):
            raise CoordinatorResponseError(
                "Data-backed answer обязан сослаться хотя бы на "
                "один доступный used_evidence_id."
            )
        return output

    async def upstream_node(
        state: CoordinatorGraphState,
    ) -> Dict[str, Any]:
        selected_operation_skills = state.get("operation_skills") or []
        selected_sql_risk_aspects = (
            state.get("operation_sql_risk_aspects") or []
        )
        decision_context = load_operation_skills(
            selected_operation_skills,
            stage="upstream_decision",
            sql_risk_aspects=selected_sql_risk_aspects,
        )
        analysis_context = load_operation_skills(
            selected_operation_skills,
            stage="upstream",
            sql_risk_aspects=selected_sql_risk_aspects,
        )
        execution_manifest = _worker_run_manifest(state["worker_runs"])
        evidence_bundle = _build_upstream_evidence_bundle(
            state["worker_runs"]
        )
        evidence_payload = list(evidence_bundle["evidence"])
        evidence_by_id = {
            item["evidence_id"]: item for item in evidence_payload
        }
        available_evidence_ids = set(evidence_by_id)
        available_display_refs: Dict[str, str] = {}
        for run in state["worker_runs"]:
            outcome = run["outcome"]
            if outcome is None:
                continue
            for artifact in outcome.evidence:
                if (
                    artifact.evidence_id in available_evidence_ids
                    and artifact.display_ref is not None
                ):
                    available_display_refs[
                        artifact.evidence_id
                    ] = artifact.display_ref
        upstream_payload = {
            "original_task": state["task"],
            "evidence": evidence_payload,
            "evidence_budget": evidence_bundle["evidence_budget"],
            "evidence_lineage": evidence_bundle["evidence_lineage"],
            "execution_manifest": execution_manifest,
        }
        decision_messages: List[BaseMessage] = [
            SystemMessage(
                content="\n\n".join(
                    part
                    for part in (
                        _UPSTREAM_DATA_DECISION_PROMPT,
                        (
                            "Разрешён ещё один полный цикл чтения: при "
                            "нехватке данных верни decision=reroute."
                            if state["cycle"] < COORDINATOR_MAX_CYCLES
                            else (
                                "Это последний цикл: верни decision=pass. "
                                "Возможную нехватку данных кратко укажи в problem."
                            )
                        ),
                        decision_context,
                    )
                    if part
                )
            ),
            HumanMessage(
                content=json.dumps(
                    upstream_payload,
                    ensure_ascii=False,
                )
            ),
        ]
        async def invoke_decision(
            messages: Sequence[BaseMessage],
        ) -> tuple[Any, UpstreamDecision]:
            can_reroute = state["cycle"] < COORDINATOR_MAX_CYCLES
            result = await invoke(
                upstream_data_decision_model,
                messages,
                stage="upstream",
            )
            try:
                decision = validate_upstream_decision(
                    result,
                    can_reroute=can_reroute,
                    available_evidence_ids=available_evidence_ids,
                )
            except CoordinatorResponseError as first_error:
                logger.warning(
                    "Upstream data decision violated schema; "
                    "requesting one LLM repair: %s",
                    first_error,
                )
                result = await invoke(
                    upstream_data_decision_model,
                    _repair_messages(
                        messages,
                        result,
                        _UPSTREAM_DATA_DECISION_REPAIR_PROMPT
                        + "\nОшибка: "
                        + str(first_error),
                    ),
                    stage="upstream",
                )
                decision = validate_upstream_decision(
                    result,
                    can_reroute=can_reroute,
                    available_evidence_ids=available_evidence_ids,
                )
            return result, decision

        async def invoke_answer(
            messages: Sequence[BaseMessage],
            *,
            selected_evidence_ids: set[str],
            selected_display_refs: Dict[str, str],
        ) -> tuple[Any, UpstreamOutput]:
            evidence_context = (
                "\nДоступные used_evidence_ids (копируй дословно): "
                + json.dumps(
                    sorted(selected_evidence_ids),
                    ensure_ascii=False,
                )
                + "\nДоступные display_evidence_ids: "
                + json.dumps(
                    sorted(selected_display_refs),
                    ensure_ascii=False,
                )
            )
            result = await invoke(
                upstream_answer_model,
                messages,
                stage="upstream",
            )
            try:
                output = validate_upstream_answer(
                    result,
                    available_evidence_ids=selected_evidence_ids,
                    available_display_refs=selected_display_refs,
                    require_used_evidence=bool(selected_evidence_ids),
                )
            except CoordinatorResponseError as first_error:
                logger.warning(
                    "Upstream answer violated schema; requesting one LLM "
                    "repair: %s",
                    first_error,
                )
                result = await invoke(
                    upstream_answer_model,
                    _repair_messages(
                        messages,
                        result,
                        _UPSTREAM_ANSWER_REPAIR_PROMPT
                        + "\nОшибка: "
                        + str(first_error)
                        + evidence_context,
                    ),
                    stage="upstream",
                )
                output = validate_upstream_answer(
                    result,
                    available_evidence_ids=selected_evidence_ids,
                    available_display_refs=selected_display_refs,
                    require_used_evidence=bool(selected_evidence_ids),
                )
            return result, output

        def data_request_update(problem: str) -> Dict[str, Any]:
            if state["cycle"] >= COORDINATOR_MAX_CYCLES:
                raise CoordinatorResponseError(
                    "Последний upstream-цикл не может запросить новые данные."
                )
            logger.info(
                "Upstream requests clean data cycle=%s problem=%s",
                state["cycle"] + 1,
                problem,
            )
            return {
                "cycle": state["cycle"] + 1,
                "plan": [],
                "next_step": 0,
                "worker_runs": [],
                "upstream_problem": problem,
                "upstream_output": None,
                "final_answer": None,
                "selected_display_refs": [],
            }

        _, decision = await invoke_decision(decision_messages)
        if decision.decision == "reroute":
            return data_request_update(decision.problem)

        selected_ids = set(decision.selected_evidence_ids)
        selected_display_ref_map = {
            evidence_id: display_ref
            for evidence_id, display_ref in available_display_refs.items()
            if evidence_id in selected_ids
        }
        answer_payload = {
            "original_task": state["task"],
            "evidence": [
                evidence_by_id[evidence_id]
                for evidence_id in decision.selected_evidence_ids
            ],
            "evidence_budget": evidence_bundle["evidence_budget"],
            "evidence_lineage": [
                item
                for item in evidence_bundle["evidence_lineage"]
                if set(item["source_evidence_ids"]) & selected_ids
            ],
            "execution_manifest": execution_manifest,
        }
        if decision.problem:
            answer_payload["data_problem"] = decision.problem
        incomplete_steps = [
            item
            for item in execution_manifest
            if item["status"] in {
                "partial",
                "failed",
                "blocked_by_dependency",
            }
        ]
        forced_final_problem = ""
        if (
            state["cycle"] >= COORDINATOR_MAX_CYCLES
            and incomplete_steps
        ):
            forced_final_problem = (
                "Не все чтения дали полный результат: "
                + ", ".join(
                    f"{item['step_id']} ({item['status']})"
                    for item in incomplete_steps
                )
                + "."
            )
            answer_payload.setdefault("data_problem", forced_final_problem)
        answer_messages: List[BaseMessage] = [
            SystemMessage(
                content="\n\n".join(
                    part
                    for part in (
                        _UPSTREAM_ANSWER_PROMPT,
                        _UPSTREAM_ANALYSIS_CONTEXT,
                        analysis_context,
                    )
                    if part
                )
            ),
            HumanMessage(
                content=json.dumps(answer_payload, ensure_ascii=False)
            ),
        ]
        _, evidence = await invoke_answer(
            answer_messages,
            selected_evidence_ids=selected_ids,
            selected_display_refs=selected_display_ref_map,
        )

        final_answer = evidence.answer
        if (
            forced_final_problem
            and forced_final_problem.casefold() not in final_answer.casefold()
        ):
            final_answer = f"{final_answer}\n\n{forced_final_problem}"
        upstream_output = {
            **evidence.model_dump(),
            "answer": final_answer,
        }
        selected_display_refs = [
            selected_display_ref_map[evidence_id]
            for evidence_id in evidence.display_evidence_ids
        ]
        record_upstream_output(
            {**upstream_output, "answer_source": "model"}
        )
        logger.info(
            "Upstream coordinator result: %s",
            json.dumps(upstream_output, ensure_ascii=False)[:8000],
        )
        return {
            "upstream_output": upstream_output,
            "final_answer": final_answer,
            "selected_display_refs": selected_display_refs,
        }

    def route_after_worker(
        state: CoordinatorGraphState,
    ) -> Literal["worker", "upstream"]:
        return "upstream"

    def route_after_downstream(
        state: CoordinatorGraphState,
    ) -> Literal["worker", "sql_risk_scope", "end"]:
        if str(state.get("final_answer") or "").strip():
            return "end"
        if state.get("operation_pipeline") == "sql_risk_scope":
            return "sql_risk_scope"
        return "worker"

    def route_after_upstream(
        state: CoordinatorGraphState,
    ) -> Literal["downstream_plan", "end"]:
        if str(state.get("final_answer") or "").strip():
            return "end"
        if state.get("upstream_problem") is not None:
            return "downstream_plan"
        raise CoordinatorResponseError(
            "Upstream не вернул ни ответ, ни запрос дополнительных данных."
        )

    graph = StateGraph(CoordinatorGraphState)
    graph.add_node("downstream_plan", downstream_plan_node)
    graph.add_node("sql_risk_scope", sql_risk_scope_node)
    graph.add_node("worker", worker_node)
    graph.add_node("upstream", upstream_node)
    graph.add_edge(START, "downstream_plan")
    graph.add_conditional_edges(
        "downstream_plan",
        route_after_downstream,
        {
            "worker": "worker",
            "sql_risk_scope": "sql_risk_scope",
            "end": END,
        },
    )
    graph.add_edge("sql_risk_scope", END)
    graph.add_conditional_edges(
        "worker",
        route_after_worker,
        {
            "worker": "worker",
            "upstream": "upstream",
        },
    )
    graph.add_conditional_edges(
        "upstream",
        route_after_upstream,
        {
            "downstream_plan": "downstream_plan",
            "end": END,
        },
    )
    return graph.compile()


async def coordinator_chat_async(
    task: str,
    *,
    context: str = "",
) -> CoordinatorAnswer:
    """Send tasks downstream to workers and return verified results upstream."""
    clean_task = str(task or "").strip()
    clean_context = str(context or "").strip()[:COORDINATOR_CONTEXT_MAX_CHARS]
    if not clean_task:
        return CoordinatorAnswer(
            answer="Задача coordinator не должна быть пустой.",
            display_refs=[],
        )

    callback = get_callback_handler()
    callbacks = [callback] if callback is not None else []
    metrics_callback = get_run_metrics_callback()
    if metrics_callback is not None and metrics_callback not in callbacks:
        callbacks.append(metrics_callback)

    collected_display_refs: List[str] = []
    graph = build_coordinator_graph(
        chat_model,
        callbacks=callbacks,
        collected_display_refs=collected_display_refs,
    )
    initial_state: CoordinatorGraphState = {
        "task": clean_task,
        "context": clean_context,
        "operation_skills": None,
        "operation_sql_risk_aspects": None,
        "operation_pipeline": None,
        "cycle": 1,
        "plan": [],
        "next_step": 0,
        "worker_runs": [],
        "upstream_problem": None,
        "upstream_output": None,
        "final_answer": None,
        "selected_display_refs": [],
    }
    config = {
        "recursion_limit": (
            COORDINATOR_MAX_CYCLES * (COORDINATOR_MAX_WORKERS + 2) + 5
        ),
        "run_name": "worker_coordinator",
    }

    async with _coordinator_runtime_scope():
        try:
            final_state = await ainvoke_graph_compat(
                graph,
                initial_state,
                config=config,
            )
            final_answer = str(final_state.get("final_answer") or "").strip()
            if not final_answer:
                raise CoordinatorResponseError(
                    "Coordinator LangGraph завершился без ответа."
                )
            selected_refs = list(final_state.get("selected_display_refs") or [])
            selected_set = set(selected_refs)
            unselected_refs = [
                ref for ref in collected_display_refs if ref not in selected_set
            ]
            if unselected_refs:
                discard_worker_display_refs(unselected_refs)
            return CoordinatorAnswer(
                answer=final_answer,
                display_refs=selected_refs,
            )
        except BaseException:
            if collected_display_refs:
                discard_worker_display_refs(collected_display_refs)
            raise


def coordinator_chat(
    task: str,
    *,
    context: str = "",
) -> CoordinatorAnswer:
    """Compatibility facade for non-ASGI callers."""

    return run_coroutine_sync(
        coordinator_chat_async(task, context=context)
    )


__all__ = [
    "COORDINATOR_MAX_CYCLES",
    "COORDINATOR_MAX_WORKERS",
    "COORDINATOR_CONTEXT_MAX_CHARS",
    "PlanStep",
    "UpstreamOutput",
    "UpstreamDecision",
    "CoordinatorAnswer",
    "CoordinatorGraphState",
    "CoordinatorResponseError",
    "OperationSkillSelection",
    "WorkerPlan",
    "build_coordinator_graph",
    "coordinator_chat",
    "coordinator_chat_async",
    "select_operation_route",
    "select_operation_route_async",
]
