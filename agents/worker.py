"""Isolated generic read-only worker experiment.

The public worker contract accepts one self-contained task. Tool, skill and
schema selection and the planner/tool/observer loop remain internal to the
worker.
The worker exposes one typed outcome with facts and bounded evidence artifacts;
later workers receive only lazy run-scoped result references, while a higher-
level coordinator performs the analysis and selects UI results.
"""

from __future__ import annotations

import asyncio
import json
import logging
from threading import Lock
from typing import Any, Dict, List, Sequence, Tuple
from uuid import uuid4

from .agent import build_chat_system_prompt, chat_model
from .async_runtime import run_coroutine_sync, run_sync_compat
from .chat_graph import (
    DEFAULT_TOOL_MESSAGE_PREVIEW_CHARS,
    WorkerCycleTrace,
    WorkerDisplayItem,
    WorkerResponseError,
    ensure_worker_tools,
    run_worker_graph,
    run_worker_graph_async,
)
from .contracts import (
    EvidenceArtifact,
    PreviousResultReference,
    SavedResultDescriptor,
    WorkerCapability,
    WorkerOutcome,
    WorkerRequestParts,
    parse_worker_request,
)
from .experiment_flags import (
    WORKER_CAPABILITY_REROUTE_EXPERIMENT_ENV,
    WORKER_SPLIT_TOOL_CALL_EXPERIMENT_ENV,
    experiment_flag_enabled,
)
from .observability import get_callback_handler
from .run_metrics import (
    get_run_metrics_callback,
    record_worker_observation,
    record_worker_route,
    record_worker_task,
)
from .tools import get_worker_tools, load_schemas, load_skills
from .tools.saved_results import (
    bind_saved_result_schemas,
    get_active_saved_result_store,
    worker_result_access_scope,
)
from .tools.routing import select_chat_route, select_chat_route_async

logger = logging.getLogger(__name__)

WORKER_MAX_STEPS = 5
WORKER_MAX_REROUTES = 5
WORKER_TOOL_MESSAGE_PREVIEW_CHARS = DEFAULT_TOOL_MESSAGE_PREVIEW_CHARS
_REROUTE_FEEDBACK_MAX_CHARS = 4000
_TOOL_ARGUMENTS_MAX_CHARS = 2000
_HANDOFF_DESCRIPTION_MAX_CHARS = 600
_READ_PREVIOUS_RESULT_TOOL_NAME = "read_previous_result"
_SPLIT_TOOL_CALL_PLANNING_ENV = WORKER_SPLIT_TOOL_CALL_EXPERIMENT_ENV
_DISPLAY_RESULTS: Dict[str, WorkerDisplayItem] = {}
_DISPLAY_RESULTS_LOCK = Lock()
_DEFAULT_SYNC_SELECT_CHAT_ROUTE = select_chat_route
_DEFAULT_SYNC_RUN_WORKER_GRAPH = run_worker_graph


async def _select_chat_route_compat(*args: Any, **kwargs: Any) -> Any:
    """Use native async routing, with a legacy sync injection boundary."""

    if select_chat_route is not _DEFAULT_SYNC_SELECT_CHAT_ROUTE:
        return await run_sync_compat(select_chat_route, *args, **kwargs)
    return await select_chat_route_async(*args, **kwargs)


async def _run_worker_graph_compat(**kwargs: Any) -> Any:
    """Use native async graph execution unless a sync adapter is injected."""

    if run_worker_graph is not _DEFAULT_SYNC_RUN_WORKER_GRAPH:
        return await run_sync_compat(run_worker_graph, **kwargs)
    return await run_worker_graph_async(**kwargs)


def _split_tool_call_planning_enabled() -> bool:
    return experiment_flag_enabled(
        _SPLIT_TOOL_CALL_PLANNING_ENV,
    )


def _capability_reroute_enabled() -> bool:
    return experiment_flag_enabled(
        WORKER_CAPABILITY_REROUTE_EXPERIMENT_ENV,
    )


def _compact_tool_arguments(
    arguments: Dict[str, Any],
) -> Dict[str, Any]:
    try:
        serialized = json.dumps(
            arguments,
            ensure_ascii=False,
            default=str,
            separators=(",", ":"),
        )
    except (TypeError, ValueError):
        serialized = str(arguments)
    if len(serialized) <= _TOOL_ARGUMENTS_MAX_CHARS:
        return dict(arguments)
    marker = "… [аргументы обрезаны]"
    preview = serialized[: _TOOL_ARGUMENTS_MAX_CHARS - len(marker)].rstrip()
    return {
        "_truncated": True,
        "json_preview": preview + marker,
    }


def _store_evidence_items(
    items: Sequence[WorkerDisplayItem],
    datasets: Sequence[SavedResultDescriptor],
) -> List[EvidenceArtifact]:
    artifacts: List[EvidenceArtifact] = []
    known_dataset_refs = {item.result_ref for item in datasets}
    with _DISPLAY_RESULTS_LOCK:
        for item in items:
            display_ref = None
            if item.name != _READ_PREVIOUS_RESULT_TOOL_NAME:
                display_ref = uuid4().hex
                _DISPLAY_RESULTS[display_ref] = item
            artifacts.append(
                EvidenceArtifact(
                    evidence_id=(
                        item.evidence_id or f"evidence_{uuid4().hex}"
                    ),
                    tool_name=item.name,
                    compact_args=_compact_tool_arguments(item.arguments),
                    preview=item.preview,
                    truncated=item.truncated,
                    display_ref=display_ref,
                    dataset_ref=(
                        item.result_ref
                        if item.result_ref in known_dataset_refs
                        else None
                    ),
                    lineage_evidence_ids=list(
                        item.lineage_evidence_ids
                    ),
                )
            )
    return artifacts


def _handoff_description(
    item: WorkerDisplayItem,
) -> str:
    """Build a deterministic label from the executed tool call only."""
    compact_args = _compact_tool_arguments(item.arguments)
    serialized_args = json.dumps(
        compact_args,
        ensure_ascii=False,
        default=str,
        sort_keys=True,
        separators=(",", ":"),
    )
    description = f"{item.name}: args={serialized_args}"
    if len(description) <= _HANDOFF_DESCRIPTION_MAX_CHARS:
        return description
    marker = "…"
    return (
        description[: _HANDOFF_DESCRIPTION_MAX_CHARS - len(marker)].rstrip()
        + marker
    )


def _register_previous_results(
    items: Sequence[WorkerDisplayItem],
    datasets: Sequence[SavedResultDescriptor],
) -> List[PreviousResultReference]:
    """Persist accepted full results and return only opaque refs for handoff."""
    store = get_active_saved_result_store()
    if store is None:
        return []
    known_dataset_refs = {item.result_ref for item in datasets}
    return [
        store.register_previous_result(
            source_tool=item.name,
            source_tool_call_id=item.tool_call_id,
            content=item.content,
            description=_handoff_description(item),
            dataset_ref=(
                item.result_ref
                if item.result_ref in known_dataset_refs
                else None
            ),
            source_evidence_ids=(
                [item.evidence_id] if item.evidence_id else []
            ),
        )
        for item in items
        if item.name != _READ_PREVIOUS_RESULT_TOOL_NAME
    ]


def resolve_worker_display_refs(refs: Sequence[str]) -> List[WorkerDisplayItem]:
    """Consume selected full results after coordination has finished."""
    with _DISPLAY_RESULTS_LOCK:
        return [
            item
            for ref in refs
            if (item := _DISPLAY_RESULTS.pop(ref, None)) is not None
        ]


def register_worker_display_items(
    items: Sequence[WorkerDisplayItem],
) -> List[str]:
    """Register verified direct-pipeline results for user-facing display."""
    refs: List[str] = []
    with _DISPLAY_RESULTS_LOCK:
        for item in items:
            display_ref = uuid4().hex
            _DISPLAY_RESULTS[display_ref] = item
            refs.append(display_ref)
    return refs


def discard_worker_display_refs(refs: Sequence[str]) -> None:
    """Discard full results that the coordinator did not select for the UI."""
    with _DISPLAY_RESULTS_LOCK:
        for ref in refs:
            _DISPLAY_RESULTS.pop(ref, None)


def _planner_reroute_feedback(context: Dict[str, Any]) -> str:
    payload = {
        "gap": str(context.get("gap") or "").strip(),
    }
    capability_reroute = (
        "reason" in context or "required_capabilities" in context
    )
    if capability_reroute:
        payload["reason"] = str(context.get("reason") or "").strip()
        payload["required_capabilities"] = list(
            context.get("required_capabilities") or []
        )
    serialized = json.dumps(payload, ensure_ascii=False)
    if len(serialized) > _REROUTE_FEEDBACK_MAX_CHARS:
        serialized = serialized[: _REROUTE_FEEDBACK_MAX_CHARS - 1] + "…"
    return (
        "Повторный запуск worker после неуспешной попытки. Ниже только "
        "диагностическая выжимка предыдущего запуска, а не новая task. "
        "Учти её при первом следующем вызове data tool и исправь описанную "
        "проблему с помощью "
        + (
            "палитры требуемых возможностей.\n"
            if capability_reroute
            else "расширенной палитры.\n"
        )
        + f"<reroute_feedback>{serialized}</reroute_feedback>"
    )


def _required_reroute_capabilities(
    graph_result: Any,
) -> List[WorkerCapability]:
    """Use only the observer's typed reroute contract."""
    return list(
        dict.fromkeys(graph_result.required_capabilities or [])
    )


def _final_outcome_summary(
    answer: Any,
    *,
    internal_gap: Any = None,
) -> str:
    """Fold internal failure feedback into the sole public summary field."""
    clean_answer = str(answer or "").strip()
    clean_gap = str(internal_gap or "").strip()
    if not clean_gap:
        return clean_answer or "Worker завершился без текстовой выжимки."
    if not clean_answer:
        return clean_gap
    if clean_gap.casefold() in clean_answer.casefold():
        return clean_answer
    return f"{clean_answer}\nПричина незавершённости: {clean_gap}"


async def _worker_chat_async(
    task: str | WorkerRequestParts,
    *,
    worker_execution_id: str,
) -> WorkerOutcome:
    """Execute one self-contained task in an isolated generic worker."""
    request_parts = parse_worker_request(task)
    clean_task = request_parts.current_task.strip()
    if not clean_task:
        return WorkerOutcome(
            summary="Worker получил пустую task.",
            status="failed",
            stop_reason="missing_input",
            unmet_requirements=["Непустая worker task не передана."],
        )
    record_worker_task(clean_task)

    callback = get_callback_handler()
    callbacks = [callback] if callback is not None else []
    metrics_callback = get_run_metrics_callback()
    if metrics_callback is not None and metrics_callback not in callbacks:
        callbacks.append(metrics_callback)
    saved_store = get_active_saved_result_store()

    def accepted_datasets(
        items: Sequence[WorkerDisplayItem],
    ) -> List[SavedResultDescriptor]:
        if saved_store is None:
            return []
        return saved_store.descriptors_for_worker(
            worker_execution_id,
            [item.result_ref for item in items],
        )

    attempted_palettes: List[Tuple[str, ...]] = []
    cycle_history: List[WorkerCycleTrace] = []
    reroute_context: Dict[str, Any] | None = None
    reroute_count = 0
    capability_reroute_enabled = _capability_reroute_enabled()

    while True:
        available_tools = bind_saved_result_schemas(
            get_worker_tools(include_general=True),
            request_parts,
        )
        if capability_reroute_enabled:
            required_capabilities = tuple(
                (reroute_context or {}).get("required_capabilities") or ()
            )
            previous_palette_names = set(
                (
                    (reroute_context or {}).get("previous_tool_palettes")
                    or [[]]
                )[-1]
            )
            routable_tool_names = {
                item.name
                for item in get_worker_tools(
                    required_capabilities=required_capabilities,
                )
            } | previous_palette_names
            catalog_stage = (
                "capability_expansion"
                if required_capabilities
                else (
                    "reroute_palette"
                    if reroute_context is not None
                    else "specialized_only"
                )
            )
            general_tools_available = False
        else:
            required_capabilities = ()
            general_tools_available = reroute_count >= 2
            routable_tool_names = {
                item.name
                for item in get_worker_tools(
                    include_general=general_tools_available,
                )
            }
            catalog_stage = (
                "general_fallback"
                if general_tools_available
                else "specialized_only"
            )
        routable_tools = tuple(
            item
            for item in available_tools
            if item.name != _READ_PREVIOUS_RESULT_TOOL_NAME
            and item.name in routable_tool_names
        )
        route_kwargs: Dict[str, Any] = {
            "model": chat_model,
            "available_tools": routable_tools,
            "callbacks": callbacks,
            "catalog_stage": catalog_stage,
        }
        if reroute_context is not None:
            route_kwargs["reroute_context"] = reroute_context
        route = await _select_chat_route_compat(
            request_parts,
            **route_kwargs,
        )
        selected_names = set(route.tools)
        if any(
            item.name == _READ_PREVIOUS_RESULT_TOOL_NAME
            for item in available_tools
        ):
            selected_names.add(_READ_PREVIOUS_RESULT_TOOL_NAME)
        selected_tools = tuple(
            item for item in available_tools if item.name in selected_names
        )
        palette = tuple(sorted(tool.name for tool in selected_tools))
        attempted_palettes.append(palette)
        worker_tools = ensure_worker_tools(selected_tools)
        selected_skills = load_skills(tuple(route.skills))
        selected_schemas = load_schemas(tuple(route.schemas))
        reroute_gap = (
            str(reroute_context.get("gap") or "").strip()
            if reroute_context is not None
            else ""
        )
        record_worker_route(
            worker_task=clean_task,
            routing_attempt=reroute_count + 1,
            tools=[tool.name for tool in worker_tools],
            skills=list(route.skills),
            schemas=list(route.schemas),
            gap=reroute_gap or None,
        )

        logger.info(
            "Worker route: %s",
            json.dumps(
                {
                    "task": clean_task,
                    "routing_attempt": reroute_count + 1,
                    "tools": [tool.name for tool in selected_tools],
                    "worker_tools": [tool.name for tool in worker_tools],
                    "skills": list(route.skills),
                    "schemas": list(route.schemas),
                    "gap": reroute_gap or None,
                    "required_capabilities": list(required_capabilities),
                    "capability_reroute_enabled": capability_reroute_enabled,
                    "general_fallback_available": general_tools_available,
                },
                ensure_ascii=False,
            )[:8000],
        )

        system_prompt = build_chat_system_prompt(
            selected_skills,
            selected_schemas,
        )
        if reroute_context is not None:
            system_prompt = (
                f"{system_prompt}\n\n"
                f"{_planner_reroute_feedback(reroute_context)}"
            )

        try:
            graph_result = await _run_worker_graph_compat(
                task=request_parts,
                system_prompt=system_prompt,
                model=chat_model,
                tools=worker_tools,
                max_steps=WORKER_MAX_STEPS,
                tool_message_preview_chars=WORKER_TOOL_MESSAGE_PREVIEW_CHARS,
                callbacks=callbacks,
                split_tool_call_planning=(
                    _split_tool_call_planning_enabled()
                ),
            )
        except WorkerResponseError as exc:
            logger.warning("Worker contract failed: %s", exc)
            reason = (
                "observer_error"
                if "Observer" in str(exc)
                else "tool_error"
            )
            return WorkerOutcome(
                summary=str(exc),
                status="failed",
                stop_reason=reason,
                unmet_requirements=[str(exc)],
            )
        first_cycle_number = len(cycle_history) + 1
        new_cycles = [
            cycle.model_copy(
                update={
                    "cycle": first_cycle_number + index,
                    "routing_attempt": reroute_count + 1,
                }
            )
            for index, cycle in enumerate(graph_result.cycle_history)
        ]
        cycle_history.extend(new_cycles)
        for cycle in new_cycles:
            observation_payload = cycle.observation.model_dump()
            record_worker_observation(
                worker_task=clean_task,
                cycle=cycle.cycle,
                routing_attempt=cycle.routing_attempt,
                observation=observation_payload,
            )
            logger.info(
                "Worker observation: %s",
                json.dumps(
                    {
                        "task": clean_task,
                        "cycle": cycle.cycle,
                        "routing_attempt": cycle.routing_attempt,
                        "observation": observation_payload,
                    },
                    ensure_ascii=False,
                )[:8000],
            )
        if graph_result.status != "reroute":
            datasets = accepted_datasets(graph_result.display_items)
            summary = _final_outcome_summary(
                graph_result.answer,
                internal_gap=graph_result.gap,
            )
            previous_results = _register_previous_results(
                graph_result.display_items,
                datasets,
            )
            outcome_status = (
                "failed"
                if graph_result.stop_reason == "no_results"
                else "partial"
                if graph_result.gap
                else "complete"
            )
            return WorkerOutcome(
                summary=summary,
                status=outcome_status,
                stop_reason=(
                    graph_result.stop_reason or "budget_exhausted"
                    if graph_result.gap
                    else None
                ),
                unmet_requirements=(
                    list(graph_result.unmet_requirements)
                    or [str(graph_result.gap)]
                    if graph_result.gap
                    else []
                ),
                facts=list(graph_result.facts),
                evidence=_store_evidence_items(
                    graph_result.display_items,
                    datasets,
                ),
                datasets=datasets,
                previous_results=previous_results,
            )

        if reroute_count >= WORKER_MAX_REROUTES:
            datasets = accepted_datasets(graph_result.display_items)
            evidence = _store_evidence_items(
                graph_result.display_items,
                datasets,
            )
            previous_results = _register_previous_results(
                graph_result.display_items,
                datasets,
            )
            return WorkerOutcome(
                summary=str(
                    graph_result.gap
                    or "Worker исчерпал лимит reroute без результата."
                ),
                status=("partial" if evidence else "failed"),
                stop_reason=(graph_result.stop_reason or "missing_capability"),
                unmet_requirements=(
                    list(graph_result.unmet_requirements)
                    or [
                        str(
                            graph_result.gap
                            or "Не удалось закрыть worker task."
                        )
                    ]
                ),
                facts=list(graph_result.facts),
                evidence=evidence,
                datasets=datasets,
                previous_results=previous_results,
            )

        reroute_count += 1
        reroute_context = {
            "gap": str(graph_result.gap or ""),
            "previous_tool_palettes": [
                list(item) for item in attempted_palettes
            ],
            "attempt": reroute_count,
        }
        if capability_reroute_enabled:
            reroute_context.update(
                {
                    "reason": str(
                        graph_result.reroute_reason
                        or "missing_capability"
                    ),
                    "required_capabilities": (
                        _required_reroute_capabilities(graph_result)
                    ),
                }
            )
        logger.info(
            "Worker returns to tool-router: attempt=%s gap=%s",
            reroute_count,
            graph_result.gap,
        )


async def worker_chat_async(
    task: str | WorkerRequestParts,
) -> WorkerOutcome:
    """Execute one task with isolated saved-result ownership and access."""
    request_parts = parse_worker_request(task)
    with worker_result_access_scope(
        request_parts.previous_results or [],
    ) as access:
        return await _worker_chat_async(
            request_parts,
            worker_execution_id=access.worker_execution_id,
        )


def worker_chat(task: str | WorkerRequestParts) -> WorkerOutcome:
    """Compatibility facade for non-ASGI callers."""

    return run_coroutine_sync(worker_chat_async(task))


__all__ = [
    "WORKER_MAX_STEPS",
    "WORKER_MAX_REROUTES",
    "WORKER_CAPABILITY_REROUTE_EXPERIMENT_ENV",
    "WORKER_SPLIT_TOOL_CALL_EXPERIMENT_ENV",
    "WORKER_TOOL_MESSAGE_PREVIEW_CHARS",
    "EvidenceArtifact",
    "WorkerOutcome",
    "discard_worker_display_refs",
    "register_worker_display_items",
    "resolve_worker_display_refs",
    "worker_chat",
    "worker_chat_async",
]
