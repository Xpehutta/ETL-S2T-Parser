"""Opt-in passive metrics for real agent runs and live scenarios."""

from __future__ import annotations

import ast
import json
from collections import OrderedDict
from contextlib import contextmanager
from contextvars import ContextVar
from threading import Lock
from time import perf_counter
from typing import Any, Dict, Iterator, List, Literal, Mapping, Optional

from langchain_core.callbacks import BaseCallbackHandler
from pydantic import BaseModel, ConfigDict, Field

from .env_flags import read_binary_env_flag


_METRICS_REGISTRY_LIMIT = 100
_VALUE_PREVIEW_CHARS = 2000
_SUPERVISOR_CONTEXT_PREVIEW_CHARS = 4000
_ENTITY_RESOLUTION_EVENT_LIMIT = 50
_ENTITY_RESOLUTION_CANDIDATE_LIMIT = 20
_ENTITY_RESOLUTION_REASON_CHARS = 600
_SQL_RISK_FACT_LIMIT = 8
_SQL_RISK_EXPRESSION_LIMIT = 4
_SQL_RISK_EVIDENCE_LIMIT = 8
_SQL_RISK_JOIN_LIMIT = 4
_SQL_RISK_JOIN_KEY_LIMIT = 8
_SQL_RISK_IDENTIFIER_CHARS = 200
_SQL_RISK_EXPRESSION_CHARS = 300
_SQL_RISK_EVIDENCE_ID_CHARS = 120
_SQL_RISK_RELATION_CHARS = 240
_SQL_RISK_PREDICATE_CHARS = 600
_SQL_RISK_JOIN_KEY_CHARS = 240
_SQL_RISK_CYCLE_LIMIT = 100
_SQL_RISK_INTEGER_LIMIT = 2_147_483_647
_UPSTREAM_ANSWER_SOURCE_CHARS = 120
_ACTIVE_RUN: ContextVar[Optional["_RunCollector"]] = ContextVar(
    "agent_run_metrics",
    default=None,
)
_ACTIVE_LLM_STAGE: ContextVar[str] = ContextVar(
    "agent_run_llm_stage",
    default="unattributed",
)
_COMPLETED_RUNS: "OrderedDict[str, AgentRunMetrics]" = OrderedDict()
_COMPLETED_RUNS_LOCK = Lock()


class LLMCallMetric(BaseModel):
    """One real model request observed through LangChain callbacks."""

    model_config = ConfigDict(extra="forbid")

    run_id: str
    stage: str = "unattributed"
    model: str = ""
    elapsed_seconds: float = 0.0
    input_tokens: int = 0
    output_tokens: int = 0
    total_tokens: int = 0
    cache_read_tokens: int = 0
    has_error: bool = False


class LLMStageMetric(BaseModel):
    """Aggregate provider usage for one semantic agent stage."""

    model_config = ConfigDict(extra="forbid")

    stage: str
    calls: int = 0
    error_calls: int = 0
    elapsed_seconds: float = 0.0
    input_tokens: int = 0
    output_tokens: int = 0
    total_tokens: int = 0
    cache_read_tokens: int = 0


class ToolCallMetric(BaseModel):
    """One executed data-tool call without its potentially large result."""

    model_config = ConfigDict(extra="forbid")

    run_id: str
    name: str
    arguments: Any = Field(default_factory=dict)
    input_preview: str = ""
    elapsed_seconds: float = 0.0
    has_error: bool = False


class ObservationMetric(BaseModel):
    """One structured worker observation retained without full tool results."""

    model_config = ConfigDict(extra="forbid")

    worker_task: str
    cycle: int
    routing_attempt: int
    status: str
    gap: Optional[str] = None
    accepted_tool_call_ids: List[str] = Field(default_factory=list)
    facts: List[Dict[str, Any]] = Field(default_factory=list)
    limitations: List[str] = Field(default_factory=list)
    reroute_reason: Optional[str] = None
    required_capabilities: List[str] = Field(default_factory=list)


class WorkerRouteMetric(BaseModel):
    """One router selection for a worker task and reroute attempt."""

    model_config = ConfigDict(extra="forbid")

    worker_task: str
    routing_attempt: int
    tools: List[str] = Field(default_factory=list)
    skills: List[str] = Field(default_factory=list)
    schemas: List[str] = Field(default_factory=list)
    gap: Optional[str] = None


class SupervisorDecisionMetric(BaseModel):
    """Bounded supervisor route and handoff fields for live diagnostics."""

    model_config = ConfigDict(extra="forbid")

    route: Literal["direct", "delegate"]
    resolved_references: str = ""
    context: str = ""


class AgentRunMetrics(BaseModel):
    """Completed metrics snapshot retained by session id for test inspection."""

    model_config = ConfigDict(extra="forbid")

    session_id: str
    elapsed_seconds: float
    llm_calls: List[LLMCallMetric] = Field(default_factory=list)
    llm_stages: List[LLMStageMetric] = Field(default_factory=list)
    tool_calls: List[ToolCallMetric] = Field(default_factory=list)
    supervisor_decision: Optional[SupervisorDecisionMetric] = None
    worker_tasks: List[str] = Field(default_factory=list)
    coordinator_plan: List[Dict[str, Any]] = Field(default_factory=list)
    coordinator_dag: List[Dict[str, Any]] = Field(default_factory=list)
    worker_routes: List[WorkerRouteMetric] = Field(default_factory=list)
    observations: List[ObservationMetric] = Field(default_factory=list)
    worker_outcomes: List[Dict[str, Any]] = Field(default_factory=list)
    entity_resolution: List[Dict[str, Any]] = Field(default_factory=list)
    sql_risk_facts: List[Dict[str, Any]] = Field(default_factory=list)
    sql_risk_operation: Optional[Dict[str, Any]] = None
    validation_protocol: Optional[Dict[str, Any]] = None
    upstream_output: Optional[Dict[str, Any]] = None
    display_tools: List[str] = Field(default_factory=list)
    input_tokens: int = 0
    output_tokens: int = 0
    total_tokens: int = 0
    cache_read_tokens: int = 0
    error: Optional[str] = None


def count_agent_reroutes(metrics: AgentRunMetrics) -> int:
    """Count worker routing retries plus coordinator data-cycle reroutes.

    ``ObservationMetric.cycle`` belongs to one worker's planner/observer loop;
    it is deliberately excluded from the coordinator cycle count.
    """

    worker_reroutes = sum(
        int(item.routing_attempt) > 1 for item in metrics.worker_routes
    )
    coordinator_cycles: set[int] = set()
    for step in metrics.coordinator_plan:
        try:
            cycle = int(step.get("cycle") or 0)
        except (AttributeError, TypeError, ValueError):
            cycle = 0
        if cycle > 0:
            coordinator_cycles.add(cycle)
    coordinator_reroutes = max(0, len(coordinator_cycles) - 1)
    return worker_reroutes + coordinator_reroutes


def _clip(value: Any, *, max_chars: int = _VALUE_PREVIEW_CHARS) -> str:
    text = str(value or "").strip()
    if len(text) <= max_chars:
        return text
    return text[: max_chars - 1].rstrip() + "…"


def _tool_arguments(value: Any) -> Any:
    """Keep exact tool arguments in a JSON-serializable form for diagnostics."""
    if isinstance(value, Mapping):
        return dict(value)
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return {}
        try:
            return json.loads(stripped)
        except (TypeError, ValueError):
            try:
                parsed = ast.literal_eval(stripped)
            except (SyntaxError, ValueError):
                return {"raw": value}
            if isinstance(parsed, (dict, list, str, int, float, bool, type(None))):
                return parsed
            return {"raw": value}
    return {"raw": str(value)}


def _bounded_json_value(value: Any, *, depth: int = 0) -> Any:
    """Keep structured diagnostics bounded without flattening their contract."""
    if depth >= 8:
        return _clip(value)
    if isinstance(value, Mapping):
        return {
            _clip(key, max_chars=120): _bounded_json_value(item, depth=depth + 1)
            for key, item in list(value.items())[:100]
        }
    if isinstance(value, (list, tuple)):
        return [
            _bounded_json_value(item, depth=depth + 1)
            for item in list(value)[:100]
        ]
    if isinstance(value, str):
        return _clip(value)
    if value is None or isinstance(value, (bool, int, float)):
        return value
    return _clip(value)


def _metrics_enabled() -> bool:
    metrics_enabled = read_binary_env_flag(
        "AGENT_RUN_METRICS_ENABLED",
        default=False,
    )
    live_scenarios_enabled = read_binary_env_flag(
        "RUN_LIVE_AGENT_SCENARIOS",
        default=False,
    )
    return metrics_enabled or live_scenarios_enabled


def _usage_values(response: Any) -> tuple[int, int, int, int]:
    llm_output = getattr(response, "llm_output", None)
    usage: Mapping[str, Any] = {}
    if isinstance(llm_output, Mapping):
        candidate = llm_output.get("token_usage") or llm_output.get("usage")
        if isinstance(candidate, Mapping):
            usage = candidate

    if not usage:
        for generation_group in getattr(response, "generations", None) or []:
            for generation in (
                generation_group
                if isinstance(generation_group, list)
                else [generation_group]
            ):
                message_usage = getattr(
                    getattr(generation, "message", None),
                    "usage_metadata",
                    None,
                )
                if isinstance(message_usage, Mapping):
                    usage = message_usage
                    break
            if usage:
                break

    input_tokens = int(
        usage.get("prompt_tokens") or usage.get("input_tokens") or 0
    )
    output_tokens = int(
        usage.get("completion_tokens") or usage.get("output_tokens") or 0
    )
    total_tokens = int(usage.get("total_tokens") or 0)
    if not total_tokens:
        total_tokens = input_tokens + output_tokens
    cache_read_tokens = int(usage.get("precached_prompt_tokens") or 0)
    details = usage.get("input_token_details")
    if isinstance(details, Mapping):
        cache_read_tokens = int(details.get("cache_read") or cache_read_tokens)
    return input_tokens, output_tokens, total_tokens, cache_read_tokens


def _stage_metrics(calls: List[LLMCallMetric]) -> List[LLMStageMetric]:
    totals: "OrderedDict[str, Dict[str, Any]]" = OrderedDict()
    for call in calls:
        item = totals.setdefault(
            call.stage,
            {
                "stage": call.stage,
                "calls": 0,
                "error_calls": 0,
                "elapsed_seconds": 0.0,
                "input_tokens": 0,
                "output_tokens": 0,
                "total_tokens": 0,
                "cache_read_tokens": 0,
            },
        )
        item["calls"] += 1
        item["error_calls"] += int(call.has_error)
        item["elapsed_seconds"] += call.elapsed_seconds
        item["input_tokens"] += call.input_tokens
        item["output_tokens"] += call.output_tokens
        item["total_tokens"] += call.total_tokens
        item["cache_read_tokens"] += call.cache_read_tokens
    return [LLMStageMetric.model_validate(item) for item in totals.values()]


class _RunCollector:
    def __init__(self, session_id: str) -> None:
        self.session_id = session_id
        self.started_at = perf_counter()
        self.lock = Lock()
        self.llm_calls: "OrderedDict[str, Dict[str, Any]]" = OrderedDict()
        self.tool_calls: "OrderedDict[str, Dict[str, Any]]" = OrderedDict()
        self.supervisor_decision: Optional[SupervisorDecisionMetric] = None
        self.worker_tasks: List[str] = []
        self.coordinator_plan: List[Dict[str, Any]] = []
        self.coordinator_dag: List[Dict[str, Any]] = []
        self.worker_routes: List[WorkerRouteMetric] = []
        self.observations: List[ObservationMetric] = []
        self.worker_outcomes: List[Dict[str, Any]] = []
        self.entity_resolution: List[Dict[str, Any]] = []
        self.sql_risk_facts: List[Dict[str, Any]] = []
        self.sql_risk_operation: Optional[Dict[str, Any]] = None
        self.validation_protocol: Optional[Dict[str, Any]] = None
        self.upstream_output: Optional[Dict[str, Any]] = None
        self.display_tools: List[str] = []
        self.error: Optional[str] = None

    def start_llm(self, run_id: Any, serialized: Any, *, stage: str) -> None:
        key = str(run_id)
        model = ""
        if isinstance(serialized, Mapping):
            kwargs = serialized.get("kwargs")
            if isinstance(kwargs, Mapping):
                model = str(
                    kwargs.get("model") or kwargs.get("model_name") or ""
                )
            if not model:
                identifier = serialized.get("id")
                if isinstance(identifier, list) and identifier:
                    model = str(identifier[-1])
        with self.lock:
            self.llm_calls.setdefault(
                key,
                {
                    "run_id": key,
                    "stage": stage,
                    "model": model,
                    "started_at": perf_counter(),
                },
            )

    def finish_llm(
        self,
        run_id: Any,
        response: Any,
        *,
        error: bool,
        stage: str,
    ) -> None:
        key = str(run_id)
        with self.lock:
            item = self.llm_calls.setdefault(
                key,
                {
                    "run_id": key,
                    "stage": stage,
                    "model": "",
                    "started_at": perf_counter(),
                },
            )
            item["elapsed_seconds"] = max(
                0.0,
                perf_counter() - float(item.get("started_at") or perf_counter()),
            )
            item["has_error"] = error
            if not error:
                (
                    item["input_tokens"],
                    item["output_tokens"],
                    item["total_tokens"],
                    item["cache_read_tokens"],
                ) = _usage_values(response)

    def start_tool(self, run_id: Any, serialized: Any, input_value: Any) -> None:
        key = str(run_id)
        name = "unknown_tool"
        if isinstance(serialized, Mapping):
            name = str(serialized.get("name") or name)
        with self.lock:
            self.tool_calls.setdefault(
                key,
                {
                    "run_id": key,
                    "name": name,
                    "arguments": _tool_arguments(input_value),
                    "input_preview": _clip(input_value),
                    "started_at": perf_counter(),
                },
            )

    def finish_tool(self, run_id: Any, *, error: bool) -> None:
        key = str(run_id)
        with self.lock:
            item = self.tool_calls.get(key)
            if item is None:
                return
            item["elapsed_seconds"] = max(
                0.0,
                perf_counter() - float(item.get("started_at") or perf_counter()),
            )
            item["has_error"] = error

    def snapshot(self) -> AgentRunMetrics:
        with self.lock:
            llm_calls = [
                LLMCallMetric.model_validate(
                    {
                        key: value
                        for key, value in item.items()
                        if key != "started_at"
                    }
                )
                for item in self.llm_calls.values()
            ]
            tool_calls = [
                ToolCallMetric.model_validate(
                    {
                        key: value
                        for key, value in item.items()
                        if key != "started_at"
                    }
                )
                for item in self.tool_calls.values()
            ]
            return AgentRunMetrics(
                session_id=self.session_id,
                elapsed_seconds=max(0.0, perf_counter() - self.started_at),
                llm_calls=llm_calls,
                llm_stages=_stage_metrics(llm_calls),
                tool_calls=tool_calls,
                supervisor_decision=self.supervisor_decision,
                worker_tasks=list(self.worker_tasks),
                coordinator_plan=[dict(item) for item in self.coordinator_plan],
                coordinator_dag=[dict(item) for item in self.coordinator_dag],
                worker_routes=list(self.worker_routes),
                observations=list(self.observations),
                worker_outcomes=[dict(item) for item in self.worker_outcomes],
                entity_resolution=[
                    dict(item) for item in self.entity_resolution
                ],
                sql_risk_facts=[dict(item) for item in self.sql_risk_facts],
                sql_risk_operation=(
                    dict(self.sql_risk_operation)
                    if self.sql_risk_operation is not None
                    else None
                ),
                validation_protocol=(
                    dict(self.validation_protocol)
                    if self.validation_protocol is not None
                    else None
                ),
                upstream_output=(
                    dict(self.upstream_output)
                    if self.upstream_output is not None
                    else None
                ),
                display_tools=list(self.display_tools),
                input_tokens=sum(item.input_tokens for item in llm_calls),
                output_tokens=sum(item.output_tokens for item in llm_calls),
                total_tokens=sum(item.total_tokens for item in llm_calls),
                cache_read_tokens=sum(item.cache_read_tokens for item in llm_calls),
                error=self.error,
            )


class _RunMetricsCallback(BaseCallbackHandler):
    def on_chat_model_start(
        self,
        serialized: Dict[str, Any],
        messages: List[List[Any]],
        *,
        run_id: Any,
        **kwargs: Any,
    ) -> None:
        del messages, kwargs
        if collector := _ACTIVE_RUN.get():
            collector.start_llm(
                run_id,
                serialized,
                stage=_ACTIVE_LLM_STAGE.get(),
            )

    def on_llm_start(
        self,
        serialized: Dict[str, Any],
        prompts: List[str],
        *,
        run_id: Any,
        **kwargs: Any,
    ) -> None:
        del prompts, kwargs
        if collector := _ACTIVE_RUN.get():
            collector.start_llm(
                run_id,
                serialized,
                stage=_ACTIVE_LLM_STAGE.get(),
            )

    def on_llm_end(self, response: Any, *, run_id: Any, **kwargs: Any) -> None:
        del kwargs
        if collector := _ACTIVE_RUN.get():
            collector.finish_llm(
                run_id,
                response,
                error=False,
                stage=_ACTIVE_LLM_STAGE.get(),
            )

    def on_llm_error(self, error: BaseException, *, run_id: Any, **kwargs: Any) -> None:
        del error, kwargs
        if collector := _ACTIVE_RUN.get():
            collector.finish_llm(
                run_id,
                None,
                error=True,
                stage=_ACTIVE_LLM_STAGE.get(),
            )

    def on_tool_start(
        self,
        serialized: Dict[str, Any],
        input_str: str,
        *,
        run_id: Any,
        **kwargs: Any,
    ) -> None:
        del kwargs
        if collector := _ACTIVE_RUN.get():
            collector.start_tool(run_id, serialized, input_str)

    def on_tool_end(self, output: Any, *, run_id: Any, **kwargs: Any) -> None:
        del output, kwargs
        if collector := _ACTIVE_RUN.get():
            collector.finish_tool(run_id, error=False)

    def on_tool_error(
        self,
        error: BaseException,
        *,
        run_id: Any,
        **kwargs: Any,
    ) -> None:
        del error, kwargs
        if collector := _ACTIVE_RUN.get():
            collector.finish_tool(run_id, error=True)


_CALLBACK = _RunMetricsCallback()


@contextmanager
def llm_stage(stage: str) -> Iterator[None]:
    """Attribute every real model request in this scope to one agent stage."""
    clean_stage = str(stage or "").strip() or "unattributed"
    token = _ACTIVE_LLM_STAGE.set(clean_stage)
    try:
        yield
    finally:
        _ACTIVE_LLM_STAGE.reset(token)


@contextmanager
def capture_agent_run(session_id: Optional[str]) -> Iterator[None]:
    """Capture one real supervisor run when metrics are explicitly enabled."""
    clean_session_id = str(session_id or "").strip()
    if not clean_session_id or not _metrics_enabled():
        yield
        return

    collector = _RunCollector(clean_session_id)
    token = _ACTIVE_RUN.set(collector)
    try:
        yield
    except Exception as exc:
        collector.error = f"{type(exc).__name__}: {exc}"
        raise
    finally:
        snapshot = collector.snapshot()
        _ACTIVE_RUN.reset(token)
        with _COMPLETED_RUNS_LOCK:
            _COMPLETED_RUNS[clean_session_id] = snapshot
            _COMPLETED_RUNS.move_to_end(clean_session_id)
            while len(_COMPLETED_RUNS) > _METRICS_REGISTRY_LIMIT:
                _COMPLETED_RUNS.popitem(last=False)


def get_run_metrics_callback() -> Optional[BaseCallbackHandler]:
    """Return the passive callback only while a captured run is active."""
    return _CALLBACK if _ACTIVE_RUN.get() is not None else None


def record_supervisor_decision(
    *,
    route: Literal["direct", "delegate"],
    resolved_references: str = "",
    context: str = "",
) -> None:
    """Retain the bounded native supervisor handoff used by this run."""
    if collector := _ACTIVE_RUN.get():
        metric = SupervisorDecisionMetric(
            route=route,
            resolved_references=_clip(resolved_references),
            context=_clip(
                context,
                max_chars=_SUPERVISOR_CONTEXT_PREVIEW_CHARS,
            ),
        )
        with collector.lock:
            collector.supervisor_decision = metric


def record_worker_task(task: str) -> None:
    if collector := _ACTIVE_RUN.get():
        with collector.lock:
            collector.worker_tasks.append(_clip(task))


def record_coordinator_plan(steps: List[Dict[str, Any]]) -> None:
    """Append one downstream cycle plan to the run trace."""
    if collector := _ACTIVE_RUN.get():
        with collector.lock:
            collector.coordinator_plan.extend(dict(item) for item in steps)


def record_coordinator_dag(result: Mapping[str, Any]) -> None:
    """Retain bounded DAG topology and execution timings for one cycle."""

    if collector := _ACTIVE_RUN.get():
        payload = dict(_bounded_json_value(result))
        with collector.lock:
            collector.coordinator_dag.append(payload)


def record_worker_route(
    *,
    worker_task: str,
    routing_attempt: int,
    tools: List[str],
    skills: List[str],
    schemas: List[str],
    gap: Optional[str] = None,
) -> None:
    """Retain an already selected router palette for live diagnostics."""
    if collector := _ACTIVE_RUN.get():
        metric = WorkerRouteMetric(
            worker_task=_clip(worker_task),
            routing_attempt=max(1, int(routing_attempt)),
            tools=[str(item) for item in tools],
            skills=[str(item) for item in skills],
            schemas=[str(item) for item in schemas],
            gap=_clip(gap) or None,
        )
        with collector.lock:
            collector.worker_routes.append(metric)


def record_worker_observation(
    *,
    worker_task: str,
    cycle: int,
    routing_attempt: int,
    observation: Mapping[str, Any],
) -> None:
    """Retain a bounded structured observation for live-run diagnostics."""
    if collector := _ACTIVE_RUN.get():
        metric = ObservationMetric(
            worker_task=_clip(worker_task),
            cycle=max(1, int(cycle)),
            routing_attempt=max(1, int(routing_attempt)),
            status=_clip(observation.get("status")),
            gap=_clip(observation.get("gap")) or None,
            accepted_tool_call_ids=[
                _clip(item)
                for item in observation.get("accepted_tool_call_ids", [])
            ],
            facts=[
                {
                    "text": _clip(item.get("text")),
                    "evidence_ids": [
                        _clip(evidence_id)
                        for evidence_id in item.get("evidence_ids", [])
                    ],
                }
                for item in observation.get("facts", [])
                if isinstance(item, Mapping)
            ],
            limitations=[
                _clip(item) for item in observation.get("limitations", [])
            ],
            reroute_reason=(
                _clip(observation.get("reroute_reason")) or None
            ),
            required_capabilities=[
                _clip(item)
                for item in observation.get("required_capabilities", [])
            ],
        )
        with collector.lock:
            collector.observations.append(metric)


def record_worker_outcome(
    *,
    cycle: int,
    step: int,
    status: str,
    stop_reason: Optional[str],
    unmet_requirements: List[str],
    evidence_count: int,
    dataset_count: int,
) -> None:
    """Retain the typed worker completion state without copying evidence."""
    if collector := _ACTIVE_RUN.get():
        payload = {
            "cycle": max(1, int(cycle)),
            "step": max(1, int(step)),
            "status": _clip(status),
            "stop_reason": _clip(stop_reason) or None,
            "unmet_requirements": [
                _clip(item) for item in unmet_requirements
            ],
            "evidence_count": max(0, int(evidence_count)),
            "dataset_count": max(0, int(dataset_count)),
        }
        with collector.lock:
            collector.worker_outcomes.append(payload)


def _resolution_candidate_identity(value: Any) -> Dict[str, Any]:
    """Project one resolver candidate without copying source-row provenance."""
    if not isinstance(value, Mapping):
        return {"canonical_name": _clip(value, max_chars=500)}
    identity = {
        key: _bounded_json_value(value[key])
        for key in (
            "canonical_name",
            "entity_type",
            "role",
            "file_id",
            "score",
            "method",
        )
        if key in value and value[key] is not None
    }
    provenance = value.get("provenance")
    if isinstance(provenance, (list, tuple)):
        identity["provenance_count"] = len(provenance)
    return identity


def _entity_resolution_summary(event: Mapping[str, Any]) -> Dict[str, Any]:
    """Keep resolver decisions useful for assertions and globally compact."""
    summary: Dict[str, Any] = {}
    for key in (
        "mention",
        "entity_type",
        "role",
        "status",
        "method",
        "canonical_name",
        "file_id",
        "error_code",
        "resolver_invoked",
        "resolution_origin",
        "resolution_stage",
    ):
        if key in event and event[key] is not None:
            summary[key] = _bounded_json_value(event[key])
    if event.get("reason") is not None:
        summary["reason"] = _clip(
            event.get("reason"),
            max_chars=_ENTITY_RESOLUTION_REASON_CHARS,
        )

    candidate_set = event.get("candidate_set")
    if isinstance(candidate_set, Mapping):
        raw_candidates = candidate_set.get("candidates")
        candidates = (
            list(raw_candidates)
            if isinstance(raw_candidates, (list, tuple))
            else []
        )
        candidate_summary: Dict[str, Any] = {
            key: _bounded_json_value(candidate_set[key])
            for key in (
                "coverage",
                "source",
                "total_candidates",
                "source_result_id",
                "threshold",
                "minimum_gap",
            )
            if key in candidate_set and candidate_set[key] is not None
        }
        candidate_summary["candidate_count"] = len(candidates)
        candidate_summary["candidates"] = [
            _resolution_candidate_identity(item)
            for item in candidates[:_ENTITY_RESOLUTION_CANDIDATE_LIMIT]
        ]
        candidate_summary["candidates_truncated"] = (
            len(candidates) > _ENTITY_RESOLUTION_CANDIDATE_LIMIT
        )
        summary["candidate_set"] = candidate_summary
    elif isinstance(event.get("candidates"), (list, tuple)):
        # Retain the compact pre-CandidateSet trace shape for compatibility.
        summary["candidates"] = [
            _bounded_json_value(item)
            for item in list(event["candidates"])[
                :_ENTITY_RESOLUTION_CANDIDATE_LIMIT
            ]
        ]
    return summary


def record_entity_resolution(events: List[Mapping[str, Any]]) -> None:
    """Append bounded resolver summaries without full candidate provenance."""
    if collector := _ACTIVE_RUN.get():
        payload = [
            _entity_resolution_summary(event)
            for event in events
            if isinstance(event, Mapping)
        ]
        with collector.lock:
            remaining = max(
                0,
                _ENTITY_RESOLUTION_EVENT_LIMIT
                - len(collector.entity_resolution),
            )
            collector.entity_resolution.extend(payload[:remaining])


def _sql_risk_fact_mapping(value: Any) -> Optional[Mapping[str, Any]]:
    """Return a mapping for a fact model without importing its domain type."""
    if isinstance(value, Mapping):
        return value
    model_dump = getattr(value, "model_dump", None)
    if not callable(model_dump):
        return None
    try:
        dumped = model_dump(mode="json")
    except TypeError:
        dumped = model_dump()
    return dumped if isinstance(dumped, Mapping) else None


def _sql_risk_fact_summary(value: Any) -> Optional[Dict[str, Any]]:
    """Project one deterministic fact without retaining source data rows."""
    fact = _sql_risk_fact_mapping(value)
    if fact is None:
        return None

    summary: Dict[str, Any] = {}
    for key in (
        "source_table",
        "source_field",
        "target_table",
        "target_field",
        "conclusion",
        "mechanism",
        "condition",
    ):
        if key in fact and fact[key] is not None:
            summary[key] = _clip(
                fact[key],
                max_chars=_SQL_RISK_IDENTIFIER_CHARS,
            )

    if "matching_rows" in fact:
        try:
            summary["matching_rows"] = min(
                _SQL_RISK_INTEGER_LIMIT,
                max(0, int(fact["matching_rows"])),
            )
        except (TypeError, ValueError):
            summary["matching_rows"] = 0

    for key in (
        "file_id",
        "source_not_null",
        "target_not_null",
        "mapping_rows",
        "exact_field_rows",
        "source_metadata_rows",
        "target_metadata_rows",
    ):
        if key not in fact:
            continue
        value = fact[key]
        if value is None and key in {"source_not_null", "target_not_null"}:
            summary[key] = None
            continue
        try:
            summary[key] = min(
                _SQL_RISK_INTEGER_LIMIT,
                max(0, int(value)),
            )
        except (TypeError, ValueError):
            summary[key] = 0

    expressions = fact.get("target_expressions")
    if isinstance(expressions, (list, tuple)):
        summary["target_expressions"] = [
            _clip(item, max_chars=_SQL_RISK_EXPRESSION_CHARS)
            for item in list(expressions)[:_SQL_RISK_EXPRESSION_LIMIT]
        ]

    detected_mechanisms = fact.get("detected_mechanisms")
    if isinstance(detected_mechanisms, (list, tuple)):
        summary["detected_mechanisms"] = [
            _clip(item, max_chars=_SQL_RISK_IDENTIFIER_CHARS)
            for item in list(detected_mechanisms)[:_SQL_RISK_EXPRESSION_LIMIT]
        ]

    conditions = fact.get("conditions")
    if isinstance(conditions, (list, tuple)):
        condition_summaries: List[Dict[str, Any]] = []
        for raw_condition in list(conditions)[:_SQL_RISK_EXPRESSION_LIMIT]:
            condition = _sql_risk_fact_mapping(raw_condition)
            if condition is None:
                continue
            item = {
                key: _clip(value, max_chars=limit)
                for key, value, limit in (
                    (
                        "kind",
                        condition.get("kind"),
                        _SQL_RISK_IDENTIFIER_CHARS,
                    ),
                    (
                        "predicate",
                        condition.get("predicate"),
                        _SQL_RISK_PREDICATE_CHARS,
                    ),
                )
                if value is not None
            }
            if item:
                condition_summaries.append(item)
        summary["conditions"] = condition_summaries

    joins = fact.get("joins")
    if isinstance(joins, (list, tuple)):
        join_summaries: List[Dict[str, Any]] = []
        for raw_join in list(joins)[:_SQL_RISK_JOIN_LIMIT]:
            join = _sql_risk_fact_mapping(raw_join)
            if join is None:
                continue
            item: Dict[str, Any] = {}
            for key, limit in (
                ("join_type", _SQL_RISK_IDENTIFIER_CHARS),
                ("relation", _SQL_RISK_RELATION_CHARS),
                ("predicate", _SQL_RISK_PREDICATE_CHARS),
                ("uniqueness_condition", _SQL_RISK_IDENTIFIER_CHARS),
            ):
                if key in join and join[key] is not None:
                    item[key] = _clip(join[key], max_chars=limit)
            equalities = join.get("join_key_equalities")
            if isinstance(equalities, (list, tuple)):
                item["join_key_equalities"] = [
                    _clip(value, max_chars=_SQL_RISK_JOIN_KEY_CHARS)
                    for value in list(equalities)[:_SQL_RISK_JOIN_KEY_LIMIT]
                ]
            if item:
                join_summaries.append(item)
        summary["joins"] = join_summaries

    evidence_ids = fact.get("evidence_ids")
    if isinstance(evidence_ids, (list, tuple)):
        summary["evidence_ids"] = [
            _clip(item, max_chars=_SQL_RISK_EVIDENCE_ID_CHARS)
            for item in list(evidence_ids)[:_SQL_RISK_EVIDENCE_LIMIT]
        ]
    return summary


def record_sql_risk_facts(
    facts_or_payload: Any,
    *,
    cycle: int | None = None,
) -> None:
    """Append bounded deterministic SQL-risk facts, never underlying rows.

    Accepts either a sequence of fact models/mappings or the structured
    payload returned by ``field_value_change_payload``.
    """
    collector = _ACTIVE_RUN.get()
    if collector is None:
        return

    raw_facts: Any = facts_or_payload
    if isinstance(facts_or_payload, Mapping) and "facts" in facts_or_payload:
        raw_facts = facts_or_payload.get("facts")
    elif isinstance(facts_or_payload, Mapping):
        raw_facts = [facts_or_payload]
    if not isinstance(raw_facts, (list, tuple)):
        return

    payload = [
        summary
        for item in raw_facts
        if (summary := _sql_risk_fact_summary(item)) is not None
    ]
    if cycle is not None:
        try:
            bounded_cycle = min(
                _SQL_RISK_CYCLE_LIMIT,
                max(1, int(cycle)),
            )
        except (TypeError, ValueError):
            bounded_cycle = 1
        for item in payload:
            item["cycle"] = bounded_cycle
    with collector.lock:
        remaining = max(
            0,
            _SQL_RISK_FACT_LIMIT - len(collector.sql_risk_facts),
        )
        collector.sql_risk_facts.extend(payload[:remaining])


def record_sql_risk_operation(result: Mapping[str, Any]) -> None:
    """Retain one bounded direct SQL-risk operation-scope trace.

    The trace contains only the immutable scope, exact reader calls, status
    and issues. Full reader payloads remain in the run-scoped saved-result
    store and are never copied into metrics.
    """

    if collector := _ACTIVE_RUN.get():
        reads: List[Dict[str, Any]] = []
        for item in list(result.get("reads") or [])[:8]:
            if not isinstance(item, Mapping):
                continue
            arguments = _bounded_json_value(item.get("arguments") or {})
            reads.append(
                {
                    key: value
                    for key, value in {
                        "requirement_index": item.get("requirement_index"),
                        "tool_name": _clip(item.get("tool_name")),
                        "arguments": arguments,
                        "call_id": _clip(item.get("call_id")),
                        "status": _clip(item.get("status")),
                        "elapsed_seconds": item.get("elapsed_seconds"),
                        "row_count": item.get("row_count"),
                        "source_total": item.get("source_total"),
                        "truncated": bool(item.get("truncated", False)),
                        "evidence_id": _clip(item.get("evidence_id")),
                        "issue_code": _clip(item.get("issue_code")),
                    }.items()
                    if value not in (None, "")
                }
            )
        issues: List[Dict[str, Any]] = []
        for item in list(result.get("issues") or [])[:8]:
            if not isinstance(item, Mapping):
                continue
            issues.append(
                {
                    key: value
                    for key, value in {
                        "code": _clip(item.get("code")),
                        "message": _clip(item.get("message")),
                        "tool_name": _clip(item.get("tool_name")),
                        "requirement_index": item.get("requirement_index"),
                    }.items()
                    if value not in (None, "")
                }
            )
        payload = {
            "pipeline": _clip(result.get("pipeline")),
            "status": _clip(result.get("status")),
            "execution_mode": _clip(result.get("execution_mode")),
            "scope": _clip(result.get("scope"), max_chars=500),
            "answer_source": _clip(result.get("answer_source")),
            "silent_fallback": bool(result.get("silent_fallback", False)),
            "reads": reads,
            "issues": issues,
        }
        raw_facts = result.get("facts") or []
        if raw_facts and isinstance(raw_facts[0], Mapping):
            raw_assessment = raw_facts[0]
            payload["assessment"] = {
                key: value
                for key, value in {
                    "status": _clip(raw_assessment.get("assessment_status")),
                    "outcome": _clip(raw_assessment.get("outcome")),
                    "structure_status": _clip(
                        raw_assessment.get("structure_status")
                    ),
                    "limitations": [
                        _clip(item)
                        for item in list(
                            raw_assessment.get("limitations") or []
                        )[:12]
                    ],
                    "reviewed_rule_ids": [
                        _clip(item)
                        for item in list(
                            raw_assessment.get("reviewed_rule_ids") or []
                        )[:96]
                    ],
                }.items()
                if value not in (None, "", [])
            }
        with collector.lock:
            collector.sql_risk_operation = payload


def record_validation_protocol(result: Mapping[str, Any]) -> None:
    """Retain bounded contracts/check SQL and readiness, never reader rows."""
    if collector := _ACTIVE_RUN.get():
        payload = dict(_bounded_json_value(result))
        with collector.lock:
            collector.validation_protocol = payload


def record_upstream_output(result: Mapping[str, Any]) -> None:
    """Retain the final upstream answer and evidence selections."""
    if collector := _ACTIVE_RUN.get():
        payload = {
            "answer": _clip(result.get("answer")),
            "used_evidence_ids": [
                _clip(item) for item in result.get("used_evidence_ids", [])
            ],
            "display_evidence_ids": [
                _clip(item)
                for item in result.get("display_evidence_ids", [])
            ],
        }
        if result.get("answer_source") is not None:
            answer_source = _clip(
                result.get("answer_source"),
                max_chars=_UPSTREAM_ANSWER_SOURCE_CHARS,
            )
            if answer_source:
                payload["answer_source"] = answer_source
        with collector.lock:
            collector.upstream_output = payload


def record_display_tools(names: List[str]) -> None:
    if collector := _ACTIVE_RUN.get():
        with collector.lock:
            collector.display_tools = [str(name) for name in names]


def consume_agent_run_metrics(session_id: str) -> Optional[AgentRunMetrics]:
    """Consume a completed metrics snapshot, primarily from live tests."""
    with _COMPLETED_RUNS_LOCK:
        return _COMPLETED_RUNS.pop(str(session_id), None)


__all__ = [
    "AgentRunMetrics",
    "LLMCallMetric",
    "LLMStageMetric",
    "ObservationMetric",
    "SupervisorDecisionMetric",
    "ToolCallMetric",
    "WorkerRouteMetric",
    "capture_agent_run",
    "consume_agent_run_metrics",
    "get_run_metrics_callback",
    "llm_stage",
    "record_upstream_output",
    "record_coordinator_dag",
    "record_coordinator_plan",
    "record_display_tools",
    "record_entity_resolution",
    "record_sql_risk_facts",
    "record_sql_risk_operation",
    "record_supervisor_decision",
    "record_validation_protocol",
    "record_worker_observation",
    "record_worker_outcome",
    "record_worker_route",
    "record_worker_task",
]
