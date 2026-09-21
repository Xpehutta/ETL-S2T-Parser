"""Opt-in end-to-end scenarios for the configured real chat model.

These tests intentionally execute one user request per test without mocks,
parameterization, batching, or parallel calls. Enable them explicitly with
``RUN_LIVE_AGENT_SCENARIOS=1``. Set ``LIVE_AGENT_MODE=single_agent`` to run
the same acceptance scenarios through the non-multiagent baseline; the default
is ``multiagent``.
"""

from __future__ import annotations

import json
import os
import re
import socket
import threading
import urllib.error
import urllib.request
import warnings
from csv import DictReader
from dataclasses import dataclass
from difflib import SequenceMatcher
from io import StringIO
from pathlib import Path
from time import perf_counter
from typing import Any, Iterator, Mapping
from uuid import uuid4

import pytest
import sqlglot
from langchain_core.callbacks import BaseCallbackHandler

import storage.database as db_storage
from agents.experiment_flags import (
    OPERATION_SQL_RISK_ASPECTS_EXPERIMENT_ENV,
    S2T_NARROW_TOOLS_EXPERIMENT_ENV,
    experiment_flag_enabled,
)
from agents.run_metrics import (
    AgentRunMetrics,
    consume_agent_run_metrics,
    count_agent_reroutes,
)
from scripts.live_agent_config import (
    read_live_agent_http_timeout,
    read_live_agent_llm_judge_enabled,
    read_live_agent_scenarios_enabled,
)
from services.sql_dialects import GREENPLUM_DIALECT  # noqa: F401


PROJECT_ROOT = Path(__file__).resolve().parents[1]
_configured_live_db_path = os.getenv("LIVE_AGENT_DB_PATH", "").strip()
LIVE_DB_PATH = (
    Path(_configured_live_db_path).expanduser().resolve()
    if _configured_live_db_path
    else PROJECT_ROOT / "excel_data.db"
)
LIVE_TRANSCRIPT_PATH = os.getenv("LIVE_AGENT_TRANSCRIPT_PATH", "").strip()
LIVE_AGENT_MODE = (
    os.getenv("LIVE_AGENT_MODE", "multiagent").strip().lower()
    or "multiagent"
)
if LIVE_AGENT_MODE not in {"multiagent", "single_agent"}:
    raise ValueError(
        "LIVE_AGENT_MODE must be 'multiagent' or 'single_agent'"
    )
LIVE_AGENT_ENABLED = read_live_agent_scenarios_enabled()
LIVE_AGENT_LLM_JUDGE = read_live_agent_llm_judge_enabled()
LIVE_AGENT_HTTP_TIMEOUT = read_live_agent_http_timeout()
STRICT_RETRIEVAL_ENABLED = experiment_flag_enabled(
    S2T_NARROW_TOOLS_EXPERIMENT_ENV,
)
_LIVE_TRANSCRIPT_LOCK = threading.Lock()
_LIVE_TRANSCRIPT_INDEX = 0

pytestmark = [
    pytest.mark.integration,
    pytest.mark.live_agent,
    pytest.mark.skipif(
        not LIVE_AGENT_ENABLED,
        reason="set RUN_LIVE_AGENT_SCENARIOS=1 to call the configured real LLM",
    ),
]


def _assert_public_answer(answer: str) -> None:
    assert answer.strip()
    lowered = answer.lower()
    for internal_term in (
        "supervisor",
        "coordinator",
        "finish_worker",
        "result_key",
        "evidence_id",
        "display_ref",
        "dataset_ref",
        "result_id",
    ):
        _warn_unless(
            internal_term not in lowered,
            "presentation",
            f"public answer exposes internal term {internal_term!r}",
        )


def _require_live_semantic_judge() -> None:
    """Keep semantic SQL-risk scenarios from passing without their oracle."""

    assert LIVE_AGENT_LLM_JUDGE, "SQL-risk semantic scenarios require --llm-judge"


def _display_payloads(result) -> list[dict]:
    payloads: list[dict] = []
    for item in result.display_items:
        try:
            payload = json.loads(item.content)
        except (TypeError, json.JSONDecodeError):
            continue
        if isinstance(payload, dict):
            payloads.append(payload)
    return payloads


def _payload_contains_value(payload, expected) -> bool:
    if isinstance(payload, dict):
        return any(
            _payload_contains_value(value, expected)
            for value in payload.values()
        )
    if isinstance(payload, list):
        return any(
            _payload_contains_value(value, expected)
            for value in payload
        )
    return payload == expected or str(payload) == str(expected)


def _assert_named_answer_value(answer: str, name: str, expected: object) -> None:
    """Require one explicit ``name=value`` scalar in a live answer."""
    value_boundary = (
        r"(?!\w|[.,]\d)"
        if isinstance(expected, (int, float)) and not isinstance(expected, bool)
        else r"(?![\w.])"
    )
    pattern = (
        rf"(?i)(?<![\w.]){re.escape(name)}\s*[:=]\s*`?"
        rf"{re.escape(str(expected))}`?{value_boundary}"
    )
    assert re.search(pattern, answer), {
        "missing": f"{name}={expected}",
        "answer": answer,
    }


def _assert_named_answer_list(
    answer: str,
    name: str,
    expected: list[str],
) -> None:
    """Require one explicit JSON list without relying on the semantic judge."""
    match = re.search(
        rf"(?i)(?<![\w.]){re.escape(name)}\s*[:=]\s*`?(\[[^\]\n]*\])`?",
        answer,
    )
    assert match is not None, {"missing": name, "answer": answer}
    parsed = json.loads(match.group(1))
    assert isinstance(parsed, list), parsed
    assert sorted(str(item).casefold() for item in parsed) == sorted(
        str(item).casefold() for item in expected
    ), {"expected": expected, "actual": parsed, "answer": answer}


def _payload_table_paths(payload: dict) -> list[list[str]]:
    paths: list[list[str]] = []
    for collection_name in ("paths", "chains", "rows"):
        collection = payload.get(collection_name)
        if not isinstance(collection, list):
            continue
        for item in collection:
            if not isinstance(item, dict):
                continue
            table_path = item.get("table_path")
            if isinstance(table_path, list):
                paths.append([str(value) for value in table_path])
    return paths


@dataclass(frozen=True)
class _LiveExchange:
    query: str
    result: object
    metrics: AgentRunMetrics
    http_elapsed_seconds: float


def _judge_usage_values(response: Any) -> tuple[int, int, int, int]:
    """Extract provider-reported usage from one completed judge attempt."""
    usage: Mapping[str, Any] = {}
    llm_output = getattr(response, "llm_output", None)
    if isinstance(llm_output, Mapping):
        candidate = llm_output.get("token_usage") or llm_output.get("usage")
        if isinstance(candidate, Mapping):
            usage = candidate
    if not usage:
        for generation_group in getattr(response, "generations", None) or []:
            generations = (
                generation_group
                if isinstance(generation_group, list)
                else [generation_group]
            )
            for generation in generations:
                candidate = getattr(
                    getattr(generation, "message", None),
                    "usage_metadata",
                    None,
                )
                if isinstance(candidate, Mapping):
                    usage = candidate
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


class _JudgeTelemetryCallback(BaseCallbackHandler):
    """Count actual judge attempts, including LangChain retry attempts."""

    def __init__(self) -> None:
        super().__init__()
        self._lock = threading.Lock()
        self._run_ids: set[str] = set()
        self._completed = 0
        self._errors = 0
        self._input_tokens = 0
        self._output_tokens = 0
        self._total_tokens = 0
        self._cache_read_tokens = 0

    def _start(self, run_id: Any) -> None:
        with self._lock:
            self._run_ids.add(str(run_id))

    def on_chat_model_start(
        self,
        serialized: dict[str, Any],
        messages: list[list[Any]],
        *,
        run_id: Any,
        **kwargs: Any,
    ) -> None:
        del serialized, messages, kwargs
        self._start(run_id)

    def on_llm_start(
        self,
        serialized: dict[str, Any],
        prompts: list[str],
        *,
        run_id: Any,
        **kwargs: Any,
    ) -> None:
        del serialized, prompts, kwargs
        self._start(run_id)

    def on_llm_end(self, response: Any, *, run_id: Any, **kwargs: Any) -> None:
        del kwargs
        usage = _judge_usage_values(response)
        with self._lock:
            self._run_ids.add(str(run_id))
            self._completed += 1
            self._input_tokens += usage[0]
            self._output_tokens += usage[1]
            self._total_tokens += usage[2]
            self._cache_read_tokens += usage[3]

    def on_llm_error(
        self,
        error: BaseException,
        *,
        run_id: Any,
        **kwargs: Any,
    ) -> None:
        del error, kwargs
        with self._lock:
            self._run_ids.add(str(run_id))
            self._errors += 1

    def snapshot(self, *, model: str) -> dict[str, Any]:
        with self._lock:
            completed = self._completed
            errors = self._errors
            attempts = max(len(self._run_ids), completed + errors)
            input_tokens = self._input_tokens
            output_tokens = self._output_tokens
            total_tokens = self._total_tokens
            cache_read_tokens = self._cache_read_tokens
        return {
            "model": str(model or "not_configured"),
            "attempts": attempts,
            "completed": completed,
            "errors": errors,
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
            "total_tokens": total_tokens,
            "cache_read_tokens": cache_read_tokens,
        }


def _chat(
    client,
    query: str,
    *,
    history: list[dict] | None = None,
):
    from agents.chat_graph import WorkerRunResult

    history_payload = list(history or [])
    session_id = f"live-agent-{uuid4()}"
    started_at = perf_counter()
    response = client.post(
        "/chat",
        json={
            "query": query,
            "history": history_payload,
            "session_id": session_id,
        },
    )
    http_elapsed_seconds = perf_counter() - started_at
    payload = response.get_json()
    metrics = consume_agent_run_metrics(session_id)
    semantic_evaluation = _record_live_exchange(
        query,
        response.status_code,
        payload,
        history=history_payload,
        metrics=metrics,
        http_elapsed_seconds=http_elapsed_seconds,
    )
    assert response.status_code == 200, payload
    assert metrics is not None, "live run did not publish agent metrics"
    if (
        semantic_evaluation is not None
        and semantic_evaluation["status"] != "passed"
    ):
        pytest.fail(
            "LLM-as-judge отклонил пользовательский результат: "
            f"{semantic_evaluation['status']}: "
            f"{semantic_evaluation['reason']}"
        )
    return _LiveExchange(
        query=query,
        result=WorkerRunResult.model_validate(payload),
        metrics=metrics,
        http_elapsed_seconds=http_elapsed_seconds,
    )


def _record_live_exchange(
    query: str,
    status_code: int,
    payload,
    *,
    history: list[dict] | None = None,
    metrics: AgentRunMetrics | None,
    http_elapsed_seconds: float,
) -> dict[str, str] | None:
    """Record an exchange and return its completed semantic evaluation."""
    global _LIVE_TRANSCRIPT_INDEX

    if isinstance(payload, dict):
        answer = payload.get("answer") or payload.get("error") or payload
        display_names = [
            str(item.get("name", ""))
            for item in payload.get("display_items", [])
            if isinstance(item, dict) and item.get("name")
        ]
    else:
        answer = payload
        display_names = []

    semantic_status = "not_evaluated"
    semantic_reason = (
        "Не выполнялась; ответ сохранён для ручного разбора и будущего "
        "LLM-as-judge."
    )
    judge_telemetry: dict[str, Any] = {
        "model": "not_configured",
        "attempts": 0,
        "completed": 0,
        "errors": 0,
        "input_tokens": 0,
        "output_tokens": 0,
        "total_tokens": 0,
        "cache_read_tokens": 0,
    }
    if LIVE_AGENT_LLM_JUDGE:
        from agents.llm_factory import (
            create_judge_chat_model,
            get_judge_model_name,
        )

        judge_model_name = get_judge_model_name()
        judge_telemetry["model"] = judge_model_name
        telemetry_callback = _JudgeTelemetryCallback()
        try:
            from agents.semantic_judge import judge_agent_response

            judge_model = create_judge_chat_model(timeout=180)
            judge_model.callbacks = [telemetry_callback]
            verdict = judge_agent_response(
                query=query,
                answer=answer,
                history=history or [],
                display_items=(
                    payload.get("display_items", [])
                    if isinstance(payload, dict)
                    else []
                ),
                model=judge_model,
            )
            semantic_status = verdict.status
            semantic_reason = verdict.reason
        except Exception as exc:
            semantic_status = "judge_error"
            semantic_reason = (
                "LLM-as-judge завершился технической ошибкой: "
                f"{type(exc).__name__}."
            )
        finally:
            judge_telemetry = telemetry_callback.snapshot(
                model=judge_model_name,
            )

        if status_code != 200 and semantic_status in {"passed", "failed"}:
            judge_status = semantic_status
            judge_reason = semantic_reason
            semantic_status = "failed"
            semantic_reason = (
                f"Технический HTTP {status_code} не решил задачу. "
                f"LLM-as-judge: {judge_status}: {judge_reason}"
            )
    if not LIVE_TRANSCRIPT_PATH:
        return (
            {
                "status": semantic_status,
                "reason": semantic_reason,
            }
            if LIVE_AGENT_LLM_JUDGE
            else None
        )
    transcript_path = Path(LIVE_TRANSCRIPT_PATH)
    if not transcript_path.is_absolute():
        transcript_path = PROJECT_ROOT / transcript_path

    metrics_block = "Метрики недоступны"
    trace_block = "Трасса недоступна"
    if metrics is not None:
        tool_errors = sum(item.has_error for item in metrics.tool_calls)
        reroutes = count_agent_reroutes(metrics)
        pipelines = list(
            dict.fromkeys(
                str(step.get("pipeline") or "").strip()
                for step in metrics.coordinator_plan
                if str(step.get("pipeline") or "").strip()
            )
        )
        if metrics.sql_risk_operation is not None:
            operation_pipeline = str(
                metrics.sql_risk_operation.get("pipeline") or ""
            ).strip()
            if operation_pipeline and operation_pipeline not in pipelines:
                pipelines.append(operation_pipeline)
        if not pipelines:
            if metrics.worker_tasks:
                pipelines = ["agentic"]
            elif metrics.supervisor_decision is not None:
                pipelines = ["direct"]
        stage_lines = "\n".join(
            (
                f"stage_tokens[{item.stage}]: calls={item.calls}, "
                f"errors={item.error_calls}, input={item.input_tokens}, "
                f"output={item.output_tokens}, total={item.total_tokens}, "
                f"cache_read={item.cache_read_tokens}, "
                f"seconds={item.elapsed_seconds:.3f}"
            )
            for item in metrics.llm_stages
        )
        dag_statuses: dict[str, int] = {}
        dag_input_result_ids: list[str] = []
        dag_output_result_ids: list[str] = []
        for dag in metrics.coordinator_dag:
            for worker in dag.get("workers") or []:
                if not isinstance(worker, Mapping):
                    continue
                status = str(worker.get("status") or "unknown")
                dag_statuses[status] = dag_statuses.get(status, 0) + 1
                for field_name, destination in (
                    ("input_result_ids", dag_input_result_ids),
                    ("output_result_ids", dag_output_result_ids),
                ):
                    for value in worker.get(field_name) or []:
                        clean_value = str(value or "").strip()
                        if clean_value and clean_value not in destination:
                            destination.append(clean_value)
        dag_depth = max(
            (int(item.get("dag_depth") or 0) for item in metrics.coordinator_dag),
            default=0,
        )
        dag_max_parallel_width = max(
            (
                int(item.get("max_parallel_width") or 0)
                for item in metrics.coordinator_dag
            ),
            default=0,
        )
        dag_observed_concurrency = max(
            (
                int(item.get("max_observed_concurrency") or 0)
                for item in metrics.coordinator_dag
            ),
            default=0,
        )
        dag_blocked = dag_statuses.get("blocked_by_dependency", 0)
        dag_cancelled = sum(
            count
            for status, count in dag_statuses.items()
            if status.startswith("cancelled_")
        )
        metrics_block = (
            f"agent_seconds: {metrics.elapsed_seconds:.3f}\n"
            f"http_seconds: {http_elapsed_seconds:.3f}\n"
            f"llm_calls: {len(metrics.llm_calls)}\n"
            f"reader_calls: {len(metrics.tool_calls)}\n"
            f"tool_errors: {tool_errors}\n"
            f"reroutes: {reroutes}\n"
            f"pipelines: {', '.join(pipelines) or 'Нет'}\n"
            f"dag_cycles: {len(metrics.coordinator_dag)}\n"
            f"dag_depth: {dag_depth}\n"
            f"dag_max_parallel_width: {dag_max_parallel_width}\n"
            f"dag_observed_concurrency: {dag_observed_concurrency}\n"
            f"dag_blocked: {dag_blocked}\n"
            f"dag_cancelled: {dag_cancelled}\n"
            "dag_statuses: "
            + json.dumps(
                dag_statuses,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            )
            + "\n"
            "dag_input_result_ids: "
            + json.dumps(dag_input_result_ids, ensure_ascii=False)
            + "\n"
            "dag_output_result_ids: "
            + json.dumps(dag_output_result_ids, ensure_ascii=False)
            + "\n"
            f"tokens: input={metrics.input_tokens}, "
            f"output={metrics.output_tokens}, total={metrics.total_tokens}, "
            f"cache_read={metrics.cache_read_tokens}\n"
            f"{stage_lines}\n"
            f"workers: {len(metrics.worker_tasks)}\n"
            "tools: "
            + (", ".join(item.name for item in metrics.tool_calls) or "Нет")
            + "\n"
            "displays: "
            + (", ".join(metrics.display_tools) or "Нет")
        )
        trace_block = "```json\n" + json.dumps(
            {
                "llm_calls": [
                    item.model_dump(mode="json")
                    for item in metrics.llm_calls
                ],
                "llm_stages": [
                    item.model_dump(mode="json")
                    for item in metrics.llm_stages
                ],
                "tool_calls": [
                    {
                        key: value
                        for key, value in item.model_dump(mode="json").items()
                        if key != "input_preview"
                    }
                    for item in metrics.tool_calls
                ],
                "supervisor_decision": (
                    metrics.supervisor_decision.model_dump(mode="json")
                    if metrics.supervisor_decision is not None
                    else None
                ),
                "coordinator_plan": metrics.coordinator_plan,
                "coordinator_dag": metrics.coordinator_dag,
                "worker_tasks": metrics.worker_tasks,
                "worker_routes": [
                    item.model_dump(mode="json")
                    for item in metrics.worker_routes
                ],
                "observations": [
                    item.model_dump(mode="json")
                    for item in metrics.observations
                ],
                "worker_outcomes": metrics.worker_outcomes,
                "entity_resolution": metrics.entity_resolution,
                "sql_risk_facts": metrics.sql_risk_facts,
                "sql_risk_operation": metrics.sql_risk_operation,
                "validation_protocol": metrics.validation_protocol,
                "upstream_output": metrics.upstream_output,
            },
            ensure_ascii=False,
            indent=2,
        ) + "\n```"

    with _LIVE_TRANSCRIPT_LOCK:
        _LIVE_TRANSCRIPT_INDEX += 1
        history_block = ""
        if history:
            history_block = (
                "### История\n\n```json\n"
                + json.dumps(history, ensure_ascii=False, indent=2)
                + "\n```\n\n"
            )
        block = (
            f"## {_LIVE_TRANSCRIPT_INDEX}. Запрос\n\n"
            f"agent_mode: {LIVE_AGENT_MODE}\n\n"
            f"{history_block}"
            f"{query}\n\n"
            f"### Ответ — HTTP {status_code}\n\n"
            f"{answer}\n\n"
            "### Display-results\n\n"
            f"{', '.join(display_names) if display_names else 'Нет'}\n\n"
            "### Execution metrics\n\n"
            f"{metrics_block}\n\n"
            "### Agent trace\n\n"
            f"{trace_block}\n\n"
            "### Judge metrics\n\n"
            f"judge_model: {judge_telemetry['model']}\n"
            "judge_calls: "
            f"attempts={judge_telemetry['attempts']}, "
            f"completed={judge_telemetry['completed']}, "
            f"errors={judge_telemetry['errors']}\n"
            "judge_tokens: "
            f"input={judge_telemetry['input_tokens']}, "
            f"output={judge_telemetry['output_tokens']}, "
            f"total={judge_telemetry['total_tokens']}, "
            f"cache_read={judge_telemetry['cache_read_tokens']}\n\n"
            "### Semantic evaluation\n\n"
            f"{semantic_reason}\n\n"
            "<!-- LIVE_SEMANTIC "
            + json.dumps(
                {
                    "scenario": _current_live_scenario(),
                    "status": semantic_status,
                    "reason": semantic_reason,
                },
                ensure_ascii=False,
                separators=(",", ":"),
            )
            + " -->\n\n"
        )
        transcript_path.parent.mkdir(parents=True, exist_ok=True)
        with transcript_path.open("a", encoding="utf-8", newline="\n") as transcript:
            transcript.write(block)

    return (
        {
            "status": semantic_status,
            "reason": semantic_reason,
        }
        if LIVE_AGENT_LLM_JUDGE
        else None
    )


def _assert_minimal_name_sequence(
    actual: list[str],
    expected: list[str | set[str]],
) -> None:
    cursor = 0
    for index, expected_name in enumerate(expected, start=1):
        accepted_names = (
            expected_name
            if isinstance(expected_name, set)
            else {expected_name}
        )
        while cursor < len(actual) and actual[cursor] not in accepted_names:
            cursor += 1
        assert cursor < len(actual), {
            "position": index,
            "actual": actual,
            "accepted": sorted(accepted_names),
        }
        cursor += 1


def _current_live_scenario() -> str:
    current_test = os.getenv("PYTEST_CURRENT_TEST", "").split(" ", 1)[0]
    return current_test.rsplit("::", 1)[-1] or "unknown"


def _record_acceptance_warning(category: str, message: str) -> None:
    clean_category = str(category or "warning").strip().lower()
    clean_message = str(message or "").strip()
    payload = {
        "category": clean_category,
        "scenario": _current_live_scenario(),
        "message": clean_message,
    }
    warnings.warn(
        f"live {clean_category} warning: {clean_message}",
        UserWarning,
        stacklevel=2,
    )
    if not LIVE_TRANSCRIPT_PATH:
        return
    transcript_path = Path(LIVE_TRANSCRIPT_PATH)
    if not transcript_path.is_absolute():
        transcript_path = PROJECT_ROOT / transcript_path
    marker = (
        "<!-- LIVE_WARNING "
        + json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
        + " -->\n"
    )
    with _LIVE_TRANSCRIPT_LOCK:
        transcript_path.parent.mkdir(parents=True, exist_ok=True)
        with transcript_path.open("a", encoding="utf-8", newline="\n") as transcript:
            transcript.write(marker)


def _warn_unless(condition: bool, category: str, message: str) -> bool:
    if condition:
        return True
    _record_acceptance_warning(category, message)
    return False


def _sequence_matches_exactly(
    actual: list[str],
    expected: list[str | set[str]],
) -> bool:
    if len(actual) != len(expected):
        return False
    return all(
        actual_name in (
            expected_name if isinstance(expected_name, set) else {expected_name}
        )
        for actual_name, expected_name in zip(actual, expected)
    )


def _assert_execution(
    exchange: _LiveExchange,
    *,
    expected_tools: list[str | set[str]],
    expected_displays: list[str | set[str]],
    forbidden_tools: set[str] | None = None,
    max_seconds: float | None,
    max_llm_calls: int,
    max_total_tokens: int,
) -> None:
    metrics = exchange.metrics
    assert metrics.error is None, metrics.error
    assert metrics.elapsed_seconds > 0, metrics
    assert exchange.http_elapsed_seconds > 0, metrics
    if max_seconds is not None:
        _warn_unless(
            metrics.elapsed_seconds <= max_seconds
            and exchange.http_elapsed_seconds <= max_seconds + 5,
            "efficiency",
            f"elapsed={metrics.elapsed_seconds:.3f}s exceeds budget={max_seconds}s",
        )
    actual_tools = [item.name for item in metrics.tool_calls]
    tools_match = True
    try:
        _assert_minimal_name_sequence(actual_tools, expected_tools)
    except AssertionError:
        tools_match = False
    _warn_unless(
        tools_match,
        "efficiency",
        f"tool route differs: actual={actual_tools}, expected={expected_tools}",
    )
    forbidden_used = sorted(set(actual_tools) & set(forbidden_tools or ()))
    _warn_unless(
        not forbidden_used,
        "efficiency",
        f"explicitly excluded tools were used: {forbidden_used}",
    )
    _warn_unless(
        _sequence_matches_exactly(actual_tools, expected_tools),
        "efficiency",
        f"tool calls include extras: actual={actual_tools}, expected={expected_tools}",
    )
    if LIVE_AGENT_MODE == "multiagent":
        actual_worker_count = len(metrics.worker_tasks)
        _warn_unless(
            len(metrics.coordinator_plan) == actual_worker_count,
            "efficiency",
            "coordinator plan and executed worker counts differ: "
            f"plan={len(metrics.coordinator_plan)}, workers={actual_worker_count}",
        )
    else:
        _warn_unless(
            metrics.worker_tasks == [] and metrics.coordinator_plan == [],
            "efficiency",
            "single-agent run unexpectedly contains coordinator activity",
        )
    displays_match = True
    try:
        _assert_minimal_name_sequence(metrics.display_tools, expected_displays)
    except AssertionError:
        displays_match = False
    _warn_unless(
        displays_match
        and _sequence_matches_exactly(metrics.display_tools, expected_displays),
        "presentation",
        "display results differ: "
        f"actual={metrics.display_tools}, expected={expected_displays}",
    )
    assert len(metrics.llm_calls) > 0, metrics.llm_calls
    _warn_unless(
        len(metrics.llm_calls) <= max_llm_calls,
        "efficiency",
        f"llm_calls={len(metrics.llm_calls)} exceeds budget={max_llm_calls}",
    )
    assert all(item.total_tokens > 0 for item in metrics.llm_calls), metrics.llm_calls
    assert metrics.input_tokens > 0
    assert metrics.output_tokens > 0
    assert metrics.total_tokens == metrics.input_tokens + metrics.output_tokens
    _warn_unless(
        metrics.total_tokens <= max_total_tokens,
        "efficiency",
        f"total_tokens={metrics.total_tokens} exceeds budget={max_total_tokens}",
    )


def _assert_supervisor_clarification(exchange: _LiveExchange) -> None:
    """Require a direct clarification with no accidental data execution."""
    result = exchange.result
    metrics = exchange.metrics

    _assert_public_answer(result.answer)
    lowered = result.answer.casefold()
    assert "?" in result.answer or any(
        marker in lowered
        for marker in (
            "уточн",
            "какую",
            "какая",
            "укажите",
            "назовите",
            "подтверд",
        )
    ), result.answer
    assert result.display_items == [], result.display_items
    assert metrics.tool_calls == [], metrics.tool_calls
    assert metrics.coordinator_plan == [], metrics.coordinator_plan
    assert metrics.worker_tasks == [], metrics.worker_tasks
    assert metrics.worker_routes == [], metrics.worker_routes
    assert metrics.observations == [], metrics.observations
    assert metrics.display_tools == [], metrics.display_tools
    assert metrics.upstream_output is None, metrics.upstream_output
    assert metrics.supervisor_decision is not None
    assert metrics.supervisor_decision.route == "direct", (
        metrics.supervisor_decision
    )
    assert metrics.supervisor_decision.resolved_references == ""
    assert metrics.supervisor_decision.context == ""
    assert {item.stage for item in metrics.llm_stages} == {"supervisor"}, (
        metrics.llm_stages
    )
    _assert_execution(
        exchange,
        expected_tools=[],
        expected_displays=[],
        max_seconds=None,
        max_llm_calls=3,
        max_total_tokens=15_000,
    )


class _LiveHttpResponse:
    def __init__(self, status_code: int, data: bytes, headers) -> None:
        self.status_code = status_code
        self.data = data
        self.headers = headers

    def get_json(self):
        return json.loads(self.data.decode("utf-8"))


class _LiveHttpClient:
    def __init__(self, base_url: str) -> None:
        self.base_url = base_url.rstrip("/")

    def _request(self, method: str, path: str, payload: dict | None = None):
        body = None
        headers = {}
        if payload is not None:
            body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            headers["Content-Type"] = "application/json; charset=utf-8"
        request = urllib.request.Request(
            f"{self.base_url}{path}",
            data=body,
            headers=headers,
            method=method,
        )
        try:
            with urllib.request.urlopen(
                request,
                timeout=LIVE_AGENT_HTTP_TIMEOUT,
            ) as response:
                return _LiveHttpResponse(
                    response.status,
                    response.read(),
                    response.headers,
                )
        except urllib.error.HTTPError as exc:
            return _LiveHttpResponse(exc.code, exc.read(), exc.headers)

    def post(self, path: str, *, json: dict):
        return self._request("POST", path, json)

    def get(self, path: str):
        return self._request("GET", path)


@pytest.fixture
def live_workspace_db(monkeypatch) -> Iterator[Path]:
    if not LIVE_DB_PATH.is_file():
        pytest.skip(
            "live SQLite database is absent; set LIVE_AGENT_DB_PATH "
            "or provide workspace excel_data.db"
        )
    monkeypatch.setattr(db_storage, "DB_PATH", str(LIVE_DB_PATH))
    yield LIVE_DB_PATH


@pytest.fixture
def live_chat_client(live_workspace_db):
    import uvicorn

    from app import app as asgi_app

    previous_testing = asgi_app.config.get("TESTING", False)
    previous_agent_mode = asgi_app.config.get("CHAT_AGENT_MODE", "multiagent")
    asgi_app.config["TESTING"] = False
    asgi_app.config["CHAT_AGENT_MODE"] = LIVE_AGENT_MODE
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(("127.0.0.1", 0))
    listener.listen(128)
    listener.setblocking(False)
    server_port = int(listener.getsockname()[1])
    server = uvicorn.Server(
        uvicorn.Config(
            asgi_app,
            host="127.0.0.1",
            port=server_port,
            log_level="warning",
            lifespan="on",
        )
    )
    server_thread = threading.Thread(
        target=server.run,
        kwargs={"sockets": [listener]},
        daemon=True,
    )
    server_thread.start()
    try:
        yield _LiveHttpClient(f"http://127.0.0.1:{server_port}")
    finally:
        server.should_exit = True
        server_thread.join(timeout=10)
        listener.close()
        asgi_app.config["TESTING"] = previous_testing
        asgi_app.config["CHAT_AGENT_MODE"] = previous_agent_mode


@pytest.fixture(autouse=True)
def generated_sql_exports() -> Iterator[set[Path]]:
    from agents.tools.sql import SQL_EXPORT_DIR

    before = set(SQL_EXPORT_DIR.glob("sql_result_*.csv"))
    registered: set[Path] = set()
    yield registered
    export_root = SQL_EXPORT_DIR.resolve()
    created = set(SQL_EXPORT_DIR.glob("sql_result_*.csv")) - before
    for path in registered | created:
        resolved = path.resolve()
        if resolved.parent == export_root and resolved.name.startswith("sql_result_"):
            resolved.unlink(missing_ok=True)


def _download_sql_export(client, payload: dict, generated: set[Path]) -> list[dict]:
    from agents.tools.sql import SQL_EXPORT_DIR

    path = Path(payload["csv_path"]).resolve()
    assert path.parent == SQL_EXPORT_DIR.resolve(), path
    assert path.is_file(), path
    generated.add(path)
    response = client.get(payload["csv_url"])
    assert response.status_code == 200
    assert "attachment" in response.headers.get("Content-Disposition", "")
    text = response.data.decode("utf-8-sig")
    return list(DictReader(StringIO(text)))


def _fetch_one(query: str, parameters: tuple[object, ...] = ()) -> tuple:
    conn = db_storage.get_db_connection()
    try:
        row = conn.execute(query, parameters).fetchone()
    finally:
        conn.close()
    if row is None:
        pytest.skip("workspace database has no row required by the scenario")
    return tuple(row)


@dataclass(frozen=True)
class _ProtocolLiveCase:
    file_id: int
    filename: str
    source_table: str
    target_table: str
    source_field: str
    target_field: str
    transformation_rule: str


def _effective_predicate_sql(predicate: str) -> str:
    """Drop only AST conjuncts that SQLGlot proves are constant true."""

    from sqlglot.optimizer.simplify import simplify

    parsed = sqlglot.parse_one(predicate, read=GREENPLUM_DIALECT)

    def conjuncts(
        node: sqlglot.exp.Expression,
    ) -> list[sqlglot.exp.Expression]:
        if isinstance(node, sqlglot.exp.And):
            return [*conjuncts(node.this), *conjuncts(node.expression)]
        return [node]

    effective: list[sqlglot.exp.Expression] = []
    for conjunct in conjuncts(parsed):
        try:
            reduced = simplify(conjunct.copy())
        except (
            sqlglot.errors.SqlglotError,
            AttributeError,
            TypeError,
            ValueError,
        ):
            reduced = None
        if isinstance(reduced, sqlglot.exp.Boolean) and reduced.this is True:
            continue
        effective.append(conjunct)
    if not effective:
        return ""
    combined = effective[0].copy()
    for conjunct in effective[1:]:
        combined = sqlglot.exp.and_(combined, conjunct.copy())
    return combined.sql(dialect=GREENPLUM_DIALECT)


def _extract_named_sql_values(
    answer: str,
    names: tuple[str, ...],
) -> dict[str, str]:
    """Extract one explicitly labelled, single-line SQL value per name."""

    alternatives = "|".join(re.escape(name) for name in names)
    pattern = re.compile(
        rf"(?im)^[ \t]*(?:[-*+]\s*)?(?:\d+[.)]\s*)?"
        rf"(?:\*\*|`)?(?P<name>{alternatives})(?:\*\*|`)?"
        r"\s*[:=]\s*(?P<value>[^\r\n]+)$"
    )
    extracted: dict[str, str] = {}
    duplicates: list[str] = []
    for match in pattern.finditer(answer):
        name = match.group("name").casefold()
        value = match.group("value").strip()
        if value.startswith("`") and value.endswith("`"):
            value = value[1:-1].strip()
        if name in extracted:
            duplicates.append(name)
        extracted[name] = value
    expected_names = {name.casefold() for name in names}
    assert not duplicates and set(extracted) == expected_names, {
        "expected_names": sorted(expected_names),
        "actual_names": sorted(extracted),
        "duplicates": duplicates,
        "answer": answer,
    }
    return extracted


def _canonical_sql_ast(value: str) -> tuple[object, ...]:
    """Return a case-normalized semantic AST signature for one expression."""

    from sqlglot.optimizer.simplify import simplify

    parsed = sqlglot.parse_one(value, read=GREENPLUM_DIALECT)
    simplified = simplify(parsed.copy())

    def flatten_boolean(
        node: sqlglot.exp.Expression,
        kind: type[sqlglot.exp.Expression],
    ) -> list[sqlglot.exp.Expression]:
        if isinstance(node, kind):
            return [
                *flatten_boolean(node.this, kind),
                *flatten_boolean(node.expression, kind),
            ]
        return [node]

    def signature(node: object) -> object:
        if isinstance(node, sqlglot.exp.Paren):
            return signature(node.this)
        if isinstance(node, (sqlglot.exp.And, sqlglot.exp.Or)):
            operands = flatten_boolean(node, type(node))
            return (
                node.key,
                tuple(sorted((signature(item) for item in operands), key=repr)),
            )
        if isinstance(node, (sqlglot.exp.EQ, sqlglot.exp.NEQ)):
            operands = sorted(
                (signature(node.this), signature(node.expression)),
                key=repr,
            )
            return node.key, tuple(operands)
        if isinstance(node, sqlglot.exp.Identifier):
            return "identifier", str(node.this).casefold()
        if isinstance(node, sqlglot.exp.Literal):
            return "literal", node.this, bool(node.is_string)
        if isinstance(node, sqlglot.exp.Expression):
            return (
                node.key,
                tuple(
                    (key, signature(argument))
                    for key, argument in sorted(node.args.items())
                    if key != "comments"
                ),
            )
        if isinstance(node, list):
            return tuple(signature(item) for item in node)
        if isinstance(node, tuple):
            return tuple(signature(item) for item in node)
        return node

    result = signature(simplified)
    assert isinstance(result, tuple), result
    return result


def _protocol_live_case(
    *,
    expression: bool = False,
    min_mapped_fields: int = 1,
    require_join: bool = False,
    require_effective_join_predicate: bool = False,
    require_filter: bool = False,
    require_target_catalog: bool = False,
    require_primary_key: bool = False,
    require_explicit_selected_projection: bool = False,
    require_non_column_selected_projection: bool = False,
) -> _ProtocolLiveCase:
    expression_filter = ""
    if expression:
        expression_filter = """
          AND (
                LOWER(s2t.transformation_rule) LIKE '%coalesce(%'
             OR LOWER(s2t.transformation_rule) LIKE '%case %'
             OR LOWER(s2t.transformation_rule) LIKE '%cast(%'
          )
        """
    catalog_filter = ""
    if require_target_catalog:
        catalog_filter += """
          AND EXISTS (
              SELECT 1 FROM target_columns AS tc
              WHERE tc.file_id = s2t.file_id
                AND TRIM(tc.table_name) = TRIM(s2t.target_table) COLLATE NOCASE
          )
        """
    if require_primary_key:
        catalog_filter += """
          AND EXISTS (
              SELECT 1 FROM target_columns AS pk
              WHERE pk.file_id = s2t.file_id
                AND TRIM(pk.table_name) = TRIM(s2t.target_table) COLLATE NOCASE
                AND pk.primary_key = 1
          )
        """
    conn = db_storage.get_db_connection()
    try:
        rows = conn.execute(
            f"""
        SELECT s2t.file_id, files.filename,
               TRIM(s2t.source_table), TRIM(s2t.target_table),
               TRIM(s2t.source_field), TRIM(s2t.target_field),
               TRIM(s2t.transformation_rule)
        FROM s2t_transformations AS s2t
        JOIN files ON files.file_id = s2t.file_id
        WHERE NULLIF(TRIM(s2t.source_table), '') IS NOT NULL
          AND NULLIF(TRIM(s2t.target_table), '') IS NOT NULL
          AND NULLIF(TRIM(s2t.source_field), '') IS NOT NULL
          AND NULLIF(TRIM(s2t.target_field), '') IS NOT NULL
          AND NULLIF(TRIM(s2t.transformation_rule), '') IS NOT NULL
          AND (
                LOWER(LTRIM(s2t.transformation_rule)) LIKE 'select%'
             OR LOWER(LTRIM(s2t.transformation_rule)) LIKE 'with%'
          )
          AND (
              SELECT COUNT(DISTINCT TRIM(pair_rule.transformation_rule))
              FROM s2t_transformations AS pair_rule
              WHERE TRIM(pair_rule.source_table) = TRIM(s2t.source_table)
                    COLLATE NOCASE
                AND TRIM(pair_rule.target_table) = TRIM(s2t.target_table)
                    COLLATE NOCASE
                AND (
                      LOWER(LTRIM(pair_rule.transformation_rule)) LIKE 'select%'
                   OR LOWER(LTRIM(pair_rule.transformation_rule)) LIKE 'with%'
                )
          ) = 1
          AND (
              SELECT COUNT(DISTINCT LOWER(TRIM(peer.target_field)))
              FROM s2t_transformations AS peer
              WHERE peer.file_id = s2t.file_id
                AND TRIM(peer.source_table) = TRIM(s2t.source_table)
                    COLLATE NOCASE
                AND TRIM(peer.target_table) = TRIM(s2t.target_table)
                    COLLATE NOCASE
                AND TRIM(peer.transformation_rule) =
                    TRIM(s2t.transformation_rule)
          ) >= {max(1, int(min_mapped_fields))}
          {expression_filter}
          {catalog_filter}
        ORDER BY LENGTH(s2t.transformation_rule), s2t.id
        """
        ).fetchall()
    finally:
        conn.close()
    if not rows:
        pytest.skip("workspace has no complete transformation fixture")
    from agents.transformation_ast import normalize_transformation

    row = None
    for candidate in rows:
        normalized = normalize_transformation(str(candidate[6]))
        if normalized.parse_status != "ok":
            continue
        if require_join and not normalized.joins:
            continue
        if require_effective_join_predicate and not any(
            join.condition and _effective_predicate_sql(join.condition)
            for join in normalized.joins
        ):
            continue
        if require_filter:
            if not any(
                _effective_predicate_sql(predicate)
                for predicate in normalized.filters
            ):
                continue
        conn = db_storage.get_db_connection()
        try:
            mappings = conn.execute(
                """
                SELECT TRIM(source_field), TRIM(target_field)
                FROM s2t_transformations
                WHERE file_id = ?
                  AND TRIM(target_table) = ? COLLATE NOCASE
                  AND TRIM(transformation_rule) = ?
                  AND NULLIF(TRIM(source_field), '') IS NOT NULL
                  AND NULLIF(TRIM(target_field), '') IS NOT NULL
                """,
                (int(candidate[0]), str(candidate[3]), str(candidate[6])),
            ).fetchall()
        finally:
            conn.close()
        outputs = {name.casefold() for name in normalized.projections}
        projected_targets = {
            str(target_field).casefold()
            for source_field, target_field in mappings
            if str(target_field).casefold() in outputs
            or str(source_field).casefold() in outputs
            or normalized.has_wildcard
        }
        if len(projected_targets) < max(1, int(min_mapped_fields)):
            continue
        if expression and not any(
            token in json.dumps(
                normalized.projections,
                ensure_ascii=False,
            ).casefold()
            for token in ("coalesce", "case", "cast")
        ):
            continue
        selected_expression = normalized.projections.get(str(candidate[5]))
        if selected_expression is None:
            selected_expression = next(
                (
                    value
                    for name, value in normalized.projections.items()
                    if name.casefold() == str(candidate[5]).casefold()
                ),
                None,
            )
        if require_explicit_selected_projection and not selected_expression:
            continue
        if require_non_column_selected_projection:
            try:
                parsed_projection = sqlglot.parse_one(
                    str(selected_expression or ""),
                    read=GREENPLUM_DIALECT,
                )
            except (TypeError, ValueError, sqlglot.errors.SqlglotError):
                continue
            if isinstance(parsed_projection, sqlglot.exp.Column):
                continue
        row = candidate
        break
    if row is None:
        fixture = "expression projection" if expression else "projection"
        pytest.skip(f"workspace has no complete {fixture} fixture")
    return _ProtocolLiveCase(
        file_id=int(row[0]),
        filename=str(row[1]),
        source_table=str(row[2]),
        target_table=str(row[3]),
        source_field=str(row[4]),
        target_field=str(row[5]),
        transformation_rule=str(row[6]),
    )


def _role_table_names(role: str) -> list[str]:
    assert role in {"source", "target"}
    column = f"{role}_table"
    conn = db_storage.get_db_connection()
    try:
        rows = conn.execute(
            f"""
            SELECT DISTINCT TRIM({column})
            FROM s2t_transformations
            WHERE NULLIF(TRIM({column}), '') IS NOT NULL
            ORDER BY TRIM({column}) COLLATE NOCASE
            """
        ).fetchall()
    finally:
        conn.close()
    names = [str(row[0]) for row in rows]
    if not names:
        pytest.skip(f"workspace has no {role} S2T table names")
    return names


def _unique_typo_case(role: str) -> tuple[str, str]:
    from agents.entity_resolution import normalize_entity_name

    names = _role_table_names(role)
    normalized_names = {
        name: normalize_entity_name(name)
        for name in names
    }
    for canonical in sorted(names, key=lambda value: (-len(value), value)):
        normalized_canonical = normalized_names[canonical]
        for index, character in enumerate(canonical):
            if not character.isalnum() or index in {0, len(canonical) - 1}:
                continue
            mention = canonical[:index] + canonical[index + 1 :]
            normalized_mention = normalize_entity_name(mention)
            if not normalized_mention or any(
                normalized_mention in candidate
                for candidate in normalized_names.values()
            ):
                continue
            scores = sorted(
                (
                    SequenceMatcher(None, normalized_mention, candidate).ratio(),
                    name,
                )
                for name, candidate in normalized_names.items()
            )
            top_score, top_name = scores[-1]
            runner_up = scores[-2][0] if len(scores) > 1 else 0.0
            if (
                top_name == canonical
                and top_score >= 0.84
                and top_score - runner_up >= 0.06
            ):
                return mention, canonical
    pytest.skip(f"workspace has no unambiguous fuzzy {role} fixture")


def _unique_partial_case(role: str) -> tuple[str, str]:
    from agents.entity_resolution import normalize_entity_name

    names = _role_table_names(role)
    normalized = {
        name: normalize_entity_name(name)
        for name in names
    }
    for canonical in sorted(names, key=lambda value: (-len(value), value)):
        for cut in range(1, max(2, len(canonical) - 4)):
            mention = canonical[:-cut]
            if not mention or not mention[-1].isalnum():
                continue
            token = normalize_entity_name(mention)
            matches = [
                name for name, value in normalized.items() if token in value
            ]
            if len(token) >= 5 and matches == [canonical]:
                return mention, canonical
    pytest.skip(f"workspace has no unique partial {role} fixture")


def _ambiguous_partial_case(role: str) -> tuple[str, list[str]]:
    from agents.entity_resolution import normalize_entity_name

    names = _role_table_names(role)
    normalized = {
        name: normalize_entity_name(name)
        for name in names
    }
    prefixes: dict[str, list[str]] = {}
    for name, value in normalized.items():
        for length in range(5, len(value)):
            prefixes.setdefault(value[:length], []).append(name)
    candidates = [
        (prefix, sorted(set(matched)))
        for prefix, matched in prefixes.items()
        if 2 <= len(set(matched)) <= 20
    ]
    if not candidates:
        pytest.skip(f"workspace has no ambiguous partial {role} fixture")
    mention, matched = max(candidates, key=lambda item: len(item[0]))
    return mention, matched


def _semantic_file_case() -> tuple[int, str, str, str, str]:
    row = _fetch_one(
        """
        SELECT file_id, filename,
               TRIM(COALESCE(NULLIF(description, ''), NULLIF(summary, ''))),
               (
                   SELECT TRIM(s2t.source_table)
                   FROM s2t_transformations AS s2t
                   WHERE s2t.file_id = files.file_id
                     AND NULLIF(TRIM(s2t.source_table), '') IS NOT NULL
                     AND NULLIF(TRIM(s2t.target_table), '') IS NOT NULL
                   ORDER BY s2t.id
                   LIMIT 1
               ),
               (
                   SELECT TRIM(s2t.target_table)
                   FROM s2t_transformations AS s2t
                   WHERE s2t.file_id = files.file_id
                     AND NULLIF(TRIM(s2t.source_table), '') IS NOT NULL
                     AND NULLIF(TRIM(s2t.target_table), '') IS NOT NULL
                   ORDER BY s2t.id
                   LIMIT 1
               )
        FROM files
        WHERE NULLIF(
            TRIM(COALESCE(NULLIF(description, ''), NULLIF(summary, ''))),
            ''
        ) IS NOT NULL
          AND EXISTS (
              SELECT 1
              FROM s2t_transformations AS s2t
              WHERE s2t.file_id = files.file_id
                AND NULLIF(TRIM(s2t.source_table), '') IS NOT NULL
                AND NULLIF(TRIM(s2t.target_table), '') IS NOT NULL
          )
          AND (
              SELECT COUNT(DISTINCT peer.file_id)
              FROM files AS peer
              WHERE LOWER(TRIM(COALESCE(
                  NULLIF(peer.description, ''), NULLIF(peer.summary, '')
              ))) = LOWER(TRIM(COALESCE(
                  NULLIF(files.description, ''), NULLIF(files.summary, '')
              )))
          ) = 1
        ORDER BY file_id
        LIMIT 1
        """
    )
    description = re.sub(r"\s+", " ", str(row[2])).strip()[:240]
    if len(description) < 12:
        pytest.skip("workspace has no meaningful semantic file description")
    return int(row[0]), str(row[1]), description, str(row[3]), str(row[4])


def _semantic_table_case(role: str) -> tuple[str, str]:
    assert role in {"source", "target"}
    catalog = f"{role}_tables"
    s2t_column = f"{role}_table"
    row = _fetch_one(
        f"""
        SELECT TRIM(catalog.table_name), TRIM(catalog.description)
        FROM {catalog} AS catalog
        WHERE NULLIF(TRIM(catalog.table_name), '') IS NOT NULL
          AND NULLIF(TRIM(catalog.description), '') IS NOT NULL
          AND EXISTS (
              SELECT 1
              FROM s2t_transformations AS s2t
              WHERE TRIM(s2t.{s2t_column}) = TRIM(catalog.table_name)
                    COLLATE NOCASE
          )
          AND (
              SELECT COUNT(DISTINCT LOWER(TRIM(peer.table_name)))
              FROM {catalog} AS peer
              WHERE LOWER(TRIM(peer.description)) =
                    LOWER(TRIM(catalog.description))
          ) = 1
        ORDER BY LENGTH(catalog.description) DESC, catalog.id
        LIMIT 1
        """
    )
    description = re.sub(r"\s+", " ", str(row[1])).strip()[:240]
    if len(description) < 12:
        pytest.skip(f"workspace has no semantic {role} table description")
    return str(row[0]), description


def _semantic_batch_column_case() -> str:
    candidate_count = int(
        _fetch_one(
            """
            SELECT COUNT(*)
            FROM (
                SELECT id FROM source_columns
                WHERE description_embedding IS NOT NULL
                UNION ALL
                SELECT id FROM target_columns
                WHERE description_embedding IS NOT NULL
            )
            """
        )[0]
    )
    if candidate_count < 2:
        pytest.skip("workspace has fewer than two semantic column candidates")
    row = _fetch_one(
        """
        SELECT TRIM(description)
        FROM (
            SELECT description, description_embedding FROM source_columns
            UNION ALL
            SELECT description, description_embedding FROM target_columns
        )
        WHERE description_embedding IS NOT NULL
          AND NULLIF(TRIM(description), '') IS NOT NULL
        ORDER BY LENGTH(TRIM(description)) DESC
        LIMIT 1
        """
    )
    description = re.sub(r"\s+", " ", str(row[0])).strip()[:240]
    if len(description) < 8:
        pytest.skip("workspace has no meaningful semantic column description")
    return description


def _s2t_work_case_fixture() -> tuple[int, str, str, str, str]:
    row = _fetch_one(
        """
        SELECT s2t.file_id, s2t.target_table, s2t.source_table,
               s2t.target_field, s2t.source_field
        FROM s2t_transformations AS s2t
        JOIN source_columns AS source_catalog
          ON source_catalog.file_id = s2t.file_id
         AND source_catalog.table_name = s2t.source_table COLLATE NOCASE
         AND source_catalog.column_name = s2t.source_field COLLATE NOCASE
        JOIN target_columns AS target_catalog
          ON target_catalog.file_id = s2t.file_id
         AND target_catalog.table_name = s2t.target_table COLLATE NOCASE
         AND target_catalog.column_name = s2t.target_field COLLATE NOCASE
        WHERE LOWER(s2t.sheet_name) = 's2t'
          AND s2t.target_table IS NOT NULL AND TRIM(s2t.target_table) <> ''
          AND s2t.source_table IS NOT NULL AND TRIM(s2t.source_table) <> ''
          AND s2t.target_field IS NOT NULL AND TRIM(s2t.target_field) <> ''
          AND s2t.source_field IS NOT NULL AND TRIM(s2t.source_field) <> ''
          AND s2t.transformation_rule IS NOT NULL
          AND source_catalog.not_null = 0
          AND target_catalog.not_null = 1
          AND source_catalog.data_type IS NOT NULL
          AND TRIM(source_catalog.data_type) <> ''
          AND target_catalog.data_type IS NOT NULL
          AND TRIM(target_catalog.data_type) <> ''
          AND LOWER(s2t.transformation_rule) LIKE '%join%'
          AND LOWER(s2t.transformation_rule) LIKE '%where%'
        ORDER BY LENGTH(s2t.transformation_rule), s2t.id
        LIMIT 1
        """
    )
    return (
        int(row[0]),
        str(row[1]),
        str(row[2]),
        str(row[3]),
        str(row[4]),
    )


def _multi_source_validation_case_fixture() -> tuple[int, str, str, str]:
    row = _fetch_one(
        """
        SELECT 2, 't_agr_dep',
               'b3050000420007_product',
               'b3050000420004_nsoadditionalinfo'
        WHERE (
            SELECT COUNT(DISTINCT LOWER(TRIM(source_table)))
            FROM s2t_transformations
            WHERE LOWER(TRIM(target_table)) = 't_agr_dep'
              AND LOWER(TRIM(source_table)) IN (
                  'b3050000420007_product',
                  'b3050000420004_nsoadditionalinfo'
              )
              AND COALESCE(TRIM(transformation_rule), '') <> ''
        ) = 2
          AND EXISTS (
              SELECT 1
              FROM target_columns
              WHERE file_id = 2
                AND LOWER(TRIM(table_name)) = 't_agr_dep'
                AND primary_key = 1
                AND not_null = 1
          )
        """
    )
    return int(row[0]), str(row[1]), str(row[2]), str(row[3])


def _multi_target_validation_case_fixture() -> tuple[int, str, str, str]:
    row = _fetch_one(
        """
        SELECT 3, 'b3050000420005_paymentdetails', 't_optn', 't_optn_type'
        WHERE (
            SELECT COUNT(DISTINCT LOWER(TRIM(target_table)))
            FROM s2t_transformations
            WHERE LOWER(TRIM(source_table)) =
                  'b3050000420005_paymentdetails'
              AND LOWER(TRIM(target_table)) IN ('t_optn', 't_optn_type')
              AND COALESCE(TRIM(transformation_rule), '') <> ''
        ) = 2
          AND (
              SELECT COUNT(DISTINCT LOWER(TRIM(table_name)))
              FROM target_columns
              WHERE file_id = 3
                AND LOWER(TRIM(table_name)) IN ('t_optn', 't_optn_type')
          ) = 2
        """
    )
    return int(row[0]), str(row[1]), str(row[2]), str(row[3])


def _independent_protocol_case_fixture() -> tuple[int, str, str]:
    row = _fetch_one(
        """
        SELECT 3, 'b3050000420007_product', 't_crncy'
        WHERE (
            SELECT COUNT(DISTINCT file_id)
            FROM s2t_transformations
            WHERE LOWER(TRIM(source_table)) = 'b3050000420007_product'
              AND LOWER(TRIM(target_table)) = 't_crncy'
        ) = 1
          AND (
            SELECT MIN(file_id)
            FROM s2t_transformations
            WHERE LOWER(TRIM(source_table)) = 'b3050000420007_product'
              AND LOWER(TRIM(target_table)) = 't_crncy'
        ) = 3
          AND (
            SELECT COUNT(DISTINCT TRIM(transformation_rule))
            FROM s2t_transformations
            WHERE LOWER(TRIM(source_table)) = 'b3050000420007_product'
              AND LOWER(TRIM(target_table)) = 't_crncy'
              AND COALESCE(TRIM(transformation_rule), '') <> ''
              AND (
                  LOWER(LTRIM(transformation_rule)) LIKE 'select%'
                  OR LOWER(LTRIM(transformation_rule)) LIKE 'with%'
              )
        ) = 1
          AND (
            SELECT COUNT(DISTINCT file_id)
            FROM s2t_transformations
            WHERE LOWER(TRIM(target_table)) = 't_crncy'
        ) = 1
          AND EXISTS (
              SELECT 1
              FROM target_columns
              WHERE file_id = 3
                AND LOWER(TRIM(table_name)) = 't_crncy'
                AND primary_key = 1
                AND not_null = 1
          )
        """
    )
    return int(row[0]), str(row[1]), str(row[2])


def _assert_s2t_work_case_execution(
    exchange: _LiveExchange,
    *,
    required_tools: set[str] | None = None,
    require_analysis: bool = False,
    max_seconds: float = 240,
    max_llm_calls: int = 100,
    max_total_tokens: int = 180_000,
) -> None:
    metrics = exchange.metrics
    allowed_tools = {
        "get_excel_row",
        "get_source_target_column_pair",
        "list_additional_objects",
        "list_column_catalog",
        "list_column_metadata",
        "list_columns",
        "list_file_sheet_headers",
        "list_source_column_catalog",
        "list_target_column_catalog",
        "list_s2t_field_mapping",
        "list_s2t_source_field",
        "list_s2t_source_table",
        "list_s2t_table_mapping",
        "list_s2t_occurrences",
        "list_s2t_target_field",
        "list_s2t_target_table",
        "list_s2t_transformations",
        "parse_sql_column_lineage",
        "parse_sql_table_lineage",
        "read_s2t_by_source_table",
        "read_s2t_by_target_table",
        "read_s2t_mapping",
        "read_s2t_source_to_target",
        "query_saved_result",
        "read_previous_result",
        "run_sql",
        "search_column_catalog",
        "search_additional_objects",
        "search_excel_values",
        "search_s2t_transformations",
        "show_plan",
        "trace_transformation_path",
    }
    tool_names = [item.name for item in metrics.tool_calls]

    assert metrics.error is None, metrics.error
    assert metrics.elapsed_seconds > 0, metrics
    assert exchange.http_elapsed_seconds > 0, metrics
    _warn_unless(
        metrics.elapsed_seconds <= max_seconds
        and exchange.http_elapsed_seconds <= max_seconds + 5,
        "efficiency",
        f"elapsed={metrics.elapsed_seconds:.3f}s exceeds budget={max_seconds}s",
    )
    _warn_unless(
        bool(tool_names),
        "efficiency",
        "scenario returned without inspecting stored data",
    )
    _warn_unless(
        set(tool_names) <= allowed_tools,
        "efficiency",
        "scenario used additional read-only tools: "
        f"{sorted(set(tool_names) - allowed_tools)}",
    )
    _warn_unless(
        set(required_tools or ()) <= set(tool_names),
        "efficiency",
        "preferred data tools were not used: "
        f"actual={tool_names}, expected={sorted(required_tools or ())}",
    )
    if require_analysis and LIVE_AGENT_MODE == "multiagent":
        _warn_unless(
            metrics.upstream_output is not None
            and str(metrics.upstream_output.get("answer") or "").strip(),
            "efficiency",
            "scenario completed without a recorded upstream analysis result",
        )
    _warn_unless(
        len(tool_names) <= 12,
        "efficiency",
        f"tool_calls={len(tool_names)} exceeds budget=12: {tool_names}",
    )
    if LIVE_AGENT_MODE == "multiagent":
        direct_pipeline = any(
            str(step.get("pipeline") or "") == "validation_protocol"
            for step in metrics.coordinator_plan
        )
        scope_pipeline = bool(
            metrics.sql_risk_operation is not None
            and str(metrics.sql_risk_operation.get("pipeline") or "")
            == "sql_risk_scope"
        )
        if scope_pipeline:
            _warn_unless(
                not metrics.coordinator_plan and not metrics.worker_tasks,
                "efficiency",
                "SQL-risk operation-scope trace contains agentic planning",
            )
        elif direct_pipeline:
            _warn_unless(
                bool(metrics.coordinator_plan) and not metrics.worker_tasks,
                "efficiency",
                "direct coordinator pipeline trace is inconsistent",
            )
        else:
            _warn_unless(
                bool(metrics.coordinator_plan)
                and 0 < len(metrics.worker_tasks) <= len(metrics.coordinator_plan),
                "efficiency",
                "coordinator/worker trace is incomplete",
            )
    else:
        _warn_unless(
            metrics.worker_tasks == [] and metrics.coordinator_plan == [],
            "efficiency",
            "single-agent run unexpectedly contains coordinator activity",
        )
    assert len(metrics.llm_calls) > 0, metrics.llm_calls
    _warn_unless(
        len(metrics.llm_calls) <= max_llm_calls,
        "efficiency",
        f"llm_calls={len(metrics.llm_calls)} exceeds budget={max_llm_calls}",
    )
    assert all(item.total_tokens > 0 for item in metrics.llm_calls), metrics.llm_calls
    assert metrics.input_tokens > 0
    assert metrics.output_tokens > 0
    assert metrics.total_tokens == metrics.input_tokens + metrics.output_tokens
    _warn_unless(
        metrics.total_tokens <= max_total_tokens,
        "efficiency",
        f"total_tokens={metrics.total_tokens} exceeds budget={max_total_tokens}",
    )


def _assert_compiled_test_protocol(
    exchange: _LiveExchange,
    *target_tables: str,
    source_tables: tuple[str, ...] = (),
    expected_pair_reads: int,
) -> None:
    answer = exchange.result.answer
    folded = answer.casefold()
    check_titles = (
        "проверка количества строк",
        "проверка уникальности ключа",
        "проверка null-rate обязательных полей",
        "проверка корректности трансформаций",
    )
    for required in (
        *check_titles,
        "цель:",
        "sql-шаблон:",
        "критерий прохождения:",
        "```sql",
        "фактические метрики не вычислялись",
    ):
        assert required in folded, answer
    expected_protocol_count = len(target_tables)
    for title in check_titles:
        assert folded.count(title) == expected_protocol_count, answer
    assert folded.count("sql-шаблон:") == 4 * expected_protocol_count, answer
    assert folded.count("```sql") == 4 * expected_protocol_count, answer
    assert "sql-шаблон не сформирован" not in folded, answer
    sql_blocks = re.findall(r"```sql\n(.*?)\n```", answer, flags=re.DOTALL)
    assert len(sql_blocks) == 4 * expected_protocol_count, answer
    for sql_template in sql_blocks:
        parseable = sql_template
        for placeholder in (
            "{{LOAD_SCOPE_PREDICATE}}",
            "{{SOURCE_SCOPE_PREDICATE}}",
            "{{TARGET_SCOPE_PREDICATE}}",
        ):
            parseable = parseable.replace(placeholder, "TRUE")
        parseable = re.sub(
            r"(?<![\w$])\$\$([A-Za-z0-9_]+)(?=\.)",
            lambda match: f'"$${match.group(1)}"',
            parseable,
        )
        statements = sqlglot.parse(parseable, read=GREENPLUM_DIALECT)
        assert len(statements) == 1, sql_template
    for source_table in source_tables:
        assert source_table.casefold() in folded, answer
    for target_table in target_tables:
        assert target_table.casefold() in folded, answer
    metrics = exchange.metrics
    assert any(
        str(step.get("pipeline") or "") == "validation_protocol"
        for step in metrics.coordinator_plan
    ), metrics.coordinator_plan
    assert not metrics.worker_tasks, metrics.worker_tasks
    tool_names = [item.name for item in metrics.tool_calls]
    assert tool_names.count("read_s2t_source_to_target") == expected_pair_reads
    assert tool_names.count("read_s2t_by_target_table") == len(target_tables)
    assert tool_names.count("list_target_column_catalog") == len(target_tables)
    display_names = [item.name for item in exchange.result.display_items]
    assert display_names.count("read_s2t_source_to_target") == len(target_tables)
    assert display_names.count("list_target_column_catalog") == len(target_tables)
    assert len(_display_payloads(exchange.result)) == 2 * len(target_tables)


_ALL_PROTOCOL_CHECKS = {
    "row_count",
    "key_uniqueness",
    "required_null_rate",
    "transformation_correctness",
    "key_reconciliation",
    "missing_rows",
    "extra_rows",
    "field_mismatch",
    "schema_compatibility",
    "expected_required_nulls",
    "aggregate_reconciliation",
    "duplicate_expected",
    "duplicate_actual",
}
_EXACT_S2T_READERS = {
    "read_s2t_source_to_target",
    "read_s2t_by_source_table",
    "read_s2t_by_target_table",
    "list_s2t_source_table",
    "list_s2t_target_table",
    "list_s2t_table_mapping",
}
_CATALOG_READERS = {
    "list_column_catalog",
    "list_source_column_catalog",
    "list_target_column_catalog",
}


def _nested_mappings(value) -> Iterator[dict]:
    if isinstance(value, dict):
        yield value
        for child in value.values():
            yield from _nested_mappings(child)
    elif isinstance(value, list):
        for child in value:
            yield from _nested_mappings(child)


def _assert_validation_pipeline(exchange: _LiveExchange) -> dict:
    metrics = exchange.metrics
    _assert_public_answer(exchange.result.answer)
    assert metrics.error is None, metrics.error
    assert not [item for item in metrics.tool_calls if item.has_error], metrics.tool_calls
    assert any(
        str(step.get("pipeline") or "") == "validation_protocol"
        for step in metrics.coordinator_plan
    ), metrics.coordinator_plan
    assert metrics.validation_protocol is not None, metrics
    assert metrics.worker_tasks == [], metrics.worker_tasks
    assert metrics.worker_routes == [], metrics.worker_routes
    assert metrics.observations == [], metrics.observations
    return dict(metrics.validation_protocol)


def _assert_agentic_pipeline(exchange: _LiveExchange) -> None:
    metrics = exchange.metrics
    _assert_public_answer(exchange.result.answer)
    assert metrics.error is None, metrics.error
    assert not [item for item in metrics.tool_calls if item.has_error], metrics.tool_calls
    assert any(
        str(step.get("pipeline") or "") == "agentic"
        for step in metrics.coordinator_plan
    ), metrics.coordinator_plan
    assert metrics.worker_tasks, metrics.worker_tasks
    assert metrics.validation_protocol is None, metrics.validation_protocol
    assert "resolve_entities" not in _tool_names(exchange), metrics.tool_calls
    assert metrics.entity_resolution == [], metrics.entity_resolution


_SQL_RISK_SCOPE_MODES = {
    "row_filtering": "row_filtering",
    "cardinality": "conditional_cardinality",
    "constraint_rejection": "nullable_constraint",
    "value_changes": "value_changes",
    "write_semantics": "write_semantics",
}


def _sql_risk_operation_scope_enabled() -> bool:
    if LIVE_AGENT_MODE != "multiagent":
        return False
    from agents.sql_risk_scope_contract import sql_risk_scope_evidence_enabled

    return sql_risk_scope_evidence_enabled()


def _assert_sql_risk_scope_pipeline(
    exchange: _LiveExchange,
    *,
    expected_execution_mode: str,
) -> dict:
    """Require internal model analysis while ordinary agentic stages stay off."""

    metrics = exchange.metrics
    _assert_public_answer(exchange.result.answer)
    assert metrics.error is None, metrics.error
    assert not [item for item in metrics.tool_calls if item.has_error], (
        metrics.tool_calls
    )
    operation = metrics.sql_risk_operation
    assert operation is not None, metrics
    assert operation.get("pipeline") == "sql_risk_scope", operation
    assert operation.get("status") == "complete", operation
    assert operation.get("execution_mode") == expected_execution_mode, (
        operation
    )
    assert operation.get("answer_source") == "sql_risk_scope_llm", operation
    assert operation.get("silent_fallback") is False, operation
    assert list(operation.get("issues") or []) == [], operation
    assessment = dict(operation.get("assessment") or {})
    assert assessment.get("status") == "complete", operation
    assert assessment.get("outcome") in {
        "risk_present",
        "risk_absent",
        "not_assessed",
    }, operation

    assert metrics.supervisor_decision is not None, metrics
    assert metrics.supervisor_decision.route == "delegate", (
        metrics.supervisor_decision
    )
    assert metrics.coordinator_plan == [], metrics.coordinator_plan
    assert metrics.worker_tasks == [], metrics.worker_tasks
    assert metrics.worker_routes == [], metrics.worker_routes
    assert metrics.observations == [], metrics.observations
    assert metrics.worker_outcomes == [], metrics.worker_outcomes
    assert metrics.validation_protocol is None, metrics.validation_protocol
    assert {item.stage for item in metrics.llm_calls} == {
        "supervisor",
        "operation_router",
        "sql_risk_scope_contract",
        "sql_risk_scope_analysis",
    }, metrics.llm_calls
    assert {item.stage for item in metrics.llm_stages} == {
        "supervisor",
        "operation_router",
        "sql_risk_scope_contract",
        "sql_risk_scope_analysis",
    }, metrics.llm_stages

    upstream = metrics.upstream_output
    assert upstream is not None, metrics
    assert upstream.get("answer_source") == "sql_risk_scope_llm", upstream
    assert str(upstream.get("answer") or "").strip(), upstream
    used_ids = list(upstream.get("used_evidence_ids") or [])
    assert used_ids and len(used_ids) == len(set(used_ids)), upstream

    reads = list(operation.get("reads") or [])
    assert reads and len(reads) == len(metrics.tool_calls), {
        "reads": reads,
        "tools": metrics.tool_calls,
    }
    assert all(read.get("status") == "complete" for read in reads), reads
    assert all(read.get("evidence_id") for read in reads), reads
    assert {
        str(read.get("evidence_id")) for read in reads
    } == set(used_ids), {"reads": reads, "upstream": upstream}
    assert [
        (read.get("tool_name"), dict(read.get("arguments") or {}))
        for read in reads
    ] == [
        (item.name, dict(item.arguments or {})) for item in metrics.tool_calls
    ], {"reads": reads, "tools": metrics.tool_calls}
    display_ids = list(upstream.get("display_evidence_ids") or [])
    assert set(display_ids).issubset(used_ids), upstream
    evidence_tool = {
        str(read.get("evidence_id")): str(read.get("tool_name"))
        for read in reads
    }
    assert metrics.display_tools == [
        evidence_tool[evidence_id] for evidence_id in display_ids
    ], {"display_ids": display_ids, "display_tools": metrics.display_tools}
    assert metrics.sql_risk_facts == [], metrics.sql_risk_facts
    return dict(operation)


def _assert_sql_risk_aspect(
    exchange: _LiveExchange,
    expected_aspect: str,
    *,
    expected_execution_mode: str | None = None,
) -> None:
    """Check either the separate scope lane or the ordinary operation skill."""

    if LIVE_AGENT_MODE != "multiagent":
        return
    expected_mode = expected_execution_mode or _SQL_RISK_SCOPE_MODES[
        expected_aspect
    ]
    if _sql_risk_operation_scope_enabled():
        assert expected_mode == _SQL_RISK_SCOPE_MODES[expected_aspect]
        _assert_sql_risk_scope_pipeline(
            exchange,
            expected_execution_mode=expected_mode,
        )
        return

    aspects_enabled = experiment_flag_enabled(
        OPERATION_SQL_RISK_ASPECTS_EXPERIMENT_ENV,
    )
    expected = [expected_aspect] if aspects_enabled else []
    routed_steps = [
        step
        for step in exchange.metrics.coordinator_plan
        if str(step.get("pipeline") or "") == "agentic"
    ]
    assert routed_steps, exchange.metrics.coordinator_plan
    actual = [list(step.get("sql_risk_aspects") or []) for step in routed_steps]
    assert actual == [expected] * len(routed_steps), {
        "expected": expected,
        "actual": actual,
        "plan": routed_steps,
    }
    assert all(
        "sql_risk_execution_mode" not in step
        and "plan_source" not in step
        and "operation_sql_risk_scope_contract" not in step
        for step in routed_steps
    ), routed_steps
    assert all(
        "Анализ SQL-рисков" in (step.get("operation_skills") or [])
        for step in routed_steps
    ), routed_steps

    from agents.operation_protocols import (
        protocol_variant_sha256,
        selected_sql_risk_protocol,
    )

    protocol = selected_sql_risk_protocol(
        os.getenv("OPERATION_SQL_RISK_PROTOCOL_EXPERIMENT")
    )
    expected_protocol = "default/current"
    expected_protocol_sha256 = None
    if protocol is not None:
        assert protocol.aspect == expected_aspect, protocol
        expected_protocol = protocol.name
        expected_protocol_sha256 = protocol_variant_sha256(protocol)
    assert all(
        step.get("operation_sql_risk_protocol") == expected_protocol
        and step.get("operation_sql_risk_protocol_sha256")
        == expected_protocol_sha256
        for step in routed_steps
    ), {
        "expected_protocol": expected_protocol,
        "expected_protocol_sha256": expected_protocol_sha256,
        "plan": routed_steps,
    }
    assert exchange.metrics.sql_risk_operation is None, (
        exchange.metrics.sql_risk_operation
    )


def _sql_risk_aspects_enabled() -> bool:
    return experiment_flag_enabled(
        OPERATION_SQL_RISK_ASPECTS_EXPERIMENT_ENV,
    )


def _assert_agentic_answer_uses_complete_evidence(
    exchange: _LiveExchange,
) -> None:
    _assert_agentic_pipeline(exchange)
    upstream = exchange.metrics.upstream_output
    assert upstream is not None, exchange.metrics
    assert str(upstream.get("answer") or "").strip(), upstream
    assert list(upstream.get("used_evidence_ids") or []), upstream
    assert exchange.metrics.worker_outcomes, exchange.metrics
    assert all(
        str(outcome.get("status") or "") == "complete"
        for outcome in exchange.metrics.worker_outcomes
    ), exchange.metrics.worker_outcomes


def _assert_exact_s2t_pair_was_read(
    exchange: _LiveExchange,
    *,
    source_table: str,
    target_table: str,
) -> list:
    calls = [
        item
        for item in exchange.metrics.tool_calls
        if item.name == "read_s2t_source_to_target"
        and str(item.arguments.get("source_table") or "").casefold()
        == source_table.casefold()
        and str(item.arguments.get("target_table") or "").casefold()
        == target_table.casefold()
    ]
    assert calls, exchange.metrics.tool_calls
    assert not [item for item in calls if item.has_error], calls
    return calls


def _assert_exact_s2t_field_pair_was_read(
    exchange: _LiveExchange,
    *,
    source_table: str,
    source_field: str,
    target_table: str,
    target_field: str,
) -> list:
    expected = {
        "source_table": source_table,
        "source_field": source_field,
        "target_table": target_table,
        "target_field": target_field,
    }
    calls = [
        item
        for item in exchange.metrics.tool_calls
        if item.name == "list_s2t_field_mapping"
        and item.arguments == expected
    ]
    assert calls, exchange.metrics.tool_calls
    assert not [item for item in calls if item.has_error], calls
    return calls


def _assert_exact_column_pair_was_read(
    exchange: _LiveExchange,
    *,
    file_id: int,
    source_table: str,
    source_field: str,
    target_table: str,
    target_field: str,
) -> None:
    pair_calls = [
        item
        for item in exchange.metrics.tool_calls
        if item.name == "get_source_target_column_pair"
        and item.arguments
        == {
            "file_id": file_id,
            "source_table": source_table,
            "source_column": source_field,
            "target_table": target_table,
            "target_column": target_field,
        }
    ]
    if pair_calls:
        assert not [item for item in pair_calls if item.has_error], pair_calls
        return

    batch_calls = [
        item
        for item in exchange.metrics.tool_calls
        if item.name == "list_column_metadata"
        and str(item.arguments.get("file_scope") or "") == str(file_id)
        and {
            value.casefold()
            for value in item.arguments.get("table_names") or []
        }
        >= {source_table.casefold(), target_table.casefold()}
    ]
    source_calls = [
        item
        for item in exchange.metrics.tool_calls
        if item.name == "list_source_column_catalog"
        and item.arguments.get("file_id") == file_id
        and str(item.arguments.get("table_name") or "").casefold()
        == source_table.casefold()
        and str(item.arguments.get("column_name") or "").casefold()
        == source_field.casefold()
    ]
    target_calls = [
        item
        for item in exchange.metrics.tool_calls
        if item.name == "list_target_column_catalog"
        and item.arguments.get("file_id") == file_id
        and str(item.arguments.get("table_name") or "").casefold()
        == target_table.casefold()
        and str(item.arguments.get("column_name") or "").casefold()
        == target_field.casefold()
    ]
    assert batch_calls or (source_calls and target_calls), (
        exchange.metrics.tool_calls
    )


def _assert_no_sql_risk_route(exchange: _LiveExchange) -> None:
    if LIVE_AGENT_MODE != "multiagent":
        return
    routed_steps = [
        step
        for step in exchange.metrics.coordinator_plan
        if str(step.get("pipeline") or "") == "agentic"
    ]
    assert routed_steps, exchange.metrics.coordinator_plan
    assert all(
        "Анализ SQL-рисков" not in (step.get("operation_skills") or [])
        and list(step.get("sql_risk_aspects") or []) == []
        for step in routed_steps
    ), routed_steps


def _trace_values(trace: dict, key: str) -> list:
    return [
        mapping[key]
        for mapping in _nested_mappings(trace)
        if key in mapping
    ]


def _assert_protocol_mode(trace: dict, expected: str) -> None:
    modes = {
        str(value).casefold()
        for value in _trace_values(trace, "mode")
        if value is not None
    }
    assert expected in modes, {"expected": expected, "modes": sorted(modes)}


def _protocol_check_records(trace: dict, kind: str) -> list[dict]:
    records: list[dict] = []
    for mapping in _nested_mappings(trace):
        discriminator = next(
            (
                mapping.get(key)
                for key in ("kind", "check", "check_id", "name")
                if mapping.get(key) is not None
            ),
            None,
        )
        if str(discriminator or "").casefold() == kind.casefold():
            records.append(mapping)
        checks = mapping.get("checks")
        if isinstance(checks, dict) and kind in checks:
            child = checks[kind]
            records.append(child if isinstance(child, dict) else {"status": child})
        elif isinstance(checks, list) and kind in checks:
            records.append({"kind": kind, "status": "selected"})
    return records


def _assert_protocol_check(
    trace: dict,
    kind: str,
    *,
    statuses: set[str] | None = None,
) -> dict:
    records = _protocol_check_records(trace, kind)
    assert records, {"missing_check": kind, "trace": trace}
    if statuses is None:
        return records[-1]
    matching = [
        record
        for record in records
        if str(record.get("status") or "").casefold() in statuses
    ]
    assert matching, {
        "check": kind,
        "expected_statuses": sorted(statuses),
        "records": records,
    }
    return matching[-1]


def _assert_protocol_status(trace: dict, *expected: str) -> str:
    status = str(trace.get("status") or "").casefold()
    assert status in set(expected), {"status": status, "trace": trace}
    return status


def _assert_protocol_phases(trace: dict, expected: set[int]) -> None:
    phases = {
        int(value)
        for value in _trace_values(trace, "phase")
        if isinstance(value, int) or str(value).isdigit()
    }
    assert expected <= phases, {"expected": sorted(expected), "phases": sorted(phases)}


def _trace_target(trace: dict, target_table: str) -> dict:
    targets = [
        mapping
        for mapping in _nested_mappings(trace.get("targets", []))
        if str(mapping.get("target_table") or "").casefold()
        == target_table.casefold()
    ]
    assert targets, {"target_table": target_table, "trace": trace}
    return targets[0]


def _trace_issue_codes(trace: dict) -> set[str]:
    issues = trace.get("issues")
    if not isinstance(issues, list):
        return set()
    return {
        str(issue.get("code") or "")
        for issue in issues
        if isinstance(issue, dict) and issue.get("code")
    }


def _assert_protocol_sql_parseable(
    exchange: _LiveExchange,
    *,
    minimum_blocks: int = 1,
) -> list[str]:
    sql_blocks = re.findall(
        r"```sql\s*\n(.*?)\n```",
        exchange.result.answer,
        flags=re.DOTALL | re.IGNORECASE,
    )
    executable_blocks = [
        block
        for block in sql_blocks
        if not block.lstrip().startswith("-- SQL-шаблон не сформирован:")
        and re.search(r"\b(select|with)\b", block, re.I)
    ]
    assert len(executable_blocks) >= minimum_blocks, exchange.result.answer
    for sql_template in executable_blocks:
        parseable = sql_template
        for placeholder in (
            "{{LOAD_SCOPE_PREDICATE}}",
            "{{SOURCE_SCOPE_PREDICATE}}",
            "{{TARGET_SCOPE_PREDICATE}}",
        ):
            parseable = parseable.replace(placeholder, "TRUE")
        parseable = re.sub(
            r"(?<![\w$])\$\$([A-Za-z0-9_]+)(?=\.)",
            lambda match: f'"$${match.group(1)}"',
            parseable,
        )
        statements = sqlglot.parse(parseable, read=GREENPLUM_DIALECT)
        assert len(statements) == 1 and statements[0] is not None, sql_template
    return executable_blocks


def _resolution_events(exchange: _LiveExchange) -> list[dict]:
    events: list[dict] = []
    seen: set[str] = set()
    for mapping in _nested_mappings(exchange.metrics.entity_resolution):
        required = {"mention", "role", "status", "method"}
        if not required <= set(mapping):
            continue
        serialized = json.dumps(mapping, ensure_ascii=False, sort_keys=True)
        if serialized not in seen:
            seen.add(serialized)
            events.append(mapping)
    return events


def _resolution_event(
    exchange: _LiveExchange,
    *,
    mention: str,
    role: str,
) -> dict:
    from agents.entity_resolution import normalize_entity_name

    normalized_mention = normalize_entity_name(mention)
    exact = [
        event
        for event in _resolution_events(exchange)
        if normalize_entity_name(event.get("mention")) == normalized_mention
        and str(event.get("role") or "").casefold() == role.casefold()
    ]
    assert exact, {
        "mention": mention,
        "role": role,
        "events": _resolution_events(exchange),
    }
    return exact[-1]


def _candidate_names(event: dict) -> list[str]:
    candidate_set = event.get("candidate_set")
    assert isinstance(candidate_set, dict), event
    candidates = candidate_set.get("candidates")
    assert isinstance(candidates, list), candidate_set
    return [
        str(candidate.get("canonical_name") or "")
        for candidate in candidates
        if isinstance(candidate, dict) and candidate.get("canonical_name")
    ]


def _assert_resolved_event(
    exchange: _LiveExchange,
    *,
    mention: str,
    role: str,
    canonical: str,
    method: str,
) -> dict:
    event = _resolution_event(exchange, mention=mention, role=role)
    assert event.get("status") == "resolved", event
    assert event.get("method") == method, event
    assert str(event.get("canonical_name") or "").casefold() == canonical.casefold(), event
    assert str(event.get("role") or "") == role, event
    assert canonical.casefold() in {
        name.casefold() for name in _candidate_names(event)
    }, event
    return event


def _tool_names(exchange: _LiveExchange) -> list[str]:
    return [item.name for item in exchange.metrics.tool_calls]


def _assert_no_worker_reroute(exchange: _LiveExchange) -> None:
    reroutes = [
        item
        for item in exchange.metrics.worker_routes
        if item.routing_attempt > 1
    ]
    assert reroutes == [], reroutes


def _assert_model_owned_table_candidate_retrieval(
    exchange: _LiveExchange,
) -> None:
    candidate_readers = {
        "list_s2t_table_names",
        "search_s2t_transformations",
        "semantic_search_descriptions",
    }
    assert candidate_readers & set(_tool_names(exchange)), exchange.metrics.tool_calls


def _assert_exact_reader_uses_canonical(
    exchange: _LiveExchange,
    *,
    canonical: str,
    role: str,
    rejected_mention: str | None = None,
) -> None:
    assert role in {"source", "target"}
    argument_name = f"{role}_table"
    role_readers = {
        "source": {
            "read_s2t_source_to_target",
            "read_s2t_by_source_table",
            "list_s2t_source_table",
            "list_s2t_table_mapping",
        },
        "target": {
            "read_s2t_source_to_target",
            "read_s2t_by_target_table",
            "list_s2t_target_table",
            "list_s2t_table_mapping",
        },
    }[role]
    exact_calls = [
        item
        for item in exchange.metrics.tool_calls
        if item.name in role_readers
        and str(item.arguments.get(argument_name) or "").casefold()
        == canonical.casefold()
    ]
    assert exact_calls, exchange.metrics.tool_calls
    if rejected_mention is not None:
        assert not [
            item
            for item in exchange.metrics.tool_calls
            if item.name in role_readers
            and str(item.arguments.get(argument_name) or "").casefold()
            == rejected_mention.casefold()
        ], exchange.metrics.tool_calls


@pytest.mark.live_smoke
def test_live_agent_answers_simple_conversation_without_display_results(
    live_chat_client,
):
    exchange = _chat(live_chat_client, "Ответь одним словом: привет")
    result = exchange.result

    _assert_public_answer(result.answer)
    _warn_unless(
        result.display_items == [],
        "presentation",
        "simple conversation returned unexpected display items",
    )
    _assert_execution(
        exchange,
        expected_tools=[],
        expected_displays=[],
        max_seconds=None,
        max_llm_calls=3,
        max_total_tokens=15_000,
    )


@pytest.mark.live_smoke
def test_live_agent_returns_exact_global_sqlite_count(live_chat_client):
    expected_count = int(
        _fetch_one("SELECT COUNT(*) FROM s2t_transformations")[0]
    )
    exchange = _chat(
        live_chat_client,
        "Через SQLite посчитай точное число строк в s2t_transformations. "
        "Нужен только итоговый count."
    )
    result = exchange.result

    _assert_public_answer(result.answer)
    assert re.findall(r"(?<!\w)\d+(?!\w)", result.answer) == [
        str(expected_count)
    ], result.answer
    _warn_unless(
        len(result.answer) <= 220,
        "presentation",
        f"count-only answer is too verbose: {len(result.answer)} chars",
    )
    assert result.display_items == [], result.display_items
    assert exchange.metrics.display_tools == [], exchange.metrics.display_tools
    run_sql_calls = [
        item for item in exchange.metrics.tool_calls if item.name == "run_sql"
    ]
    assert len(run_sql_calls) == 1, exchange.metrics.tool_calls
    assert not run_sql_calls[0].has_error, run_sql_calls[0]
    assert len(exchange.metrics.tool_calls) == 1, exchange.metrics.tool_calls
    assert set(run_sql_calls[0].arguments) == {"query"}, run_sql_calls[0]
    statement = sqlglot.parse_one(
        str(run_sql_calls[0].arguments["query"]),
        read="sqlite",
    )
    assert isinstance(statement, sqlglot.exp.Select), statement
    assert [
        table.name.casefold()
        for table in statement.find_all(sqlglot.exp.Table)
    ] == ["s2t_transformations"], statement
    counts = list(statement.find_all(sqlglot.exp.Count))
    assert len(counts) == 1 and isinstance(counts[0].this, sqlglot.exp.Star), statement
    assert statement.args.get("where") is None, statement
    assert statement.args.get("group") is None, statement
    assert statement.args.get("limit") is None, statement
    assert not [
        item
        for item in exchange.metrics.tool_calls
        if item.name
        in {
            "run_cypher",
            "trace_neo4j_table_path",
            "trace_transformation_path",
        }
    ], exchange.metrics.tool_calls
    _assert_execution(
        exchange,
        expected_tools=["run_sql"],
        expected_displays=[],
        max_seconds=90,
        max_llm_calls=12,
        max_total_tokens=60_000,
    )


@pytest.mark.live_history
def test_live_agent_resolves_history_reference_into_task(
    live_chat_client,
):
    expected_count = int(
        _fetch_one("SELECT COUNT(*) FROM s2t_transformations")[0]
    )
    history = [
        {
            "role": "user",
            "content": "Речь о физической SQLite-таблице s2t_transformations.",
        },
        {
            "role": "assistant",
            "content": "Таблица s2t_transformations зафиксирована.",
        },
    ]
    exchange = _chat(
        live_chat_client,
        "Через SQLite посчитай в ней точное количество строк. Только число.",
        history=history,
    )
    result = exchange.result
    supervisor_decision = exchange.metrics.supervisor_decision

    _assert_public_answer(result.answer)
    assert re.findall(r"(?<!\w)\d+(?!\w)", result.answer) == [
        str(expected_count)
    ], result.answer
    assert supervisor_decision is not None
    assert supervisor_decision.route == "delegate", supervisor_decision
    assert "s2t_transformations" in supervisor_decision.resolved_references
    assert supervisor_decision.context == "", supervisor_decision
    _warn_unless(
        result.display_items == [],
        "presentation",
        "history-resolution response returned unexpected display items",
    )
    _assert_execution(
        exchange,
        expected_tools=[{"run_sql", "list_s2t_transformations"}],
        expected_displays=[],
        max_seconds=90,
        max_llm_calls=12,
        max_total_tokens=60_000,
    )


@pytest.mark.live_history
@pytest.mark.skipif(
    LIVE_AGENT_MODE != "multiagent",
    reason="scenario verifies the multiagent supervisor",
)
def test_live_agent_asks_when_history_reference_is_ambiguous(
    live_chat_client,
):
    exchange = _chat(
        live_chat_client,
        "Через SQLite посчитай в ней точное количество строк. Только число.",
        history=[
            {
                "role": "user",
                "content": (
                    "Для следующего шага рассматриваю две физические "
                    "SQLite-таблицы: source_tables и target_tables. "
                    "Конкретную пока не выбрал."
                ),
            },
            {
                "role": "assistant",
                "content": "Понял, выбор между двумя таблицами ещё не сделан.",
            },
        ],
    )

    _assert_supervisor_clarification(exchange)


@pytest.mark.live_history
@pytest.mark.skipif(
    LIVE_AGENT_MODE != "multiagent",
    reason="scenario verifies the multiagent supervisor",
)
def test_live_agent_rejects_assistant_only_history_assumption(
    live_chat_client,
):
    exchange = _chat(
        live_chat_client,
        "Через SQLite посчитай в ней точное количество строк. Только число.",
        history=[
            {
                "role": "user",
                "content": (
                    "Не выбирай за меня физическую SQLite-таблицу: "
                    "я назову её сам позже."
                ),
            },
            {
                "role": "assistant",
                "content": "Буду считать, что речь о source_tables.",
            },
        ],
    )

    _assert_supervisor_clarification(exchange)


@pytest.mark.live_history
@pytest.mark.skipif(
    LIVE_AGENT_MODE != "multiagent",
    reason="scenario verifies the multiagent supervisor",
)
def test_live_agent_uses_latest_user_history_rule(
    live_chat_client,
):
    expected_count = int(_fetch_one("SELECT COUNT(*) FROM target_tables")[0])
    exchange = _chat(
        live_chat_client,
        "Через SQLite посчитай точное количество строк в рабочем каталоге. "
        "Только число.",
        history=[
            {
                "role": "user",
                "content": (
                    "Для следующих задач «рабочий каталог» = source_tables."
                ),
            },
            {
                "role": "assistant",
                "content": "Принял это определение рабочего каталога.",
            },
            {
                "role": "user",
                "content": (
                    "Отменяю прежнее правило. Теперь для следующих задач "
                    "«рабочий каталог» = target_tables."
                ),
            },
            {
                "role": "assistant",
                "content": "Принял новое определение рабочего каталога.",
            },
        ],
    )
    result = exchange.result
    metrics = exchange.metrics
    supervisor_decision = metrics.supervisor_decision

    _assert_public_answer(result.answer)
    assert re.findall(r"(?<!\w)\d+(?!\w)", result.answer) == [
        str(expected_count)
    ], result.answer
    assert result.display_items == [], result.display_items

    assert supervisor_decision is not None
    assert supervisor_decision.route == "delegate", supervisor_decision
    assert supervisor_decision.resolved_references == "", supervisor_decision
    stable_context = supervisor_decision.context.casefold()
    assert "target_tables" in stable_context, supervisor_decision
    assert "source_tables" not in stable_context, supervisor_decision
    assert "source_tables" not in "\n".join(metrics.worker_tasks).casefold(), (
        metrics.worker_tasks
    )

    sql_calls = [item for item in metrics.tool_calls if item.name == "run_sql"]
    assert sql_calls, metrics.tool_calls
    assert "source_tables" not in json.dumps(
        [item.arguments for item in metrics.tool_calls],
        ensure_ascii=False,
    ).casefold(), metrics.tool_calls
    queried_tables: set[str] = set()
    for call in sql_calls:
        query = str(call.arguments.get("query") or "")
        statement = sqlglot.parse_one(query, read="sqlite")
        queried_tables.update(
            table.name.casefold()
            for table in statement.find_all(sqlglot.exp.Table)
        )
    assert "target_tables" in queried_tables, queried_tables
    assert "source_tables" not in queried_tables, queried_tables
    _assert_execution(
        exchange,
        expected_tools=["run_sql"],
        expected_displays=[],
        max_seconds=90,
        max_llm_calls=12,
        max_total_tokens=60_000,
    )


@pytest.mark.live_display
def test_live_agent_selects_full_sql_result_for_scrollable_ui(
    live_chat_client,
    generated_sql_exports,
):
    conn = db_storage.get_db_connection()
    try:
        expected_rows = [
            {"file_id": row[0], "filename": row[1]}
            for row in conn.execute(
                "SELECT file_id, filename FROM files ORDER BY file_id"
            ).fetchall()
        ]
    finally:
        conn.close()
    if not expected_rows:
        pytest.skip("workspace files table is empty")

    exchange = _chat(
        live_chat_client,
        "Через SQLite выполни SELECT file_id, filename FROM files "
        "ORDER BY file_id и покажи полный результат отдельно в scrollable UI."
    )
    result = exchange.result

    _assert_public_answer(result.answer)
    _warn_unless(
        len(result.answer) <= 500,
        "presentation",
        f"SQL summary is too verbose: {len(result.answer)} chars",
    )
    assert result.display_items, "full SQL result was not selected for display"
    assert exchange.metrics.display_tools == ["run_sql"], (
        exchange.metrics.display_tools
    )
    matching_payloads = [
        payload
        for payload in _display_payloads(result)
        if payload.get("preview_rows") == expected_rows
        or payload.get("rows") == expected_rows
    ]
    assert len(matching_payloads) == 1, matching_payloads
    payload = matching_payloads[0]
    assert payload.get("returned_rows") == len(expected_rows), payload
    assert payload.get("truncated") is False, payload
    if payload.get("csv_url"):
        assert payload.get("preview_rows") == expected_rows, payload
        downloaded_rows = _download_sql_export(
            live_chat_client,
            payload,
            generated_sql_exports,
        )
        assert downloaded_rows == [
            {key: str(value) for key, value in row.items()}
            for row in expected_rows
        ], downloaded_rows
    else:
        assert payload.get("rows") == expected_rows, payload
    run_sql_calls = [
        item for item in exchange.metrics.tool_calls if item.name == "run_sql"
    ]
    assert len(run_sql_calls) == 1, exchange.metrics.tool_calls
    assert not run_sql_calls[0].has_error, run_sql_calls[0]
    assert len(exchange.metrics.tool_calls) == 1, exchange.metrics.tool_calls
    assert set(run_sql_calls[0].arguments) == {"query"}, run_sql_calls[0]
    actual_statement = sqlglot.parse_one(
        str(run_sql_calls[0].arguments["query"]),
        read="sqlite",
    )
    expected_statement = sqlglot.parse_one(
        "SELECT file_id, filename FROM files ORDER BY file_id",
        read="sqlite",
    )
    assert actual_statement == expected_statement, actual_statement
    assert not [
        item
        for item in exchange.metrics.tool_calls
        if item.name
        in {
            "run_cypher",
            "trace_neo4j_table_path",
            "trace_transformation_path",
        }
    ], exchange.metrics.tool_calls
    _assert_execution(
        exchange,
        expected_tools=["run_sql"],
        expected_displays=["run_sql"],
        max_seconds=90,
        max_llm_calls=12,
        max_total_tokens=60_000,
    )


@pytest.mark.live_handoff
def test_live_agent_runs_dependent_workers_sequentially(
    live_chat_client,
):
    target_table = _fetch_one(
        """
        SELECT target_table
        FROM s2t_transformations
        WHERE target_table IS NOT NULL AND TRIM(target_table) <> ''
        GROUP BY target_table
        ORDER BY COUNT(*) DESC, target_table
        LIMIT 1
        """
    )[0]
    source_count = int(
        _fetch_one(
            """
            SELECT COUNT(DISTINCT source_table)
            FROM s2t_transformations
            WHERE target_table = ?
              AND source_table IS NOT NULL
              AND TRIM(source_table) <> ''
            """,
            (target_table,),
        )[0]
    )
    target_row_count = int(
        _fetch_one(
            """
            SELECT COUNT(*)
            FROM s2t_transformations
            WHERE target_table = ?
            """,
            (target_table,),
        )[0]
    )

    exchange = _chat(
        live_chat_client,
        "Через SQLite сначала найди target_table с максимальным числом строк "
        "в s2t_transformations. Затем отдельным зависимым шагом для найденной "
        "target_table посчитай точное число различных непустых source_table. "
        "Верни одной строкой строго target_table=<имя>, "
        "row_count=<число>, source_count=<число>. "
        "Полный результат второго шага покажи отдельно.",
    )
    result = exchange.result

    _assert_public_answer(result.answer)
    _assert_named_answer_value(result.answer, "target_table", target_table)
    _assert_named_answer_value(result.answer, "row_count", target_row_count)
    _assert_named_answer_value(result.answer, "source_count", source_count)
    payloads = _display_payloads(result)
    assert any(
        _payload_contains_value(payload, target_table)
        and _payload_contains_value(payload, source_count)
        for payload in payloads
    ), {
        "expected_target_table": target_table,
        "expected_source_count": source_count,
        "display_payloads": payloads,
    }
    run_sql_calls = [
        item for item in exchange.metrics.tool_calls if item.name == "run_sql"
    ]
    assert len(run_sql_calls) == 2, exchange.metrics.tool_calls
    assert not [item for item in run_sql_calls if item.has_error], run_sql_calls
    recorded_plan = [
        step
        for step in exchange.metrics.coordinator_plan
        if str(step.get("pipeline") or "") == "agentic"
    ]
    assert len(recorded_plan) == 2, recorded_plan
    assert recorded_plan[0]["depends_on"] == [], recorded_plan
    assert recorded_plan[1]["depends_on"] == [
        recorded_plan[0]["id"]
    ], recorded_plan
    assert len(exchange.metrics.worker_tasks) == 2, exchange.metrics.worker_tasks
    assert all(
        str(outcome.get("status") or "") == "complete"
        for outcome in exchange.metrics.worker_outcomes
    ), exchange.metrics.worker_outcomes
    assert (
        any(
            _payload_contains_value(payload, source_count)
            for payload in payloads
        )
    ), payloads
    _assert_execution(
        exchange,
        expected_tools=["run_sql", "run_sql"],
        expected_displays=["run_sql"],
        max_seconds=150,
        max_llm_calls=20,
        max_total_tokens=120_000,
    )


def _two_hop_neo4j_path() -> list[str]:
    from graph_storage import execute_neo4j_read

    try:
        rows = execute_neo4j_read(
            """
            MATCH path=(source:ETLProjection:ETLTable)
                  -[:TABLE_TRANSFORMS_TO*2]->
                  (target:ETLProjection:ETLTable)
            WITH [node IN nodes(path) | node.name] AS names
            WHERE all(name IN names WHERE name IS NOT NULL AND trim(name) <> '')
            RETURN names
            LIMIT 1
            """,
            {},
        )
    except Exception as exc:
        pytest.skip(f"Neo4j is unavailable: {type(exc).__name__}")
    if not rows:
        pytest.skip("Neo4j has no two-hop ETLTable path")
    names = [str(value) for value in rows[0].get("names") or []]
    if len(names) != 3:
        pytest.skip("Neo4j path fixture did not return exactly three nodes")
    return names


def _neo4j_path(edge_count: int) -> list[str]:
    from graph_storage import execute_neo4j_read

    try:
        rows = execute_neo4j_read(
            f"""
            MATCH path=(source:ETLProjection:ETLTable)
                  -[:TABLE_TRANSFORMS_TO*{edge_count}]->
                  (target:ETLProjection:ETLTable)
            WITH DISTINCT [node IN nodes(path) | node.name] AS names
            WHERE all(name IN names WHERE name IS NOT NULL AND trim(name) <> '')
            RETURN names
            LIMIT 1
            """,
            {},
        )
    except Exception as exc:
        pytest.skip(f"Neo4j is unavailable: {type(exc).__name__}")
    if not rows:
        pytest.skip(f"Neo4j has no {edge_count}-edge ETLTable path")
    names = [str(value) for value in rows[0].get("names") or []]
    if len(names) != edge_count + 1:
        pytest.skip("Neo4j path fixture returned an unexpected node count")
    return names


def _neo4j_paths_between(
    source: str,
    target: str,
    edge_count: int,
) -> list[list[str]]:
    from graph_storage import execute_neo4j_read

    rows = execute_neo4j_read(
        f"""
        MATCH path=(source:ETLProjection:ETLTable {{name: $source}})
              -[:TABLE_TRANSFORMS_TO*{edge_count}]->
              (target:ETLProjection:ETLTable {{name: $target}})
        WITH DISTINCT [node IN nodes(path) | node.name] AS names
        WHERE all(name IN names WHERE name IS NOT NULL AND trim(name) <> '')
        RETURN names
        ORDER BY names
        LIMIT 20
        """,
        {"source": source, "target": target},
    )
    paths = [
        [str(value) for value in row.get("names") or []]
        for row in rows
    ]
    return [path for path in paths if len(path) == edge_count + 1]


@pytest.mark.live_graph
def test_live_agent_returns_exact_neo4j_path_and_full_result(live_chat_client):
    source, middle, target = _two_hop_neo4j_path()
    exchange = _chat(
        live_chat_client,
        f"Через Neo4j найди точный путь длины 2 от таблицы {source} "
        f"до таблицы {target}. Не используй SQLite. Покажи только все узлы "
        "по порядку и глубину, а полный результат инструмента — отдельно."
    )
    result = exchange.result

    _assert_public_answer(result.answer)
    _warn_unless(
        "sqlite" not in result.answer.lower(),
        "presentation",
        "Neo4j-only answer mentions SQLite",
    )
    display_contents = [item.content for item in result.display_items]
    _warn_unless(
        any(
            all(table_name in content for table_name in (source, middle, target))
            for content in display_contents
        ),
        "presentation",
        "Neo4j display does not contain the complete two-edge path",
    )
    _assert_execution(
        exchange,
        expected_tools=[{"run_cypher", "trace_neo4j_table_path"}],
        expected_displays=[{"run_cypher", "trace_neo4j_table_path"}],
        forbidden_tools={"run_sql"},
        max_seconds=150,
        max_llm_calls=12,
        max_total_tokens=80_000,
    )


@pytest.mark.live_graph
def test_live_agent_returns_complete_three_edge_neo4j_path(live_chat_client):
    fixture_path = _neo4j_path(3)
    source, target = fixture_path[0], fixture_path[-1]
    expected_paths = _neo4j_paths_between(source, target, 3)
    if not expected_paths:
        pytest.skip("Neo4j has no stable three-edge path for the selected endpoints")
    exchange = _chat(
        live_chat_client,
        f"Через Neo4j найди полный точный направленный путь длины 3 от таблицы "
        f"{source} до таблицы {target}. Не используй SQLite. В ответе покажи "
        "только все четыре узла по порядку и глубину. Полный результат со всеми "
        "шагами пути покажи отдельно в scrollable UI."
    )
    result = exchange.result

    _assert_public_answer(result.answer)
    lowered = result.answer.lower()
    _warn_unless(
        "sqlite" not in lowered,
        "presentation",
        "Neo4j-only answer mentions SQLite",
    )
    _warn_unless(
        "трансформац" not in lowered and "mapping" not in lowered,
        "presentation",
        "path-only answer includes transformation commentary",
    )
    matching_paths = expected_paths

    path_payloads = _display_payloads(result)
    _warn_unless(
        any(
            any(
                expected_path in _payload_table_paths(payload)
                for expected_path in matching_paths
            )
            and _payload_contains_value(payload, 3)
            for payload in path_payloads
        ),
        "presentation",
        "Neo4j display does not contain a complete three-edge path",
    )
    _assert_execution(
        exchange,
        expected_tools=[{"run_cypher", "trace_neo4j_table_path"}],
        expected_displays=[{"run_cypher", "trace_neo4j_table_path"}],
        forbidden_tools={"run_sql"},
        max_seconds=180,
        max_llm_calls=12,
        max_total_tokens=100_000,
    )


@pytest.mark.live_display
def test_live_agent_preserves_exact_s2t_pairs_in_answer_and_full_result(
    live_chat_client,
    generated_sql_exports,
):
    query = (
        "SELECT source_table, source_field, target_table, target_field "
        "FROM s2t_transformations "
        "WHERE source_table IS NOT NULL AND TRIM(source_table) <> '' "
        "AND source_field IS NOT NULL AND TRIM(source_field) <> '' "
        "AND target_table IS NOT NULL AND TRIM(target_table) <> '' "
        "AND target_field IS NOT NULL AND TRIM(target_field) <> '' "
        "ORDER BY id LIMIT 4"
    )
    conn = db_storage.get_db_connection()
    try:
        expected_rows = [dict(row) for row in conn.execute(query).fetchall()]
    finally:
        conn.close()
    if len(expected_rows) != 4:
        pytest.skip("workspace database has fewer than four complete S2T pairs")

    exchange = _chat(
        live_chat_client,
        f"Через SQLite выполни ровно этот read-only запрос: {query}. "
        "Перечисли все 4 точные пары source_table.source_field -> "
        "target_table.target_field, не разделяя связанные стороны на отдельные "
        "списки. Полный табличный результат покажи отдельно в scrollable UI."
    )
    result = exchange.result

    _assert_public_answer(result.answer)
    answer_without_quotes = re.sub(r"[`\"']", "", result.answer).casefold()
    qualified_name = r"(?:[a-z_][a-z0-9_$]*\.)+[a-z_][a-z0-9_$]*"
    actual_pairs = re.findall(
        rf"({qualified_name})\s*(?:→|->|=>)\s*({qualified_name})",
        answer_without_quotes,
    )
    expected_pairs = [
        (
            f"{row['source_table']}.{row['source_field']}".casefold(),
            f"{row['target_table']}.{row['target_field']}".casefold(),
        )
        for row in expected_rows
    ]
    assert sorted(actual_pairs) == sorted(expected_pairs), {
        "expected": expected_pairs,
        "actual": actual_pairs,
        "answer": result.answer,
    }
    assert exchange.metrics.display_tools == ["run_sql"], (
        exchange.metrics.display_tools
    )
    matching_payloads = [
        payload
        for payload in _display_payloads(result)
        if payload.get("preview_rows") == expected_rows
        or payload.get("rows") == expected_rows
    ]
    assert len(matching_payloads) == 1, matching_payloads
    payload = matching_payloads[0]
    assert payload.get("returned_rows") == len(expected_rows), payload
    assert payload.get("truncated") is False, payload
    if payload.get("csv_url"):
        assert payload.get("preview_rows") == expected_rows, payload
        downloaded_rows = _download_sql_export(
            live_chat_client,
            payload,
            generated_sql_exports,
        )
        assert downloaded_rows == [
            {key: str(value) for key, value in row.items()}
            for row in expected_rows
        ], downloaded_rows
    else:
        assert payload.get("rows") == expected_rows, payload
    run_sql_calls = [
        item for item in exchange.metrics.tool_calls if item.name == "run_sql"
    ]
    assert len(run_sql_calls) == 1, exchange.metrics.tool_calls
    assert not run_sql_calls[0].has_error, run_sql_calls[0]
    assert len(exchange.metrics.tool_calls) == 1, exchange.metrics.tool_calls
    assert set(run_sql_calls[0].arguments) == {"query"}, run_sql_calls[0]
    actual_statement = sqlglot.parse_one(
        str(run_sql_calls[0].arguments["query"]),
        read="sqlite",
    )
    expected_statement = sqlglot.parse_one(query, read="sqlite")
    assert actual_statement == expected_statement, actual_statement
    assert not [
        item
        for item in exchange.metrics.tool_calls
        if item.name
        in {
            "run_cypher",
            "trace_neo4j_table_path",
            "trace_transformation_path",
        }
    ], exchange.metrics.tool_calls
    _assert_execution(
        exchange,
        expected_tools=["run_sql"],
        expected_displays=["run_sql"],
        max_seconds=150,
        max_llm_calls=20,
        max_total_tokens=100_000,
    )


@pytest.mark.live_display
def test_live_agent_returns_compound_sqlite_summary(
    live_chat_client,
):
    target_table, target_row_count = _fetch_one(
        """
        SELECT target_table, COUNT(*) AS row_count
        FROM s2t_transformations
        WHERE target_table IS NOT NULL AND TRIM(target_table) <> ''
        GROUP BY target_table
        ORDER BY COUNT(*) DESC, target_table
        LIMIT 1
        """
    )
    source_count = int(
        _fetch_one(
            """
            SELECT COUNT(DISTINCT source_table)
            FROM s2t_transformations
            WHERE target_table = ?
              AND source_table IS NOT NULL
              AND TRIM(source_table) <> ''
            """,
            (target_table,),
        )[0]
    )
    top_source, top_source_count = _fetch_one(
        """
        SELECT source_table, COUNT(*) AS row_count
        FROM s2t_transformations
        WHERE target_table = ?
          AND source_table IS NOT NULL
          AND TRIM(source_table) <> ''
        GROUP BY source_table
        ORDER BY row_count DESC, source_table
        LIMIT 1
        """,
        (target_table,),
    )

    exchange = _chat(
        live_chat_client,
        "Через SQLite составь сводку для target_table с наибольшим числом строк "
        "в s2t_transformations: имя и число строк target_table, число различных "
        "непустых source_table, а также самый частый source_table и число его "
        "строк. При равенстве выбери лексикографически первый source_table. "
        "Верни одной строкой строго target_table=<имя>, row_count=<число>, "
        "source_count=<число>, top_source=<имя>, "
        "top_source_count=<число>. Полную сводку с этими же пятью колонками "
        "покажи отдельно.",
    )
    result = exchange.result

    _assert_public_answer(result.answer)
    expected_summary = {
        "target_table": target_table,
        "row_count": int(target_row_count),
        "source_count": source_count,
        "top_source": top_source,
        "top_source_count": int(top_source_count),
    }
    for name, value in expected_summary.items():
        _assert_named_answer_value(result.answer, name, value)
    assert exchange.metrics.display_tools == ["run_sql"], (
        exchange.metrics.display_tools
    )
    matching_payloads = []
    for payload in _display_payloads(result):
        rows = payload.get("preview_rows") or payload.get("rows") or []
        if rows == [expected_summary]:
            matching_payloads.append(payload)
    assert len(matching_payloads) == 1, _display_payloads(result)
    payload = matching_payloads[0]
    assert payload.get("returned_rows") == 1, payload
    assert payload.get("truncated") is False, payload

    run_sql_calls = [
        item for item in exchange.metrics.tool_calls if item.name == "run_sql"
    ]
    assert len(run_sql_calls) == 1, exchange.metrics.tool_calls
    assert not run_sql_calls[0].has_error, run_sql_calls[0]
    assert len(exchange.metrics.tool_calls) == 1, exchange.metrics.tool_calls
    assert set(run_sql_calls[0].arguments) == {"query"}, run_sql_calls[0]
    statement = sqlglot.parse_one(
        str(run_sql_calls[0].arguments["query"]),
        read="sqlite",
    )
    cte_names = {
        cte.alias_or_name.casefold()
        for cte in statement.find_all(sqlglot.exp.CTE)
        if cte.alias_or_name
    }
    physical_tables = {
        table.name.casefold()
        for table in statement.find_all(sqlglot.exp.Table)
        if table.name.casefold() not in cte_names
    }
    assert physical_tables == {"s2t_transformations"}, statement
    assert len(list(statement.find_all(sqlglot.exp.Count))) >= 3, statement
    normalized_sql = statement.sql(dialect="sqlite").casefold()
    assert re.search(
        r"count\s*\(\s*distinct\s+[^)]*source_table",
        normalized_sql,
    ), normalized_sql
    order_sql = "\n".join(
        order.sql(dialect="sqlite").casefold()
        for order in statement.find_all(sqlglot.exp.Order)
    )
    assert "target_table" in order_sql and "source_table" in order_sql, order_sql
    assert order_sql.count("desc") >= 2, order_sql
    assert len(list(statement.find_all(sqlglot.exp.Limit))) >= 2, statement
    assert not [
        item
        for item in exchange.metrics.tool_calls
        if item.name
        in {
            "run_cypher",
            "trace_neo4j_table_path",
            "trace_transformation_path",
        }
    ], exchange.metrics.tool_calls
    _assert_execution(
        exchange,
        expected_tools=["run_sql"],
        expected_displays=["run_sql"],
        max_seconds=180,
        max_llm_calls=28,
        max_total_tokens=160_000,
    )


@pytest.mark.live_graph
def test_live_agent_returns_full_neo4j_path_for_known_endpoints(
    live_chat_client,
):
    expected_path = _neo4j_path(3)
    root_source = expected_path[0]
    expected_target = expected_path[-1]

    exchange = _chat(
        live_chat_client,
        f"Через Neo4j покажи полный направленный путь от {root_source} до "
        f"{expected_target}: все узлы по порядку и глубину. Полный результат "
        "пути покажи отдельно.",
    )
    result = exchange.result

    _assert_public_answer(result.answer)
    payloads = _display_payloads(result)
    _warn_unless(
        any(
            expected_path in _payload_table_paths(payload)
            for payload in payloads
        ),
        "presentation",
        "Neo4j display does not contain the dependent full path",
    )
    _assert_execution(
        exchange,
        expected_tools=[{"run_cypher", "trace_neo4j_table_path"}],
        expected_displays=[{"run_cypher", "trace_neo4j_table_path"}],
        max_seconds=180,
        max_llm_calls=20,
        max_total_tokens=140_000,
    )


@pytest.mark.live_validation
def test_live_agent_checks_nulls_in_required_target_fields(live_chat_client):
    _require_live_semantic_judge()
    file_id, target_table, source_table, target_field, source_field = (
        _s2t_work_case_fixture()
    )
    filename = str(
        _fetch_one(
            "SELECT filename FROM files WHERE file_id = ?",
            (file_id,),
        )[0]
    )
    exchange = _chat(
        live_chat_client,
        f"Для файла {filename!r} оцени совместимость nullable-ограничений "
        f"{source_table}.{source_field} → {target_table}.{target_field}. Верни "
        "source_not_null=<0|1>, target_not_null=<0|1> и вывод.",
    )
    _assert_nullable_paraphrase(
        exchange,
        file_id=file_id,
        source_table=source_table,
        source_field=source_field,
        target_table=target_table,
        target_field=target_field,
    )
    assert any(
        item.name == "resolve_file"
        and item.arguments == {"filename": filename}
        and not item.has_error
        for item in exchange.metrics.tool_calls
    ), exchange.metrics.tool_calls


@pytest.mark.live_validation
def test_live_agent_checks_source_and_target_type_compatibility(live_chat_client):
    _require_live_semantic_judge()
    file_id, target_table, source_table, target_field, source_field = (
        _s2t_work_case_fixture()
    )
    source_data_type, target_data_type = _fetch_one(
        """
        SELECT source_catalog.data_type, target_catalog.data_type
        FROM source_columns AS source_catalog
        JOIN target_columns AS target_catalog
          ON target_catalog.file_id = source_catalog.file_id
        WHERE source_catalog.file_id = ?
          AND source_catalog.table_name = ? COLLATE NOCASE
          AND source_catalog.column_name = ? COLLATE NOCASE
          AND target_catalog.table_name = ? COLLATE NOCASE
          AND target_catalog.column_name = ? COLLATE NOCASE
        ORDER BY source_catalog.id, target_catalog.id
        LIMIT 1
        """,
        (
            file_id,
            source_table,
            source_field,
            target_table,
            target_field,
        ),
    )
    exchange = _chat(
        live_chat_client,
        f"Для file_id={file_id} оцени совместимость типов "
        f"{source_table}.{source_field} → {target_table}.{target_field}. Верни "
        "source_data_type=<тип>, target_data_type=<тип> и вывод.",
    )
    result = exchange.result

    _assert_public_answer(result.answer)
    _assert_named_answer_value(
        result.answer,
        "source_data_type",
        source_data_type,
    )
    _assert_named_answer_value(
        result.answer,
        "target_data_type",
        target_data_type,
    )
    _assert_agentic_answer_uses_complete_evidence(exchange)

    exact_arguments = {
        "file_id": file_id,
        "source_table": source_table,
        "source_column": source_field,
        "target_table": target_table,
        "target_column": target_field,
    }
    pair_calls = [
        item
        for item in exchange.metrics.tool_calls
        if item.name == "get_source_target_column_pair"
        and item.arguments == exact_arguments
        and not item.has_error
    ]
    metadata_calls = [
        item
        for item in exchange.metrics.tool_calls
        if item.name == "list_column_metadata"
        and str(item.arguments.get("file_scope") or "") == str(file_id)
        and {
            str(value).casefold()
            for value in item.arguments.get("table_names") or []
        }
        == {source_table.casefold(), target_table.casefold()}
        and not item.has_error
    ]
    assert (len(pair_calls), len(metadata_calls)) in {(1, 0), (0, 1)}, (
        exchange.metrics.tool_calls
    )
    assert len(exchange.metrics.tool_calls) == 1, exchange.metrics.tool_calls
    required_tool = (
        "get_source_target_column_pair" if pair_calls else "list_column_metadata"
    )
    _assert_s2t_work_case_execution(
        exchange,
        required_tools={required_tool},
        require_analysis=True,
    )
    assert exchange.metrics.coordinator_plan, exchange.metrics
    assert all(
        "sql_risk_execution_mode" not in step
        and "plan_source" not in step
        and "operation_sql_risk_scope_contract" not in step
        for step in exchange.metrics.coordinator_plan
    ), exchange.metrics.coordinator_plan
    assert any(
        item.stage == "downstream_plan"
        for item in exchange.metrics.llm_calls
    ), exchange.metrics.llm_calls


@pytest.mark.live_validation
def test_live_agent_checks_duplicate_risk_in_target(live_chat_client):
    _require_live_semantic_judge()
    case = _protocol_live_case(require_join=True)
    exchange = _chat(
        live_chat_client,
        f"Оцени риск появления дубликатов при сохранённой S2T-трансформации "
        f"{case.source_table} → {case.target_table}. Назови фактический JOIN "
        "и явно отдели подтверждённый механизм от условия по уникальности.",
    )

    _assert_cardinality_paraphrase(exchange, case)


def _assert_scope_has_no_agentic_llm_stages(exchange: _LiveExchange) -> None:
    forbidden_stages = {
        "downstream_plan",
        "router",
        "planner",
        "worker_planner",
        "observer",
        "finish_worker",
        "upstream",
    }
    assert not [
        item
        for item in exchange.metrics.llm_calls
        if item.stage in forbidden_stages
    ], exchange.metrics.llm_calls


def _assert_cardinality_paraphrase(
    exchange: _LiveExchange,
    case: _ProtocolLiveCase,
) -> None:
    """Check semantics in both arms and direct facts in operation scope."""

    assert LIVE_AGENT_LLM_JUDGE, "SQL-risk semantic scenarios require --llm-judge"
    _assert_public_answer(exchange.result.answer)
    scope_pipeline = _sql_risk_operation_scope_enabled()
    if scope_pipeline:
        _assert_sql_risk_aspect(
            exchange,
            "cardinality",
            expected_execution_mode="conditional_cardinality",
        )
    else:
        _assert_agentic_answer_uses_complete_evidence(exchange)
    exact_calls = _assert_exact_s2t_pair_was_read(
        exchange,
        source_table=case.source_table,
        target_table=case.target_table,
    )
    if not scope_pipeline:
        _assert_sql_risk_aspect(
            exchange,
            "cardinality",
            expected_execution_mode="conditional_cardinality",
        )
        return

    assert len(exact_calls) == 1, exact_calls
    assert _tool_names(exchange) == ["read_s2t_source_to_target"], (
        exchange.metrics.tool_calls
    )
    upstream = exchange.metrics.upstream_output
    assert upstream is not None, exchange.metrics
    assert upstream.get("answer_source") == "sql_risk_scope_llm", upstream
    used_ids = list(upstream.get("used_evidence_ids") or [])
    assert len(used_ids) == 1, upstream
    assert set(upstream.get("display_evidence_ids") or []).issubset(used_ids)
    _assert_scope_has_no_agentic_llm_stages(exchange)
    _assert_s2t_work_case_execution(
        exchange,
        required_tools={"read_s2t_source_to_target"},
        require_analysis=True,
    )


def _nullable_flags_for_case(
    *,
    file_id: int,
    source_table: str,
    source_field: str,
    target_table: str,
    target_field: str,
) -> tuple[int, int]:
    source_not_null = int(
        _fetch_one(
            """
            SELECT not_null
            FROM source_columns
            WHERE file_id = ?
              AND table_name = ? COLLATE NOCASE
              AND column_name = ? COLLATE NOCASE
            ORDER BY id
            LIMIT 1
            """,
            (file_id, source_table, source_field),
        )[0]
    )
    target_not_null = int(
        _fetch_one(
            """
            SELECT not_null
            FROM target_columns
            WHERE file_id = ?
              AND table_name = ? COLLATE NOCASE
              AND column_name = ? COLLATE NOCASE
            ORDER BY id
            LIMIT 1
            """,
            (file_id, target_table, target_field),
        )[0]
    )
    return source_not_null, target_not_null


def _assert_nullable_paraphrase(
    exchange: _LiveExchange,
    *,
    file_id: int,
    source_table: str,
    source_field: str,
    target_table: str,
    target_field: str,
) -> None:
    assert LIVE_AGENT_LLM_JUDGE, "SQL-risk semantic scenarios require --llm-judge"
    source_not_null, target_not_null = _nullable_flags_for_case(
        file_id=file_id,
        source_table=source_table,
        source_field=source_field,
        target_table=target_table,
        target_field=target_field,
    )
    _assert_public_answer(exchange.result.answer)
    _assert_named_answer_value(
        exchange.result.answer,
        "source_not_null",
        source_not_null,
    )
    _assert_named_answer_value(
        exchange.result.answer,
        "target_not_null",
        target_not_null,
    )
    _assert_agentic_answer_uses_complete_evidence(exchange)
    _assert_exact_column_pair_was_read(
        exchange,
        file_id=file_id,
        source_table=source_table,
        source_field=source_field,
        target_table=target_table,
        target_field=target_field,
    )
    assert exchange.metrics.sql_risk_operation is None, (
        exchange.metrics.sql_risk_operation
    )
    routed_steps = [
        step
        for step in exchange.metrics.coordinator_plan
        if str(step.get("pipeline") or "") == "agentic"
    ]
    assert routed_steps, exchange.metrics.coordinator_plan
    assert all(
        "Совместимость колонок" in (step.get("operation_skills") or [])
        and list(step.get("sql_risk_aspects") or []) == []
        for step in routed_steps
    ), routed_steps
    assert any(
        item.stage == "downstream_plan"
        for item in exchange.metrics.llm_calls
    ), exchange.metrics.llm_calls
    _assert_s2t_work_case_execution(
        exchange,
        required_tools={"get_source_target_column_pair"},
        require_analysis=True,
    )


@pytest.mark.live_validation
def test_live_sql_risk_cardinality_paraphrase_scope_first(live_chat_client):
    _require_live_semantic_judge()
    case = _protocol_live_case(require_join=True)
    exchange = _chat(
        live_chat_client,
        f"По направлению {case.source_table} → {case.target_table} нужен "
        "только условный анализ размножения строк. Укажи JOIN из "
        "сохранённого S2T и отдельно поясни, что известно о механизме, "
        "уникальности ключей и наличии реальных дубликатов.",
    )

    _assert_cardinality_paraphrase(exchange, case)


@pytest.mark.live_validation
def test_live_sql_risk_cardinality_paraphrase_english(live_chat_client):
    _require_live_semantic_judge()
    case = _protocol_live_case(require_join=True)
    exchange = _chat(
        live_chat_client,
        f"Using the stored S2T for {case.source_table} → "
        f"{case.target_table}, assess only conditional row multiplication. "
        "State the concrete JOIN, then distinguish the established mechanism "
        "from unknown join-key uniqueness and unproven actual duplicates.",
    )

    _assert_cardinality_paraphrase(exchange, case)


@pytest.mark.live_validation
def test_live_sql_risk_cardinality_paraphrase_reordered(live_chat_client):
    _require_live_semantic_judge()
    case = _protocol_live_case(require_join=True)
    exchange = _chat(
        live_chat_client,
        "Реальные дубликаты заранее не утверждай. Для "
        f"{case.source_table} → {case.target_table} оцени по сохранённому "
        "S2T только условный cardinality risk: сначала состояние "
        "уникальности ключей, затем подтверждённый JOIN-механизм.",
    )

    _assert_cardinality_paraphrase(exchange, case)


@pytest.mark.live_validation
def test_live_sql_risk_nullable_paraphrase_scope_first(live_chat_client):
    _require_live_semantic_judge()
    file_id, target_table, source_table, target_field, source_field = (
        _s2t_work_case_fixture()
    )
    exchange = _chat(
        live_chat_client,
        f"В file_id={file_id} для {source_table}.{source_field} → "
        f"{target_table}.{target_field} проверь исключительно, может ли "
        "nullable источника привести к отказу записи по NOT NULL цели. "
        "Сообщи source_not_null, target_not_null и заключение; другие "
        "SQL-риски не рассматривай.",
    )

    _assert_nullable_paraphrase(
        exchange,
        file_id=file_id,
        source_table=source_table,
        source_field=source_field,
        target_table=target_table,
        target_field=target_field,
    )


@pytest.mark.live_validation
def test_live_sql_risk_nullable_paraphrase_english(live_chat_client):
    _require_live_semantic_judge()
    file_id, target_table, source_table, target_field, source_field = (
        _s2t_work_case_fixture()
    )
    exchange = _chat(
        live_chat_client,
        "Ignore every other SQL risk. For "
        f"{source_table}.{source_field} → {target_table}.{target_field} in "
        f"file_id={file_id}, determine only whether the source/target "
        "nullability contract can cause a NOT NULL constraint rejection. "
        "State source_not_null, target_not_null, and the conclusion.",
    )

    _assert_nullable_paraphrase(
        exchange,
        file_id=file_id,
        source_table=source_table,
        source_field=source_field,
        target_table=target_table,
        target_field=target_field,
    )


@pytest.mark.live_validation
def test_live_sql_risk_nullable_paraphrase_reordered(live_chat_client):
    _require_live_semantic_judge()
    file_id, target_table, source_table, target_field, source_field = (
        _s2t_work_case_fixture()
    )
    exchange = _chat(
        live_chat_client,
        "Нужны source_not_null и target_not_null плюс вывод. Анализ ограничь "
        "только nullable constraint rejection для пары "
        f"{source_table}.{source_field} → {target_table}.{target_field}, "
        f"file_id={file_id}.",
    )

    _assert_nullable_paraphrase(
        exchange,
        file_id=file_id,
        source_table=source_table,
        source_field=source_field,
        target_table=target_table,
        target_field=target_field,
    )


@pytest.mark.live_validation
def test_live_agent_checks_unmapped_required_target_fields(live_chat_client):
    file_id, target_table, _, _, _ = _s2t_work_case_fixture()
    filename = str(
        _fetch_one(
            "SELECT filename FROM files WHERE file_id = ?",
            (file_id,),
        )[0]
    )
    conn = db_storage.get_db_connection()
    try:
        mandatory_fields = sorted(
            {
                str(row[0])
                for row in conn.execute(
                    """
                    SELECT column_name
                    FROM target_columns
                    WHERE file_id = ?
                      AND table_name = ? COLLATE NOCASE
                      AND not_null = 1
                      AND column_name IS NOT NULL
                      AND TRIM(column_name) <> ''
                    """,
                    (file_id, target_table),
                ).fetchall()
            },
            key=str.casefold,
        )
        mapped_fields = {
            str(row[0]).casefold()
            for row in conn.execute(
                """
                SELECT target_field
                FROM s2t_transformations
                WHERE target_table = ? COLLATE NOCASE
                  AND target_field IS NOT NULL
                  AND TRIM(target_field) <> ''
                """,
                (target_table,),
            ).fetchall()
        }
    finally:
        conn.close()
    unmapped_required = [
        field
        for field in mandatory_fields
        if field.casefold() not in mapped_fields
    ]
    exchange = _chat(
        live_chat_client,
        f"Для файла {filename!r} найди обязательные поля {target_table} без "
        "сохранённого S2T-маппинга. Верни "
        "mandatory_fields_count=<число>, "
        "mandatory_fields_without_mapping_count=<число> и имена полей без "
        "маппинга как fields_without_mapping=<JSON-массив>.",
    )
    result = exchange.result

    _assert_public_answer(result.answer)
    _assert_named_answer_value(
        result.answer,
        "mandatory_fields_count",
        len(mandatory_fields),
    )
    _assert_named_answer_value(
        result.answer,
        "mandatory_fields_without_mapping_count",
        len(unmapped_required),
    )
    _assert_named_answer_list(
        result.answer,
        "fields_without_mapping",
        unmapped_required,
    )
    _assert_s2t_work_case_execution(
        exchange,
        required_tools=(
            {"list_target_column_catalog", "read_s2t_by_target_table"}
            if STRICT_RETRIEVAL_ENABLED
            else {"list_column_catalog"}
        ),
        require_analysis=True,
    )


@pytest.mark.live_validation
def test_live_agent_checks_row_loss_risk(live_chat_client):
    _require_live_semantic_judge()
    case = _protocol_live_case(require_filter=True)
    exchange = _chat(
        live_chat_client,
        f"Оцени риск потери строк в сохранённой S2T-трансформации "
        f"{case.source_table} → {case.target_table}. Назови точный "
        "WHERE/HAVING/QUALIFY или JOIN predicate, который может отсеять строки.",
    )
    result = exchange.result

    _assert_public_answer(result.answer)
    scope_pipeline = _sql_risk_operation_scope_enabled()
    if scope_pipeline:
        _assert_sql_risk_aspect(
            exchange,
            "row_filtering",
            expected_execution_mode="row_filtering",
        )
    else:
        _assert_agentic_answer_uses_complete_evidence(exchange)
    exact_calls = _assert_exact_s2t_pair_was_read(
        exchange,
        source_table=case.source_table,
        target_table=case.target_table,
    )
    from agents.transformation_ast import quote_dollar_schemas

    statement = sqlglot.parse_one(
        quote_dollar_schemas(case.transformation_rule),
        read=GREENPLUM_DIALECT,
    )
    expected_filter_predicates = {
        clause.this.sql(dialect=GREENPLUM_DIALECT).casefold()
        for clause_type in (
            sqlglot.exp.Where,
            sqlglot.exp.Having,
            sqlglot.exp.Qualify,
        )
        for clause in statement.find_all(clause_type)
    }
    assert expected_filter_predicates, statement
    if scope_pipeline or _sql_risk_aspects_enabled():
        assert len(exact_calls) == 1, exact_calls
        assert _tool_names(exchange) == ["read_s2t_source_to_target"], (
            exchange.metrics.tool_calls
        )
    if scope_pipeline:
        upstream = exchange.metrics.upstream_output
        assert upstream is not None
        assert upstream.get("answer_source") == "sql_risk_scope_llm"
        assert set(upstream.get("display_evidence_ids") or []).issubset(
            set(upstream.get("used_evidence_ids") or [])
        )
        operation = exchange.metrics.sql_risk_operation or {}
        assessment = dict(operation.get("assessment") or {})
        assert assessment.get("outcome") == "risk_present", assessment
        _assert_scope_has_no_agentic_llm_stages(exchange)
    _assert_s2t_work_case_execution(
        exchange,
        required_tools={"read_s2t_source_to_target"},
        require_analysis=True,
    )
    if not scope_pipeline:
        _assert_sql_risk_aspect(exchange, "row_filtering")


@pytest.mark.live_validation
def test_live_agent_checks_value_change_risk(live_chat_client):
    _require_live_semantic_judge()
    case = _protocol_live_case(
        require_explicit_selected_projection=True,
        require_non_column_selected_projection=True,
    )
    exchange = _chat(
        live_chat_client,
        "Проверь только возможность изменения значения в сохранённой "
        f"S2T-паре {case.source_table}.{case.source_field} → "
        f"{case.target_table}.{case.target_field}. Может ли вычисление "
        "целевого поля изменить исходное значение? Остальные SQL-риски "
        "не анализируй.",
    )

    _assert_public_answer(exchange.result.answer)
    scope_pipeline = _sql_risk_operation_scope_enabled()
    if scope_pipeline:
        _assert_sql_risk_aspect(
            exchange,
            "value_changes",
            expected_execution_mode="value_changes",
        )
    else:
        _assert_agentic_answer_uses_complete_evidence(exchange)
    _assert_s2t_work_case_execution(
        exchange,
        required_tools=(
            {
                "list_s2t_field_mapping"
                if scope_pipeline
                else "read_s2t_source_to_target"
            }
            if STRICT_RETRIEVAL_ENABLED
            else None
        ),
        require_analysis=True,
    )
    if not scope_pipeline:
        _assert_sql_risk_aspect(exchange, "value_changes")
    exact_reader_name = (
        "list_s2t_field_mapping"
        if scope_pipeline
        else "read_s2t_source_to_target"
    )
    exact_reads = [
        item
        for item in exchange.metrics.tool_calls
        if item.name == exact_reader_name
    ]
    assert len(exact_reads) == 1, exchange.metrics.tool_calls
    assert len(exchange.metrics.tool_calls) == 1, exchange.metrics.tool_calls
    expected_arguments = {
        "source_table": case.source_table,
        "target_table": case.target_table,
    }
    if scope_pipeline:
        expected_arguments.update(
            {
                "source_field": case.source_field,
                "target_field": case.target_field,
            }
        )
    assert exact_reads[0].arguments == expected_arguments
    from agents.transformation_ast import normalize_transformation

    normalized = normalize_transformation(case.transformation_rule)
    target_expression = next(
        (
            value
            for name, value in normalized.projections.items()
            if name.casefold() == case.target_field.casefold()
        ),
        None,
    )
    assert target_expression, normalized
    parsed_projection = sqlglot.parse_one(
        target_expression,
        read=GREENPLUM_DIALECT,
    )
    assert not isinstance(
        parsed_projection,
        sqlglot.exp.Column,
    ), parsed_projection
    folded_answer = exchange.result.answer.casefold()
    assert exchange.metrics.upstream_output is not None
    if scope_pipeline:
        assert exchange.metrics.upstream_output.get("answer_source") == (
            "sql_risk_scope_llm"
        )
        assert set(
            exchange.metrics.upstream_output.get("display_evidence_ids") or []
        ).issubset(
            set(
                exchange.metrics.upstream_output.get("used_evidence_ids")
                or []
            )
        )
        operation = exchange.metrics.sql_risk_operation or {}
        assessment = dict(operation.get("assessment") or {})
        assert assessment.get("outcome") == "risk_present", assessment
        _assert_scope_has_no_agentic_llm_stages(exchange)
    else:
        assert (
            f"{case.source_table}.{case.source_field}".casefold()
            in folded_answer
        ), exchange.result.answer
        assert (
            f"{case.target_table}.{case.target_field}".casefold()
            in folded_answer
        ), exchange.result.answer
        assert exchange.metrics.upstream_output.get("answer_source") == "model"
        assert len(
            [
                item
                for item in exchange.metrics.llm_calls
                if item.stage == "upstream"
            ]
        ) >= 2, exchange.metrics.llm_calls


@pytest.mark.live_validation
def test_live_agent_checks_write_semantics_risk(live_chat_client):
    _require_live_semantic_judge()
    _, target_table, source_table, _, _ = _s2t_work_case_fixture()
    exchange = _chat(
        live_chat_client,
        "Оцени только SQL-аспект write semantics для сохранённой "
        f"S2T-загрузки {source_table} → {target_table}: append, overwrite, "
        "MERGE/UPSERT или conflict handling. Если write statement не "
        "сохранён, честно отметь «не оценено» и не выводи режим из PK.",
    )

    _assert_public_answer(exchange.result.answer)
    scope_pipeline = _sql_risk_operation_scope_enabled()
    if scope_pipeline:
        _assert_sql_risk_aspect(
            exchange,
            "write_semantics",
            expected_execution_mode="write_semantics",
        )
    else:
        _assert_agentic_answer_uses_complete_evidence(exchange)
    _assert_s2t_work_case_execution(
        exchange,
        required_tools=(
            {"read_s2t_source_to_target"}
            if STRICT_RETRIEVAL_ENABLED
            else None
        ),
        require_analysis=True,
    )
    if not scope_pipeline:
        _assert_sql_risk_aspect(exchange, "write_semantics")
    exact_reads = [
        item
        for item in exchange.metrics.tool_calls
        if item.name == "read_s2t_source_to_target"
    ]
    assert len(exact_reads) == 1, exchange.metrics.tool_calls
    assert len(exchange.metrics.tool_calls) == 1, exchange.metrics.tool_calls
    assert exact_reads[0].arguments == {
        "source_table": source_table,
        "target_table": target_table,
    }
    assert exchange.metrics.upstream_output is not None
    if scope_pipeline:
        assert exchange.metrics.upstream_output.get("answer_source") == (
            "sql_risk_scope_llm"
        )
        assert set(
            exchange.metrics.upstream_output.get("display_evidence_ids") or []
        ).issubset(
            set(
                exchange.metrics.upstream_output.get("used_evidence_ids")
                or []
            )
        )
        operation = exchange.metrics.sql_risk_operation or {}
        assessment = dict(operation.get("assessment") or {})
        assert assessment.get("outcome") == "not_assessed", assessment
        assert assessment.get("limitations"), assessment
        _assert_scope_has_no_agentic_llm_stages(exchange)
    else:
        assert {
            int(step.get("cycle") or 0)
            for step in exchange.metrics.coordinator_plan
        } == {1}, exchange.metrics.coordinator_plan
        assert exchange.metrics.upstream_output.get("answer_source") == "model"
        assert len(
            [
                item
                for item in exchange.metrics.llm_calls
                if item.stage == "upstream"
            ]
        ) >= 2, exchange.metrics.llm_calls


@pytest.mark.live_validation
def test_live_agent_explains_table_transformation(live_chat_client):
    case = _protocol_live_case(
        require_join=True,
        require_effective_join_predicate=True,
        require_filter=True,
        require_explicit_selected_projection=True,
    )
    target_table = case.target_table
    source_table = case.source_table
    target_field = case.target_field
    source_field = case.source_field
    transformation_rule = case.transformation_rule
    from agents.transformation_ast import normalize_transformation

    normalized = normalize_transformation(transformation_rule)
    assert normalized.parse_status == "ok", normalized
    selected_expression = next(
        (
            expression
            for name, expression in normalized.projections.items()
            if name.casefold() == target_field.casefold()
        ),
        None,
    )
    assert selected_expression, normalized
    assert normalized.joins and normalized.filters, normalized
    effective_join_predicate = next(
        (
            effective
            for join in normalized.joins
            if join.condition
            if (effective := _effective_predicate_sql(join.condition))
        ),
        None,
    )
    effective_filter_predicate = next(
        (
            effective
            for predicate in normalized.filters
            if (effective := _effective_predicate_sql(predicate))
        ),
        None,
    )
    assert effective_join_predicate, normalized
    assert effective_filter_predicate, normalized
    exchange = _chat(
        live_chat_client,
        f"Объясни сохранённую S2T-трансформацию "
        f"{source_table}.{source_field} → {target_table}.{target_field}. "
        "Укажи три нормализованных эффективных значения из сохранённого SQL; "
        "служебные тождества TRUE и 1=1 не включай. Не приписывай выбранному "
        "полю выражения других target-полей. Формат без дополнительных "
        "пояснений, каждое значение на отдельной строке:\n"
        "selected_projection=<выражение AS target_field>\n"
        "join_predicate=<условие JOIN>\n"
        "filter_predicate=<условие WHERE>",
    )
    result = exchange.result

    _assert_public_answer(result.answer)
    expected_values = {
        "selected_projection": f"{selected_expression} AS {target_field}",
        "join_predicate": effective_join_predicate,
        "filter_predicate": effective_filter_predicate,
    }
    actual_values = _extract_named_sql_values(
        result.answer,
        tuple(expected_values),
    )
    for name, expected in expected_values.items():
        actual = actual_values[name]
        assert _canonical_sql_ast(actual) == _canonical_sql_ast(expected), {
            "name": name,
            "expected": expected,
            "actual": actual,
            "answer": result.answer,
        }
    _assert_agentic_answer_uses_complete_evidence(exchange)
    exact_calls = _assert_exact_s2t_pair_was_read(
        exchange,
        source_table=source_table,
        target_table=target_table,
    )
    assert len(exact_calls) == 1, exact_calls
    assert exact_calls[0].arguments == {
        "source_table": source_table,
        "target_table": target_table,
    }, exact_calls[0]
    assert len(exchange.metrics.tool_calls) == 1, exchange.metrics.tool_calls
    assert not [item for item in exchange.metrics.tool_calls if item.has_error], (
        exchange.metrics.tool_calls
    )
    assert not {
        "run_cypher",
        "trace_neo4j_table_path",
        "trace_transformation_path",
    } & set(_tool_names(exchange)), exchange.metrics.tool_calls
    _assert_s2t_work_case_execution(
        exchange,
        required_tools={"read_s2t_source_to_target"},
        require_analysis=True,
    )
    _assert_no_sql_risk_route(exchange)


@pytest.mark.live_validation
def test_live_agent_writes_s2t_test_protocol(live_chat_client):
    file_id, target_table, source_table, _, _ = _s2t_work_case_fixture()
    filename = str(
        _fetch_one(
            "SELECT filename FROM files WHERE file_id = ?",
            (file_id,),
        )[0]
    )
    exchange = _chat(
        live_chat_client,
        f"Для файла {filename!r} по сохранённой S2T-спецификации "
        f"{source_table} → {target_table} составь тест-протокол для проверки "
        "ETL-загрузки во внешней СУБД. Включи проверки количества строк, "
        "уникальности ключа, null-rate обязательных полей и корректности "
        "трансформаций. Для каждой проверки дай цель, SQL-шаблон и критерий "
        "прохождения. Используй подтверждённые таблицы, колонки и правила; "
        "фактические метрики не вычисляй.",
    )
    result = exchange.result

    _assert_public_answer(result.answer)
    _assert_compiled_test_protocol(
        exchange,
        target_table,
        source_tables=(source_table,),
        expected_pair_reads=1,
    )
    _assert_s2t_work_case_execution(
        exchange,
        required_tools=(
            {
                "read_s2t_by_target_table",
                "list_target_column_catalog",
            }
            if STRICT_RETRIEVAL_ENABLED
            else None
        ),
        require_analysis=True,
    )


@pytest.mark.live_validation
def test_live_agent_writes_independent_s2t_test_protocol(live_chat_client):
    file_id, source_table, target_table = _independent_protocol_case_fixture()
    exchange = _chat(
        live_chat_client,
        f"Для file_id={file_id} подготовь приёмочный протокол из Greenplum SQL "
        f"для сохранённой загрузки {source_table} → {target_table}. Ничего не "
        "запускай. Нужны четыре контроля: совпадает ли рассчитанный по правилу "
        "набор с target; отсутствуют ли NULL в обязательных колонках; не "
        "повторяется ли подтверждённый ключ; одинаково ли число ожидаемых и "
        "загруженных строк. Для каждого укажи назначение, запрос и однозначное "
        "условие успешной приёмки.",
    )

    _assert_public_answer(exchange.result.answer)
    _assert_compiled_test_protocol(
        exchange,
        target_table,
        source_tables=(source_table,),
        expected_pair_reads=1,
    )
    sql_blocks = re.findall(
        r"```sql\n(.*?)\n```",
        exchange.result.answer,
        flags=re.DOTALL,
    )
    folded_blocks = [block.casefold() for block in sql_blocks]
    assert all(target_table.casefold() in block for block in folded_blocks)
    assert all("{{load_scope_predicate}}" in block for block in folded_blocks)
    combined_sql = "\n".join(folded_blocks)
    assert "group by" in combined_sql
    assert "having count(*) > 1" in combined_sql
    assert "is null" in combined_sql
    assert combined_sql.count("except all") == 2
    _assert_s2t_work_case_execution(
        exchange,
        required_tools=(
            {
                "read_s2t_source_to_target",
                "read_s2t_by_target_table",
                "list_target_column_catalog",
            }
            if STRICT_RETRIEVAL_ENABLED
            else None
        ),
        require_analysis=True,
    )


@pytest.mark.live_validation
def test_live_agent_analyzes_s2t_validation_risks(live_chat_client):
    file_id, target_table, source_table, _, _ = _s2t_work_case_fixture()
    exchange = _chat(
        live_chat_client,
        f"Для file_id={file_id} по сохранённой S2T-спецификации "
        f"{source_table} → {target_table} оцени риск потери строк, риск "
        "дубликатов, обязательные target-поля без S2T-маппинга и "
        "согласованность трансформации. Используй только S2T и каталог "
        "колонок; не обращайся к физическим данным ETL-таблиц.",
    )

    _assert_public_answer(exchange.result.answer)
    _assert_s2t_work_case_execution(
        exchange,
        required_tools=(
            {"read_s2t_by_target_table", "list_target_column_catalog"}
            if STRICT_RETRIEVAL_ENABLED
            else None
        ),
        require_analysis=True,
    )


@pytest.mark.live_validation
def test_live_agent_writes_multi_source_s2t_validation_protocol(
    live_chat_client,
):
    file_id, target_table, first_source, second_source = (
        _multi_source_validation_case_fixture()
    )
    exchange = _chat(
        live_chat_client,
        f"Для file_id={file_id} по сохранённой S2T-загрузке из "
        f"{first_source} и {second_source} в {target_table} составь единый "
        "тест-протокол для внешней СУБД. Включи проверки количества строк, "
        "уникальности ключа, null-rate обязательных полей и корректности "
        "трансформаций. Для каждой проверки дай цель, SQL-шаблон и критерий "
        "прохождения; фактические метрики не вычисляй.",
    )

    _assert_public_answer(exchange.result.answer)
    _assert_compiled_test_protocol(
        exchange,
        target_table,
        source_tables=(first_source, second_source),
        expected_pair_reads=2,
    )
    _assert_s2t_work_case_execution(
        exchange,
        required_tools=(
            {"read_s2t_by_target_table", "list_target_column_catalog"}
            if STRICT_RETRIEVAL_ENABLED
            else None
        ),
        require_analysis=True,
    )


@pytest.mark.live_validation
def test_live_agent_writes_multi_target_s2t_validation_protocol(
    live_chat_client,
):
    file_id, source_table, first_target, second_target = (
        _multi_target_validation_case_fixture()
    )
    exchange = _chat(
        live_chat_client,
        f"Для file_id={file_id} по сохранённым S2T-загрузкам из "
        f"{source_table} в {first_target} и {second_target} составь отдельный "
        "тест-протокол для каждой target-таблицы во внешней СУБД. В каждый "
        "включи проверки количества строк, уникальности ключа, null-rate "
        "обязательных полей и корректности трансформаций. Для каждой проверки "
        "дай цель, SQL-шаблон и критерий прохождения; фактические метрики не "
        "вычисляй.",
    )

    _assert_public_answer(exchange.result.answer)
    _assert_compiled_test_protocol(
        exchange,
        first_target,
        second_target,
        source_tables=(source_table,),
        expected_pair_reads=2,
    )
    _assert_s2t_work_case_execution(
        exchange,
        required_tools=(
            {
                "read_s2t_by_target_table",
                "list_target_column_catalog",
            }
            if STRICT_RETRIEVAL_ENABLED
            else None
        ),
        require_analysis=True,
    )


def _assert_s2t_catalog_scenario(
    exchange: _LiveExchange,
) -> None:
    answer = exchange.result.answer
    _assert_public_answer(answer)

    metrics = exchange.metrics
    assert metrics.error is None, metrics.error
    assert metrics.elapsed_seconds > 0, metrics
    assert exchange.http_elapsed_seconds > 0, metrics
    _warn_unless(
        metrics.elapsed_seconds <= 300
        and exchange.http_elapsed_seconds <= 305,
        "efficiency",
        f"elapsed={metrics.elapsed_seconds:.3f}s exceeds catalog budget=300s",
    )
    _warn_unless(
        bool(metrics.tool_calls),
        "efficiency",
        "scenario returned without inspecting stored data",
    )
    if LIVE_AGENT_MODE == "multiagent":
        _warn_unless(
            bool(metrics.coordinator_plan)
            and 0 < len(metrics.worker_tasks) <= len(metrics.coordinator_plan),
            "efficiency",
            "coordinator/worker trace is incomplete",
        )
    else:
        _warn_unless(
            metrics.worker_tasks == [] and metrics.coordinator_plan == [],
            "efficiency",
            "single-agent run unexpectedly contains coordinator activity",
        )
    assert len(metrics.llm_calls) > 0, metrics.llm_calls
    _warn_unless(
        len(metrics.llm_calls) <= 80,
        "efficiency",
        f"llm_calls={len(metrics.llm_calls)} exceeds catalog budget=80",
    )
    assert metrics.total_tokens == metrics.input_tokens + metrics.output_tokens
    assert metrics.total_tokens > 0, metrics
    _warn_unless(
        metrics.total_tokens <= 320_000,
        "efficiency",
        f"total_tokens={metrics.total_tokens} exceeds catalog budget=320000",
    )


@pytest.mark.live_catalog
def test_live_agent_catalog_01_finds_target_field_source(live_chat_client):
    exchange = _chat(
        live_chat_client,
        "Откуда заполняется optn_id в t_optn? Найди source table, source field "
        "и покажи transformation rule. Используй глобальную s2t_transformations.",
    )
    _assert_s2t_catalog_scenario(exchange)


@pytest.mark.live_catalog
def test_live_agent_catalog_02_finds_source_field_targets(live_chat_client):
    exchange = _chat(
        live_chat_client,
        "В какие целевые таблицы передаётся c_closedate из "
        "s_grnplm_as_t_didsd_700_db_stg.a_000025_t_loanscontract? Найди все "
        "downstream S2T, не останавливайся на первом совпадении.",
    )
    _assert_s2t_catalog_scenario(exchange)


@pytest.mark.live_catalog
def test_live_agent_catalog_03_lists_table_mapping(live_chat_client):
    exchange = _chat(
        live_chat_client,
        "Покажи полный маппинг b3050000420005_paymentdetails -> t_optn: "
        "перечисли source column -> target column и transformation rules.",
    )
    _assert_s2t_catalog_scenario(exchange)


@pytest.mark.live_catalog
def test_live_agent_catalog_04_explains_calculated_field(live_chat_client):
    exchange = _chat(
        live_chat_client,
        "Как рассчитывается agr_cred_sum_crncy_amt в "
        "b7000000250004_loansagreement? Покажи expression и все исходные поля.",
    )
    _assert_s2t_catalog_scenario(exchange)


@pytest.mark.live_catalog
def test_live_agent_catalog_05_finds_business_metric_source(live_chat_client):
    exchange = _chat(
        live_chat_client,
        "Из какого поля берётся сумма задолженности или кредитного лимита "
        "клиента? Ищи по бизнес-смыслу и описаниям, верни наиболее вероятные "
        "S2T и объясни выбор техническими полями.",
    )
    _assert_s2t_catalog_scenario(exchange)


@pytest.mark.live_catalog
def test_live_agent_catalog_06_semantic_close_date_search(live_chat_client):
    exchange = _chat(
        live_chat_client,
        "Где у нас хранится дата закрытия договора? Найди технические поля без "
        "требования точного совпадения русского текста и покажи S2T.",
    )
    _assert_s2t_catalog_scenario(exchange)


@pytest.mark.live_catalog
def test_live_agent_catalog_07_finds_business_filter_rule(live_chat_client):
    exchange = _chat(
        live_chat_client,
        "Как определяется, что клиент связан с депозитным договором в "
        "t_agr_dep_cust? Покажи условия отбора и поля клиента.",
    )
    _assert_s2t_catalog_scenario(exchange)


@pytest.mark.live_catalog
def test_live_agent_catalog_08_searches_client_id_synonyms(live_chat_client):
    exchange = _chat(
        live_chat_client,
        "Найди идентификатор клиента в S2T, учитывая варианты client_id, "
        "cust_id, client_entityid_uid и baseclientid. Верни таблицы и поля.",
    )
    _assert_s2t_catalog_scenario(exchange)


@pytest.mark.live_catalog
def test_live_agent_catalog_09_maps_russian_term_to_technical_field(
    live_chat_client,
):
    exchange = _chat(
        live_chat_client,
        "Найди техническое поле для даты удаления записи и соответствующее "
        "S2T-правило. Ищи по русскому бизнес-термину, а не по заданному имени.",
    )
    _assert_s2t_catalog_scenario(exchange)


@pytest.mark.live_catalog
def test_live_agent_catalog_10_builds_full_lineage(live_chat_client):
    exchange = _chat(
        live_chat_client,
        "Покажи всю цепочку происхождения b700000025_agr_cred.c_closedate до "
        "первичных source-таблиц. Включи subquery и branch по порядку.",
    )
    _assert_s2t_catalog_scenario(exchange)


@pytest.mark.live_catalog
def test_live_agent_catalog_11_lists_intermediate_tables(live_chat_client):
    exchange = _chat(
        live_chat_client,
        "Через какие промежуточные таблицы проходит c_closedate от "
        "s_grnplm_as_t_didsd_700_db_stg.a_000025_t_loanscontract до "
        "b700000025_agr_cred? Перечисли маршрут по порядку.",
    )
    _assert_s2t_catalog_scenario(exchange)


@pytest.mark.live_catalog
def test_live_agent_catalog_12_compares_two_field_origins(live_chat_client):
    exchange = _chat(
        live_chat_client,
        "fk_status_id в b700000025_agr_cred и fk_status_id в "
        "b700000025_agr_grntee берутся из одного источника? Построй lineage для "
        "обоих и дай явный итог с общими и различающимися источниками.",
    )
    _assert_s2t_catalog_scenario(exchange)


@pytest.mark.live_catalog
def test_live_agent_catalog_13_finds_join_condition(live_chat_client):
    source_table = "l_000025_t_loansagreement_stg"
    target_table = "l_000025_t_loanscontract_stg"
    object_name, object_sql = _fetch_one(
        """
        SELECT name, sql
        FROM additional_objects
        WHERE LOWER(sql) LIKE '%' || LOWER(?) || '%'
          AND LOWER(sql) LIKE '%' || LOWER(?) || '%'
        ORDER BY id
        LIMIT 1
        """,
        (source_table, target_table),
    )
    statement = sqlglot.parse_one(str(object_sql), read=GREENPLUM_DIALECT)
    join = next(statement.find_all(sqlglot.exp.Join), None)
    assert join is not None and join.args.get("on") is not None, object_sql
    join_condition = join.args["on"]
    expected_join_tokens = {
        value.casefold()
        for column in join_condition.find_all(sqlglot.exp.Column)
        for value in (column.table, column.name)
        if value
    }
    assert expected_join_tokens, join_condition.sql()
    exchange = _chat(
        live_chat_client,
        f"По каким полям соединяются {source_table} и {target_table} "
        "в сохранённых Additional objects? "
        "Покажи имя найденного объекта, JOIN condition и роли алиасов.",
    )
    _assert_s2t_catalog_scenario(exchange)
    answer = exchange.result.answer.casefold()
    assert str(object_name).casefold() in answer, exchange.result.answer
    assert all(token in answer for token in expected_join_tokens), {
        "expected_join_tokens": sorted(expected_join_tokens),
        "answer": exchange.result.answer,
    }
    object_calls = [
        item
        for item in exchange.metrics.tool_calls
        if item.name in {"list_additional_objects", "search_additional_objects"}
    ]
    assert object_calls, exchange.metrics.tool_calls
    assert not [item for item in object_calls if item.has_error], object_calls
    assert exchange.metrics.worker_tasks, exchange.metrics
    assert exchange.metrics.upstream_output is not None, exchange.metrics


@pytest.mark.live_catalog
def test_live_agent_catalog_14_finds_filtering(live_chat_client):
    exchange = _chat(
        live_chat_client,
        "Какие записи из b3050000420007_product не попадут в t_agr_dep? "
        "Найди WHERE/FILTER условия и объясни исключение записей.",
    )
    _assert_s2t_catalog_scenario(exchange)


@pytest.mark.live_catalog
def test_live_agent_catalog_15_finds_constant_or_default(live_chat_client):
    exchange = _chat(
        live_chat_client,
        "Где при загрузке fk_productkind_id в "
        "b700000025_agr_cred::subquery::v_agr_cred2 устанавливается константа "
        "или default? Покажи literal и правило.",
    )
    _assert_s2t_catalog_scenario(exchange)


@pytest.mark.live_catalog
def test_live_agent_catalog_16_finds_case_transformation(live_chat_client):
    exchange = _chat(
        live_chat_client,
        "Где используется CASE при расчёте del_dt в "
        "b700000025_agr_cred::subquery::v_agr_cred1? Покажи условия и "
        "результирующие значения.",
    )
    _assert_s2t_catalog_scenario(exchange)


@pytest.mark.live_catalog
def test_live_agent_catalog_17_finds_aggregation(live_chat_client):
    exchange = _chat(
        live_chat_client,
        "Откуда берётся agr_dep_purpose_type_cd в t_agr_dep_purpose_type и "
        "как данные агрегируются? Покажи агрегат и уровень GROUP BY.",
    )
    _assert_s2t_catalog_scenario(exchange)


@pytest.mark.live_catalog
def test_live_agent_catalog_18_investigates_wrong_value(live_chat_client):
    exchange = _chat(
        live_chat_client,
        "В b700000025_agr_cred.c_closedate неправильная дата. Из каких "
        "источников и преобразований она могла прийти? Восстанови lineage назад "
        "и выдели места возможного изменения.",
    )
    _assert_s2t_catalog_scenario(exchange)


@pytest.mark.live_catalog
def test_live_agent_catalog_19_investigates_null(live_chat_client):
    exchange = _chat(
        live_chat_client,
        "del_dt в b700000025_agr_cred пустое. Посмотри, откуда оно загружается "
        "и какие CASE/JOIN/FILTER могут привести к NULL.",
    )
    _assert_s2t_catalog_scenario(exchange)


@pytest.mark.live_catalog
def test_live_agent_catalog_20_finds_data_loss_points(live_chat_client):
    exchange = _chat(
        live_chat_client,
        "В a_000025_t_loanscontract запись есть, а в b700000025_agr_cred её "
        "нет. Какие S2T, промежуточные таблицы, JOIN и FILTER надо проверить? "
        "Выдели возможные места потери записи.",
    )
    _assert_s2t_catalog_scenario(exchange)


@pytest.mark.live_catalog
def test_live_agent_catalog_21_traces_value_change(live_chat_client):
    exchange = _chat(
        live_chat_client,
        "В источнике ctl_action='D', а в b700000025_agr_cred рассчитано del_dt. "
        "Найди все преобразования по пути и укажи, где меняется представление "
        "значения.",
    )
    _assert_s2t_catalog_scenario(exchange)


@pytest.mark.live_catalog
def test_live_agent_catalog_22_finds_multiple_sources(live_chat_client):
    exchange = _chat(
        live_chat_client,
        "Из каких источников может заполняться agr_cred_sum_crncy_amt в "
        "b7000000250004_loansagreement? Учти CASE, COALESCE и альтернативные "
        "source fields.",
    )
    _assert_s2t_catalog_scenario(exchange)


@pytest.mark.live_catalog
def test_live_agent_catalog_23_performs_impact_analysis(live_chat_client):
    exchange = _chat(
        live_chat_client,
        "Что затронет изменение "
        "s_grnplm_as_t_didsd_700_db_stg.a_000025_t_loanscontract.c_closedate? "
        "Выполни reverse lineage и перечисли downstream-поля, таблицы и "
        "зависимые transformations.",
    )
    _assert_s2t_catalog_scenario(exchange)


@pytest.mark.live_catalog
def test_live_agent_catalog_24_compares_two_mart_rules(live_chat_client):
    exchange = _chat(
        live_chat_client,
        "Сравни расчёт del_dt в b700000025_agr_cred и "
        "b700000025_agr_grntee. Найди оба lineage и rules, явно скажи, "
        "совпадает логика или различается и чем.",
    )
    _assert_s2t_catalog_scenario(exchange)


@pytest.mark.live_catalog
def test_live_agent_catalog_25_finds_conflicting_s2t(live_chat_client):
    exchange = _chat(
        live_chat_client,
        "Есть ли несколько S2T, которые описывают загрузку "
        "b700000025_agr_cred::subquery::v_agr_cred1.del_dt по-разному? Найди "
        "все mappings, сравни source fields и transformation, выдели конфликт "
        "или объясни, почему mappings дополняют друг друга.",
    )
    _assert_s2t_catalog_scenario(exchange)


# Extended deterministic protocol scenarios from the multiagent improvement
# plan. They remain opt-in real-HTTP tests, but assert the execution trace and
# generated SQL directly instead of delegating correctness to LLM-as-judge.


@pytest.mark.live_validation
@pytest.mark.skipif(
    LIVE_AGENT_MODE != "multiagent",
    reason="deterministic validation_protocol exists only in multiagent mode",
)
def test_live_validation_protocol_standard_mode(live_chat_client):
    case = _protocol_live_case(
        min_mapped_fields=2,
        require_target_catalog=True,
    )
    exchange = _chat(
        live_chat_client,
        f"Для file_id={case.file_id} составь стандартный тест-протокол "
        f"проверки загрузки {case.source_table} → {case.target_table}. "
        "SQL не выполняй.",
    )

    trace = _assert_validation_pipeline(exchange)
    _assert_protocol_mode(trace, "standard")
    _assert_protocol_status(trace, "ready", "partial_protocol")
    for check in (
        "row_count",
        "key_uniqueness",
        "required_null_rate",
        "transformation_correctness",
    ):
        _assert_protocol_check(trace, check)
    _assert_protocol_phases(trace, {0, 1, 2, 3})
    _assert_protocol_sql_parseable(exchange, minimum_blocks=2)


@pytest.mark.live_validation
@pytest.mark.skipif(
    LIVE_AGENT_MODE != "multiagent",
    reason="deterministic validation_protocol exists only in multiagent mode",
)
def test_live_validation_protocol_exhaustive_mode(live_chat_client):
    case = _protocol_live_case(
        min_mapped_fields=2,
        require_target_catalog=True,
        require_primary_key=True,
    )
    exchange = _chat(
        live_chat_client,
        f"Для file_id={case.file_id} составь максимально полный exhaustive "
        f"тест-протокол загрузки {case.source_table} → {case.target_table}; "
        f"comparison key явно {case.target_field}. Включи все поддерживаемые "
        "статические и SQL-проверки, но не выполняй их.",
    )

    trace = _assert_validation_pipeline(exchange)
    _assert_protocol_mode(trace, "exhaustive")
    _assert_protocol_status(trace, "ready")
    for kind in _ALL_PROTOCOL_CHECKS:
        _assert_protocol_check(trace, kind, statuses={"ready"})
    _assert_protocol_phases(trace, {0, 1, 2, 3})
    phase_summaries = [
        phase
        for phase in trace.get("phases") or []
        if isinstance(phase, dict)
    ]
    assert phase_summaries, trace
    assert all(
        int(phase.get("partial_count") or 0) == 0
        and int(phase.get("unavailable_count") or 0) == 0
        and int(phase.get("failed_count") or 0) == 0
        for phase in phase_summaries
    ), phase_summaries
    _assert_protocol_sql_parseable(
        exchange,
        minimum_blocks=len(_ALL_PROTOCOL_CHECKS),
    )


@pytest.mark.live_validation
@pytest.mark.skipif(
    LIVE_AGENT_MODE != "multiagent",
    reason="deterministic validation_protocol exists only in multiagent mode",
)
def test_live_validation_protocol_key_reconciliation(live_chat_client):
    case = _protocol_live_case()
    exchange = _chat(
        live_chat_client,
        f"Для загрузки {case.source_table} → {case.target_table} составь explicit "
        f"protocol только с check=key_reconciliation; comparison key явно "
        f"задаю {case.target_field}. Файл не указываю, SQL не выполняй.",
    )

    trace = _assert_validation_pipeline(exchange)
    check = _assert_protocol_check(trace, "key_reconciliation", statuses={"ready"})
    target = _trace_target(trace, case.target_table)
    assert [
        str(value).casefold() for value in target.get("comparison_key") or []
    ] == [case.target_field.casefold()], target
    assert target.get("comparison_key_source") == "explicit", target
    assert "expected" in json.dumps(check, ensure_ascii=False).casefold()
    _assert_protocol_sql_parseable(exchange)


@pytest.mark.live_validation
@pytest.mark.skipif(
    LIVE_AGENT_MODE != "multiagent",
    reason="deterministic validation_protocol exists only in multiagent mode",
)
def test_live_validation_protocol_field_level_reconciliation(live_chat_client):
    case = _protocol_live_case(min_mapped_fields=2)
    conn = db_storage.get_db_connection()
    try:
        mapped_fields = conn.execute(
            """
            SELECT TRIM(source_field), TRIM(target_field)
            FROM s2t_transformations
            WHERE file_id = ?
              AND TRIM(source_table) = ? COLLATE NOCASE
              AND TRIM(target_table) = ? COLLATE NOCASE
              AND TRIM(transformation_rule) = ?
              AND NULLIF(TRIM(source_field), '') IS NOT NULL
              AND NULLIF(TRIM(target_field), '') IS NOT NULL
            ORDER BY id
            """,
            (
                case.file_id,
                case.source_table,
                case.target_table,
                case.transformation_rule,
            ),
        ).fetchall()
    finally:
        conn.close()
    unique_target_fields = list(
        dict.fromkeys(str(row[1]) for row in mapped_fields)
    )
    assert len(unique_target_fields) >= 2, mapped_fields
    comparison_key = unique_target_fields[0]
    compared_field = unique_target_fields[1]
    exchange = _chat(
        live_chat_client,
        f"Составь explicit тест-протокол {case.source_table} → "
        f"{case.target_table} только для check=field_mismatch. Сравни поле "
        f"{compared_field} по сохранённой transformation; comparison key "
        f"явно задаю {comparison_key}. SQL не выполняй.",
    )

    trace = _assert_validation_pipeline(exchange)
    _assert_protocol_check(trace, "field_mismatch", statuses={"ready"})
    target = _trace_target(trace, case.target_table)
    assert [
        str(value).casefold() for value in target.get("comparison_key") or []
    ] == [comparison_key.casefold()], target
    sql_blocks = _assert_protocol_sql_parseable(exchange)
    combined = "\n".join(sql_blocks).casefold()
    assert compared_field.casefold() in combined
    assert "expected" in combined and "actual" in combined
    mismatch_pattern = re.compile(
        r"where\s+row\([^)]*\b"
        + re.escape(compared_field.casefold())
        + r"\b[^)]*\)\s+is\s+distinct\s+from\s+row\([^)]*\b"
        + re.escape(compared_field.casefold())
        + r"\b[^)]*\)",
        re.DOTALL,
    )
    assert mismatch_pattern.search(combined), combined


@pytest.mark.live_validation
@pytest.mark.skipif(
    LIVE_AGENT_MODE != "multiagent",
    reason="deterministic validation_protocol exists only in multiagent mode",
)
def test_live_validation_protocol_preload_constraint_checks(live_chat_client):
    file_id, target, source, _, _ = _s2t_work_case_fixture()
    exchange = _chat(
        live_chat_client,
        f"Для file_id={file_id} и загрузки {source} → {target} составь explicit "
        "protocol с check=expected_required_nulls и check=schema_compatibility. "
        "Сначала выведи Phase 0 preflight; SQL не выполняй.",
    )

    trace = _assert_validation_pipeline(exchange)
    _assert_protocol_check(trace, "expected_required_nulls", statuses={"ready"})
    _assert_protocol_check(trace, "schema_compatibility", statuses={"ready", "partial"})
    _assert_protocol_phases(trace, {0})
    preflight = _trace_values(_trace_target(trace, target), "preflight")
    assert preflight and any(preflight), trace
    tools = _tool_names(exchange)
    assert "list_source_column_catalog" in tools, tools
    assert "list_target_column_catalog" in tools, tools
    _assert_protocol_sql_parseable(exchange)


@pytest.mark.live_validation
@pytest.mark.skipif(
    LIVE_AGENT_MODE != "multiagent",
    reason="deterministic validation_protocol exists only in multiagent mode",
)
def test_live_validation_protocol_expression_projection(live_chat_client):
    case = _protocol_live_case(expression=True, min_mapped_fields=2)
    exchange = _chat(
        live_chat_client,
        f"Для file_id={case.file_id} составь explicit protocol "
        f"{case.source_table} → {case.target_table} с checks=field_mismatch и "
        f"transformation_correctness. Comparison key явно {case.target_field}. "
        "Сохрани expression projections из S2T; SQL не выполняй.",
    )

    trace = _assert_validation_pipeline(exchange)
    _assert_protocol_check(trace, "field_mismatch", statuses={"ready"})
    target = _trace_target(trace, case.target_table)
    normalized = target.get("normalized_transformation")
    assert isinstance(normalized, dict), target
    assert normalized.get("parse_status") == "ok", normalized
    projections = normalized.get("projections")
    assert projections, normalized
    serialized = json.dumps(projections, ensure_ascii=False).casefold()
    assert any(token in serialized for token in ("coalesce", "case", "cast")), normalized
    _assert_protocol_sql_parseable(exchange)


@pytest.mark.live_validation
@pytest.mark.skipif(
    LIVE_AGENT_MODE != "multiagent",
    reason="deterministic validation_protocol exists only in multiagent mode",
)
def test_live_validation_protocol_explicit_key_without_catalog_pk(live_chat_client):
    case = _protocol_live_case()
    exchange = _chat(
        live_chat_client,
        f"Без file selector составь explicit protocol {case.source_table} → "
        f"{case.target_table} с checks=key_uniqueness,key_reconciliation. "
        f"Явный comparison key={case.target_field}; PK каталога не используй. "
        "SQL не выполняй.",
    )

    trace = _assert_validation_pipeline(exchange)
    target = _trace_target(trace, case.target_table)
    assert target.get("comparison_key_source") == "explicit", target
    assert [
        str(value).casefold() for value in target.get("comparison_key") or []
    ] == [case.target_field.casefold()], target
    _assert_protocol_check(trace, "key_uniqueness", statuses={"ready"})
    _assert_protocol_check(trace, "key_reconciliation", statuses={"ready"})
    assert not (set(_tool_names(exchange)) & _CATALOG_READERS), exchange.metrics.tool_calls
    _assert_protocol_sql_parseable(exchange, minimum_blocks=2)


@pytest.mark.live_validation
@pytest.mark.skipif(
    LIVE_AGENT_MODE != "multiagent",
    reason="deterministic validation_protocol exists only in multiagent mode",
)
def test_live_validation_protocol_separate_load_scopes(live_chat_client):
    case = _protocol_live_case()
    exchange = _chat(
        live_chat_client,
        f"Составь explicit protocol {case.source_table} → {case.target_table} "
        "с checks=row_count,missing_rows,extra_rows. В каждом SQL оставь "
        f"comparison key={case.target_field}. "
        "раздельные {{SOURCE_SCOPE_PREDICATE}} и {{TARGET_SCOPE_PREDICATE}}; "
        "не заменяй их общим load scope и ничего не выполняй.",
    )

    trace = _assert_validation_pipeline(exchange)
    for check in ("row_count", "missing_rows", "extra_rows"):
        _assert_protocol_check(trace, check, statuses={"ready"})
    sql_blocks = _assert_protocol_sql_parseable(exchange, minimum_blocks=3)
    combined = "\n".join(sql_blocks)
    assert "{{SOURCE_SCOPE_PREDICATE}}" in combined
    assert "{{TARGET_SCOPE_PREDICATE}}" in combined


@pytest.mark.live_validation
@pytest.mark.skipif(
    LIVE_AGENT_MODE != "multiagent",
    reason="deterministic validation_protocol exists only in multiagent mode",
)
def test_live_validation_protocol_minimal_readers(live_chat_client):
    case = _protocol_live_case()
    exchange = _chat(
        live_chat_client,
        f"Без файла составь explicit protocol {case.source_table} → "
        f"{case.target_table} только с check=row_count. SQL не выполняй.",
    )

    trace = _assert_validation_pipeline(exchange)
    _assert_protocol_check(trace, "row_count", statuses={"ready"})
    tools = _tool_names(exchange)
    assert tools.count("read_s2t_source_to_target") == 1, tools
    assert not (set(tools) & _CATALOG_READERS), tools
    assert set(tools) <= {"read_s2t_source_to_target"}, tools
    _assert_protocol_sql_parseable(exchange)


@pytest.mark.live_validation
@pytest.mark.skipif(
    LIVE_AGENT_MODE != "multiagent",
    reason="deterministic validation_protocol exists only in multiagent mode",
)
def test_live_validation_protocol_table_typo_resolution(live_chat_client):
    target_mention, canonical_target = _unique_typo_case("target")
    source = str(
        _fetch_one(
            """
            SELECT TRIM(source_table)
            FROM s2t_transformations
            WHERE TRIM(target_table) = ? COLLATE NOCASE
              AND NULLIF(TRIM(source_table), '') IS NOT NULL
            ORDER BY id LIMIT 1
            """,
            (canonical_target,),
        )[0]
    )
    exchange = _chat(
        live_chat_client,
        f"Составь explicit protocol {source} → {target_mention} только с "
        "check=row_count. В target есть опечатка: разреши имя до exact reader; "
        "SQL не выполняй.",
    )

    trace = _assert_validation_pipeline(exchange)
    _assert_resolved_event(
        exchange,
        mention=target_mention,
        role="target",
        canonical=canonical_target,
        method="fuzzy",
    )
    _assert_exact_reader_uses_canonical(
        exchange,
        canonical=canonical_target,
        role="target",
        rejected_mention=target_mention,
    )
    _assert_protocol_check(trace, "row_count", statuses={"ready"})


@pytest.mark.live_validation
@pytest.mark.skipif(
    LIVE_AGENT_MODE != "multiagent",
    reason="deterministic validation_protocol exists only in multiagent mode",
)
def test_live_validation_protocol_ambiguous_typo(live_chat_client):
    mention, expected_candidates = _ambiguous_partial_case("target")
    source = str(
        _fetch_one(
            """
            SELECT TRIM(source_table)
            FROM s2t_transformations
            WHERE TRIM(target_table) = ? COLLATE NOCASE
              AND NULLIF(TRIM(source_table), '') IS NOT NULL
            ORDER BY id LIMIT 1
            """,
            (expected_candidates[0],),
        )[0]
    )
    exchange = _chat(
        live_chat_client,
        f"Составь explicit protocol для source {source!r} и target "
        f"mention={mention!r} только с check=row_count. Это неточное имя: "
        "не угадывай между кандидатами.",
    )

    trace = _assert_validation_pipeline(exchange)
    event = _resolution_event(exchange, mention=mention, role="target")
    assert event.get("status") == "ambiguous", event
    assert event.get("method") == "partial", event
    assert event.get("canonical_name") in {None, ""}, event
    actual_candidates = _candidate_names(event)
    assert len(actual_candidates) >= 2, event
    assert {name.casefold() for name in expected_candidates} <= {
        name.casefold() for name in actual_candidates
    }, event
    assert event["candidate_set"].get("coverage") == "complete", event
    _assert_protocol_status(trace, "ambiguous_entity")
    assert "ambiguous_entity" in _trace_issue_codes(trace), trace
    exact_arguments = json.dumps(
        [
            item.arguments
            for item in exchange.metrics.tool_calls
            if item.name in _EXACT_S2T_READERS
        ],
        ensure_ascii=False,
    ).casefold()
    assert not any(name.casefold() in exact_arguments for name in expected_candidates)


@pytest.mark.live_validation
@pytest.mark.skipif(
    LIVE_AGENT_MODE != "multiagent",
    reason="deterministic validation_protocol exists only in multiagent mode",
)
def test_live_validation_protocol_semantic_file_resolution(live_chat_client):
    file_id, filename, description, source, target = _semantic_file_case()
    exchange = _chat(
        live_chat_client,
        f"Для файла по смысловому описанию {description!r} составь explicit "
        f"protocol {source} → {target} только с check=row_count. Не используй "
        "filename как подсказку и не выполняй SQL.",
    )

    trace = _assert_validation_pipeline(exchange)
    semantic_events = [
        event
        for event in _resolution_events(exchange)
        if event.get("role") == "file" and event.get("method") == "semantic"
    ]
    assert semantic_events, _resolution_events(exchange)
    event = semantic_events[-1]
    assert event.get("status") == "resolved", event
    assert int(event.get("file_id") or 0) == file_id, event
    assert str(event.get("canonical_name") or "").casefold() == filename.casefold(), event
    assert event["candidate_set"].get("source") == "semantic_search_descriptions", event
    assert "semantic_search_descriptions" in _tool_names(exchange)
    _assert_protocol_check(trace, "row_count", statuses={"ready"})


@pytest.mark.live_validation
@pytest.mark.skipif(
    LIVE_AGENT_MODE != "multiagent",
    reason="deterministic validation_protocol exists only in multiagent mode",
)
def test_live_validation_protocol_without_file(live_chat_client):
    case = _protocol_live_case()
    exchange = _chat(
        live_chat_client,
        f"Без file_id и filename составь standard protocol "
        f"{case.source_table} → {case.target_table}. Доступные проверки "
        "сформируй, catalog-dependent явно отметь unavailable; SQL не выполняй.",
    )

    trace = _assert_validation_pipeline(exchange)
    _assert_protocol_mode(trace, "standard")
    _assert_protocol_status(trace, "partial_protocol")
    _assert_protocol_check(trace, "row_count", statuses={"ready"})
    unavailable = [
        record
        for kind in _ALL_PROTOCOL_CHECKS
        for record in _protocol_check_records(trace, kind)
        if record.get("status") == "unavailable"
    ]
    assert unavailable, trace
    assert not (set(_tool_names(exchange)) & _CATALOG_READERS), exchange.metrics.tool_calls
    _assert_protocol_sql_parseable(exchange)


@pytest.mark.live_validation
@pytest.mark.skipif(
    LIVE_AGENT_MODE != "multiagent",
    reason="deterministic validation_protocol exists only in multiagent mode",
)
def test_live_validation_protocol_without_file_no_catalog_dependency(live_chat_client):
    case = _protocol_live_case()
    exchange = _chat(
        live_chat_client,
        f"Без file selector составь explicit protocol {case.source_table} → "
        f"{case.target_table} только с checks=row_count,transformation_correctness. "
        "SQL не выполняй.",
    )

    trace = _assert_validation_pipeline(exchange)
    _assert_protocol_status(trace, "ready")
    _assert_protocol_check(trace, "row_count", statuses={"ready"})
    _assert_protocol_check(trace, "transformation_correctness", statuses={"ready"})
    assert not (set(_tool_names(exchange)) & _CATALOG_READERS), exchange.metrics.tool_calls
    _assert_protocol_sql_parseable(exchange, minimum_blocks=2)


@pytest.mark.live_validation
@pytest.mark.skipif(
    LIVE_AGENT_MODE != "multiagent",
    reason="deterministic validation_protocol exists only in multiagent mode",
)
def test_live_validation_protocol_source_catalog_dependency(live_chat_client):
    file_id, target, source, _, _ = _s2t_work_case_fixture()
    exchange = _chat(
        live_chat_client,
        f"Для file_id={file_id} составь explicit protocol {source} → {target} "
        "только с check=schema_compatibility. Прочитай source и target catalog, "
        "SQL не выполняй.",
    )

    trace = _assert_validation_pipeline(exchange)
    _assert_protocol_check(trace, "schema_compatibility", statuses={"ready", "partial"})
    tools = _tool_names(exchange)
    assert tools.count("list_source_column_catalog") == 1, tools
    assert tools.count("list_target_column_catalog") == 1, tools
    assert tools.count("read_s2t_source_to_target") == 1, tools
    assert tools.count("read_s2t_by_target_table") == 1, tools
    source_call = next(
        item
        for item in exchange.metrics.tool_calls
        if item.name == "list_source_column_catalog"
    )
    target_call = next(
        item
        for item in exchange.metrics.tool_calls
        if item.name == "list_target_column_catalog"
    )
    assert source_call.arguments.get("file_id") == file_id, source_call
    assert source_call.arguments.get("table_name") == source, source_call
    assert target_call.arguments.get("file_id") == file_id, target_call
    assert target_call.arguments.get("table_name") == target, target_call
    _assert_protocol_sql_parseable(exchange)


@pytest.mark.live_resolution
@pytest.mark.skipif(
    LIVE_AGENT_MODE != "multiagent",
    reason="scenario verifies model-owned agentic candidate selection",
)
def test_live_agent_resolves_table_typo_before_exact_reader(live_chat_client):
    mention, canonical = _unique_typo_case("target")
    exchange = _chat(
        live_chat_client,
        f"Покажи сохранённые S2T mappings для target table {mention!r}. "
        "В имени опечатка: сначала прочитай кандидатов обычным read-only "
        "инструментом, самостоятельно выбери подтверждённое имя, затем "
        "используй exact reader; автоматический fuzzy resolver не используй.",
    )

    _assert_agentic_pipeline(exchange)
    _assert_model_owned_table_candidate_retrieval(exchange)
    _assert_exact_reader_uses_canonical(
        exchange,
        canonical=canonical,
        role="target",
        rejected_mention=mention,
    )
    _assert_no_worker_reroute(exchange)


@pytest.mark.live_resolution
@pytest.mark.skipif(
    LIVE_AGENT_MODE != "multiagent",
    reason="scenario verifies exact agentic reads need no resolver",
)
def test_live_agent_skips_resolution_for_exact_table(live_chat_client):
    canonical = _role_table_names("target")[0]
    exchange = _chat(
        live_chat_client,
        f"Покажи сохранённые S2T mappings для точной canonical target table "
        f"{canonical!r}; используй ролевой exact reader.",
    )

    _assert_agentic_pipeline(exchange)
    assert "resolve_entities" not in _tool_names(exchange), exchange.metrics.tool_calls
    assert not [
        event
        for event in _resolution_events(exchange)
        if str(event.get("mention") or "").casefold() == canonical.casefold()
    ], _resolution_events(exchange)
    _assert_exact_reader_uses_canonical(
        exchange,
        canonical=canonical,
        role="target",
    )
    _assert_no_worker_reroute(exchange)


@pytest.mark.live_resolution
@pytest.mark.skipif(
    LIVE_AGENT_MODE != "multiagent",
    reason="scenario verifies model-owned agentic candidate selection",
)
def test_live_agent_resolves_partial_table_name(live_chat_client):
    mention, canonical = _unique_partial_case("source")
    exchange = _chat(
        live_chat_client,
        f"Покажи сохранённые S2T mappings для неполного source table mention "
        f"{mention!r}; сначала прочитай кандидатов обычным read-only "
        "инструментом, самостоятельно выбери подтверждённое имя, затем "
        "вызови exact reader; автоматический resolver не используй.",
    )

    _assert_agentic_pipeline(exchange)
    _assert_model_owned_table_candidate_retrieval(exchange)
    _assert_exact_reader_uses_canonical(
        exchange,
        canonical=canonical,
        role="source",
        rejected_mention=mention,
    )
    _assert_no_worker_reroute(exchange)


@pytest.mark.live_resolution
@pytest.mark.skipif(
    LIVE_AGENT_MODE != "multiagent",
    reason="scenario verifies semantic retrieval without an agentic resolver",
)
def test_live_agent_resolves_semantic_table_mention(live_chat_client):
    canonical, description = _semantic_table_case("target")
    exchange = _chat(
        live_chat_client,
        f"Найди target table по бизнес-смыслу {description!r}, затем покажи "
        "её сохранённые S2T mappings ролевым exact reader. Имя таблицы в "
        "запросе намеренно не указано.",
    )

    _assert_agentic_pipeline(exchange)
    assert "semantic_search_descriptions" in _tool_names(exchange)
    _assert_exact_reader_uses_canonical(
        exchange,
        canonical=canonical,
        role="target",
    )
    _assert_no_worker_reroute(exchange)


@pytest.mark.live_resolution
@pytest.mark.skipif(
    LIVE_AGENT_MODE != "multiagent",
    reason="scenario verifies lossless semantic candidate handoff",
)
def test_live_agent_batches_all_semantic_candidates_into_s2t_search(
    live_chat_client,
):
    description = _semantic_batch_column_case()
    exchange = _chat(
        live_chat_client,
        f"Найди по бизнес-смыслу {description!r} до пяти колонок "
        "через semantic_search_descriptions с scope=columns. Не выбирай "
        "один лучший кандидат: для всех различающихся "
        "технических имён найди S2T одним batch-вызовом. Отдельно "
        "покажи и semantic candidates, и S2T-result.",
    )

    _assert_agentic_pipeline(exchange)
    calls = exchange.metrics.tool_calls
    semantic_calls = [
        (index, item)
        for index, item in enumerate(calls)
        if item.name == "semantic_search_descriptions"
    ]
    previous_calls = [
        (index, item)
        for index, item in enumerate(calls)
        if item.name == "read_previous_result"
    ]
    search_calls = [
        (index, item)
        for index, item in enumerate(calls)
        if item.name == "search_s2t_transformations"
    ]
    assert len(semantic_calls) == len(previous_calls) == len(search_calls) == 1, [
        item.name for item in calls
    ]
    assert semantic_calls[0][0] < previous_calls[0][0] < search_calls[0][0], [
        item.name for item in calls
    ]
    semantic_args = semantic_calls[0][1].arguments
    assert semantic_args.get("scope") == "columns", semantic_args
    assert 2 <= int(semantic_args.get("limit") or 10) <= 5, semantic_args
    batch_args = search_calls[0][1].arguments
    needles = [str(value) for value in batch_args.get("needles") or []]
    assert batch_args.get("needle") in {None, ""}, batch_args
    assert len(needles) >= 2, batch_args
    assert len(needles) == len({value.casefold() for value in needles}), batch_args

    semantic_payloads = []
    for item in exchange.result.display_items:
        if item.name != "semantic_search_descriptions":
            continue
        try:
            payload = json.loads(item.content)
        except (TypeError, json.JSONDecodeError):
            continue
        if isinstance(payload, dict):
            semantic_payloads.append(payload)
    assert semantic_payloads, exchange.result.display_items
    displayed_names = {
        str(mapping.get("column_name") or mapping.get("name") or "").strip()
        for payload in semantic_payloads
        for mapping in _nested_mappings(payload)
        if str(mapping.get("scope") or "")
        in {"source_columns", "target_columns"}
        and str(mapping.get("column_name") or mapping.get("name") or "").strip()
    }
    assert len(displayed_names) >= 2, semantic_payloads
    assert {value.casefold() for value in displayed_names} == {
        value.casefold() for value in needles
    }, {"displayed_names": displayed_names, "needles": needles}
    _assert_no_worker_reroute(exchange)


@pytest.mark.live_resolution
@pytest.mark.skipif(
    LIVE_AGENT_MODE != "multiagent",
    reason="scenario verifies explicit ambiguity without an agentic resolver",
)
def test_live_agent_does_not_guess_ambiguous_entity(live_chat_client):
    mention, expected_candidates = _ambiguous_partial_case("source")
    exchange = _chat(
        live_chat_client,
        f"Покажи S2T mappings для неточного source table mention {mention!r}. "
        "Если кандидатов несколько, не выбирай один и явно запроси уточнение.",
    )

    _assert_agentic_pipeline(exchange)
    _assert_model_owned_table_candidate_retrieval(exchange)
    exact_arguments = json.dumps(
        [
            item.arguments
            for item in exchange.metrics.tool_calls
            if item.name in _EXACT_S2T_READERS
        ],
        ensure_ascii=False,
    ).casefold()
    assert not any(name.casefold() in exact_arguments for name in expected_candidates)
    assert "?" in exchange.result.answer or "уточн" in exchange.result.answer.casefold()
    _assert_no_worker_reroute(exchange)


@pytest.mark.live_resolution
@pytest.mark.skipif(
    LIVE_AGENT_MODE != "multiagent",
    reason="scenario verifies model-owned role-aware candidate selection",
)
def test_live_entity_resolution_preserves_source_target_role(live_chat_client):
    source_mention, source = _unique_typo_case("source")
    target_mention, target = _unique_typo_case("target")
    exchange = _chat(
        live_chat_client,
        f"Отдельно прочитай mappings для source mention {source_mention!r} и "
        f"для target mention {target_mention!r}. В обоих именах опечатки; "
        "не смешивай роли и используй соответствующие exact readers.",
    )

    _assert_agentic_pipeline(exchange)
    _assert_model_owned_table_candidate_retrieval(exchange)
    _assert_exact_reader_uses_canonical(
        exchange,
        canonical=source,
        role="source",
    )
    _assert_exact_reader_uses_canonical(
        exchange,
        canonical=target,
        role="target",
    )
    _assert_no_worker_reroute(exchange)


@pytest.mark.live_resolution
@pytest.mark.skipif(
    LIVE_AGENT_MODE != "multiagent",
    reason="scenario verifies resolver isolation to validation",
)
def test_live_validation_and_agentic_use_same_resolution_semantics(live_chat_client):
    mention, canonical = _unique_typo_case("target")
    source = str(
        _fetch_one(
            """
            SELECT TRIM(source_table)
            FROM s2t_transformations
            WHERE TRIM(target_table) = ? COLLATE NOCASE
              AND NULLIF(TRIM(source_table), '') IS NOT NULL
            ORDER BY id LIMIT 1
            """,
            (canonical,),
        )[0]
    )
    validation_exchange = _chat(
        live_chat_client,
        f"Составь explicit protocol {source} → {mention} только с "
        "check=row_count. Target mention содержит опечатку; SQL не выполняй.",
    )
    agentic_exchange = _chat(
        live_chat_client,
        f"Покажи S2T mappings для target table mention {mention!r}; в имени "
        "опечатка, поэтому прочитай кандидатов обычным read-only инструментом, "
        "выбери подтверждённое имя моделью и затем используй exact reader.",
    )

    _assert_validation_pipeline(validation_exchange)
    _assert_agentic_pipeline(agentic_exchange)
    validation_event = _resolution_event(
        validation_exchange,
        mention=mention,
        role="target",
    )
    assert validation_event.get("status") == "resolved", validation_event
    assert validation_event.get("method") == "fuzzy", validation_event
    assert (
        str(validation_event.get("canonical_name") or "").casefold()
        == canonical.casefold()
    ), validation_event
    _assert_model_owned_table_candidate_retrieval(agentic_exchange)
    _assert_exact_reader_uses_canonical(
        agentic_exchange,
        canonical=canonical,
        role="target",
        rejected_mention=mention,
    )
    _assert_no_worker_reroute(agentic_exchange)


@pytest.mark.live_handoff
@pytest.mark.skipif(
    LIVE_AGENT_MODE != "multiagent",
    reason="DAG concurrency A/B is multiagent-only",
)
def test_live_dag_ab_independent_roots(live_chat_client):
    exchange = _chat(
        live_chat_client,
        "Выполни двумя независимыми root workers два отдельных чтения SQLite: "
        "A — точный COUNT(*) таблицы source_tables, B — точный COUNT(*) "
        "таблицы target_tables. Не связывай A и B зависимостью. Верни строго "
        "source_tables=<число>, target_tables=<число>.",
    )
    _assert_public_answer(exchange.result.answer)
    assert exchange.metrics.coordinator_dag, exchange.metrics


@pytest.mark.live_handoff
@pytest.mark.skipif(
    LIVE_AGENT_MODE != "multiagent",
    reason="DAG concurrency A/B is multiagent-only",
)
def test_live_dag_ab_ready_child(live_chat_client):
    exchange = _chat(
        live_chat_client,
        "Построй DAG чтений: A быстро находит MAX(file_id) в files; B независимо "
        "читает общий S2T summary по всем target tables и возвращает три лидера "
        "по mapping_count; C зависит только от A и считает точный COUNT(*) "
        "target_tables для найденного file_id. C не должен ждать B. Верни "
        "file_id, target_count и три target_table из B.",
    )
    _assert_public_answer(exchange.result.answer)
    assert exchange.metrics.coordinator_dag, exchange.metrics


@pytest.mark.live_handoff
@pytest.mark.skipif(
    LIVE_AGENT_MODE != "multiagent",
    reason="DAG concurrency A/B is multiagent-only",
)
def test_live_dag_ab_fan_in(live_chat_client):
    exchange = _chat(
        live_chat_client,
        "Построй fan-in DAG. Root A отдельно находит самый частый непустой "
        "source_table в s2t_transformations. Root B отдельно находит самый "
        "частый непустой target_table. MERGE зависит прямо от A и B и читает "
        "точное число S2T-строк для направленной пары A→B. Верни source_table, "
        "target_table и pair_count.",
    )
    _assert_public_answer(exchange.result.answer)
    assert exchange.metrics.coordinator_dag, exchange.metrics


@pytest.mark.live_handoff
@pytest.mark.skipif(
    LIVE_AGENT_MODE != "multiagent",
    reason="DAG concurrency A/B is multiagent-only",
)
def test_live_dag_ab_fan_out(live_chat_client):
    exchange = _chat(
        live_chat_client,
        "Построй fan-out DAG. Root A находит target_table с максимальным числом "
        "S2T-строк. После A параллельно запусти B и C: B считает все строки "
        "этого target_table, C считает различные непустые source_table для "
        "него. B и C зависят только от A. Верни target_table, row_count и "
        "source_count.",
    )
    _assert_public_answer(exchange.result.answer)
    assert exchange.metrics.coordinator_dag, exchange.metrics


@pytest.mark.live_handoff
@pytest.mark.skipif(
    LIVE_AGENT_MODE != "multiagent",
    reason="DAG concurrency A/B is multiagent-only",
)
def test_live_dag_ab_failed_parent(live_chat_client):
    exchange = _chat(
        live_chat_client,
        "Проверь отказоустойчивый DAG. Root A пытается точным catalog-reader "
        "прочитать заведомо отсутствующую target_table "
        "__dag_ab_missing_target_7f31__. C зависит от A и не должен запускаться, "
        "если A завершился failed без usable result. Независимый root B считает "
        "COUNT(*) таблицы files и должен завершиться. Честно сообщи отсутствие "
        "A/C и верни files=<число>.",
    )
    _assert_public_answer(exchange.result.answer)
    assert exchange.metrics.coordinator_dag, exchange.metrics


@pytest.mark.live_handoff
@pytest.mark.skipif(
    LIVE_AGENT_MODE != "multiagent",
    reason="DAG concurrency A/B is multiagent-only",
)
def test_live_dag_ab_partial_parent(live_chat_client):
    exchange = _chat(
        live_chat_client,
        "Построй DAG с partial parent. A пытается прочитать все строки общего "
        "target column catalog через list_column_catalog с scope=target_columns "
        "и limit=50; bounded/truncated preview обязан завершиться как structured "
        "partial с usable previous result. B зависит прямо от A, лениво читает "
        "этот result и одним batch-вызовом ищет S2T для полученных технических "
        "имён. Не выдавай preview за полный набор; верни первые кандидаты и "
        "доступные mappings.",
    )
    _assert_public_answer(exchange.result.answer)
    assert exchange.metrics.coordinator_dag, exchange.metrics


@pytest.mark.live_handoff
@pytest.mark.skipif(
    LIVE_AGENT_MODE != "multiagent",
    reason="DAG concurrency A/B is multiagent-only",
)
def test_live_dag_ab_eight_roots(live_chat_client):
    exchange = _chat(
        live_chat_client,
        "Создай максимально допустимый DAG ровно из восьми независимых root "
        "workers без dependencies. Каждый делает отдельный точный COUNT(*) "
        "одной таблицы: files, source_tables, target_tables, source_columns, "
        "target_columns, additional_objects, pxf_to_a, s2t_transformations. "
        "Верни восемь подписанных значений и ничего не объединяй в один worker.",
    )
    _assert_public_answer(exchange.result.answer)
    assert exchange.metrics.coordinator_dag, exchange.metrics


@pytest.mark.live_handoff
@pytest.mark.skipif(
    LIVE_AGENT_MODE != "multiagent",
    reason="DAG concurrency A/B is multiagent-only",
)
def test_live_dag_ab_sequential_control(live_chat_client):
    exchange = _chat(
        live_chat_client,
        "Построй последовательный контрольный DAG A→B→C. A находит file_id "
        "самого нового файла. B зависит только от A и находит target_table этого "
        "файла с максимальным числом catalog-колонок. C зависит только от B и "
        "читает точное число S2T-строк для найденного target_table. Верни "
        "file_id, target_table и s2t_count.",
    )
    _assert_public_answer(exchange.result.answer)
    assert exchange.metrics.coordinator_dag, exchange.metrics
