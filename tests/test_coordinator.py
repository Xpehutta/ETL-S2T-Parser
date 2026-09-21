import json
import re
from contextlib import nullcontext
from unittest.mock import MagicMock, patch

import pytest
from langchain_core.messages import AIMessage, ToolMessage

from agents.contracts import (
    EvidenceArtifact,
    EvidenceFact,
    PreviousResultReference,
    WorkerOutcome,
    WorkerPlan,
    WorkerRequestParts,
    parse_worker_request,
)
from agents.coordinator import CoordinatorAnswer
from agents.tools.saved_results import (
    SavedResultColumn,
    SavedResultDescriptor,
)


def _tool_message(name, args, call_id):
    clean_args = dict(args)
    if name == "submit_worker_plan":
        raw_steps = clean_args.get("steps")
        if isinstance(raw_steps, list) and all(
            isinstance(item, dict) and set(item) <= {"task"}
            for item in raw_steps
        ):
            prior_ids = []
            explicit_steps = []
            for index, item in enumerate(raw_steps, start=1):
                step_id = f"step_{index}"
                explicit_steps.append(
                    {
                        "id": step_id,
                        "task": item.get("task"),
                        "depends_on": list(prior_ids),
                    }
                )
                prior_ids.append(step_id)
            clean_args["steps"] = explicit_steps
    if name == "submit_upstream_output":
        action = str(clean_args.pop("action", "answer") or "answer")
        if action == "request_more_data":
            name = "submit_upstream_data_decision"
            clean_args = {
                "decision": "reroute",
                "problem": clean_args.get("problem", ""),
            }
        else:
            name = "submit_upstream_answer"
            clean_args.pop("problem", None)
    return AIMessage(
        content="",
        tool_calls=[
            {
                "name": name,
                "args": clean_args,
                "id": call_id,
                "type": "tool_call",
            }
        ],
    )


def _artifact(
    display_ref,
    tool_name,
    preview,
    *,
    evidence_id=None,
    compact_args=None,
    truncated=False,
    dataset_ref=None,
):
    return EvidenceArtifact(
        evidence_id=evidence_id or f"evidence-{display_ref}",
        tool_name=tool_name,
        compact_args=dict(compact_args or {}),
        preview=preview,
        truncated=truncated,
        display_ref=display_ref,
        dataset_ref=dataset_ref,
    )


def _outcome(
    summary,
    *,
    status="complete",
    stop_reason=None,
    unmet_requirements=(),
    evidence=(),
    datasets=(),
    previous_results=(),
    facts=None,
):
    evidence_items = list(evidence)
    fact_items = facts
    if fact_items is None and evidence_items:
        fact_items = [
            EvidenceFact(
                text=summary,
                evidence_ids=[item.evidence_id for item in evidence_items],
            )
        ]
    return WorkerOutcome(
        summary=summary,
        status=status,
        stop_reason=stop_reason,
        unmet_requirements=list(unmet_requirements),
        facts=list(fact_items or []),
        evidence=evidence_items,
        datasets=list(datasets),
        previous_results=list(previous_results),
    )


class _BoundModel:
    def __init__(self, parent, tool_name):
        self.parent = parent
        self.tool_name = tool_name

    def invoke(self, messages, **kwargs):
        del kwargs
        self.parent.messages.append((self.tool_name, list(messages)))
        response = self.parent.responses[self.tool_name].pop(0)
        if callable(response):
            return response(messages)
        return response


class _CoordinatorModel:
    def __init__(self, responses):
        self.responses = {name: list(items) for name, items in responses.items()}
        self.responses.setdefault(
            "select_operation_skills",
            [
                _tool_message(
                    "select_operation_skills",
                    {
                        "pipeline": "agentic",
                        "skills": [],
                        "sql_risk_aspects": [],
                    },
                    "operation-skills-1",
                )
            ],
        )
        self.responses.setdefault(
            "submit_validation_contract_review",
            [
                _tool_message(
                    "submit_validation_contract_review",
                    {"decision": "accept", "issues": []},
                    "validation-contract-review-1",
                )
            ],
        )
        legacy_upstream = self.responses.pop("submit_upstream_output", [])
        if legacy_upstream:
            decisions = self.responses.setdefault(
                "submit_upstream_data_decision", []
            )
            answers = self.responses.setdefault("submit_upstream_answer", [])
            for item in legacy_upstream:
                calls = list(item.tool_calls) if isinstance(item, AIMessage) else []
                if len(calls) == 1 and calls[0].get("name") == (
                    "submit_upstream_data_decision"
                ):
                    decisions.append(item)
                    continue
                if len(calls) == 1 and calls[0].get("name") == (
                    "submit_upstream_answer"
                ):
                    decisions.append(
                        _tool_message(
                            "submit_upstream_data_decision",
                            {"decision": "pass"},
                            str(calls[0].get("id") or "upstream")
                            + "-decision",
                        )
                    )
                    answers.append(item)
                    continue
                decisions.append(item)
        self.messages = []
        self.tool_choices = []

    def bind_tools(self, tools, tool_choice=None):
        tool_names = [tool["function"]["name"] for tool in tools]
        recorded_choice = (
            tuple(tool_names) if len(tool_names) > 1 else tool_names[0]
        )
        tool_name = tool_names[0]
        self.tool_choices.append((recorded_choice, tool_choice))
        return _BoundModel(self, tool_name)


def _payload(model, tool_name, occurrence=0):
    matching = [messages for name, messages in model.messages if name == tool_name]
    return json.loads(matching[occurrence][1].content)


def _scope_extraction_message(
    *,
    execution_mode,
    source_table,
    target_table,
    source_field=None,
    target_field=None,
    source_table_attestation=None,
    target_table_attestation=None,
    source_field_attestation=None,
    target_field_attestation=None,
    file_id=None,
    file_attestation=None,
    call_id="scope-extraction-1",
):
    source = {"table_name": source_table}
    target = {"table_name": target_table}
    origin = {
        "source": {
            "table_name": source_table_attestation or source_table,
        },
        "target": {
            "table_name": target_table_attestation or target_table,
        },
    }
    if source_field is not None:
        source["field_name"] = source_field
        origin["source"]["field_name"] = (
            source_field_attestation or source_field
        )
    if target_field is not None:
        target["field_name"] = target_field
        origin["target"]["field_name"] = (
            target_field_attestation or target_field
        )
    payload = {
        "execution_mode": execution_mode,
        "source": source,
        "target": target,
        "origin": origin,
    }
    if file_id is not None:
        payload["file_id"] = file_id
    if file_attestation is not None:
        origin["file_id"] = file_attestation
    return _tool_message("submit_sql_risk_scope", payload, call_id)


def _scope_assessment_message(
    answer,
    *,
    outcome="risk_present",
    display=True,
    limitations=(),
    call_id="scope-assessment-1",
):
    def response(messages):
        serialized = str(messages[1].content).removeprefix(
            "ASSESSMENT_INPUT:\n"
        )
        context = json.loads(serialized)["untrusted_evidence"]
        display_ids = (
            context["displayable_evidence_ids"] if display else []
        )
        return _tool_message(
            "submit_sql_risk_assessment",
            {
                "status": "complete",
                "outcome": outcome,
                "answer": answer,
                "used_evidence_ids": context["required_evidence_ids"],
                "reviewed_rule_ids": context["required_rule_ids"],
                "display_evidence_ids": display_ids,
                "limitations": list(limitations),
            },
            call_id,
        )

    return response


def _patches(model):
    return (
        patch("agents.coordinator.chat_model", model),
        patch("agents.coordinator.get_callback_handler", return_value=None),
        patch(
            "agents.coordinator.langfuse_trace_context",
            return_value=nullcontext(),
        ),
    )


def _responses(
    *,
    answer,
    used_evidence_ids=(),
    display_evidence_ids=(),
    plan_task="Получи факт.",
):
    return {
        "submit_worker_plan": [
            _tool_message(
                "submit_worker_plan",
                {
                    "steps": [
                        {
                            "task": plan_task,
                        }
                    ]
                },
                "plan-1",
            )
        ],
        "submit_upstream_output": [
            _tool_message(
                "submit_upstream_output",
                {
                    "answer": answer,
                    "used_evidence_ids": list(used_evidence_ids),
                    "display_evidence_ids": list(display_evidence_ids),
                },
                "upstream-1",
            )
        ],
    }


def test_coordinator_graph_routes_tasks_downstream_and_one_result_upstream():
    from agents.coordinator import build_coordinator_graph

    model = _CoordinatorModel({})
    graph = build_coordinator_graph(model)
    graph_view = graph.get_graph()

    assert model.tool_choices == [
        ("submit_worker_plan", "submit_worker_plan"),
        (
            "submit_upstream_data_decision",
            "submit_upstream_data_decision",
        ),
        ("submit_upstream_answer", "submit_upstream_answer"),
    ]
    assert {
        "downstream_plan",
        "sql_risk_scope",
        "worker",
        "upstream",
    }.issubset(graph_view.nodes)
    assert "upstream_answer" not in graph_view.nodes
    edges = {(edge.source, edge.target) for edge in graph_view.edges}
    assert ("__start__", "downstream_plan") in edges
    assert ("downstream_plan", "worker") in edges
    assert ("downstream_plan", "sql_risk_scope") in edges
    assert ("sql_risk_scope", "__end__") in edges
    assert ("upstream", "downstream_plan") in edges
    assert ("upstream", "__end__") in edges


def test_operation_skill_is_selected_once_and_applied_by_stage(monkeypatch):
    from agents.coordinator import coordinator_chat
    from agents.tools.context import (
        OPERATION_SQL_RISK_ASPECTS_EXPERIMENT_ENV,
    )

    monkeypatch.setenv(OPERATION_SQL_RISK_ASPECTS_EXPERIMENT_ENV, "0")

    responses = _responses(answer="Риск оценён.", plan_task="Прочитай правило.")
    responses["select_operation_skills"] = [
        _tool_message(
            "select_operation_skills",
            {
                "pipeline": "agentic",
                "skills": ["Анализ SQL-рисков"],
                "sql_risk_aspects": [],
            },
            "operation-skills-risk",
        )
    ]
    model = _CoordinatorModel(responses)
    model_patch, callback_patch, trace_patch = _patches(model)
    with (
        model_patch,
        callback_patch,
        trace_patch,
        patch(
            "agents.coordinator.worker_chat",
            return_value=_outcome("Правило прочитано."),
        ) as worker,
    ):
        result = coordinator_chat("Оцени риск потери строк в A → B.")

    assert result.answer == "Риск оценён."
    assert [name for name, _ in model.messages].count(
        "select_operation_skills"
    ) == 1
    worker_parts = parse_worker_request(worker.call_args.args[0])
    normalized_execution = " ".join(
        worker_parts.operation_execution_context.split()
    )
    normalized_completeness = " ".join(
        worker_parts.operation_completeness_context.split()
    )
    assert (
        "используй единственный directed mapping-reader только с обязательными "
        "`source_table` и `target_table`"
    ) in normalized_execution
    assert "Вызов по ID нерелевантен" in (
        worker_parts.operation_completeness_context
    )
    assert "Вызов по ID нерелевантен" not in (
        worker_parts.operation_execution_context
    )
    assert "используй единственный directed mapping-reader" not in (
        normalized_completeness
    )

    plan_system = next(
        messages[0].content
        for name, messages in model.messages
        if name == "submit_worker_plan"
    )
    decision_system = next(
        messages[0].content
        for name, messages in model.messages
        if name == "submit_upstream_data_decision"
    )
    answer_system = next(
        messages[0].content
        for name, messages in model.messages
        if name == "submit_upstream_answer"
    )
    normalized_decision = " ".join(decision_system.split())
    assert "Не создавай transformation ID" in plan_system
    assert "Нулевой mapping подтверждает отсутствие" in decision_system
    assert "не требуй больше evidence" in normalized_decision
    assert "условный вывод" in normalized_decision
    assert "не являются обязательным условием `pass`" in normalized_decision
    assert "Вызов по ID нерелевантен" not in decision_system
    assert "Различай итоговые" in answer_system
    assert "Нулевой mapping подтверждает отсутствие" not in answer_system


def test_sql_risk_router_propagates_only_requested_aspect(monkeypatch):
    from agents.coordinator import (
        OperationSkillSelection,
        _OPERATION_SKILL_PROMPT,
        _operation_skill_prompt,
        _operation_skill_tool_schema,
        coordinator_chat,
    )
    from agents.tools.context import (
        OPERATION_SQL_RISK_ASPECTS_EXPERIMENT_ENV,
    )

    monkeypatch.setenv(OPERATION_SQL_RISK_ASPECTS_EXPERIMENT_ENV, "1")

    responses = _responses(answer="JOIN может размножить строки.")
    responses["select_operation_skills"] = [
        _tool_message(
            "select_operation_skills",
            {
                "pipeline": "agentic",
                "skills": ["Анализ SQL-рисков"],
                "sql_risk_aspects": ["cardinality"],
            },
            "operation-cardinality",
        )
    ]
    model = _CoordinatorModel(responses)
    model_patch, callback_patch, trace_patch = _patches(model)
    with (
        model_patch,
        callback_patch,
        trace_patch,
        patch(
            "agents.coordinator.worker_chat",
            return_value=_outcome("Mapping прочитан."),
        ) as worker,
    ):
        result = coordinator_chat("Может ли JOIN размножить строки?")

    assert result.answer == "JOIN может размножить строки."
    operation_schema = _operation_skill_tool_schema()["function"][
        "parameters"
    ]
    assert operation_schema["properties"]["sql_risk_aspects"]["items"][
        "enum"
    ] == [
        "row_filtering",
        "cardinality",
        "constraint_rejection",
        "value_changes",
        "write_semantics",
    ]
    assert operation_schema["properties"]["sql_risk_aspects"][
        "maxItems"
    ] == 5
    assert "Может ли этот JOIN" not in _OPERATION_SKILL_PROMPT
    assert "sql_risk_execution_mode" not in _operation_skill_prompt()
    assert "sql_risk_execution_mode" not in operation_schema["properties"]
    assert "sql_risk_execution_mode" not in operation_schema["required"]
    assert "sql_risk_scope" not in operation_schema["properties"]["pipeline"][
        "enum"
    ]
    assert 'pipeline="sql_risk_scope"' not in _operation_skill_prompt()
    worker_parts = parse_worker_request(worker.call_args.args[0])
    for forbidden_aspect in (
        "row_filtering",
        "constraint_rejection",
        "value_changes",
        "write_semantics",
    ):
        assert forbidden_aspect not in worker_parts.operation_execution_context
        assert forbidden_aspect not in worker_parts.operation_completeness_context

    assert "`cardinality`" in worker_parts.operation_execution_context
    assert "`cardinality`" in worker_parts.operation_completeness_context
    for tool_name in (
        "submit_worker_plan",
        "submit_upstream_data_decision",
        "submit_upstream_answer",
    ):
        system_prompt = next(
            messages[0].content
            for name, messages in model.messages
            if name == tool_name
        )
        assert "`cardinality`" in system_prompt
        assert "`row_filtering`" not in system_prompt

    with pytest.raises(ValueError, match="sql_risk_aspects require"):
        OperationSkillSelection(
            pipeline="agentic",
            skills=[],
            sql_risk_aspects=["cardinality"],
        )
    with pytest.raises(ValueError, match="requires at least one"):
        OperationSkillSelection(
            pipeline="agentic",
            skills=["Анализ SQL-рисков"],
            sql_risk_aspects=[],
        )


def test_operation_scope_router_requires_distinct_pipeline_without_skill(
    monkeypatch,
):
    from agents.coordinator import (
        OperationSkillSelection,
        _operation_skill_prompt,
        _operation_skill_tool_schema,
    )
    from agents.sql_risk_scope_contract import (
        OPERATION_SQL_RISK_SCOPE_EVIDENCE_EXPERIMENT_ENV,
    )
    from agents.tools.context import OPERATION_SQL_RISK_ASPECTS_EXPERIMENT_ENV

    monkeypatch.setenv(OPERATION_SQL_RISK_ASPECTS_EXPERIMENT_ENV, "0")
    monkeypatch.setenv(
        OPERATION_SQL_RISK_SCOPE_EVIDENCE_EXPERIMENT_ENV,
        "1",
    )

    parameters = _operation_skill_tool_schema()["function"]["parameters"]
    assert "sql_risk_execution_mode" not in parameters["properties"]
    assert "sql_risk_execution_mode" not in parameters["required"]
    assert "sql_risk_aspects" not in parameters["properties"]
    assert "sql_risk_aspects" not in parameters["required"]
    assert "sql_risk_scope" in parameters["properties"]["pipeline"]["enum"]
    prompt = _operation_skill_prompt()
    assert "sql_risk_aspects" not in prompt
    assert "Конкретный вид риска" in prompt
    assert "внутренний native LLM-вызов" in prompt
    assert "conditional_cardinality" not in prompt
    assert "nullable_constraint" not in prompt
    assert 'pipeline="sql_risk_scope"' in prompt
    assert "JOIN, predicate" in prompt
    assert "исполнения/вычисления фактических метрик" in prompt
    assert "`not_null` или признака ключа" in prompt
    assert "`Совместимость колонок`" in prompt
    assert "nullable source с NOT NULL target" in prompt
    assert "полностью определяет прямое сравнение catalog-атрибутов" in prompt
    assert "определяется требуемым evidence, а не лексикой запроса" in prompt
    assert "regardless of the query language" in prompt
    assert "depends on transformation SQL structure" in prompt
    assert "Может ли этот JOIN" not in prompt

    from agents.coordinator import _operation_skill_repair_prompt

    repair_prompt = _operation_skill_repair_prompt()
    normalized_repair = " ".join(repair_prompt.split())
    assert "а не механически удаляй skills" in normalized_repair
    assert "без анализа transformation SQL" in normalized_repair
    assert "`agentic`" in repair_prompt
    assert "`Совместимость колонок`" in repair_prompt

    selection = OperationSkillSelection(
        pipeline="sql_risk_scope",
        skills=[],
        sql_risk_aspects=[],
    )
    assert selection.pipeline == "sql_risk_scope"
    assert selection.skills == []
    assert selection.sql_risk_aspects == []

    with pytest.raises(ValueError, match="pipeline=sql_risk_scope requires no"):
        OperationSkillSelection(
            pipeline="sql_risk_scope",
            skills=["Анализ SQL-рисков"],
            sql_risk_aspects=[],
        )

    with pytest.raises(ValueError, match="sql_risk_aspects require"):
        OperationSkillSelection(
            pipeline="agentic",
            skills=[],
            sql_risk_aspects=["row_filtering"],
        )
    with pytest.raises(ValueError, match="validation_protocol requires"):
        OperationSkillSelection(
            pipeline="validation_protocol",
            skills=["Проектирование проверки"],
            sql_risk_aspects=[],
        )


def test_internal_scope_schemas_are_gigachat_compatible():
    from langchain_gigachat.utils.function_calling import gigachat_fix_schema

    from agents.coordinator import (
        _sql_risk_assessment_tool_schema,
        _sql_risk_scope_extraction_tool_schema,
    )

    extraction_schema = _sql_risk_scope_extraction_tool_schema()
    assessment_schema = _sql_risk_assessment_tool_schema()
    serialized = json.dumps(extraction_schema, ensure_ascii=False)

    assert "$ref" not in serialized
    assert "$defs" not in serialized
    assert "anyOf" not in serialized
    assert gigachat_fix_schema(extraction_schema) == extraction_schema
    assert gigachat_fix_schema(assessment_schema) == assessment_schema
    parameters = extraction_schema["function"]["parameters"]
    assert set(parameters["required"]) == {
        "execution_mode",
        "source",
        "target",
        "origin",
    }
    assert set(parameters["properties"]["origin"]["required"]) == {
        "source",
        "target",
    }
    assert "file_id" not in parameters["required"]


def test_native_scope_boundary_rejects_an_extra_tool_call():
    from agents.coordinator import CoordinatorResponseError, _native_call_arguments

    message = AIMessage(
        content="",
        tool_calls=[
            {
                "name": "submit_sql_risk_scope",
                "args": {"execution_mode": "row_filtering"},
                "id": "scope",
                "type": "tool_call",
            },
            {
                "name": "unrequested_tool",
                "args": {},
                "id": "extra",
                "type": "tool_call",
            },
        ],
    )

    with pytest.raises(CoordinatorResponseError, match="ровно один native call"):
        _native_call_arguments(message, "submit_sql_risk_scope")

def test_operation_router_leaves_scope_extraction_to_internal_llm(
    monkeypatch,
):
    from agents.coordinator import select_operation_route
    from agents.sql_risk_scope_contract import (
        OPERATION_SQL_RISK_SCOPE_EVIDENCE_EXPERIMENT_ENV,
    )

    monkeypatch.setenv(
        OPERATION_SQL_RISK_SCOPE_EVIDENCE_EXPERIMENT_ENV,
        "1",
    )
    task = (
        "Для файла 'Mapping.xlsx' оцени nullable для "
        "stage.orders.id → mart.orders.id."
    )
    scope_route = {
        "pipeline": "sql_risk_scope",
        "skills": [],
        "sql_risk_aspects": [],
    }
    model = _CoordinatorModel(
        {
            "select_operation_skills": [
                _tool_message(
                    "select_operation_skills",
                    scope_route,
                    "scope-route",
                ),
            ]
        }
    )

    selection = select_operation_route(
        task,
        model=model,
        stable_context=(
            "Общая проверка риска включает только сохранённую SQL-структуру."
        ),
    )

    assert selection.pipeline == "sql_risk_scope"
    assert selection.skills == []
    assert selection.sql_risk_aspects == []
    assert len(model.messages) == 1
    router_payload = _payload(model, "select_operation_skills")
    assert router_payload["stable_context"] == (
        "Общая проверка риска включает только сохранённую SQL-структуру."
    )


def test_operation_router_repairs_missing_required_pipeline(
    monkeypatch,
):
    from agents.coordinator import (
        CoordinatorResponseError,
        select_operation_route,
    )
    from agents.sql_risk_scope_contract import (
        OPERATION_SQL_RISK_SCOPE_EVIDENCE_EXPERIMENT_ENV,
    )

    monkeypatch.setenv(
        OPERATION_SQL_RISK_SCOPE_EVIDENCE_EXPERIMENT_ENV,
        "1",
    )
    invalid = {
        "skills": [],
        "sql_risk_aspects": [],
    }
    repaired = {
        "pipeline": "sql_risk_scope",
        "skills": [],
        "sql_risk_aspects": [],
    }
    model = _CoordinatorModel(
        {
            "select_operation_skills": [
                _tool_message(
                    "select_operation_skills",
                    invalid,
                    "missing-pipeline",
                ),
                _tool_message(
                    "select_operation_skills",
                    repaired,
                    "repaired-scope-route",
                ),
            ]
        }
    )

    selection = select_operation_route(
        "Для 'Mapping.xlsx' проверь stage.orders.id → mart.orders.id.",
        model=model,
    )

    assert selection.pipeline == "sql_risk_scope"
    assert len(model.messages) == 2
    assert "pipeline" in model.messages[1][1][-1].content


def test_operation_router_repair_reconsiders_mixed_scope_and_skill(
    monkeypatch,
):
    from agents.coordinator import select_operation_route
    from agents.sql_risk_scope_contract import (
        OPERATION_SQL_RISK_SCOPE_EVIDENCE_EXPERIMENT_ENV,
    )

    monkeypatch.setenv(
        OPERATION_SQL_RISK_SCOPE_EVIDENCE_EXPERIMENT_ENV,
        "1",
    )
    model = _CoordinatorModel(
        {
            "select_operation_skills": [
                _tool_message(
                    "select_operation_skills",
                    {
                        "pipeline": "sql_risk_scope",
                        "skills": ["Совместимость колонок"],
                        "sql_risk_aspects": [],
                    },
                    "mixed-route",
                ),
                _tool_message(
                    "select_operation_skills",
                    {
                        "pipeline": "agentic",
                        "skills": ["Совместимость колонок"],
                        "sql_risk_aspects": [],
                    },
                    "repaired-agentic",
                ),
            ]
        }
    )

    selection = select_operation_route(
        "Compare source_not_null and target_not_null for src.id → tgt.id.",
        model=model,
    )

    assert selection.pipeline == "agentic"
    assert selection.skills == ["Совместимость колонок"]
    assert len(model.messages) == 2
    repair = model.messages[1][1][-1].content
    normalized_repair = " ".join(repair.split())
    assert "а не механически удаляй skills" in normalized_repair
    assert "без анализа transformation SQL" in normalized_repair


def test_operation_router_rejects_scope_call_when_feature_is_disabled(
    monkeypatch,
):
    from agents.coordinator import select_operation_route
    from agents.sql_risk_scope_contract import (
        OPERATION_SQL_RISK_SCOPE_EVIDENCE_EXPERIMENT_ENV,
    )

    monkeypatch.setenv(
        OPERATION_SQL_RISK_SCOPE_EVIDENCE_EXPERIMENT_ENV,
        "0",
    )
    model = _CoordinatorModel(
        {
            "select_operation_skills": [
                _tool_message(
                    "select_operation_skills",
                    {
                        "pipeline": "sql_risk_scope",
                        "skills": [],
                        "sql_risk_aspects": [],
                    },
                    "disabled-scope",
                ),
                _tool_message(
                    "select_operation_skills",
                    {
                        "pipeline": "agentic",
                        "skills": [],
                        "sql_risk_aspects": [],
                    },
                    "repaired-agentic",
                ),
            ]
        }
    )

    selection = select_operation_route(
        "Проверь src_orders → tgt_orders.",
        model=model,
    )

    assert selection.pipeline == "agentic"
    assert [name for name, _ in model.messages] == [
        "select_operation_skills",
        "select_operation_skills",
    ]


@pytest.mark.parametrize("raw_setting", [None, "", "default", "current"])
def test_sql_risk_protocol_attestation_marks_current_baseline(
    monkeypatch,
    raw_setting,
):
    from agents.coordinator import _sql_risk_protocol_attestation
    from agents.operation_protocols import (
        OPERATION_SQL_RISK_PROTOCOL_EXPERIMENT_ENV,
    )

    if raw_setting is None:
        monkeypatch.delenv(
            OPERATION_SQL_RISK_PROTOCOL_EXPERIMENT_ENV,
            raising=False,
        )
    else:
        monkeypatch.setenv(
            OPERATION_SQL_RISK_PROTOCOL_EXPERIMENT_ENV,
            raw_setting,
        )

    assert _sql_risk_protocol_attestation(
        ["Анализ SQL-рисков"],
        ["cardinality"],
    ) == {
        "operation_sql_risk_protocol": "default/current",
        "operation_sql_risk_protocol_sha256": None,
    }
    assert _sql_risk_protocol_attestation([], []) == {}


def test_coordinator_records_candidate_protocol_attestation(monkeypatch):
    from agents.coordinator import coordinator_chat
    from agents.operation_protocols import (
        OPERATION_SQL_RISK_PROTOCOL_EXPERIMENT_ENV,
        protocol_variant_sha256,
    )
    from agents.tools.context import OPERATION_SQL_RISK_ASPECTS_EXPERIMENT_ENV

    candidate = "cardinality__evidence_ledger"
    monkeypatch.setenv(OPERATION_SQL_RISK_ASPECTS_EXPERIMENT_ENV, "1")
    monkeypatch.setenv(
        OPERATION_SQL_RISK_PROTOCOL_EXPERIMENT_ENV,
        candidate,
    )
    responses = _responses(answer="JOIN может размножить строки.")
    responses["select_operation_skills"] = [
        _tool_message(
            "select_operation_skills",
            {
                "pipeline": "agentic",
                "skills": ["Анализ SQL-рисков"],
                "sql_risk_aspects": ["cardinality"],
            },
            "operation-cardinality",
        )
    ]
    model = _CoordinatorModel(responses)
    model_patch, callback_patch, trace_patch = _patches(model)
    with (
        model_patch,
        callback_patch,
        trace_patch,
        patch(
            "agents.coordinator.worker_chat",
            return_value=_outcome("Mapping прочитан."),
        ),
        patch("agents.coordinator.record_coordinator_plan") as record_plan,
    ):
        result = coordinator_chat("Может ли JOIN размножить строки?")

    assert result.answer == "JOIN может размножить строки."
    recorded_step = record_plan.call_args.args[0][0]
    assert recorded_step["plan_origin"] == "explicit_model_dag"
    assert recorded_step["operation_sql_risk_protocol"] == candidate
    assert recorded_step["operation_sql_risk_protocol_sha256"] == (
        protocol_variant_sha256(candidate)
    )


@pytest.mark.parametrize(
    ("candidate", "selected_aspect", "error_pattern"),
    [
        (
            "not-a-protocol",
            "cardinality",
            "Unknown OPERATION_SQL_RISK_PROTOCOL_EXPERIMENT",
        ),
        (
            "row_filtering__minimal_artifact",
            "cardinality",
            "is for aspect 'row_filtering'",
        ),
    ],
)
def test_invalid_protocol_fails_before_plan_attestation_is_recorded(
    monkeypatch,
    candidate,
    selected_aspect,
    error_pattern,
):
    from agents.coordinator import coordinator_chat
    from agents.operation_protocols import (
        OPERATION_SQL_RISK_PROTOCOL_EXPERIMENT_ENV,
    )
    from agents.tools.context import OPERATION_SQL_RISK_ASPECTS_EXPERIMENT_ENV

    monkeypatch.setenv(OPERATION_SQL_RISK_ASPECTS_EXPERIMENT_ENV, "1")
    monkeypatch.setenv(
        OPERATION_SQL_RISK_PROTOCOL_EXPERIMENT_ENV,
        candidate,
    )
    model = _CoordinatorModel(
        {
            "select_operation_skills": [
                _tool_message(
                    "select_operation_skills",
                    {
                        "pipeline": "agentic",
                        "skills": ["Анализ SQL-рисков"],
                        "sql_risk_aspects": [selected_aspect],
                    },
                    "operation-risk",
                )
            ]
        }
    )
    model_patch, callback_patch, trace_patch = _patches(model)
    with (
        model_patch,
        callback_patch,
        trace_patch,
        patch("agents.coordinator.record_coordinator_plan") as record_plan,
        pytest.raises(ValueError, match=error_pattern),
    ):
        coordinator_chat("Оцени SQL-риск.")

    record_plan.assert_not_called()
    assert "submit_worker_plan" not in [name for name, _ in model.messages]


def test_agentic_value_change_keeps_analysis_model_owned(
    monkeypatch,
):
    from agents.coordinator import coordinator_chat
    from agents.tools.context import (
        OPERATION_SQL_RISK_ASPECTS_EXPERIMENT_ENV,
    )
    from agents.tools.saved_results import get_active_saved_result_store

    monkeypatch.setenv(OPERATION_SQL_RISK_ASPECTS_EXPERIMENT_ENV, "1")
    original_task = (
        "Оцени только SQL-аспект value changes: меняется ли значение для "
        "точной пары src_np.id → tgt_np.id."
    )
    model = _CoordinatorModel(
        {
            "select_operation_skills": [
                _tool_message(
                    "select_operation_skills",
                    {
                        "pipeline": "agentic",
                        "skills": ["Анализ SQL-рисков"],
                        "sql_risk_aspects": ["value_changes"],
                    },
                    "operation-value-change",
                )
            ],
            "submit_worker_plan": [
                _tool_message(
                    "submit_worker_plan",
                    {
                        "steps": [
                            {
                                "task": (
                                    "Прочитать точный полный S2T mapping "
                                    "src_np.id → tgt_np.id."
                                )
                            }
                        ]
                    },
                    "plan-value-change",
                )
            ],
            "submit_upstream_data_decision": [
                _tool_message(
                    "submit_upstream_data_decision",
                    {"decision": "pass"},
                    "decision-value-change",
                )
            ],
            "submit_upstream_answer": [
                _tool_message(
                    "submit_upstream_answer",
                    {
                        "answer": "Для точной пары изменение не обнаружено.",
                        "used_evidence_ids": ["evidence-exact-id"],
                        "display_evidence_ids": [],
                    },
                    "answer-value-change",
                )
            ],
        }
    )

    def worker_with_saved_mapping(_request):
        store = get_active_saved_result_store()
        assert store is not None
        row = {
            "source_table": "src_np",
            "source_field": "id",
            "target_table": "tgt_np",
            "target_field": "id",
            "transformation_rule": (
                "SELECT s.id AS id, COALESCE(s.value, 0) AS value "
                "FROM src_np AS s"
            ),
        }
        descriptor = store.save_payload(
            source_tool="read_s2t_source_to_target",
            source_tool_call_id="call-exact-mapping",
            payload={"rows": [row], "truncated": False},
        )
        assert descriptor is not None
        return _outcome(
            "Точный mapping прочитан.",
            evidence=[
                _artifact(
                    None,
                    "read_s2t_source_to_target",
                    '{"rows":[{"target_field":"id"}]}',
                    evidence_id="evidence-exact-id",
                    compact_args={
                        "source_table": "src_np",
                        "target_table": "tgt_np",
                    },
                    dataset_ref=descriptor.result_ref,
                )
            ],
            datasets=[descriptor],
        )

    model_patch, callback_patch, trace_patch = _patches(model)
    with (
        model_patch,
        callback_patch,
        trace_patch,
        patch(
            "agents.coordinator.worker_chat",
            side_effect=worker_with_saved_mapping,
        ),
        patch("agents.coordinator.record_upstream_output") as record_upstream,
    ):
        result = coordinator_chat(original_task)

    assert result.answer == "Для точной пары изменение не обнаружено."
    assert "COALESCE" not in result.answer
    assert result.display_refs == []
    assert any(name == "submit_upstream_answer" for name, _ in model.messages)

    decision_payload = _payload(model, "submit_upstream_data_decision")
    assert "deterministic_sql_risk" not in decision_payload
    recorded_output = record_upstream.call_args.args[0]
    assert recorded_output["used_evidence_ids"] == ["evidence-exact-id"]
    assert recorded_output["display_evidence_ids"] == []
    assert recorded_output["answer_source"] == "model"


def test_agentic_write_semantics_keeps_analysis_model_owned(
    monkeypatch,
):
    from agents.coordinator import coordinator_chat
    from agents.tools.context import (
        OPERATION_SQL_RISK_ASPECTS_EXPERIMENT_ENV,
    )
    from agents.tools.saved_results import get_active_saved_result_store

    monkeypatch.setenv(OPERATION_SQL_RISK_ASPECTS_EXPERIMENT_ENV, "1")
    original_task = (
        "Оцени только SQL-аспект write semantics для сохранённой "
        "S2T-загрузки src_np → tgt_np: append, overwrite, MERGE/UPSERT "
        "или conflict handling. Если write statement не сохранён, "
        "честно отметь «не оценено» и не выводи режим из PK."
    )
    model = _CoordinatorModel(
        {
            "select_operation_skills": [
                _tool_message(
                    "select_operation_skills",
                    {
                        "pipeline": "agentic",
                        "skills": ["Анализ SQL-рисков"],
                        "sql_risk_aspects": ["write_semantics"],
                    },
                    "operation-write-semantics",
                )
            ],
            "submit_worker_plan": [
                _tool_message(
                    "submit_worker_plan",
                    {
                        "steps": [
                            {
                                "task": (
                                    "Прочитать полный точный directed S2T "
                                    "mapping src_np → tgt_np."
                                )
                            }
                        ]
                    },
                    "plan-write-semantics",
                ),
                _tool_message(
                    "submit_worker_plan",
                    {
                        "steps": [
                            {
                                "task": (
                                    "Повторно прочитать полный точный "
                                    "directed S2T mapping src_np → tgt_np."
                                )
                            }
                        ]
                    },
                    "plan-write-semantics-cycle-2",
                ),
            ],
            "submit_upstream_data_decision": [
                _tool_message(
                    "submit_upstream_data_decision",
                    {
                        "decision": "reroute",
                        "problem": (
                            "Нужно ещё раз прочитать metadata для "
                            "определения write mode."
                        ),
                    },
                    "decision-write-semantics-reroute",
                ),
                _tool_message(
                    "submit_upstream_data_decision",
                    {"decision": "pass"},
                    "decision-write-semantics-pass",
                ),
            ],
            "submit_upstream_answer": [
                _tool_message(
                    "submit_upstream_answer",
                    {
                        "answer": "Write semantics не оценена.",
                        "used_evidence_ids": [
                            "evidence-write-select-only"
                        ],
                        "display_evidence_ids": [],
                    },
                    "answer-write-semantics",
                )
            ],
        }
    )

    def worker_with_full_select_only_mapping(_request):
        store = get_active_saved_result_store()
        assert store is not None
        rule = (
            "SELECT s.id AS id, COALESCE(s.value, 0) AS value "
            "FROM src_np AS s"
        )
        rows = [
            {
                "source_table": "src_np",
                "source_field": "id",
                "target_table": "tgt_np",
                "target_field": "id",
                "transformation_rule": rule,
            },
            {
                "source_table": "src_np",
                "source_field": "value",
                "target_table": "tgt_np",
                "target_field": "value",
                "transformation_rule": rule,
            },
        ]
        descriptor = store.save_payload(
            source_tool="read_s2t_source_to_target",
            source_tool_call_id="call-write-mapping",
            payload={"rows": rows, "total": len(rows), "truncated": False},
        )
        assert descriptor is not None
        return _outcome(
            "Полный SELECT-only mapping прочитан.",
            evidence=[
                _artifact(
                    None,
                    "read_s2t_source_to_target",
                    '{"rows":[{"target_field":"id"}]}',
                    evidence_id="evidence-write-select-only",
                    compact_args={
                        "source_table": "src_np",
                        "target_table": "tgt_np",
                    },
                    dataset_ref=descriptor.result_ref,
                )
            ],
            datasets=[descriptor],
        )

    model_patch, callback_patch, trace_patch = _patches(model)
    with (
        model_patch,
        callback_patch,
        trace_patch,
        patch(
            "agents.coordinator.worker_chat",
            side_effect=worker_with_full_select_only_mapping,
        ) as worker,
        patch("agents.coordinator.record_upstream_output") as record_upstream,
    ):
        result = coordinator_chat(original_task)

    assert result.answer == "Write semantics не оценена."
    assert result.display_refs == []
    assert worker.call_count == 2
    assert len(
        [name for name, _ in model.messages if name == "submit_worker_plan"]
    ) == 2
    assert len(
        [
            name
            for name, _ in model.messages
            if name == "submit_upstream_data_decision"
        ]
    ) == 2
    assert any(name == "submit_upstream_answer" for name, _ in model.messages)

    decision_payload = _payload(model, "submit_upstream_data_decision")
    assert set(decision_payload) == {
        "original_task",
        "evidence",
        "execution_manifest",
    }
    assert "deterministic_write_semantics" not in decision_payload
    record_upstream.assert_called_once()
    recorded_output = record_upstream.call_args.args[0]
    assert recorded_output["used_evidence_ids"] == [
        "evidence-write-select-only"
    ]
    assert recorded_output["display_evidence_ids"] == []
    assert recorded_output["answer_source"] == "model"


def test_sql_risk_aspect_experiment_can_load_legacy_full_profile(monkeypatch):
    from agents.coordinator import (
        OperationSkillSelection,
        _operation_skill_prompt,
        _operation_skill_repair_prompt,
        _operation_skill_tool_schema,
    )
    from agents.tools.context import (
        OPERATION_SQL_RISK_ASPECTS_EXPERIMENT_ENV,
        load_operation_skills,
    )

    monkeypatch.setenv(OPERATION_SQL_RISK_ASPECTS_EXPERIMENT_ENV, "0")
    selection = OperationSkillSelection(
        pipeline="agentic",
        skills=["Анализ SQL-рисков"],
        sql_risk_aspects=["cardinality"],
    )
    context = load_operation_skills(
        selection.skills,
        stage="upstream",
        sql_risk_aspects=selection.sql_risk_aspects,
    )

    assert selection.sql_risk_aspects == []
    operation_parameters = _operation_skill_tool_schema()["function"][
        "parameters"
    ]
    assert "sql_risk_aspects" not in operation_parameters["properties"]
    assert "sql_risk_aspects" not in operation_parameters["required"]
    assert "sql_risk_execution_mode" not in _operation_skill_tool_schema()[
        "function"
    ]["parameters"]["properties"]
    assert "sql_risk_aspects" not in _operation_skill_prompt()
    assert "sql_risk_aspects" not in _operation_skill_repair_prompt()
    assert "conditional_cardinality" not in _operation_skill_prompt()
    assert "sql_risk_execution_mode" not in _operation_skill_repair_prompt()
    assert "JOIN размножает строку" in context
    assert "rejection" in context
    assert "write semantics" in context


def test_operation_router_repairs_removed_s2t_pipeline_to_agentic():
    from agents.coordinator import coordinator_chat

    responses = _responses(
        answer="Mapping прочитан.",
        plan_task="Прочитай сохранённый mapping.",
    )
    responses["select_operation_skills"] = [
        _tool_message(
            "select_operation_skills",
            {
                "pipeline": "s2t_analysis",
                "skills": ["Проектирование проверки"],
                "sql_risk_aspects": [],
            },
            "removed-route",
        ),
        _tool_message(
            "select_operation_skills",
            {
                "pipeline": "agentic",
                "skills": [],
                "sql_risk_aspects": [],
            },
            "repaired-route",
        ),
    ]
    model = _CoordinatorModel(responses)
    model_patch, callback_patch, trace_patch = _patches(model)
    with (
        model_patch,
        callback_patch,
        trace_patch,
        patch(
            "agents.coordinator.worker_chat",
            return_value=_outcome("Mapping прочитан."),
        ) as worker,
    ):
        result = coordinator_chat("Покажи сохранённый mapping.")

    assert result.answer == "Mapping прочитан."
    worker.assert_called_once()
    invoked = [name for name, _ in model.messages]
    assert invoked == [
        "select_operation_skills",
        "select_operation_skills",
        "submit_worker_plan",
        "submit_upstream_data_decision",
        "submit_upstream_answer",
    ]
    repair_messages = model.messages[1][1]
    assert any(
        "исправленный call" in str(message.content)
        and "validation_protocol" in str(message.content)
        for message in repair_messages
    )


def test_operation_router_never_silently_falls_back_after_failed_repair():
    from agents.coordinator import CoordinatorResponseError, coordinator_chat

    invalid_route = {
        "pipeline": "s2t_analysis",
        "skills": [],
        "sql_risk_aspects": [],
    }
    model = _CoordinatorModel(
        {
            "select_operation_skills": [
                _tool_message(
                    "select_operation_skills",
                    invalid_route,
                    "removed-route-1",
                ),
                _tool_message(
                    "select_operation_skills",
                    invalid_route,
                    "removed-route-2",
                ),
            ]
        }
    )
    model_patch, callback_patch, trace_patch = _patches(model)
    with (
        model_patch,
        callback_patch,
        trace_patch,
        patch("agents.coordinator.worker_chat") as worker,
        pytest.raises(CoordinatorResponseError),
    ):
        coordinator_chat("Проанализируй сохранённый S2T.")

    worker.assert_not_called()
    assert [name for name, _ in model.messages] == [
        "select_operation_skills",
        "select_operation_skills",
    ]


def test_operation_router_can_compile_external_sql_test_protocol():
    from agents.coordinator import coordinator_chat
    from agents.test_protocol import (
        ResolvedTestProtocolContract,
        TestProtocolLoad,
    )
    from agents.test_protocol_resolution import TestProtocolResolutionResult

    task = (
        "Для file_id=41 по saved_source → saved_target составь тест-протокол "
        "для внешней СУБД: количество строк, уникальность ключа, null-rate "
        "обязательных полей и корректность трансформаций."
    )
    model = _CoordinatorModel(
        {
            "select_operation_skills": [
                _tool_message(
                    "select_operation_skills",
                    {
                        "pipeline": "validation_protocol",
                        "skills": [],
                    },
                    "operation-protocol",
                )
            ],
            "submit_validation_protocol_contract": [
                _tool_message(
                    "submit_validation_protocol_contract",
                    {
                        "file_scope_kind": "file_id",
                        "file_id": 41,
                        "mode": "explicit",
                        "requested_checks": [],
                        "loads": [
                            {
                                "source_mentions": ["saved_source"],
                                "target_mention": "saved_target",
                                "requested_checks": [
                                    "row_count",
                                    "key_uniqueness",
                                    "required_null_rate",
                                    "transformation_correctness",
                                ],
                            }
                        ],
                    },
                    "protocol-contract",
                )
            ],
        }
    )
    model_patch, callback_patch, trace_patch = _patches(model)
    with (
        model_patch,
        callback_patch,
        trace_patch,
        patch(
            "agents.coordinator.resolve_test_protocol_contract",
            return_value=TestProtocolResolutionResult(
                status="resolved",
                contract=ResolvedTestProtocolContract(
                    file_id=41,
                    mode="explicit",
                    loads=[
                        TestProtocolLoad(
                            sources=["saved_source"],
                            target="saved_target",
                            checks=[
                                "row_count",
                                "key_uniqueness",
                                "required_null_rate",
                                "transformation_correctness",
                            ],
                        )
                    ],
                ),
                exact_bypass_count=2,
            ),
        ),
        patch(
            "agents.coordinator.read_test_protocol_inputs",
            return_value=[],
        ) as readers,
        patch("agents.coordinator.record_validation_protocol") as record_protocol,
        patch("agents.coordinator.worker_chat") as worker,
    ):
        result = coordinator_chat(task)

    assert "Тест-протокол проверки ETL-загрузки" in result.answer
    assert result.answer.count("SQL-шаблон:") == 4
    assert "фактические метрики не вычислялись" in result.answer
    readers.assert_called_once()
    trace = record_protocol.call_args.args[0]
    assert trace["mode"] == "explicit"
    assert trace["status"] == "unavailable"
    assert trace["silent_fallback"] is False
    assert trace["exact_bypass_count"] == 2
    assert trace["reader_calls"] == []
    worker.assert_not_called()
    assert [name for name, _ in model.messages] == [
        "select_operation_skills",
        "submit_validation_protocol_contract",
        "submit_validation_contract_review",
    ]


def test_validation_protocol_keeps_schema_valid_llm_contract_model_owned():
    from agents.coordinator import coordinator_chat
    from agents.test_protocol import (
        ResolvedTestProtocolContract,
        TestProtocolLoad,
    )
    from agents.test_protocol_resolution import TestProtocolResolutionResult

    task = (
        "For 'Mapping.xlsx', prepare an explicit protocol for "
        "stage.orders → mart.orders with check=row_count."
    )
    route = {
        "pipeline": "validation_protocol",
        "skills": [],
        "sql_risk_aspects": [],
    }
    extracted = {
        "file_scope_kind": "file_mention",
        "file_mention": "Mapping.xlsx",
        "mode": "explicit",
        "requested_checks": [],
        "loads": [
            {
                "source_mentions": ["stage.orders"],
                "target_mention": "mart.orders",
                "requested_checks": ["row_count"],
            }
        ],
    }
    model = _CoordinatorModel(
        {
            "select_operation_skills": [
                _tool_message("select_operation_skills", route, "route"),
            ],
            "submit_validation_protocol_contract": [
                _tool_message(
                    "submit_validation_protocol_contract",
                    extracted,
                    "model-owned-contract",
                ),
            ],
        }
    )
    resolution = TestProtocolResolutionResult(
        status="resolved",
        contract=ResolvedTestProtocolContract(
            file_id=7,
            mode="explicit",
            loads=[
                TestProtocolLoad(
                    sources=["stage.orders"],
                    target="mart.orders",
                    checks=["row_count"],
                )
            ],
        ),
        exact_bypass_count=3,
    )
    model_patch, callback_patch, trace_patch = _patches(model)
    with (
        model_patch,
        callback_patch,
        trace_patch,
        patch(
            "agents.coordinator.resolve_test_protocol_contract",
            return_value=resolution,
        ) as resolve_contract,
        patch(
            "agents.coordinator.read_test_protocol_inputs",
            return_value=[],
        ),
        patch("agents.coordinator.worker_chat") as worker,
    ):
        coordinator_chat(task)

    extracted_contract = resolve_contract.call_args.args[0]
    assert extracted_contract.file_scope_kind == "file_mention"
    assert extracted_contract.file_mention == "Mapping.xlsx"
    assert extracted_contract.loads[0].source_mentions == ["stage.orders"]
    assert extracted_contract.loads[0].target_mention == "mart.orders"
    assert [name for name, _ in model.messages] == [
        "select_operation_skills",
        "submit_validation_protocol_contract",
        "submit_validation_contract_review",
    ]
    worker.assert_not_called()


def test_validation_contract_review_repairs_schema_valid_missing_file_scope():
    from agents.coordinator import coordinator_chat
    from agents.test_protocol import (
        ResolvedTestProtocolContract,
        TestProtocolLoad,
    )
    from agents.test_protocol_resolution import TestProtocolResolutionResult

    task = (
        "For 'Mapping.xlsx', prepare an explicit protocol for "
        "stage.orders → mart.orders with check=row_count."
    )
    load = {
        "source_mentions": ["stage.orders"],
        "target_mention": "mart.orders",
        "requested_checks": ["row_count"],
    }
    overbroad_load = {
        **load,
        "requested_checks": ["row_count", "key_uniqueness"],
    }
    model = _CoordinatorModel(
        {
            "select_operation_skills": [
                _tool_message(
                    "select_operation_skills",
                    {"pipeline": "validation_protocol", "skills": []},
                    "route",
                )
            ],
            "submit_validation_protocol_contract": [
                _tool_message(
                    "submit_validation_protocol_contract",
                    {
                        "file_scope_kind": "not_provided",
                        "mode": "explicit",
                        "requested_checks": [],
                        "loads": [overbroad_load],
                    },
                    "contract-omitted-file",
                ),
                _tool_message(
                    "submit_validation_protocol_contract",
                    {
                        "file_scope_kind": "file_mention",
                        "file_mention": "Mapping.xlsx",
                        "mode": "explicit",
                        "requested_checks": [],
                        "loads": [load],
                    },
                    "contract-repaired-file",
                ),
            ],
            "submit_validation_contract_review": [
                _tool_message(
                    "submit_validation_contract_review",
                    {
                        "decision": "repair",
                        "issues": [
                            {
                                "code": "missing_file_scope",
                                "location": "file_scope_kind",
                                "message": (
                                    "The explicit Mapping.xlsx scope is missing."
                                ),
                            },
                            {
                                "code": "unexpected_check",
                                "location": "loads[0].requested_checks[1]",
                                "message": "key_uniqueness was not requested.",
                            },
                        ],
                    },
                    "review-repair",
                ),
                _tool_message(
                    "submit_validation_contract_review",
                    {"decision": "accept", "issues": []},
                    "review-accept",
                ),
            ],
        }
    )
    resolution = TestProtocolResolutionResult(
        status="resolved",
        contract=ResolvedTestProtocolContract(
            file_id=7,
            mode="explicit",
            loads=[
                TestProtocolLoad(
                    sources=["stage.orders"],
                    target="mart.orders",
                    checks=["row_count"],
                )
            ],
        ),
        exact_bypass_count=3,
    )
    model_patch, callback_patch, trace_patch = _patches(model)
    with (
        model_patch,
        callback_patch,
        trace_patch,
        patch(
            "agents.coordinator.resolve_test_protocol_contract",
            return_value=resolution,
        ) as resolve_contract,
        patch("agents.coordinator.read_test_protocol_inputs", return_value=[]),
        patch("agents.coordinator.worker_chat") as worker,
    ):
        coordinator_chat(task)

    repaired = resolve_contract.call_args.args[0]
    assert repaired.file_scope_kind == "file_mention"
    assert repaired.file_mention == "Mapping.xlsx"
    assert [name for name, _ in model.messages] == [
        "select_operation_skills",
        "submit_validation_protocol_contract",
        "submit_validation_contract_review",
        "submit_validation_protocol_contract",
        "submit_validation_contract_review",
    ]
    repair_messages = model.messages[3][1]
    assert "missing_file_scope" in str(repair_messages[-1].content)
    assert "unexpected_check" in str(repair_messages[-1].content)
    assert "The explicit Mapping.xlsx scope is missing." not in str(
        repair_messages[-1].content
    )
    worker.assert_not_called()


def test_repeated_validation_contract_review_rejection_is_structured():
    from agents.coordinator import coordinator_chat

    task = (
        "For 'Mapping.xlsx', prepare an explicit protocol for "
        "stage.orders → mart.orders with check=row_count."
    )
    omitted = {
        "file_scope_kind": "not_provided",
        "mode": "explicit",
        "requested_checks": [],
        "loads": [
            {
                "source_mentions": ["stage.orders"],
                "target_mention": "mart.orders",
                "requested_checks": ["row_count"],
            }
        ],
    }
    repair_review = {
        "decision": "repair",
        "issues": [
            {
                "code": "missing_file_scope",
                "location": "file_scope_kind",
                "message": "The explicit file scope is still missing.",
            }
        ],
    }
    model = _CoordinatorModel(
        {
            "select_operation_skills": [
                _tool_message(
                    "select_operation_skills",
                    {"pipeline": "validation_protocol", "skills": []},
                    "route",
                )
            ],
            "submit_validation_protocol_contract": [
                _tool_message(
                    "submit_validation_protocol_contract",
                    omitted,
                    "contract-1",
                ),
                _tool_message(
                    "submit_validation_protocol_contract",
                    omitted,
                    "contract-2",
                ),
            ],
            "submit_validation_contract_review": [
                _tool_message(
                    "submit_validation_contract_review",
                    repair_review,
                    "review-1",
                ),
                _tool_message(
                    "submit_validation_contract_review",
                    repair_review,
                    "review-2",
                ),
            ],
        }
    )
    model_patch, callback_patch, trace_patch = _patches(model)
    with (
        model_patch,
        callback_patch,
        trace_patch,
        patch("agents.coordinator.resolve_test_protocol_contract") as resolver,
        patch("agents.coordinator.read_test_protocol_inputs") as readers,
        patch("agents.coordinator.record_validation_protocol") as record_protocol,
        patch("agents.coordinator.worker_chat") as worker,
    ):
        result = coordinator_chat(task)

    assert "Статус: missing_parameter" in result.answer
    trace = record_protocol.call_args.args[0]
    assert trace["status"] == "missing_parameter"
    assert [item["status"] for item in trace["contract_reviews"]] == [
        "repair",
        "repair",
    ]
    assert trace["silent_fallback"] is False
    resolver.assert_not_called()
    readers.assert_not_called()
    worker.assert_not_called()


def test_validation_contract_review_transport_error_is_structured():
    from agents.coordinator import coordinator_chat

    def raise_transport(_messages):
        raise RuntimeError("provider unavailable")

    task = (
        "For 'Mapping.xlsx', prepare an explicit protocol for "
        "stage.orders → mart.orders with check=row_count."
    )
    model = _CoordinatorModel(
        {
            "select_operation_skills": [
                _tool_message(
                    "select_operation_skills",
                    {"pipeline": "validation_protocol", "skills": []},
                    "route",
                )
            ],
            "submit_validation_protocol_contract": [
                _tool_message(
                    "submit_validation_protocol_contract",
                    {
                        "file_scope_kind": "file_mention",
                        "file_mention": "Mapping.xlsx",
                        "mode": "explicit",
                        "requested_checks": [],
                        "loads": [
                            {
                                "source_mentions": ["stage.orders"],
                                "target_mention": "mart.orders",
                                "requested_checks": ["row_count"],
                            }
                        ],
                    },
                    "contract",
                )
            ],
            "submit_validation_contract_review": [raise_transport],
        }
    )
    model_patch, callback_patch, trace_patch = _patches(model)
    with (
        model_patch,
        callback_patch,
        trace_patch,
        patch("agents.coordinator.resolve_test_protocol_contract") as resolver,
        patch("agents.coordinator.read_test_protocol_inputs") as readers,
        patch("agents.coordinator.record_validation_protocol") as record_protocol,
        patch("agents.coordinator.worker_chat") as worker,
    ):
        result = coordinator_chat(task)

    assert "Статус: unavailable" in result.answer
    trace = record_protocol.call_args.args[0]
    assert trace["status"] == "unavailable"
    assert trace["contract_reviews"][0]["status"] == "unavailable"
    assert trace["silent_fallback"] is False
    resolver.assert_not_called()
    readers.assert_not_called()
    worker.assert_not_called()


def test_validation_contract_extraction_transport_error_is_structured():
    from agents.coordinator import coordinator_chat

    def raise_transport(_messages):
        raise RuntimeError("provider unavailable")

    model = _CoordinatorModel(
        {
            "select_operation_skills": [
                _tool_message(
                    "select_operation_skills",
                    {"pipeline": "validation_protocol", "skills": []},
                    "route",
                )
            ],
            "submit_validation_protocol_contract": [raise_transport],
        }
    )
    model_patch, callback_patch, trace_patch = _patches(model)
    with (
        model_patch,
        callback_patch,
        trace_patch,
        patch("agents.coordinator.resolve_test_protocol_contract") as resolver,
        patch("agents.coordinator.read_test_protocol_inputs") as readers,
        patch("agents.coordinator.record_validation_protocol") as record_protocol,
        patch("agents.coordinator.worker_chat") as worker,
    ):
        result = coordinator_chat(
            "Prepare a protocol for stage.orders → mart.orders."
        )

    assert "Статус: unavailable" in result.answer
    trace = record_protocol.call_args.args[0]
    assert trace["status"] == "unavailable"
    assert trace["contract_reviews"] == []
    assert trace["silent_fallback"] is False
    resolver.assert_not_called()
    readers.assert_not_called()
    worker.assert_not_called()


def test_validation_virtual_target_returns_unavailable_without_fallback():
    from agents.coordinator import coordinator_chat
    from agents.test_protocol import (
        ResolvedTestProtocolContract,
        TestProtocolLoad,
    )
    from agents.test_protocol_resolution import TestProtocolResolutionResult

    source_table = "raw.orders"
    virtual_target = "mart.result::cte::src"
    task = (
        f"Составь explicit test protocol {source_table} → {virtual_target} "
        "только с check=row_count."
    )
    model = _CoordinatorModel(
        {
            "select_operation_skills": [
                _tool_message(
                    "select_operation_skills",
                    {
                        "pipeline": "validation_protocol",
                        "skills": [],
                        "sql_risk_aspects": [],
                    },
                    "operation-virtual-target",
                )
            ],
            "submit_validation_protocol_contract": [
                _tool_message(
                    "submit_validation_protocol_contract",
                    {
                        "file_scope_kind": "not_provided",
                        "mode": "explicit",
                        "requested_checks": [],
                        "loads": [
                            {
                                "source_mentions": [source_table],
                                "target_mention": virtual_target,
                                "requested_checks": ["row_count"],
                            }
                        ],
                    },
                    "protocol-virtual-target",
                )
            ],
        }
    )
    resolution = TestProtocolResolutionResult(
        status="resolved",
        contract=ResolvedTestProtocolContract(
            mode="explicit",
            loads=[
                TestProtocolLoad(
                    sources=[source_table],
                    target=virtual_target,
                    checks=["row_count"],
                )
            ],
        ),
        exact_bypass_count=2,
    )
    mapping_row = {
        "source_table": source_table,
        "source_field": "id",
        "target_table": virtual_target,
        "target_field": "id",
        "transformation_rule": "SELECT o.id FROM raw.orders AS o",
    }
    reader_results = [
        {
            "kind": "s2t_pair",
            "load_index": 1,
            "tool_name": "read_s2t_source_to_target",
            "args": {
                "source_table": source_table,
                "target_table": virtual_target,
            },
            "payload": {"rows": [mapping_row], "truncated": False},
        }
    ]
    model_patch, callback_patch, trace_patch = _patches(model)
    with (
        model_patch,
        callback_patch,
        trace_patch,
        patch(
            "agents.coordinator.resolve_test_protocol_contract",
            return_value=resolution,
        ),
        patch(
            "agents.coordinator.read_test_protocol_inputs",
            return_value=reader_results,
        ) as readers,
        patch(
            "agents.coordinator.register_worker_display_items",
            return_value=[],
        ),
        patch("agents.coordinator.record_validation_protocol") as record_protocol,
        patch("agents.coordinator.worker_chat") as worker,
    ):
        result = coordinator_chat(task)

    assert "статус: unavailable" in result.answer
    assert "виртуальным lineage scope" in result.answer
    assert "SQL-шаблон не сформирован" in result.answer
    readers.assert_called_once()
    worker.assert_not_called()
    trace = record_protocol.call_args.args[0]
    assert trace["status"] == "unavailable"
    assert trace["silent_fallback"] is False
    assert trace["targets"][0]["target_table"] == virtual_target
    assert trace["targets"][0]["checks"][0]["status"] == "unavailable"
    assert trace["targets"][0]["checks"][0]["missing_dependencies"] == [
        "target_relation"
    ]


def test_validation_reader_exception_returns_structured_protocol_without_fallback():
    from agents.coordinator import coordinator_chat
    from agents.test_protocol import (
        ResolvedTestProtocolContract,
        TestProtocolLoad,
    )
    from agents.test_protocol_resolution import TestProtocolResolutionResult

    task = (
        "Составь explicit тест-протокол saved_source → "
        "saved_target только с check=row_count."
    )
    model = _CoordinatorModel(
        {
            "select_operation_skills": [
                _tool_message(
                    "select_operation_skills",
                    {
                        "pipeline": "validation_protocol",
                        "skills": [],
                        "sql_risk_aspects": [],
                    },
                    "operation-validation",
                )
            ],
            "submit_validation_protocol_contract": [
                _tool_message(
                    "submit_validation_protocol_contract",
                    {
                        "file_scope_kind": "not_provided",
                        "mode": "explicit",
                        "requested_checks": [],
                        "loads": [
                            {
                                "source_mentions": ["saved_source"],
                                "target_mention": "saved_target",
                                "requested_checks": ["row_count"],
                            }
                        ],
                    },
                    "protocol-contract",
                )
            ],
        }
    )
    resolution = TestProtocolResolutionResult(
        status="resolved",
        contract=ResolvedTestProtocolContract(
            mode="explicit",
            loads=[
                TestProtocolLoad(
                    sources=["saved_source"],
                    target="saved_target",
                    checks=["row_count"],
                )
            ],
        ),
        exact_bypass_count=2,
    )
    raising_reader = MagicMock()
    raising_reader.invoke.side_effect = RuntimeError(
        "database is temporarily unavailable"
    )
    model_patch, callback_patch, trace_patch = _patches(model)
    with (
        model_patch,
        callback_patch,
        trace_patch,
        patch(
            "agents.coordinator.resolve_test_protocol_contract",
            return_value=resolution,
        ),
        patch(
            "agents.validation_protocol.read_s2t_source_to_target",
            raising_reader,
        ),
        patch("agents.coordinator.record_validation_protocol") as record_protocol,
        patch("agents.coordinator.worker_chat") as worker,
    ):
        result = coordinator_chat(task)

    assert "статус: unavailable" in result.answer
    assert "SQL-шаблон не сформирован" in result.answer
    assert result.display_refs == []
    worker.assert_not_called()
    assert "submit_worker_plan" not in [name for name, _ in model.messages]
    trace = record_protocol.call_args.args[0]
    assert trace["status"] == "unavailable"
    assert trace["silent_fallback"] is False
    assert trace["reader_calls"] == [
        {
            "kind": "reader_issue",
            "tool_name": "read_s2t_source_to_target",
            "args": {
                "source_table": "saved_source",
                "target_table": "saved_target",
            },
            "error": "RuntimeError: database is temporarily unavailable",
        },
        {
            "kind": "s2t_pair",
            "tool_name": "read_s2t_source_to_target",
            "args": {
                "source_table": "saved_source",
                "target_table": "saved_target",
            },
        },
    ]


def test_invalid_validation_contract_returns_structured_state_without_fallback():
    from agents.coordinator import coordinator_chat

    task = "Составь тест-протокол для source → target."
    invalid = {
        "mode": "explicit",
        "requested_checks": ["row_count"],
        "loads": [],
    }
    model = _CoordinatorModel(
        {
            "select_operation_skills": [
                _tool_message(
                    "select_operation_skills",
                    {"pipeline": "validation_protocol", "skills": []},
                    "operation-validation",
                )
            ],
            "submit_validation_protocol_contract": [
                _tool_message(
                    "submit_validation_protocol_contract",
                    invalid,
                    "invalid-1",
                ),
                _tool_message(
                    "submit_validation_protocol_contract",
                    invalid,
                    "invalid-2",
                ),
            ],
        }
    )
    model_patch, callback_patch, trace_patch = _patches(model)
    with (
        model_patch,
        callback_patch,
        trace_patch,
        patch("agents.coordinator.record_validation_protocol") as record_protocol,
        patch("agents.coordinator.worker_chat") as worker,
    ):
        result = coordinator_chat(task)

    assert "Статус: missing_parameter" in result.answer
    assert result.display_refs == []
    worker.assert_not_called()
    assert "submit_worker_plan" not in [name for name, _ in model.messages]
    trace = record_protocol.call_args.args[0]
    assert trace["status"] == "missing_parameter"
    assert trace["silent_fallback"] is False
    assert trace["targets"] == []


def test_ambiguous_validation_entity_stops_before_readers_and_workers():
    from agents.coordinator import coordinator_chat
    from agents.test_protocol import ProtocolIssue
    from agents.test_protocol_resolution import TestProtocolResolutionResult

    task = "Для stage.order_ → mart.orders составь стандартный тест-протокол."
    model = _CoordinatorModel(
        {
            "select_operation_skills": [
                _tool_message(
                    "select_operation_skills",
                    {"pipeline": "validation_protocol", "skills": []},
                    "operation-validation",
                )
            ],
            "submit_validation_protocol_contract": [
                _tool_message(
                    "submit_validation_protocol_contract",
                    {
                        "file_scope_kind": "not_provided",
                        "mode": "standard",
                        "requested_checks": [],
                        "loads": [
                            {
                                "source_mentions": ["stage.order_"],
                                "target_mention": "mart.orders",
                                "requested_checks": [],
                            }
                        ],
                    },
                    "protocol-contract",
                )
            ],
        }
    )
    resolution = TestProtocolResolutionResult(
        status="ambiguous_entity",
        issues=[
            ProtocolIssue(
                code="ambiguous_entity",
                message="Найдено несколько кандидатов.",
                load_index=1,
                candidates=["stage.order_items", "stage.order_lines"],
            )
        ],
    )
    model_patch, callback_patch, trace_patch = _patches(model)
    with (
        model_patch,
        callback_patch,
        trace_patch,
        patch(
            "agents.coordinator.resolve_test_protocol_contract",
            return_value=resolution,
        ),
        patch("agents.coordinator.read_test_protocol_inputs") as readers,
        patch("agents.coordinator.record_validation_protocol") as record_protocol,
        patch("agents.coordinator.worker_chat") as worker,
    ):
        result = coordinator_chat(task)

    assert "Статус: ambiguous_entity" in result.answer
    assert "stage.order_items" in result.answer
    assert "stage.order_lines" in result.answer
    readers.assert_not_called()
    worker.assert_not_called()
    trace = record_protocol.call_args.args[0]
    assert trace["status"] == "ambiguous_entity"
    assert trace["silent_fallback"] is False
    assert trace["issues"][0]["candidates"] == [
        "stage.order_items",
        "stage.order_lines",
    ]


def test_operation_skill_loader_returns_only_requested_stage():
    from agents.tools.context import load_operation_skills

    contexts = {
        stage: load_operation_skills(
            ["Совместимость колонок"],
            stage=stage,
        )
        for stage in (
            "plan",
            "planner",
            "observer",
            "upstream_decision",
            "upstream",
        )
    }

    assert "создай одну задачу" in contexts["plan"]
    assert "filename" in contexts["plan"]
    assert "соответствующий имени текущей пары" in contexts["planner"]
    assert "точном совпадении `filename`" in contexts["observer"]
    assert "evidence его однозначного" in contexts["upstream_decision"]
    assert "exact pair-reader" in contexts["planner"]
    assert "Pair-result" in contexts["observer"]
    assert "`pass` допустим" in contexts["upstream_decision"]
    assert "Верни фактические" in contexts["upstream"]
    assert len(set(contexts.values())) == 5
    assert all("Покрытие маппинга" not in text for text in contexts.values())
    assert load_operation_skills([], stage="planner") == ""


def test_sql_risk_operation_skill_allows_pass_for_conditional_answer():
    from agents.tools.context import load_operation_skills

    decision = load_operation_skills(
        ["Анализ SQL-рисков"],
        stage="upstream_decision",
    )
    normalized = " ".join(decision.split())

    assert "достаточность только относительно `original_task`" in normalized
    assert "условный вывод" in normalized
    assert "не являются обязательным условием `pass`" in normalized
    assert "Не делай `reroute` ради оценки вероятности" in normalized
    assert "`pass` требует exact mapping" not in normalized


def test_sql_risk_operation_skill_separates_risk_layers_by_stage():
    from agents.tools.context import load_operation_skills

    contexts = {
        stage: " ".join(
            load_operation_skills(
                ["Анализ SQL-рисков"],
                stage=stage,
            ).split()
        )
        for stage in (
            "plan",
            "planner",
            "observer",
            "upstream_decision",
            "upstream",
        )
    }

    assert "полный mapping достаточен" in contexts["plan"]
    assert "Не добавляй metadata только ради проверки ключей" in contexts["plan"]
    assert "Не сокращай metadata-задачу до одной стороны" in contexts["plan"]
    assert "Planner получает факты" in contexts["planner"]
    assert "не оценивай безопасность" in contexts["observer"]
    assert "target-only evidence подтверждает ограничение" in (
        contexts["upstream_decision"]
    )
    answer = contexts["upstream"]
    assert "отсечение входных строк самим SQL" in answer
    assert "размножение или схлопывание строк" in answer
    assert "rejection результата ограничениями загрузки" in answer
    assert "write semantics" in answer
    assert "`Не оценено` никогда не является фактором снижения риска" in answer
    assert "Прямой field mapping не доказывает кардинальность 1:1" in answer
    assert "его нельзя назвать минимальным" in answer
    assert "отсечение входных строк самим SQL" not in contexts["observer"]


def test_explicit_adapter_normalizes_legacy_steps_to_a_linear_dag():
    from agents.contracts import adapt_legacy_worker_plan

    plan = adapt_legacy_worker_plan(
        [
            {"task": "  Первый шаг.  "},
            {"task": "Второй шаг."},
        ]
    )
    assert [step.task for step in plan.steps] == [
        "Первый шаг.",
        "Второй шаг.",
    ]
    assert [step.model_dump() for step in plan.steps] == [
        {"id": "step_1", "task": "Первый шаг.", "depends_on": []},
        {
            "id": "step_2",
            "task": "Второй шаг.",
            "depends_on": ["step_1"],
        },
    ]

    obsolete_fields = {
        "constraints": ["Только подтверждённые строки"],
        "entity": {"role": "source", "table": "src_orders"},
        "scope": {"file_id": 7},
        "coverage": "all_matches",
        "dependencies": [1],
        "needs_from_previous": "Несуществующий прошлый факт",
        "required_evidence": ["Лишний критерий"],
    }
    for field_name, field_value in obsolete_fields.items():
        with pytest.raises(ValueError, match=field_name):
            adapt_legacy_worker_plan(
                [
                    {
                        "task": "Первый шаг.",
                        field_name: field_value,
                    }
                ]
            )

    with pytest.raises(ValueError, match="task"):
        adapt_legacy_worker_plan([{}])

    with pytest.raises(ValueError, match="task"):
        adapt_legacy_worker_plan([{"task": "   "}])


def test_contracts_keep_runtime_refs_out_of_llm_payloads():
    dataset = SavedResultDescriptor(
        result_ref="saved-first",
        source_tool="lookup",
        row_count=1,
        columns=[SavedResultColumn(name="name", sqlite_type="TEXT")],
    )
    artifact = _artifact(
        "display-first",
        "lookup",
        '{"name":"t_example"}',
        evidence_id="evidence-first",
        compact_args={"query": "t_example"},
        dataset_ref=dataset.result_ref,
    )
    outcome = _outcome(
        "Точное имя найдено.",
        evidence=[artifact],
        datasets=[dataset],
        previous_results=[
            PreviousResultReference(
                result_id="result-first",
                description="lookup: точное имя t_example.",
            )
        ],
    )

    assert "display_ref" not in artifact.model_dump()
    assert "dataset_ref" not in artifact.model_dump()
    assert "datasets" not in outcome.model_dump()
    handoff = outcome.handoff_payload()
    assert handoff == {
        "previous_results": [
            {
                "result_id": "result-first",
                "description": "lookup: точное имя t_example.",
            }
        ]
    }
    assert "display_ref" not in json.dumps(handoff)
    assert "dataset_ref" not in json.dumps(handoff)
    assert "preview" not in json.dumps(handoff)
    upstream = outcome.upstream_payload()
    assert "gap" not in handoff
    assert "gap" not in upstream
    assert "datasets" not in upstream
    assert "dataset_ref" not in json.dumps(upstream)
    assert "display_ref" not in json.dumps(upstream)
    assert upstream == {
        "evidence": [
            {
                "evidence_id": "evidence-first",
                "tool_name": "lookup",
                "args": {"query": "t_example"},
                "preview": '{"name":"t_example"}',
                "truncated": False,
                "displayable": True,
            }
        ]
    }
    nondisplayable = _outcome(
        "Факт найден без отдельного display.",
        evidence=[
            _artifact(
                None,
                "lookup",
                '{"name":"t_hidden"}',
                evidence_id="evidence-hidden",
            )
        ],
    ).upstream_payload()
    assert nondisplayable["evidence"][0]["displayable"] is False
    assert "display_id" not in nondisplayable["evidence"][0]

    failed = WorkerOutcome(
        summary="Имя не разрешено.",
        status="failed",
        stop_reason="unresolved_entity",
        unmet_requirements=["Нужно каноническое имя target_table."],
    )
    assert failed.upstream_payload() == {"evidence": []}
    with pytest.raises(ValueError, match="requires stop_reason"):
        WorkerOutcome(summary="Не завершено.", status="partial")
    with pytest.raises(ValueError, match="cannot have unmet_requirements"):
        WorkerOutcome(
            summary="Ошибочно complete.",
            status="complete",
            unmet_requirements=["Факт не получен."],
        )

    with pytest.raises(ValueError, match="unknown evidence_id"):
        _outcome(
            "Некорректный provenance.",
            facts=[EvidenceFact(text="Факт", evidence_ids=["unknown"])],
        )


def test_upstream_data_decision_is_separate_from_answer_payload():
    from agents.contracts import UpstreamDecision

    decision = UpstreamDecision.model_validate(
        {
            "decision": "reroute",
            "problem": "Не получена вторая запрошенная метрика.",
        }
    )

    assert decision.decision == "reroute"
    assert decision.problem == "Не получена вторая запрошенная метрика."

    with pytest.raises(ValueError, match="answer"):
        UpstreamDecision.model_validate(
            {
                "decision": "pass",
                "answer": "Ответ относится к следующему этапу.",
            }
        )


def test_coordinator_prompts_and_schemas_match_contracts():
    from agents.contracts import WorkerPlan
    from agents.coordinator import (
        _DOWNSTREAM_PLAN_PROMPT,
        _DOWNSTREAM_PLAN_REPAIR_PROMPT,
        _DOWNSTREAM_CAPABILITY_CONTEXT,
        _DOWNSTREAM_TABLE_CONTEXT,
        _OPERATION_SKILL_PROMPT,
        _UPSTREAM_ANALYSIS_CONTEXT,
        _UPSTREAM_ANSWER_PROMPT,
        _UPSTREAM_DATA_DECISION_PROMPT,
        _plan_tool_schema,
        _operation_skill_tool_schema,
        _upstream_answer_tool_schema,
        _upstream_data_decision_tool_schema,
    )

    combined = "\n".join(
        (_UPSTREAM_DATA_DECISION_PROMPT, _UPSTREAM_ANSWER_PROMPT)
    )
    for domain_detail in (
        "target_table",
        "source_table",
        "GROUP BY",
        "COUNT(DISTINCT",
        "Neo4j",
        "SQLite",
    ):
        assert domain_detail not in combined

    assert len(_DOWNSTREAM_PLAN_PROMPT) < 5300
    assert (
        len(_DOWNSTREAM_PLAN_PROMPT)
        - len(_DOWNSTREAM_TABLE_CONTEXT)
        - len(_DOWNSTREAM_CAPABILITY_CONTEXT)
        < 3800
    )
    assert _DOWNSTREAM_CAPABILITY_CONTEXT in _DOWNSTREAM_PLAN_PROMPT
    assert _DOWNSTREAM_TABLE_CONTEXT in _DOWNSTREAM_PLAN_PROMPT
    assert len(_UPSTREAM_DATA_DECISION_PROMPT) < 1300
    assert len(_UPSTREAM_ANSWER_PROMPT) < 2300
    assert len(_UPSTREAM_ANALYSIS_CONTEXT) < 3500
    operation_pipeline_schema = _operation_skill_tool_schema()["function"][
        "parameters"
    ]["properties"]["pipeline"]
    assert operation_pipeline_schema["enum"] == [
        "agentic",
        "validation_protocol",
    ]
    assert "s2t_analysis" not in _OPERATION_SKILL_PROMPT
    operation_parameters = _operation_skill_tool_schema()["function"][
        "parameters"
    ]
    assert "sql_risk_aspects" not in operation_parameters["required"]
    assert "sql_risk_aspects" not in operation_parameters["properties"]
    assert (
        "row_format=named_records_with_dictionary_refs"
        in _UPSTREAM_ANALYSIS_CONTEXT
    )
    assert "0-based индексом" in _UPSTREAM_ANALYSIS_CONTEXT
    assert "не схлопывай одинаковые occurrences" in (
        _UPSTREAM_ANALYSIS_CONTEXT
    )
    assert "`depends_on`" in _DOWNSTREAM_PLAN_PROMPT
    assert "`depends_on=[]` для независимых steps" in _DOWNSTREAM_PLAN_PROMPT
    assert "needs_from_previous" not in _DOWNSTREAM_PLAN_PROMPT
    assert "required_evidence" not in _DOWNSTREAM_PLAN_PROMPT
    assert "не выбирай tools/skills" in (
        _DOWNSTREAM_PLAN_PROMPT.lower().replace("\n", " ")
    )
    assert "`steps` чтения" in _DOWNSTREAM_PLAN_PROMPT
    assert "Каждый step незаменим" in _DOWNSTREAM_PLAN_PROMPT
    assert "`file_id` допустим лишь из original_task либо принятого" in (
        _DOWNSTREAM_PLAN_PROMPT
    )
    assert "Каждая task должна быть самодостаточной" in (
        _DOWNSTREAM_PLAN_PROMPT
    )
    assert "поручение задаёт task" in _DOWNSTREAM_PLAN_PROMPT
    normalized_downstream_prompt = " ".join(_DOWNSTREAM_PLAN_PROMPT.split())
    assert "`filename` даёт `file_id`, но не определяет и не заменяет `table_name`" in (
        normalized_downstream_prompt
    )
    assert "не заменить уже заданный идентификатор другой сущности" in (
        normalized_downstream_prompt
    )
    assert "problem не заменяет и не переопределяет явные идентификаторы" in (
        _DOWNSTREAM_PLAN_PROMPT
    )
    assert "чтение справочника без необходимости" in (
        _DOWNSTREAM_PLAN_PROMPT
    )
    assert "получает lazy-ссылки только прямых `depends_on`" in (
        _DOWNSTREAM_PLAN_PROMPT
    )
    assert "не связывай независимые чтения ради порядка" in (
        _DOWNSTREAM_PLAN_PROMPT
    )
    assert "отдельный worker может сначала получить" in (
        _DOWNSTREAM_PLAN_PROMPT
    )
    assert "следующий — найти эти кандидаты в S2T" in (
        _DOWNSTREAM_PLAN_PROMPT
    )
    assert "S2T-поиск по подстроке лексический, не семантический" in (
        _DOWNSTREAM_PLAN_PROMPT
    )
    assert "делает upstream" in _DOWNSTREAM_PLAN_PROMPT
    assert "Сравнение, оценку, объяснение, вывод" in _DOWNSTREAM_PLAN_PROMPT
    assert "роль source/target известна,\nтолько если" in (
        _DOWNSTREAM_PLAN_PROMPT.lower()
    )
    assert "роль результата не задаёт роль кандидата" in (
        _DOWNSTREAM_PLAN_PROMPT.lower()
    )
    assert "Не превращай бизнес-термин в техническое имя" in (
        _DOWNSTREAM_PLAN_PROMPT
    )
    assert "не пиши task как вызов функции" in _DOWNSTREAM_PLAN_PROMPT
    assert "при неизвестной роли — сразу в обоих" in (
        _DOWNSTREAM_PLAN_PROMPT.lower()
    )
    assert "семантический кандидат не\nимеет s2t-роли" in (
        _DOWNSTREAM_PLAN_PROMPT.lower()
    )
    assert "Сохраняй тип поиска из original_task" in _DOWNSTREAM_PLAN_PROMPT
    assert "смысловой поиск цельной естественной фразой" in (
        _DOWNSTREAM_PLAN_PROMPT
    )
    assert "буквальный поиск, только если фрагмент явно дан" in (
        _DOWNSTREAM_PLAN_PROMPT
    )
    assert "Не превращай смысловой поиск в «найти содержащие»" in (
        _DOWNSTREAM_PLAN_PROMPT
    )
    assert "Планируй чтение только тех фактов" in _DOWNSTREAM_PLAN_PROMPT
    assert "просит описать способ будущего действия" in _DOWNSTREAM_PLAN_PROMPT
    assert "Для вывода по сохранённому выражению" in _DOWNSTREAM_PLAN_PROMPT
    assert "одна task пути до\nконечных endpoint" in (
        _DOWNSTREAM_PLAN_PROMPT
    )
    assert "чужой result не вход" in (
        _DOWNSTREAM_PLAN_PROMPT
    )
    assert "сравнит upstream" in (
        _DOWNSTREAM_PLAN_PROMPT
    )
    assert "глобальную `s2t_transformations` не ограничивай `file_id`" in (
        _DOWNSTREAM_PLAN_PROMPT
    )
    assert "справка, не список шагов" in _DOWNSTREAM_TABLE_CONTEXT
    assert "наличие таблицы не требует её чтения" in _DOWNSTREAM_TABLE_CONTEXT
    assert "описывай нужные данные, не инструмент" in (
        _DOWNSTREAM_CAPABILITY_CONTEXT
    )
    assert "буквальный поиск" in _DOWNSTREAM_CAPABILITY_CONTEXT
    assert "явно данному фрагменту" in _DOWNSTREAM_CAPABILITY_CONTEXT
    assert "смысловой поиск по описаниям" in _DOWNSTREAM_CAPABILITY_CONTEXT
    assert "S2T-строки" in _DOWNSTREAM_CAPABILITY_CONTEXT
    assert "сохранённые результаты прошлых workers" in (
        _DOWNSTREAM_CAPABILITY_CONTEXT
    )
    for tool_name in (
        "list_column_catalog",
        "semantic_search_descriptions",
        "list_s2t_transformations",
        "run_sql",
        "read_previous_result",
    ):
        assert tool_name not in _DOWNSTREAM_CAPABILITY_CONTEXT
    for table_name in (
        "files",
        "file_sheet_headers",
        "source_tables",
        "target_tables",
        "source_columns",
        "target_columns",
        "additional_objects",
        "pxf_to_a",
        "s2t_transformations",
        "data",
    ):
        assert f"`{table_name}`" in _DOWNSTREAM_TABLE_CONTEXT
    assert "бизнес-описания таблиц" in _DOWNSTREAM_TABLE_CONTEXT
    assert "текст правила" in _DOWNSTREAM_TABLE_CONTEXT
    assert "неизвестные бизнес-объекты оставляй текстом поиска" in (
        _DOWNSTREAM_PLAN_REPAIR_PROMPT
    )
    assert "скопируй `original_task`" not in _DOWNSTREAM_PLAN_PROMPT
    plan_schema_text = str(_plan_tool_schema())
    assert "По умолчанию один шаг" not in plan_schema_text
    assert "без отдельных шагов производного анализа" in plan_schema_text
    assert "input_steps" not in plan_schema_text
    assert _plan_tool_schema()["function"]["parameters"]["properties"][
        "steps"
    ]["items"]["required"] == ["id", "task", "depends_on"]
    assert set(
        _plan_tool_schema()["function"]["parameters"]["properties"][
            "steps"
        ]["items"]["properties"]
    ) == {"id", "task", "depends_on"}
    plan_parameters = _plan_tool_schema()["function"]["parameters"]
    object_schemas = []

    def collect_object_schemas(schema):
        if not isinstance(schema, dict):
            return
        if schema.get("type") == "object":
            object_schemas.append(schema)
        for value in schema.values():
            if isinstance(value, dict):
                collect_object_schemas(value)
            elif isinstance(value, list):
                for item in value:
                    collect_object_schemas(item)

    collect_object_schemas(plan_parameters)
    assert object_schemas
    assert all(
        isinstance(schema.get("properties"), dict)
        for schema in object_schemas
    )
    worker_plan_schema_text = str(WorkerPlan.model_json_schema())
    assert "По умолчанию один шаг" not in worker_plan_schema_text
    assert "лениво использовать принятые результаты" in worker_plan_schema_text
    assert "результаты между шагами не передаются" not in worker_plan_schema_text
    assert "decision=\"pass\"" in _UPSTREAM_DATA_DECISION_PROMPT
    assert "decision=\"reroute\"" in _UPSTREAM_DATA_DECISION_PROMPT
    assert "не предлагай имена таблиц, колонок" in (
        _UPSTREAM_DATA_DECISION_PROMPT
    )
    assert "Не формируй пользовательский ответ" in (
        _UPSTREAM_DATA_DECISION_PROMPT
    )
    assert "used_evidence_ids" in _UPSTREAM_ANSWER_PROMPT
    assert "display_evidence_ids" in _UPSTREAM_ANSWER_PROMPT
    assert "`displayable`" in _UPSTREAM_ANSWER_PROMPT
    assert "физический идентификатор таблицы, поля или схемы" in (
        _UPSTREAM_ANSWER_PROMPT
    )
    assert "выбери подтверждающий его displayable evidence" in (
        _UPSTREAM_ANSWER_PROMPT
    )
    assert "новые физические идентификаторы" in (
        _upstream_answer_tool_schema()["function"]["parameters"]
        ["properties"]["display_evidence_ids"]["description"]
    )
    assert "`display_id`" not in _UPSTREAM_ANSWER_PROMPT
    assert "summary" not in combined.lower()
    assert "facts" not in combined.lower()
    assert "limitations" not in combined.lower()
    assert "каждый запрошенный" in combined
    assert "значение одной метрики" in combined
    assert "evidence всех операндов" in combined
    assert "не доказывает пустое множество или ноль" in combined
    assert "Промежуточный список кандидатов" in (
        _UPSTREAM_DATA_DECISION_PROMPT
    )
    assert "подпиши смысл каждого значения" in _UPSTREAM_ANSWER_PROMPT
    assert "безымянную CSV-последовательность" in _UPSTREAM_ANSWER_PROMPT
    assert "observations" not in combined
    assert "Правила upstream-анализа" in _UPSTREAM_ANALYSIS_CONTEXT
    assert "`LEFT JOIN ... ON p`" in _UPSTREAM_ANALYSIS_CONTEXT
    assert "не влияет на запрошенное target-поле" in (
        _UPSTREAM_ANALYSIS_CONTEXT
    )
    assert "из этой relation ничего не выводится" in (
        _UPSTREAM_ANALYSIS_CONTEXT
    )
    assert "Полевой маппинг задаёт конкретная S2T-строка" in (
        _UPSTREAM_ANALYSIS_CONTEXT
    )
    assert "`ON TRUE AND p` эквивалентно `ON p`" in " ".join(
        _UPSTREAM_ANALYSIS_CONTEXT.split()
    )
    assert "Технические поля хранилища" in _UPSTREAM_ANALYSIS_CONTEXT
    assert "не переименовывай" in _UPSTREAM_ANALYSIS_CONTEXT
    assert "Не подставляй фиктивное значение" in _UPSTREAM_ANALYSIS_CONTEXT
    assert "`{PLACEHOLDER}`" not in _UPSTREAM_ANALYSIS_CONTEXT
    assert "`analyze`" not in _UPSTREAM_ANALYSIS_CONTEXT

    answer_schema = _upstream_answer_tool_schema()["function"]["parameters"]
    assert answer_schema["required"] == ["answer"]
    assert set(answer_schema["properties"]) == {
        "answer",
        "used_evidence_ids",
        "display_evidence_ids",
    }
    request_schema = _upstream_data_decision_tool_schema()["function"][
        "parameters"
    ]
    assert request_schema["required"] == ["decision"]
    assert request_schema["properties"]["decision"]["enum"] == [
        "pass",
        "reroute",
    ]
    assert set(request_schema["properties"]) == {"decision", "problem"}
    plan_schema = _plan_tool_schema()["function"]["parameters"]
    step_schema = plan_schema["properties"]["steps"]["items"]
    assert step_schema["required"] == ["id", "task", "depends_on"]
    assert set(step_schema["properties"]) == {"id", "task", "depends_on"}
    assert "input_steps" not in step_schema["properties"]
    assert step_schema["properties"]["depends_on"]["uniqueItems"] is True
    assert "needs_from_previous" not in step_schema["properties"]
    assert "required_evidence" not in step_schema["properties"]


def test_upstream_native_tools_enforce_linear_payloads():
    from agents.coordinator import (
        CoordinatorResponseError,
        _native_upstream_answer,
        _native_upstream_decision,
    )

    pass_decision = _native_upstream_decision(
        AIMessage(
            content="",
            tool_calls=[
                {
                    "name": "submit_upstream_data_decision",
                    "args": {"decision": "pass"},
                    "id": "decision-pass",
                    "type": "tool_call",
                }
            ],
        )
    )
    assert pass_decision.decision == "pass"

    valid_request = _native_upstream_decision(
        AIMessage(
            content="",
            tool_calls=[
                {
                    "name": "submit_upstream_data_decision",
                    "args": {
                        "decision": "reroute",
                        "problem": "Не найден исходный путь.",
                    },
                    "id": "decision-reroute",
                    "type": "tool_call",
                }
            ],
        )
    )
    assert valid_request.decision == "reroute"
    assert valid_request.problem == "Не найден исходный путь."

    with pytest.raises(
        CoordinatorResponseError,
        match="reroute decision requires a non-empty problem",
    ):
        _native_upstream_decision(
            AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "submit_upstream_data_decision",
                        "args": {"decision": "reroute"},
                        "id": "request-without-problem",
                        "type": "tool_call",
                    }
                ],
            )
        )

    with pytest.raises(CoordinatorResponseError, match="decision: Field required"):
        _native_upstream_decision(
            AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "submit_upstream_data_decision",
                        "args": {},
                        "id": "request-without-decision",
                        "type": "tool_call",
                    }
                ],
            )
        )

    valid_answer = _native_upstream_answer(
        AIMessage(
            content="",
            tool_calls=[
                {
                    "name": "submit_upstream_answer",
                    "args": {
                        "answer": "Путь найден.",
                        "used_evidence_ids": ["evidence-path"],
                        "display_evidence_ids": ["evidence-path"],
                    },
                    "id": "answer-1",
                    "type": "tool_call",
                }
            ],
        )
    )
    assert valid_answer.answer == "Путь найден."

    minimal_answer = _native_upstream_answer(
        AIMessage(
            content="",
            tool_calls=[
                {
                    "name": "submit_upstream_answer",
                    "args": {"answer": "Данных достаточно."},
                    "id": "answer-minimal",
                    "type": "tool_call",
                }
            ],
        )
    )
    assert minimal_answer.used_evidence_ids == []
    assert minimal_answer.display_evidence_ids == []

    with pytest.raises(CoordinatorResponseError, match="answer: Field required"):
        _native_upstream_answer(
            AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "submit_upstream_answer",
                        "args": {
                            "used_evidence_ids": ["evidence-path"],
                            "display_evidence_ids": ["evidence-path"],
                        },
                        "id": "answer-without-text",
                        "type": "tool_call",
                    }
                ],
            )
        )


def test_coordinator_keeps_workers_isolated_and_combines_upstream_output(caplog):
    from agents.coordinator import _UPSTREAM_ANALYSIS_CONTEXT, coordinator_chat

    dataset = SavedResultDescriptor(
        result_ref="saved-first",
        source_tool="lookup",
        row_count=1,
        columns=[SavedResultColumn(name="name", sqlite_type="TEXT")],
    )
    first_artifact = _artifact(
        "display-first",
        "lookup",
        '{"name":"t_example"}',
        evidence_id="evidence-first",
        compact_args={"query": "t_example"},
        dataset_ref="saved-first",
    )
    second_artifact = _artifact(
        "display-second",
        "inspect",
        '{"name":"t_example","valid":true}',
        evidence_id="evidence-second",
        compact_args={"name": "t_example"},
    )
    model = _CoordinatorModel(
        {
            "submit_worker_plan": [
                _tool_message(
                    "submit_worker_plan",
                    {
                        "steps": [
                            {
                                "task": (
                                    "Найди точное target-имя t_example для "
                                    "file_id=7, учитывая только "
                                    "подтверждённые имена."
                                ),
                            },
                            {
                                "task": "Проверь найденное имя.",
                            },
                        ]
                    },
                    "plan-1",
                )
            ],
            "submit_upstream_output": [
                _tool_message(
                    "submit_upstream_output",
                    {
                        "answer": "Имя t_example проверено.",
                        "used_evidence_ids": [
                            "evidence-first",
                            "evidence-second",
                        ],
                        "display_evidence_ids": [
                            "evidence-first",
                            "evidence-second",
                        ],
                    },
                    "upstream-1",
                )
            ],
        }
    )
    worker_results = [
        _outcome(
            "Точное имя: t_example.",
            evidence=[first_artifact],
            datasets=[dataset],
            previous_results=[
                PreviousResultReference(
                    result_id="result-first",
                    description="lookup: точное имя t_example.",
                )
            ],
        ),
        _outcome("Имя t_example проверено.", evidence=[second_artifact]),
    ]
    model_patch, callback_patch, trace_patch = _patches(model)
    with caplog.at_level("INFO", logger="agents.coordinator"):
        with (
            model_patch,
            callback_patch,
            trace_patch,
            patch(
                "agents.coordinator.worker_chat",
                side_effect=worker_results,
            ) as worker,
            patch("agents.coordinator.discard_worker_display_refs") as discard,
        ):
            result = coordinator_chat(
                "Найди имя для file_id=7 и проверь его.",
                context="Общий фон: target table t_example.",
            )

    assert result == CoordinatorAnswer(
        answer="Имя t_example проверено.",
        display_refs=["display-first", "display-second"],
    )
    assert worker.call_count == 2
    assert worker.call_args_list[0].kwargs == {}
    assert worker.call_args_list[1].kwargs == {}
    first_task = worker.call_args_list[0].args[0]
    first_parts = parse_worker_request(first_task)
    assert first_parts.current_task == (
        "Найди точное target-имя t_example для file_id=7, учитывая только "
        "подтверждённые имена."
    )
    assert first_parts.previous_results is None
    assert "Структурированные ограничения шага" not in first_parts.current_task
    second_task = worker.call_args_list[1].args[0]
    second_parts = parse_worker_request(second_task)
    assert second_parts.current_task == "Проверь найденное имя."
    assert "Общий фон" not in first_parts.current_task
    assert "Общий фон" not in second_parts.current_task
    operation_payload = _payload(model, "select_operation_skills")
    assert operation_payload == {
        "original_task": "Найди имя для file_id=7 и проверь его.",
        "stable_context": "Общий фон: target table t_example.",
    }
    plan_payload = _payload(model, "submit_worker_plan")
    assert plan_payload["context"] == "Общий фон: target table t_example."
    assert [
        item.model_dump(mode="json", exclude_none=True)
        for item in (second_parts.previous_results or [])
    ] == [
        {
            "result_id": "result-first",
            "description": "lookup: точное имя t_example.",
        }
    ]
    assert isinstance(second_task, WorkerRequestParts)
    discard.assert_not_called()

    upstream = _payload(model, "submit_upstream_answer")
    serialized_upstream = json.dumps(upstream, ensure_ascii=False)
    assert set(upstream) == {
        "original_task",
        "evidence",
        "execution_manifest",
    }
    assert upstream["original_task"] == (
        "Найди имя для file_id=7 и проверь его."
    )
    assert upstream["evidence"] == [
        {
            "evidence_id": "evidence-first",
            "tool_name": "lookup",
            "args": {"query": "t_example"},
            "preview": '{"name":"t_example"}',
            "truncated": False,
            "displayable": True,
        },
        {
            "evidence_id": "evidence-second",
            "tool_name": "inspect",
            "args": {"name": "t_example"},
            "preview": '{"name":"t_example","valid":true}',
            "truncated": False,
            "displayable": True,
        },
    ]
    for forbidden in (
        "summary",
        "facts",
        "limitations",
        "observation",
        "cycle_history",
        "display_ref",
        "dataset_ref",
        "datasets",
    ):
        assert forbidden not in serialized_upstream
    upstream_messages = [
        messages
        for name, messages in model.messages
        if name == "submit_upstream_answer"
    ][0]
    assert _UPSTREAM_ANALYSIS_CONTEXT in str(upstream_messages[0].content)
    assert "Upstream coordinator result:" in caplog.text


def test_upstream_selects_only_requested_display_and_discards_other_refs():
    from agents.coordinator import coordinator_chat

    stages = []

    def stage_scope(stage):
        stages.append(stage)
        return nullcontext()

    model = _CoordinatorModel(
        _responses(
            answer="Факт получен.",
            used_evidence_ids=("evidence-second",),
            display_evidence_ids=("evidence-second",),
        )
    )
    worker_result = _outcome(
        "Факт получен.",
        evidence=[
            _artifact(
                "display-first",
                "lookup",
                "first",
                evidence_id="evidence-first",
            ),
            _artifact(
                "display-second",
                "inspect",
                "second",
                evidence_id="evidence-second",
            ),
        ],
    )
    model_patch, callback_patch, trace_patch = _patches(model)
    with (
        model_patch,
        callback_patch,
        trace_patch,
        patch("agents.coordinator.llm_stage", side_effect=stage_scope),
        patch("agents.coordinator.worker_chat", return_value=worker_result),
        patch("agents.coordinator.discard_worker_display_refs") as discard,
    ):
        result = coordinator_chat("Получи факт.")

    assert result == CoordinatorAnswer(
        answer="Факт получен.",
        display_refs=["display-second"],
    )
    discard.assert_called_once_with(["display-first"])
    assert stages == [
        "operation_router",
        "downstream_plan",
        "upstream",
        "upstream",
    ]


def test_coordinator_passes_lazy_result_references_between_workers():
    from agents.coordinator import coordinator_chat

    model = _CoordinatorModel(
        {
            "submit_worker_plan": [
                _tool_message(
                    "submit_worker_plan",
                    {
                        "steps": [
                            {
                                "task": "Получи первый факт.",
                            },
                            {
                                "task": "Получи второй факт.",
                            },
                            {
                                "task": "Проверь второй факт.",
                            },
                        ]
                    },
                    "plan-1",
                )
            ],
            "submit_upstream_output": [
                _tool_message(
                    "submit_upstream_output",
                    {
                        "answer": "Проверка завершена.",
                        "used_evidence_ids": [],
                        "display_evidence_ids": [],
                    },
                    "upstream-1",
                )
            ],
        }
    )
    worker_results = [
        _outcome(
            "Первый факт: A.",
            evidence=[
                _artifact(
                    "display-first",
                    "first_lookup",
                    '{"value":"A"}',
                    evidence_id="evidence-first",
                )
            ],
            previous_results=[
                PreviousResultReference(
                    result_id="result-first",
                    description="first_lookup: первый факт A.",
                )
            ],
        ),
        _outcome(
            "Второй факт: B.",
            evidence=[
                _artifact(
                    "display-second",
                    "second_lookup",
                    '{"value":"B"}',
                    evidence_id="evidence-second",
                )
            ],
            previous_results=[
                PreviousResultReference(
                    result_id="result-second",
                    description="second_lookup: второй факт B.",
                )
            ],
        ),
        _outcome("Второй факт B проверен."),
    ]
    model_patch, callback_patch, trace_patch = _patches(model)
    with (
        model_patch,
        callback_patch,
        trace_patch,
        patch("agents.coordinator.worker_chat", side_effect=worker_results) as worker,
        patch("agents.coordinator.discard_worker_display_refs"),
    ):
        result = coordinator_chat("Получи два факта и проверь второй.")

    assert result.answer == "Проверка завершена."
    first_parts, second_parts, third_parts = [
        parse_worker_request(call.args[0]) for call in worker.call_args_list
    ]
    assert first_parts.current_task == "Получи первый факт."
    assert first_parts.previous_results is None
    assert second_parts.current_task == "Получи второй факт."
    assert [
        item.result_id for item in (second_parts.previous_results or [])
    ] == ["result-first"]
    assert third_parts.current_task == "Проверь второй факт."
    assert [
        item.result_id for item in (third_parts.previous_results or [])
    ] == ["result-first", "result-second"]


def test_upstream_receives_partial_worker_evidence():
    from agents.coordinator import coordinator_chat

    answer = "Не удалось подтвердить требуемый факт."
    model = _CoordinatorModel(_responses(answer=answer))
    worker_result = _outcome(
        "Tool вернул данные не по той сущности; факт не подтверждён.",
        status="partial",
        stop_reason="truncated_source",
        unmet_requirements=["Не подтверждена полная выборка сущности."],
        evidence=[
            _artifact(
                "display-partial",
                "lookup",
                '{"value":"partial"}',
                evidence_id="evidence-partial",
            )
        ],
    )
    model_patch, callback_patch, trace_patch = _patches(model)
    with (
        model_patch,
        callback_patch,
        trace_patch,
        patch("agents.coordinator.worker_chat", return_value=worker_result),
        patch("agents.coordinator.record_worker_outcome") as record_outcome,
        patch("agents.coordinator.discard_worker_display_refs") as discard,
    ):
        result = coordinator_chat("Получи факт.")

    assert result == CoordinatorAnswer(answer=answer, display_refs=[])
    upstream = _payload(model, "submit_upstream_answer")
    assert set(upstream) == {
        "original_task",
        "evidence",
        "execution_manifest",
    }
    assert upstream["evidence"] == [
        {
            "evidence_id": "evidence-partial",
            "tool_name": "lookup",
            "args": {},
            "preview": '{"value":"partial"}',
            "truncated": False,
            "displayable": True,
        }
    ]
    record_outcome.assert_called_once_with(
        cycle=1,
        step=1,
        status="partial",
        stop_reason="truncated_source",
        unmet_requirements=["Не подтверждена полная выборка сущности."],
        evidence_count=1,
        dataset_count=0,
    )
    discard.assert_called_once_with(["display-partial"])


def test_upstream_normalizes_structured_answer_after_data_pass():
    from agents.coordinator import coordinator_chat

    model = _CoordinatorModel(
        _responses(
            answer=[{"value": 42}],
            plan_task="Верни JSON-массив со значением 42.",
        )
    )
    model_patch, callback_patch, trace_patch = _patches(model)
    with (
        model_patch,
        callback_patch,
        trace_patch,
        patch(
            "agents.coordinator.worker_chat",
            return_value=_outcome("Значение: 42."),
        ),
    ):
        result = coordinator_chat("Верни JSON-массив со значением 42.")

    assert result == CoordinatorAnswer(answer='[{"value":42}]', display_refs=[])
    assert [name for name, _ in model.messages] == [
        "select_operation_skills",
        "submit_worker_plan",
        "submit_upstream_data_decision",
        "submit_upstream_answer",
    ]


def test_upstream_repairs_json_serialization_noise_with_model():
    from agents.coordinator import coordinator_chat

    evidence_id = "evidence_0123456789abcdef"
    serialized_fragment = (
        f'["{evidence_id}"], "display_evidence_ids": '
        f'["{evidence_id}"], "displayable": true'
    )
    responses = _responses(answer="unused")
    responses["submit_upstream_data_decision"] = [
        _tool_message(
            "submit_upstream_data_decision",
            {"decision": "pass"},
            "decision-pass",
        )
    ]
    responses["submit_upstream_answer"] = [
        _tool_message(
            "submit_upstream_answer",
            {
                "answer": "Факт получен.",
                "used_evidence_ids": [
                    serialized_fragment,
                    "display_evidence_ids",
                    "displayable",
                ],
                "display_evidence_ids": [serialized_fragment],
            },
            "upstream-malformed",
        ),
        _tool_message(
            "submit_upstream_answer",
            {
                "answer": "Факт получен.",
                "used_evidence_ids": [evidence_id],
                "display_evidence_ids": [evidence_id],
            },
            "upstream-repaired",
        ),
    ]
    model = _CoordinatorModel(responses)
    worker_result = _outcome(
        "Факт получен.",
        evidence=[
            _artifact(
                "display-result",
                "lookup",
                '{"value":1}',
                evidence_id=evidence_id,
            )
        ],
    )
    model_patch, callback_patch, trace_patch = _patches(model)
    with (
        model_patch,
        callback_patch,
        trace_patch,
        patch("agents.coordinator.worker_chat", return_value=worker_result),
    ):
        result = coordinator_chat("Получи факт.")

    assert result == CoordinatorAnswer(
        answer="Факт получен.",
        display_refs=["display-result"],
    )
    upstream_calls = [
        messages
        for name, messages in model.messages
        if name == "submit_upstream_answer"
    ]
    assert len(upstream_calls) == 2
    assert "неизвестные evidence_id" in str(upstream_calls[1][-1].content)


@pytest.mark.parametrize(
    "bad_value",
    [
        (
            '["evidence_0123456789abcdef", '
            '"evidence_deadbeef"]'
        ),
        "answer says evidence_0123456789abcdef is sufficient",
    ],
    ids=["unknown-serialized-id", "semantic-junk"],
)
def test_upstream_does_not_recover_unsafe_evidence_strings(bad_value):
    from agents.coordinator import coordinator_chat

    evidence_id = "evidence_0123456789abcdef"
    model = _CoordinatorModel(
        {
            "submit_worker_plan": _responses(answer="unused")[
                "submit_worker_plan"
            ],
            "submit_upstream_output": [
                _tool_message(
                    "submit_upstream_output",
                    {
                        "answer": "Факт получен.",
                        "used_evidence_ids": [bad_value],
                        "display_evidence_ids": [bad_value],
                    },
                    "upstream-unsafe",
                ),
                _tool_message(
                    "submit_upstream_output",
                    {
                        "answer": "Факт получен.",
                        "used_evidence_ids": [evidence_id],
                        "display_evidence_ids": [evidence_id],
                    },
                    "upstream-repaired",
                ),
            ],
        }
    )
    worker_result = _outcome(
        "Факт получен.",
        evidence=[
            _artifact(
                "display-result",
                "lookup",
                '{"value":1}',
                evidence_id=evidence_id,
            )
        ],
    )
    model_patch, callback_patch, trace_patch = _patches(model)
    with (
        model_patch,
        callback_patch,
        trace_patch,
        patch("agents.coordinator.worker_chat", return_value=worker_result),
    ):
        result = coordinator_chat("Получи факт.")

    assert result.display_refs == ["display-result"]
    upstream_calls = [
        messages
        for name, messages in model.messages
        if name == "submit_upstream_answer"
    ]
    assert len(upstream_calls) == 2
    assert "неизвестные evidence_id" in upstream_calls[1][-1].content


def test_upstream_repairs_unknown_evidence_id():
    from agents.coordinator import coordinator_chat

    model = _CoordinatorModel(
        {
            "submit_worker_plan": _responses(answer="unused")[
                "submit_worker_plan"
            ],
            "submit_upstream_output": [
                _tool_message(
                    "submit_upstream_output",
                    {
                        "answer": "Факт получен.",
                        "used_evidence_ids": ["unknown"],
                        "display_evidence_ids": ["unknown"],
                    },
                    "upstream-invalid",
                ),
                _tool_message(
                    "submit_upstream_output",
                    {
                        "answer": "Факт получен.",
                        "used_evidence_ids": ["evidence-result"],
                        "display_evidence_ids": ["evidence-result"],
                    },
                    "upstream-repaired",
                ),
            ],
        }
    )
    worker_result = _outcome(
        "Факт получен.",
        evidence=[
            _artifact(
                "display-result",
                "lookup",
                '{"value":1}',
                evidence_id="evidence-result",
            )
        ],
    )
    model_patch, callback_patch, trace_patch = _patches(model)
    with (
        model_patch,
        callback_patch,
        trace_patch,
        patch("agents.coordinator.worker_chat", return_value=worker_result),
    ):
        result = coordinator_chat("Получи факт.")

    assert result.display_refs == ["display-result"]
    upstream_calls = [
        messages
        for name, messages in model.messages
        if name == "submit_upstream_answer"
    ]
    assert len(upstream_calls) == 2
    assert isinstance(upstream_calls[1][-2], ToolMessage)
    assert upstream_calls[1][-2].tool_call_id == "upstream-invalid"
    assert "rejected" in upstream_calls[1][-2].content
    assert "доступные evidence_id" in upstream_calls[1][-1].content
    assert '"evidence-result"' in upstream_calls[1][-1].content


def test_sql_risk_upstream_repairs_empty_used_evidence_ids():
    from agents.coordinator import coordinator_chat

    model = _CoordinatorModel(
        {
            "select_operation_skills": [
                _tool_message(
                    "select_operation_skills",
                    {
                        "pipeline": "agentic",
                        "skills": ["Анализ SQL-рисков"],
                        "sql_risk_aspects": ["cardinality"],
                    },
                    "operation-cardinality",
                )
            ],
            "submit_worker_plan": [
                _tool_message(
                    "submit_worker_plan",
                    {
                        "steps": [
                            {
                                "task": (
                                    "Прочитать полный exact S2T mapping "
                                    "src_np → tgt_np."
                                )
                            }
                        ]
                    },
                    "plan-cardinality",
                )
            ],
            "submit_upstream_output": [
                _tool_message(
                    "submit_upstream_output",
                    {
                        "answer": "JOIN может размножить строки.",
                        "used_evidence_ids": [],
                        "display_evidence_ids": [],
                    },
                    "answer-without-provenance",
                ),
                _tool_message(
                    "submit_upstream_output",
                    {
                        "answer": "JOIN может размножить строки.",
                        "used_evidence_ids": ["evidence-mapping"],
                        "display_evidence_ids": [],
                    },
                    "answer-with-provenance",
                ),
            ],
        }
    )
    worker_result = _outcome(
        "Mapping прочитан.",
        evidence=[
            _artifact(
                None,
                "read_s2t_source_to_target",
                '{"rows":[{"transformation_rule":"SELECT ... JOIN ..."}]}',
                evidence_id="evidence-mapping",
                compact_args={
                    "source_table": "src_np",
                    "target_table": "tgt_np",
                },
            )
        ],
    )
    model_patch, callback_patch, trace_patch = _patches(model)
    with (
        model_patch,
        callback_patch,
        trace_patch,
        patch("agents.coordinator.worker_chat", return_value=worker_result),
    ):
        result = coordinator_chat(
            "Оцени риск появления дубликатов для src_np → tgt_np."
        )

    assert result.answer == "JOIN может размножить строки."
    answer_calls = [
        messages
        for name, messages in model.messages
        if name == "submit_upstream_answer"
    ]
    assert len(answer_calls) == 2
    assert "обязан сослаться" in answer_calls[1][-1].content


def test_agentic_cardinality_does_not_apply_a_hidden_task_intent_bypass(
    monkeypatch,
):
    from agents.coordinator import coordinator_chat
    from agents.tools.context import (
        OPERATION_SQL_RISK_ASPECTS_EXPERIMENT_ENV,
    )
    from agents.tools.saved_results import get_active_saved_result_store

    monkeypatch.setenv(OPERATION_SQL_RISK_ASPECTS_EXPERIMENT_ENV, "1")
    original_task = (
        "Оцени риск появления дубликатов при сохранённой S2T-трансформации "
        "src_np → tgt_np. Назови фактический JOIN и явно отдели "
        "подтверждённый механизм от условия по уникальности."
    )
    model = _CoordinatorModel(
        {
            "select_operation_skills": [
                _tool_message(
                    "select_operation_skills",
                    {
                        "pipeline": "agentic",
                        "skills": ["Анализ SQL-рисков"],
                        "sql_risk_aspects": ["cardinality"],
                    },
                    "operation-cardinality",
                )
            ],
            "submit_worker_plan": [
                _tool_message(
                    "submit_worker_plan",
                    {
                        "steps": [
                            {
                                "task": (
                                    "Прочитать полный exact directed S2T "
                                    "mapping src_np → tgt_np."
                                )
                            }
                        ]
                    },
                    "plan-cardinality",
                ),
                _tool_message(
                    "submit_worker_plan",
                    {
                        "steps": [
                            {
                                "task": (
                                    "Повторно прочитать полный exact directed "
                                    "S2T mapping src_np → tgt_np."
                                )
                            }
                        ]
                    },
                    "plan-cardinality-cycle-2",
                ),
            ],
            "submit_upstream_data_decision": [
                _tool_message(
                    "submit_upstream_data_decision",
                    {
                        "decision": "reroute",
                        "problem": (
                            "Нужно дополнительно прочитать ключи aux_np."
                        ),
                    },
                    "decision-cardinality-reroute",
                ),
                _tool_message(
                    "submit_upstream_data_decision",
                    {"decision": "pass"},
                    "decision-cardinality-pass",
                ),
            ],
            "submit_upstream_answer": [
                _tool_message(
                    "submit_upstream_answer",
                    {
                        "answer": (
                            "JOIN aux_np по id может размножить строки, если "
                            "ключ aux_np.id не уникален."
                        ),
                        "used_evidence_ids": ["evidence-cardinality"],
                        "display_evidence_ids": [],
                    },
                    "answer-cardinality",
                )
            ],
        }
    )

    def worker_with_complete_mapping(_request):
        store = get_active_saved_result_store()
        assert store is not None
        rows = [
            {
                "source_table": "src_np",
                "target_table": "tgt_np",
                "transformation_rule": (
                    "SELECT s.id FROM src_np s JOIN aux_np d ON d.id=s.id"
                ),
            }
        ]
        descriptor = store.save_payload(
            source_tool="read_s2t_source_to_target",
            source_tool_call_id="call-cardinality",
            payload={
                "rows": rows,
                "total_matches": len(rows),
                "truncated": False,
            },
        )
        assert descriptor is not None
        return _outcome(
            "Полный exact mapping прочитан.",
            evidence=[
                _artifact(
                    None,
                    "read_s2t_source_to_target",
                    '{"rows":[{"transformation_rule":"SELECT ..."}]}',
                    evidence_id="evidence-cardinality",
                    compact_args={
                        "source_table": "src_np",
                        "target_table": "tgt_np",
                    },
                    dataset_ref=descriptor.result_ref,
                )
            ],
            datasets=[descriptor],
        )

    model_patch, callback_patch, trace_patch = _patches(model)
    with (
        model_patch,
        callback_patch,
        trace_patch,
        patch(
            "agents.coordinator.worker_chat",
            side_effect=worker_with_complete_mapping,
        ) as worker,
    ):
        result = coordinator_chat(original_task)

    assert "может размножить" in result.answer
    assert worker.call_count == 2
    assert [name for name, _ in model.messages].count(
        "submit_worker_plan"
    ) == 2
    assert [name for name, _ in model.messages].count(
        "submit_upstream_answer"
    ) == 1


def test_cardinality_factual_uniqueness_does_not_bypass_reroute(monkeypatch):
    from agents.coordinator import coordinator_chat
    from agents.tools.context import (
        OPERATION_SQL_RISK_ASPECTS_EXPERIMENT_ENV,
    )
    from agents.tools.saved_results import get_active_saved_result_store

    monkeypatch.setenv(OPERATION_SQL_RISK_ASPECTS_EXPERIMENT_ENV, "1")
    model = _CoordinatorModel(
        {
            "select_operation_skills": [
                _tool_message(
                    "select_operation_skills",
                    {
                        "pipeline": "agentic",
                        "skills": ["Анализ SQL-рисков"],
                        "sql_risk_aspects": ["cardinality"],
                    },
                    "operation-cardinality",
                )
            ],
            "submit_worker_plan": [
                _tool_message(
                    "submit_worker_plan",
                    {
                        "steps": [
                            {"task": "Прочитать mapping src_np → tgt_np."}
                        ]
                    },
                    "plan-cardinality-1",
                ),
                _tool_message(
                    "submit_worker_plan",
                    {
                        "steps": [
                            {
                                "task": (
                                    "Повторно прочитать полный exact S2T "
                                    "mapping src_np → tgt_np."
                                )
                            }
                        ]
                    },
                    "plan-cardinality-2",
                ),
            ],
            "submit_upstream_data_decision": [
                _tool_message(
                    "submit_upstream_data_decision",
                    {
                        "decision": "reroute",
                        "problem": "Нужно проверить фактическую уникальность.",
                    },
                    "decision-cardinality-reroute",
                ),
                _tool_message(
                    "submit_upstream_data_decision",
                    {"decision": "pass"},
                    "decision-cardinality-pass",
                ),
            ],
            "submit_upstream_answer": [
                _tool_message(
                    "submit_upstream_answer",
                    {
                        "answer": "Фактическая уникальность проверена.",
                        "used_evidence_ids": ["evidence-cycle-2"],
                        "display_evidence_ids": [],
                    },
                    "answer-cardinality",
                )
            ],
        }
    )
    call_number = 0

    def worker_with_complete_mapping(_request):
        nonlocal call_number
        call_number += 1
        store = get_active_saved_result_store()
        assert store is not None
        descriptor = store.save_payload(
            source_tool="read_s2t_source_to_target",
            source_tool_call_id=f"call-cardinality-{call_number}",
            payload={
                "rows": [
                    {
                        "source_table": "src_np",
                        "target_table": "tgt_np",
                        "transformation_rule": (
                            "SELECT s.id FROM src_np s JOIN aux_np d "
                            "ON d.id=s.id"
                        ),
                    }
                ],
                "total_matches": 1,
                "truncated": False,
            },
        )
        assert descriptor is not None
        evidence_id = f"evidence-cycle-{call_number}"
        return _outcome(
            "Mapping прочитан.",
            evidence=[
                _artifact(
                    None,
                    "read_s2t_source_to_target",
                    "mapping",
                    evidence_id=evidence_id,
                    compact_args={
                        "source_table": "src_np",
                        "target_table": "tgt_np",
                    },
                    dataset_ref=descriptor.result_ref,
                )
            ],
            datasets=[descriptor],
        )

    model_patch, callback_patch, trace_patch = _patches(model)
    with (
        model_patch,
        callback_patch,
        trace_patch,
        patch(
            "agents.coordinator.worker_chat",
            side_effect=worker_with_complete_mapping,
        ) as worker,
    ):
        result = coordinator_chat(
            "Оцени риск дубликатов для src_np → tgt_np и проверь "
            "фактическую уникальность aux_np.id."
        )

    assert result.answer == "Фактическая уникальность проверена."
    assert worker.call_count == 2
    assert [name for name, _ in model.messages].count(
        "submit_worker_plan"
    ) == 2


def test_upstream_has_no_separate_semantic_review():
    from agents.coordinator import coordinator_chat

    model = _CoordinatorModel(
        {
            "submit_worker_plan": _responses(
                answer="unused",
                plan_task=(
                    "Верни имя и число строк. Полный результат числа "
                    "покажи отдельно."
                ),
            )["submit_worker_plan"],
            "submit_upstream_output": [
                _tool_message(
                    "submit_upstream_output",
                    {
                        "answer": "Имя: t_example.",
                        "used_evidence_ids": ["evidence-name"],
                        "display_evidence_ids": [],
                    },
                    "upstream-incomplete",
                ),
            ],
        }
    )
    worker_result = _outcome(
        "Найдены имя и число строк.",
        evidence=[
            _artifact(
                "display-name",
                "lookup",
                '{"name":"t_example"}',
                evidence_id="evidence-name",
            ),
            _artifact(
                "display-count",
                "count",
                '{"row_count":42}',
                evidence_id="evidence-count",
            ),
        ],
    )
    model_patch, callback_patch, trace_patch = _patches(model)
    with (
        model_patch,
        callback_patch,
        trace_patch,
        patch("agents.coordinator.worker_chat", return_value=worker_result),
    ):
        result = coordinator_chat(
            "Верни имя и число строк. Полный результат числа покажи отдельно."
        )

    assert result.answer == "Имя: t_example."
    assert result.display_refs == []
    upstream_calls = [
        messages
        for name, messages in model.messages
        if name == "submit_upstream_answer"
    ]
    assert len(upstream_calls) == 1
    assert "submit_upstream_review" not in [name for name, _ in model.messages]


def test_upstream_restarts_cleanly_with_only_problem():
    from agents.coordinator import coordinator_chat

    original_task = "Верни top target_table и все три метрики source_table."
    model = _CoordinatorModel(
        {
            "submit_worker_plan": [
                _tool_message(
                    "submit_worker_plan",
                    {
                        "steps": [
                            {
                                "task": original_task
                            }
                        ]
                    },
                    "plan-cycle-1",
                ),
                _tool_message(
                    "submit_worker_plan",
                    {
                        "steps": [
                            {
                                "task": original_task
                            }
                        ]
                    },
                    "plan-cycle-2",
                ),
            ],
            "submit_upstream_output": [
                _tool_message(
                    "submit_upstream_output",
                    {
                        "action": "request_more_data",
                        "problem": "Отсутствуют три метрики source_table.",
                    },
                    "upstream-request-more",
                ),
                _tool_message(
                    "submit_upstream_output",
                    {
                        "answer": (
                            "target_table=t_example; строк=42; разных "
                            "source_table=3; top_source=s_example; строк=20"
                        ),
                        "used_evidence_ids": [
                            "evidence-complete",
                        ],
                        "display_evidence_ids": ["evidence-complete"],
                    },
                    "upstream-final",
                ),
            ],
        }
    )
    first_outcome = _outcome(
        "target_table=t_example; строк=42",
        evidence=[
            _artifact(
                "display-target",
                "run_sql",
                '{"target_table":"t_example","row_count":42}',
                evidence_id="evidence-target",
            )
        ],
    )
    second_outcome = _outcome(
        (
            "target_table=t_example; строк=42; разных source_table=3; "
            "top_source=s_example; строк=20"
        ),
        evidence=[
            _artifact(
                "display-complete",
                "run_sql",
                (
                    '{"target_table":"t_example","target_rows":42,'
                    '"distinct_sources":3,"top_source":"s_example",'
                    '"source_rows":20}'
                ),
                evidence_id="evidence-complete",
            )
        ],
    )
    model_patch, callback_patch, trace_patch = _patches(model)
    with (
        model_patch,
        callback_patch,
        trace_patch,
        patch(
            "agents.coordinator.worker_chat",
            side_effect=[first_outcome, second_outcome],
        ) as worker,
    ):
        result = coordinator_chat(original_task)

    assert result == CoordinatorAnswer(
        answer=(
            "target_table=t_example; строк=42; разных source_table=3; "
            "top_source=s_example; строк=20"
        ),
        display_refs=["display-complete"],
    )
    assert worker.call_count == 2
    assert len(
        [
            name
            for name, _ in model.messages
            if name == "submit_upstream_data_decision"
        ]
    ) == 2
    second_plan_payload = _payload(model, "submit_worker_plan", 1)
    assert second_plan_payload == {
        "original_task": original_task,
        "context": "",
        "problem": "Отсутствуют три метрики source_table.",
    }
    second_worker_task = worker.call_args_list[1].args[0]
    assert "target_table=t_example; строк=42" not in parse_worker_request(
        second_worker_task
    ).current_task
    final_upstream_payload = _payload(model, "submit_upstream_answer")
    assert set(final_upstream_payload) == {
        "original_task",
        "evidence",
        "execution_manifest",
    }
    assert [
        item["evidence_id"] for item in final_upstream_payload["evidence"]
    ] == ["evidence-complete"]


def test_coordinator_returns_limited_answer_after_two_data_cycles():
    from agents.coordinator import CoordinatorAnswer, coordinator_chat

    model = _CoordinatorModel(
        {
            "submit_worker_plan": [
                _tool_message(
                    "submit_worker_plan",
                    {"steps": [{"task": "Получи полный результат."}]},
                    "plan-cycle-1",
                ),
                _tool_message(
                    "submit_worker_plan",
                    {"steps": [{"task": "Получи полный результат."}]},
                    "plan-cycle-2",
                ),
            ],
            "submit_upstream_output": [
                _tool_message(
                    "submit_upstream_output",
                    {
                        "action": "request_more_data",
                        "answer": "",
                        "used_evidence_ids": [],
                        "display_evidence_ids": [],
                        "problem": "Первой части недостаточно.",
                    },
                    "request-cycle-1",
                ),
                _tool_message(
                    "submit_upstream_output",
                    {
                        "answer": (
                            "Не удалось получить достаточно подтверждённых "
                            "данных для полного ответа. После второго цикла "
                            "данных всё ещё мало."
                        ),
                        "used_evidence_ids": [],
                        "display_evidence_ids": [],
                    },
                    "pass-cycle-2",
                ),
            ],
        }
    )
    model_patch, callback_patch, trace_patch = _patches(model)
    with (
        model_patch,
        callback_patch,
        trace_patch,
        patch(
            "agents.coordinator.worker_chat",
            side_effect=[_outcome("Первая часть."), _outcome("Вторая часть.")],
        ),
    ):
        result = coordinator_chat("Получи полный результат.")

    assert result == CoordinatorAnswer(
        answer=(
            "Не удалось получить достаточно подтверждённых данных для "
            "полного ответа. После второго цикла данных всё ещё мало."
        ),
        display_refs=[],
    )


def test_last_cycle_answer_explicitly_reports_failed_steps():
    from agents.coordinator import coordinator_chat

    model = _CoordinatorModel(
        {
            "submit_worker_plan": [
                _tool_message(
                    "submit_worker_plan",
                    {"steps": [{"task": "Получи обязательный факт."}]},
                    "plan-failed-1",
                ),
                _tool_message(
                    "submit_worker_plan",
                    {"steps": [{"task": "Повтори обязательный факт."}]},
                    "plan-failed-2",
                ),
            ],
            "submit_upstream_output": [
                _tool_message(
                    "submit_upstream_output",
                    {
                        "action": "request_more_data",
                        "problem": "Обязательный факт не получен.",
                    },
                    "reroute-failed",
                ),
                _tool_message(
                    "submit_upstream_output",
                    {
                        "answer": "Подтверждённых данных нет.",
                        "used_evidence_ids": [],
                        "display_evidence_ids": [],
                    },
                    "answer-failed",
                ),
            ],
        }
    )
    failed = _outcome(
        "Источник недоступен.",
        status="failed",
        stop_reason="tool_error",
        unmet_requirements=["Обязательный факт не получен."],
    )
    model_patch, callback_patch, trace_patch = _patches(model)
    with (
        model_patch,
        callback_patch,
        trace_patch,
        patch(
            "agents.coordinator.worker_chat",
            side_effect=[failed, failed],
        ),
    ):
        result = coordinator_chat("Получи обязательный факт.")

    assert result.answer.startswith("Подтверждённых данных нет.")
    assert "step_1 (failed)" in result.answer
    answer_payload = _payload(model, "submit_upstream_answer")
    assert answer_payload["execution_manifest"][0]["status"] == "failed"
    assert "step_1 (failed)" in answer_payload["data_problem"]


def test_coordinator_empty_task_does_not_call_llm():
    from agents.coordinator import coordinator_chat

    result = coordinator_chat("   ")

    assert result.display_refs == []
    assert "пуст" in result.answer.lower()


def test_coordinator_repairs_plan_that_exceeds_worker_limit():
    from agents.coordinator import coordinator_chat

    invalid_steps = [
        {
            "task": f"Проверка {index}",
        }
        for index in range(1, 10)
    ]
    model = _CoordinatorModel(
        {
            "submit_worker_plan": [
                _tool_message(
                    "submit_worker_plan",
                    {"steps": invalid_steps},
                    "plan-invalid",
                ),
                _tool_message(
                    "submit_worker_plan",
                    {
                        "steps": [
                            {
                                "task": "Выполни девять связанных проверок.",
                            }
                        ]
                    },
                    "plan-repaired",
                ),
            ],
            "submit_upstream_output": [
                _tool_message(
                    "submit_upstream_output",
                    {
                        "answer": "Все проверки выполнены.",
                        "used_evidence_ids": [],
                        "display_evidence_ids": [],
                    },
                    "upstream-1",
                )
            ],
        }
    )
    model_patch, callback_patch, trace_patch = _patches(model)
    with (
        model_patch,
        callback_patch,
        trace_patch,
        patch(
            "agents.coordinator.worker_chat",
            return_value=_outcome("Все проверки выполнены."),
        ),
    ):
        result = coordinator_chat("Выполни девять связанных проверок.")

    assert result == CoordinatorAnswer(
        answer="Все проверки выполнены.",
        display_refs=[],
    )
    plan_messages = [
        messages
        for name, messages in model.messages
        if name == "submit_worker_plan"
    ]
    assert len(plan_messages) == 2
    assert isinstance(plan_messages[1][-2], ToolMessage)
    assert plan_messages[1][-2].tool_call_id == "plan-invalid"
    assert "rejected" in plan_messages[1][-2].content
    assert "от 1 до 8 элементов" in plan_messages[1][-1].content
    assert "непустую `task`" in plan_messages[1][-1].content


def test_schema_valid_model_tasks_bypass_semantic_origin_rejection():
    from agents.coordinator import coordinator_chat

    original_task = (
        "Для файла synthetic.xlsx оцени точную загрузку src_np → tgt_np."
    )
    model = _CoordinatorModel(
        {
            "submit_worker_plan": [
                _tool_message(
                    "submit_worker_plan",
                    {
                        "steps": [
                            {
                                "task": (
                                    "Перечислить все таблицы source_np и "
                                    "target_np для file_id=1."
                                )
                            },
                            {
                                "task": (
                                    "Проверь найденные таблицы, не повторяя "
                                    "исходную пару."
                                )
                            },
                            {
                                "task": "Прочитай произвольный финальный факт."
                            },
                        ]
                    },
                    "plan-model-owned",
                ),
            ],
            "submit_upstream_output": [
                _tool_message(
                    "submit_upstream_output",
                    {
                        "answer": "Модельные задачи выполнены.",
                        "used_evidence_ids": [],
                        "display_evidence_ids": [],
                    },
                    "upstream-model-owned",
                )
            ],
        }
    )
    worker_results = [
        _outcome(
            "Первый факт.",
            previous_results=[
                PreviousResultReference(
                    result_id="result-first",
                    description="first_lookup: первый факт.",
                )
            ],
        ),
        _outcome(
            "Второй факт.",
            previous_results=[
                PreviousResultReference(
                    result_id="result-second",
                    description="second_lookup: второй факт.",
                )
            ],
        ),
        _outcome("Финальный факт."),
    ]
    model_patch, callback_patch, trace_patch = _patches(model)
    with (
        model_patch,
        callback_patch,
        trace_patch,
        patch(
            "agents.coordinator.worker_chat",
            side_effect=worker_results,
        ) as worker,
    ):
        result = coordinator_chat(original_task)

    assert result.answer == "Модельные задачи выполнены."
    assert worker.call_count == 3
    plan_messages = [
        messages
        for name, messages in model.messages
        if name == "submit_worker_plan"
    ]
    assert len(plan_messages) == 1
    first, second, third = [
        parse_worker_request(call.args[0]) for call in worker.call_args_list
    ]
    assert [part.original_task for part in (first, second, third)] == [
        original_task,
        original_task,
        original_task,
    ]
    assert first.current_task == (
        "Перечислить все таблицы source_np и target_np для file_id=1."
    )
    assert first.previous_results is None
    assert second.current_task == (
        "Проверь найденные таблицы, не повторяя исходную пару."
    )
    assert [
        item.result_id for item in (second.previous_results or [])
    ] == ["result-first"]
    assert third.current_task == "Прочитай произвольный финальный факт."
    assert [
        item.result_id for item in (third.previous_results or [])
    ] == ["result-first", "result-second"]


def test_coordinator_does_not_semantically_reparse_agentic_reroute_plan():
    from agents.coordinator import coordinator_chat

    # This explicitly asks for a factual uniqueness check, so catalog metadata
    # is a real requirement.  Pure conditional-cardinality requests are
    # intentionally kept to one exact mapping step by the initial plan guard.
    original_task = (
        "Проверь фактическую уникальность ключей и оцени риск дубликатов "
        "для src_orders → tgt_orders."
    )
    model = _CoordinatorModel(
        {
            "select_operation_skills": [
                _tool_message(
                    "select_operation_skills",
                    {
                        "pipeline": "agentic",
                        "skills": ["Анализ SQL-рисков"],
                        "sql_risk_aspects": ["cardinality"],
                    },
                    "operation-risk",
                )
            ],
            "submit_worker_plan": [
                _tool_message(
                    "submit_worker_plan",
                    {
                        "steps": [
                            {
                                "task": (
                                    "Прочитать полный S2T mapping "
                                    "src_orders → tgt_orders."
                                )
                            }
                        ]
                    },
                    "plan-cycle-1",
                ),
                _tool_message(
                    "submit_worker_plan",
                    {
                        "steps": [
                            {
                                "task": (
                                    "Прочитать metadata src_orders и "
                                    "tgt_orders для проверки ключей."
                                )
                            }
                        ]
                    },
                    "plan-cycle-2-invalid",
                ),
            ],
            "submit_upstream_output": [
                _tool_message(
                    "submit_upstream_output",
                    {
                        "action": "request_more_data",
                        "problem": "Нужны metadata ключей обеих таблиц.",
                    },
                    "reroute-risk",
                ),
                _tool_message(
                    "submit_upstream_output",
                    {
                        "answer": "Риск размножения строк условный.",
                        "used_evidence_ids": [],
                        "display_evidence_ids": [],
                    },
                    "answer-risk",
                ),
            ],
        }
    )
    model_patch, callback_patch, trace_patch = _patches(model)
    with (
        model_patch,
        callback_patch,
        trace_patch,
        patch(
            "agents.coordinator.worker_chat",
            side_effect=[
                _outcome("Mapping прочитан."),
                _outcome("Metadata прочитаны."),
            ],
        ) as worker,
    ):
        result = coordinator_chat(original_task)

    assert result.answer == "Риск размножения строк условный."
    assert worker.call_count == 2
    assert "metadata src_orders и tgt_orders" in parse_worker_request(
        worker.call_args_list[1].args[0]
    ).current_task
    plan_messages = [
        messages
        for name, messages in model.messages
        if name == "submit_worker_plan"
    ]
    assert len(plan_messages) == 2


def test_coordinator_accepts_agentic_reroute_plan_without_prose_requirements():
    from agents.coordinator import coordinator_chat

    original_task = "Оцени риск дубликатов для src_orders → tgt_orders."
    metadata_only = {
        "steps": [
            {
                "task": (
                    "Прочитать metadata src_orders и tgt_orders для "
                    "проверки ключей."
                )
            }
        ]
    }
    model = _CoordinatorModel(
        {
            "select_operation_skills": [
                _tool_message(
                    "select_operation_skills",
                    {
                        "pipeline": "agentic",
                        "skills": ["Анализ SQL-рисков"],
                        "sql_risk_aspects": ["cardinality"],
                    },
                    "operation-risk",
                )
            ],
            "submit_worker_plan": [
                _tool_message(
                    "submit_worker_plan",
                    {
                        "steps": [
                            {
                                "task": (
                                    "Прочитать S2T mapping "
                                    "src_orders → tgt_orders."
                                )
                            }
                        ]
                    },
                    "plan-cycle-1",
                ),
                _tool_message(
                    "submit_worker_plan",
                    metadata_only,
                    "plan-cycle-2-invalid",
                ),
            ],
            "submit_upstream_output": [
                _tool_message(
                    "submit_upstream_output",
                    {
                        "action": "request_more_data",
                        "problem": "Нужны metadata ключей обеих таблиц.",
                    },
                    "reroute-risk",
                ),
                _tool_message(
                    "submit_upstream_output",
                    {
                        "answer": "Metadata ключей прочитаны.",
                        "used_evidence_ids": [],
                        "display_evidence_ids": [],
                    },
                    "answer-risk",
                ),
            ],
        }
    )
    model_patch, callback_patch, trace_patch = _patches(model)
    with (
        model_patch,
        callback_patch,
        trace_patch,
        patch(
            "agents.coordinator.worker_chat",
            side_effect=[
                _outcome("Mapping прочитан."),
                _outcome("Metadata ключей прочитаны."),
            ],
        ) as worker,
    ):
        result = coordinator_chat(original_task)

    assert result.answer == "Metadata ключей прочитаны."
    assert worker.call_count == 2
    plan_messages = [
        messages
        for name, messages in model.messages
        if name == "submit_worker_plan"
    ]
    assert len(plan_messages) == 2


def test_coordinator_uses_generated_task_without_semantic_checks():
    from agents.coordinator import coordinator_chat

    model = _CoordinatorModel(
        {
            "submit_worker_plan": [
                _tool_message(
                    "submit_worker_plan",
                    {"steps": [{"task": "Получи факт напрямую."}]},
                    "plan-shortened",
                )
            ],
            "submit_upstream_output": _responses(
                answer="Факт получен."
            )["submit_upstream_output"],
        }
    )
    model_patch, callback_patch, trace_patch = _patches(model)
    with (
        model_patch,
        callback_patch,
        trace_patch,
        patch(
            "agents.coordinator.worker_chat",
            return_value=_outcome("Факт получен."),
        ) as worker,
    ):
        result = coordinator_chat("Получи факт.")

    assert result == CoordinatorAnswer(answer="Факт получен.", display_refs=[])
    worker.assert_called_once()
    worker_parts = parse_worker_request(worker.call_args.args[0])
    assert worker_parts.current_task == "Получи факт напрямую."
    assert worker_parts.original_task == "Получи факт."
    plan_calls = [
        messages
        for name, messages in model.messages
        if name == "submit_worker_plan"
    ]
    assert len(plan_calls) == 1
    assert all(name != "dispatch_worker" for name, _ in model.messages)


def test_scope_evidence_experiment_off_preserves_worker_call_and_answer(monkeypatch):
    from agents.coordinator import coordinator_chat
    from agents.sql_risk_scope_contract import (
        OPERATION_SQL_RISK_SCOPE_EVIDENCE_EXPERIMENT_ENV,
    )
    from agents.tools.context import OPERATION_SQL_RISK_ASPECTS_EXPERIMENT_ENV

    monkeypatch.setenv(OPERATION_SQL_RISK_ASPECTS_EXPERIMENT_ENV, "1")
    monkeypatch.setenv(OPERATION_SQL_RISK_SCOPE_EVIDENCE_EXPERIMENT_ENV, "0")
    model = _CoordinatorModel(
        {
            "select_operation_skills": [
                _tool_message(
                    "select_operation_skills",
                    {
                        "pipeline": "agentic",
                        "skills": ["Анализ SQL-рисков"],
                        "sql_risk_aspects": ["row_filtering"],
                    },
                    "operation-row-filtering",
                )
            ],
            "submit_worker_plan": [
                _tool_message(
                    "submit_worker_plan",
                    {
                        "steps": [
                            {
                                "task": (
                                    "Read the exact directed S2T mapping "
                                    "src_alpha → tgt_beta."
                                )
                            }
                        ]
                    },
                    "plan-row-filtering",
                )
            ],
            "submit_upstream_data_decision": [
                _tool_message(
                    "submit_upstream_data_decision",
                    {"decision": "pass"},
                    "decision-pass",
                )
            ],
            "submit_upstream_answer": [
                _tool_message(
                    "submit_upstream_answer",
                    {
                        "answer": "A WHERE predicate can remove rows.",
                        "used_evidence_ids": ["evidence-mapping"],
                        "display_evidence_ids": [],
                    },
                    "answer-row-filtering",
                )
            ],
        }
    )
    worker_result = _outcome(
        "Mapping read.",
        evidence=[
            _artifact(
                None,
                "read_s2t_source_to_target",
                '{"rows":[{"transformation_rule":"WHERE active"}]}',
                evidence_id="evidence-mapping",
                compact_args={
                    "source_table": "src_alpha",
                    "target_table": "tgt_beta",
                },
            )
        ],
    )
    model_patch, callback_patch, trace_patch = _patches(model)
    with (
        model_patch,
        callback_patch,
        trace_patch,
        patch(
            "agents.coordinator.worker_chat",
            return_value=worker_result,
        ) as worker,
    ):
        result = coordinator_chat(
            "Assess row filtering for src_alpha → tgt_beta."
        )

    assert result.answer == "A WHERE predicate can remove rows."
    assert "Scope:" not in result.answer
    assert worker.call_count == 1
    assert worker.call_args.kwargs == {}


@pytest.mark.parametrize("configured", ["unknown-architecture", "operation_scope"])
def test_non_binary_scope_flag_fails_before_operation_routing(
    monkeypatch,
    configured,
):
    from agents.coordinator import coordinator_chat
    from agents.sql_risk_scope_contract import (
        OPERATION_SQL_RISK_SCOPE_EVIDENCE_EXPERIMENT_ENV,
    )

    monkeypatch.setenv(
        OPERATION_SQL_RISK_SCOPE_EVIDENCE_EXPERIMENT_ENV,
        configured,
    )
    model = _CoordinatorModel({})
    model_patch, callback_patch, trace_patch = _patches(model)
    with (
        model_patch,
        callback_patch,
        trace_patch,
        patch("agents.coordinator.worker_chat") as worker,
        pytest.raises(
            ValueError,
            match="must be 0 or 1",
        ),
    ):
        coordinator_chat("Прочитай сохранённый факт.")

    worker.assert_not_called()
    assert model.messages == []


def test_operation_scope_cardinality_bypasses_worker_plan_and_upstream_llm(
    monkeypatch,
):
    from agents.coordinator import coordinator_chat
    from agents.sql_risk_scope_contract import (
        OPERATION_SQL_RISK_SCOPE_EVIDENCE_EXPERIMENT_ENV,
    )
    from agents.tools.context import OPERATION_SQL_RISK_ASPECTS_EXPERIMENT_ENV
    from agents.tools.saved_results import get_active_saved_result_store

    monkeypatch.setenv(OPERATION_SQL_RISK_ASPECTS_EXPERIMENT_ENV, "1")
    monkeypatch.setenv(
        OPERATION_SQL_RISK_SCOPE_EVIDENCE_EXPERIMENT_ENV,
        "1",
    )
    original_task = (
        "Оцени риск появления дубликатов при сохранённой S2T-трансформации "
        "src_np → tgt_np. Назови фактический JOIN и явно отдели "
        "подтверждённый механизм от условия по уникальности."
    )
    model = _CoordinatorModel(
        {
            "select_operation_skills": [
                _tool_message(
                    "select_operation_skills",
                    {
                        "pipeline": "sql_risk_scope",
                        "skills": [],
                        "sql_risk_aspects": [],
                    },
                    "operation-cardinality",
                )
            ],
            "submit_sql_risk_scope": [
                _scope_extraction_message(
                    execution_mode="conditional_cardinality",
                    source_table="src_np",
                    target_table="tgt_np",
                )
            ],
            "submit_sql_risk_assessment": [
                _scope_assessment_message(
                    "Scope: src_np → tgt_np. JOIN с aux_np по d.id = s.id "
                    "создаёт условный риск размножения строк; уникальность "
                    "правой стороны не подтверждена."
                )
            ],
        }
    )

    mapping_calls = []

    def mapping_reader(**arguments):
        mapping_calls.append(arguments)
        return {
            "rows": [
                {
                    "source_table": "src_np",
                    "source_field": "id",
                    "target_table": "tgt_np",
                    "target_field": "id",
                    "transformation_rule": (
                        "SELECT s.id AS id, "
                        "COALESCE(s.value, d.value) AS value "
                        "FROM src_np AS s JOIN aux_np AS d "
                        "ON d.id = s.id WHERE s.ok = TRUE"
                    ),
                }
            ],
            "total_matches": 1,
            "returned_rows": 1,
            "truncated": False,
        }
    model_patch, callback_patch, trace_patch = _patches(model)
    with (
        model_patch,
        callback_patch,
        trace_patch,
        patch(
            "agents.sql_risk_operation_pipeline._DEFAULT_READERS",
            {"read_s2t_source_to_target": mapping_reader},
        ),
        patch(
            "agents.coordinator.worker_chat",
        ) as worker,
        patch(
            "agents.coordinator.register_worker_display_items",
            return_value=["display-cardinality"],
        ) as register_display,
        patch("agents.coordinator.record_coordinator_plan") as record_plan,
        patch("agents.coordinator.record_upstream_output") as record_output,
        patch("agents.coordinator.record_sql_risk_operation") as record_scope,
    ):
        result = coordinator_chat(
            original_task,
            context=(
                "В этой беседе под риском понимается только размножение строк."
            ),
        )

    assert "src_np → tgt_np" in result.answer
    folded_answer = result.answer.casefold()
    assert "join" in folded_answer
    assert "aux_np" in folded_answer
    assert re.search(r"d\.id\s*=\s*s\.id", folded_answer), result.answer
    assert re.search(
        r"уникальн.{0,150}(?:неизвест|не подтвержд|не установ)",
        folded_answer,
    ), result.answer
    assert "where" not in folded_answer
    assert "coalesce" not in folded_answer
    assert result.display_refs == ["display-cardinality"]
    assert all(
        name
        not in {
            "submit_worker_plan",
            "submit_upstream_data_decision",
            "submit_upstream_answer",
        }
        for name, _ in model.messages
    )
    assert [name for name, _ in model.messages] == [
        "select_operation_skills",
        "submit_sql_risk_scope",
        "submit_sql_risk_assessment",
    ]
    extraction_payload = _payload(model, "submit_sql_risk_scope")
    assert extraction_payload["stable_context"] == (
        "В этой беседе под риском понимается только размножение строк."
    )
    assessment_messages = next(
        messages
        for name, messages in model.messages
        if name == "submit_sql_risk_assessment"
    )
    assessment_payload = json.loads(
        str(assessment_messages[1].content).removeprefix("ASSESSMENT_INPUT:\n")
    )
    assert assessment_payload["user_request"]["stable_context"] == (
        "В этой беседе под риском понимается только размножение строк."
    )
    assert mapping_calls == [
        {"source_table": "src_np", "target_table": "tgt_np"}
    ]
    worker.assert_not_called()
    record_plan.assert_not_called()
    register_display.assert_called_once()
    recorded_output = record_output.call_args.args[0]
    assert recorded_output["pipeline"] == "sql_risk_scope"
    assert recorded_output["status"] == "complete"
    assert recorded_output["answer_source"] == "sql_risk_scope_llm"
    assert len(recorded_output["used_evidence_ids"]) == 1
    assert recorded_output["display_evidence_ids"] == recorded_output[
        "used_evidence_ids"
    ]
    scope_trace = record_scope.call_args.args[0]
    assert scope_trace["pipeline"] == "sql_risk_scope"
    assert scope_trace["status"] == "complete"
    assert scope_trace["silent_fallback"] is False


def test_operation_scope_nullable_reads_exact_roles_without_worker_observer(
    monkeypatch,
):
    from agents.coordinator import coordinator_chat
    from agents.sql_risk_scope_contract import (
        OPERATION_SQL_RISK_SCOPE_EVIDENCE_EXPERIMENT_ENV,
    )
    from agents.tools.context import OPERATION_SQL_RISK_ASPECTS_EXPERIMENT_ENV
    monkeypatch.setenv(OPERATION_SQL_RISK_ASPECTS_EXPERIMENT_ENV, "1")
    monkeypatch.setenv(
        OPERATION_SQL_RISK_SCOPE_EVIDENCE_EXPERIMENT_ENV,
        "1",
    )
    original_task = (
        "Для file_id=17 оцени только SQL-риск constraint rejection из-за "
        "nullable-ограничений src_alpha.code → tgt_beta.code. Верни "
        "source_not_null=<0|1>, target_not_null=<0|1> и вывод."
    )
    model = _CoordinatorModel(
        {
            "select_operation_skills": [
                _tool_message(
                    "select_operation_skills",
                    {
                        "pipeline": "sql_risk_scope",
                        "skills": [],
                        "sql_risk_aspects": [],
                    },
                    "operation-constraint",
                )
            ],
            "submit_sql_risk_scope": [
                _scope_extraction_message(
                    execution_mode="nullable_constraint",
                    source_table="src_alpha",
                    source_field="code",
                    target_table="tgt_beta",
                    target_field="code",
                    file_id=17,
                    file_attestation="17",
                )
            ],
            "submit_sql_risk_assessment": [
                _scope_assessment_message(
                    "Scope src_alpha.code → tgt_beta.code: "
                    "source_not_null=0, target_not_null=1; NULL из source "
                    "может быть отклонён target constraint.",
                    display=False,
                )
            ],
        }
    )

    mapping_calls = []
    metadata_calls = []

    def mapping_reader(**arguments):
        mapping_calls.append(arguments)
        return {
            "rows": [
                {
                    "source_table": "src_alpha",
                    "source_field": "code",
                    "target_table": "tgt_beta",
                    "target_field": "code",
                    "transformation_rule": "src_alpha.code",
                }
            ],
            "total_matches": 1,
            "returned_rows": 1,
            "truncated": False,
        }

    def metadata_reader(**arguments):
        metadata_calls.append(arguments)
        return {
            "rows": [
                {
                    "column_role": "source",
                    "file_id": 17,
                    "table_name": "src_alpha",
                    "column_name": "code",
                    "not_null": 0,
                },
                {
                    "column_role": "target",
                    "file_id": 17,
                    "table_name": "tgt_beta",
                    "column_name": "code",
                    "not_null": 1,
                },
            ],
            "total_matches": 2,
            "returned_rows": 2,
            "truncated": False,
        }

    model_patch, callback_patch, trace_patch = _patches(model)
    with (
        model_patch,
        callback_patch,
        trace_patch,
        patch(
            "agents.sql_risk_operation_pipeline._DEFAULT_READERS",
            {
                "list_s2t_field_mapping": mapping_reader,
                "get_source_target_column_pair": metadata_reader,
            },
        ),
        patch(
            "agents.coordinator.worker_chat",
        ) as worker,
        patch("agents.coordinator.record_coordinator_plan") as record_plan,
        patch("agents.coordinator.record_upstream_output") as record_output,
        patch("agents.coordinator.record_sql_risk_operation") as record_scope,
    ):
        result = coordinator_chat(original_task)

    assert re.search(r"(?i)source_not_null\s*[:=]\s*`?0`?", result.answer)
    assert re.search(r"(?i)target_not_null\s*[:=]\s*`?1`?", result.answer)
    assert "src_alpha.code → tgt_beta.code" in result.answer
    assert result.display_refs == []
    assert mapping_calls == [
        {
            "source_table": "src_alpha",
            "source_field": "code",
            "target_table": "tgt_beta",
            "target_field": "code",
        }
    ]
    assert metadata_calls == [
        {
            "file_id": 17,
            "source_table": "src_alpha",
            "source_column": "code",
            "target_table": "tgt_beta",
            "target_column": "code",
        }
    ]
    worker.assert_not_called()
    assert all(
        name
        not in {
            "submit_worker_plan",
            "submit_upstream_data_decision",
            "submit_upstream_answer",
        }
        for name, _ in model.messages
    )
    record_plan.assert_not_called()
    recorded_output = record_output.call_args.args[0]
    assert recorded_output["pipeline"] == "sql_risk_scope"
    assert recorded_output["status"] == "complete"
    assert recorded_output["answer_source"] == "sql_risk_scope_llm"
    assert len(recorded_output["used_evidence_ids"]) == 2
    scope_trace = record_scope.call_args.args[0]
    assert scope_trace["pipeline"] == "sql_risk_scope"
    assert scope_trace["status"] == "complete"
    assert scope_trace["silent_fallback"] is False
    assert [read["tool_name"] for read in scope_trace["reads"]] == [
        "list_s2t_field_mapping",
        "get_source_target_column_pair",
    ]


def test_operation_scope_router_can_keep_ineligible_request_agentic(monkeypatch):
    from agents.coordinator import coordinator_chat
    from agents.sql_risk_scope_contract import (
        OPERATION_SQL_RISK_SCOPE_EVIDENCE_EXPERIMENT_ENV,
    )
    from agents.tools.context import OPERATION_SQL_RISK_ASPECTS_EXPERIMENT_ENV

    monkeypatch.setenv(OPERATION_SQL_RISK_ASPECTS_EXPERIMENT_ENV, "1")
    monkeypatch.setenv(
        OPERATION_SQL_RISK_SCOPE_EVIDENCE_EXPERIMENT_ENV,
        "1",
    )
    original_task = "Assess row filtering for src_alpha → tgt_beta."
    model = _CoordinatorModel(
        {
            "select_operation_skills": [
                _tool_message(
                    "select_operation_skills",
                    {
                        "pipeline": "agentic",
                        "skills": ["Анализ SQL-рисков"],
                        "sql_risk_aspects": ["row_filtering"],
                    },
                    "operation-row-filtering",
                )
            ],
            "submit_worker_plan": [
                _tool_message(
                    "submit_worker_plan",
                    {
                        "steps": [
                            {
                                "task": (
                                    "Read baseline planned facts for "
                                    "src_alpha → tgt_beta."
                                )
                            }
                        ]
                    },
                    "plan-row-filtering",
                )
            ],
            "submit_upstream_data_decision": [
                _tool_message(
                    "submit_upstream_data_decision",
                    {"decision": "pass"},
                    "decision-row-filtering",
                )
            ],
            "submit_upstream_answer": [
                _tool_message(
                    "submit_upstream_answer",
                    {
                        "answer": "A WHERE predicate can remove rows.",
                        "used_evidence_ids": ["evidence-row-filtering"],
                        "display_evidence_ids": [],
                    },
                    "answer-row-filtering",
                )
            ],
        }
    )
    worker_result = _outcome(
        "Row filtering fact read.",
        evidence=[
            _artifact(
                None,
                "read_s2t_source_to_target",
                '{"rows":[{"transformation_rule":"WHERE active"}]}',
                evidence_id="evidence-row-filtering",
                compact_args={
                    "source_table": "src_alpha",
                    "target_table": "tgt_beta",
                },
            )
        ],
    )
    model_patch, callback_patch, trace_patch = _patches(model)
    with (
        model_patch,
        callback_patch,
        trace_patch,
        patch(
            "agents.coordinator.worker_chat",
            return_value=worker_result,
        ) as worker,
        patch("agents.coordinator.record_coordinator_plan") as record_plan,
    ):
        result = coordinator_chat(original_task)

    assert result.answer == "A WHERE predicate can remove rows."
    assert [name for name, _ in model.messages].count(
        "submit_worker_plan"
    ) == 1
    worker.assert_called_once()
    assert parse_worker_request(worker.call_args.args[0]).current_task == (
        "Read baseline planned facts for src_alpha → tgt_beta."
    )
    assert worker.call_args.kwargs == {}
    recorded_steps = record_plan.call_args.args[0]
    assert len(recorded_steps) == 1
    assert "plan_source" not in recorded_steps[0]
    assert "operation_sql_risk_scope_contract" not in recorded_steps[0]


def test_operation_scope_missing_evidence_returns_structured_unavailable_once(
    monkeypatch,
):
    from agents.coordinator import coordinator_chat
    from agents.sql_risk_scope_contract import (
        OPERATION_SQL_RISK_SCOPE_EVIDENCE_EXPERIMENT_ENV,
    )
    from agents.tools.context import OPERATION_SQL_RISK_ASPECTS_EXPERIMENT_ENV
    monkeypatch.setenv(OPERATION_SQL_RISK_ASPECTS_EXPERIMENT_ENV, "1")
    monkeypatch.setenv(
        OPERATION_SQL_RISK_SCOPE_EVIDENCE_EXPERIMENT_ENV,
        "1",
    )
    original_task = (
        "Оцени риск появления дубликатов при сохранённой S2T-трансформации "
        "src_alpha → tgt_beta. Назови фактический JOIN и явно отдели "
        "подтверждённый механизм от условия по уникальности."
    )
    model = _CoordinatorModel(
        {
            "select_operation_skills": [
                _tool_message(
                    "select_operation_skills",
                    {
                        "pipeline": "sql_risk_scope",
                        "skills": [],
                        "sql_risk_aspects": [],
                    },
                    "operation-cardinality",
                )
            ],
            "submit_sql_risk_scope": [
                _scope_extraction_message(
                    execution_mode="conditional_cardinality",
                    source_table="src_alpha",
                    target_table="tgt_beta",
                )
            ],
        }
    )
    mapping_calls = []

    def mapping_reader(**arguments):
        mapping_calls.append(arguments)
        return {
            "columns": [
                "source_table",
                "target_table",
                "transformation_rule",
            ],
            "rows": [],
            "total_matches": 0,
            "returned_rows": 0,
            "truncated": False,
        }
    model_patch, callback_patch, trace_patch = _patches(model)
    with (
        model_patch,
        callback_patch,
        trace_patch,
        patch(
            "agents.sql_risk_operation_pipeline._DEFAULT_READERS",
            {"read_s2t_source_to_target": mapping_reader},
        ),
        patch(
            "agents.coordinator.worker_chat",
        ) as worker,
        patch("agents.coordinator.record_coordinator_plan") as record_plan,
        patch("agents.coordinator.record_upstream_output") as record_output,
        patch("agents.coordinator.record_sql_risk_operation") as record_scope,
    ):
        result = coordinator_chat(original_task)

    assert mapping_calls == [
        {"source_table": "src_alpha", "target_table": "tgt_beta"}
    ]
    worker.assert_not_called()
    assert "src_alpha → tgt_beta" in result.answer
    assert "недоступна" in result.answer.casefold()
    assert "подтверждённый вывод" in result.answer.casefold()
    assert all(
        name
        not in {
            "submit_worker_plan",
            "submit_upstream_data_decision",
            "submit_upstream_answer",
        }
        for name, _ in model.messages
    )
    record_plan.assert_not_called()
    assert record_output.call_args.args[0]["answer_source"] == (
        "sql_risk_scope_unavailable"
    )
    assert record_output.call_args.args[0]["pipeline"] == "sql_risk_scope"
    assert record_output.call_args.args[0]["status"] == "unavailable"
    scope_trace = record_scope.call_args.args[0]
    assert scope_trace["pipeline"] == "sql_risk_scope"
    assert scope_trace["status"] == "unavailable"
    assert scope_trace["silent_fallback"] is False


def test_operation_scope_invalid_model_extraction_fails_without_fallback(
    monkeypatch,
):
    from agents.coordinator import coordinator_chat
    from agents.sql_risk_scope_contract import (
        OPERATION_SQL_RISK_SCOPE_EVIDENCE_EXPERIMENT_ENV,
    )

    monkeypatch.setenv(
        OPERATION_SQL_RISK_SCOPE_EVIDENCE_EXPERIMENT_ENV,
        "1",
    )
    invalid = _scope_extraction_message(
        execution_mode="conditional_cardinality",
        source_table="invented_source",
        target_table="invented_target",
    )
    invalid_again = invalid.model_copy(deep=True)
    invalid_again.tool_calls[0]["id"] = "scope-extraction-2"
    model = _CoordinatorModel(
        {
            "select_operation_skills": [
                _tool_message(
                    "select_operation_skills",
                    {
                        "pipeline": "sql_risk_scope",
                        "skills": [],
                        "sql_risk_aspects": [],
                    },
                    "scope-route",
                )
            ],
            "submit_sql_risk_scope": [invalid, invalid_again],
        }
    )
    mapping_reader = MagicMock()
    model_patch, callback_patch, trace_patch = _patches(model)
    with (
        model_patch,
        callback_patch,
        trace_patch,
        patch(
            "agents.sql_risk_operation_pipeline._DEFAULT_READERS",
            {"read_s2t_source_to_target": mapping_reader},
        ),
        patch("agents.coordinator.worker_chat") as worker,
        patch("agents.coordinator.record_upstream_output") as record_output,
        patch("agents.coordinator.record_sql_risk_operation") as record_scope,
    ):
        result = coordinator_chat(
            "Assess cardinality for actual_source → actual_target."
        )

    assert "недоступна" in result.answer.casefold()
    assert "fallback" in result.answer.casefold()
    assert [name for name, _ in model.messages] == [
        "select_operation_skills",
        "submit_sql_risk_scope",
        "submit_sql_risk_scope",
    ]
    mapping_reader.assert_not_called()
    worker.assert_not_called()
    recorded = record_output.call_args.args[0]
    assert recorded["pipeline"] == "sql_risk_scope"
    assert recorded["status"] == "unavailable"
    assert recorded["answer_source"] == "sql_risk_scope_unavailable"
    trace = record_scope.call_args.args[0]
    assert trace["status"] == "unavailable"
    assert trace["execution_mode"] == "unresolved"
    assert trace["silent_fallback"] is False


def test_operation_scope_extraction_transport_failure_is_structured_unavailable(
    monkeypatch,
):
    from agents.coordinator import coordinator_chat
    from agents.sql_risk_scope_contract import (
        OPERATION_SQL_RISK_SCOPE_EVIDENCE_EXPERIMENT_ENV,
    )

    monkeypatch.setenv(
        OPERATION_SQL_RISK_SCOPE_EVIDENCE_EXPERIMENT_ENV,
        "1",
    )

    def fail_scope_extraction(_messages):
        raise RuntimeError("provider unavailable")

    model = _CoordinatorModel(
        {
            "select_operation_skills": [
                _tool_message(
                    "select_operation_skills",
                    {
                        "pipeline": "sql_risk_scope",
                        "skills": [],
                        "sql_risk_aspects": [],
                    },
                    "scope-route",
                )
            ],
            "submit_sql_risk_scope": [fail_scope_extraction],
        }
    )
    mapping_reader = MagicMock()
    model_patch, callback_patch, trace_patch = _patches(model)
    with (
        model_patch,
        callback_patch,
        trace_patch,
        patch(
            "agents.sql_risk_operation_pipeline._DEFAULT_READERS",
            {"read_s2t_source_to_target": mapping_reader},
        ),
        patch("agents.coordinator.worker_chat") as worker,
        patch("agents.coordinator.record_upstream_output") as record_output,
        patch("agents.coordinator.record_sql_risk_operation") as record_scope,
    ):
        result = coordinator_chat(
            "Assess cardinality for actual_source to actual_target."
        )

    assert "недоступна" in result.answer.casefold()
    assert "fallback" in result.answer.casefold()
    assert [name for name, _ in model.messages] == [
        "select_operation_skills",
        "submit_sql_risk_scope",
    ]
    mapping_reader.assert_not_called()
    worker.assert_not_called()
    recorded = record_output.call_args.args[0]
    assert recorded["pipeline"] == "sql_risk_scope"
    assert recorded["status"] == "unavailable"
    assert recorded["answer_source"] == "sql_risk_scope_unavailable"
    trace = record_scope.call_args.args[0]
    assert trace["status"] == "unavailable"
    assert trace["execution_mode"] == "unresolved"
    assert trace["silent_fallback"] is False
