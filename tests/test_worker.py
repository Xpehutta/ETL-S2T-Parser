import json
from contextlib import nullcontext
from unittest.mock import patch

import pytest
from langchain_core.messages import AIMessage, ToolMessage
from langchain_core.tools import StructuredTool

from agents.chat_graph import (
    Observation as ObservationContract,
    WorkerCycleTrace,
    WorkerDisplayItem,
    WorkerResponseError,
    WorkerRunResult as WorkerRunResultContract,
    run_worker_graph,
)
from agents.tools import get_worker_tools, load_schemas, load_skills
from agents.tools.routing import ToolRoute


def Observation(
    *,
    summary="",
    goal_satisfied=True,
    problem=None,
    accepted_tool_call_ids=None,
    important_facts=None,
    limitations=None,
    reroute_required=False,
    status=None,
    gap=None,
    facts=None,
    reroute_reason=None,
    required_capabilities=None,
):
    """Build the new Observation contract from concise test fixtures."""
    selected_status = status or (
        "complete"
        if goal_satisfied
        else "reroute" if reroute_required else "continue"
    )
    selected_gap = gap if gap is not None else problem
    selected_facts = facts
    if selected_facts is None:
        selected_facts = [
            {"text": text, "evidence_ids": []}
            for text in (important_facts or [])
        ]
        if not selected_facts and selected_status == "complete" and summary:
            selected_facts = [{"text": summary, "evidence_ids": []}]
    return ObservationContract(
        status=selected_status,
        gap=selected_gap,
        accepted_tool_call_ids=list(accepted_tool_call_ids or []),
        facts=selected_facts,
        limitations=list(limitations or []),
        reroute_reason=reroute_reason,
        required_capabilities=list(required_capabilities or []),
    )


def WorkerRunResult(
    *,
    answer,
    display_items=None,
    cycle_history=None,
    goal_satisfied=True,
    problem=None,
    reroute_required=False,
    status=None,
    gap=None,
    facts=None,
    accepted_tool_call_ids=None,
    stop_reason=None,
    reroute_reason=None,
    required_capabilities=None,
    unmet_requirements=None,
):
    selected_status = status or (
        "complete"
        if goal_satisfied or not reroute_required
        else "reroute"
    )
    selected_gap = gap if gap is not None else problem
    return WorkerRunResultContract(
        answer=answer,
        display_items=list(display_items or []),
        cycle_history=list(cycle_history or []),
        status=selected_status,
        gap=selected_gap,
        facts=list(facts or []),
        accepted_tool_call_ids=list(accepted_tool_call_ids or []),
        stop_reason=stop_reason,
        reroute_reason=reroute_reason,
        required_capabilities=list(required_capabilities or []),
        unmet_requirements=list(unmet_requirements or []),
    )


def _as_tool(function, name=None):
    tool_name = name or function.__name__
    return StructuredTool.from_function(
        func=function,
        name=tool_name,
        description=f"Test tool {tool_name}",
    )


def _finish_message(summary, *, extra_args=None):
    args = {"summary": summary}
    args.update(extra_args or {})
    return AIMessage(
        content="",
        tool_calls=[
            {
                "name": "finish_worker",
                "args": args,
                "id": "finish-1",
                "type": "tool_call",
            }
        ],
    )


def test_worker_prompt_does_not_require_full_table_in_text():
    from agents.chat_graph import _WORKER_PLANNER_PROMPT

    normalized_prompt = " ".join(_WORKER_PLANNER_PROMPT.split())
    assert "не копируй результаты" in normalized_prompt
    assert "краткую внутреннюю отметку" in normalized_prompt
    assert "не формулируй финальный ответ" in normalized_prompt
    assert "для upstream coordinator" not in normalized_prompt
    assert "не переименовывай заданную операцию" in _WORKER_PLANNER_PROMPT
    assert "Не конструируй отсутствующий объект" in _WORKER_PLANNER_PROMPT
    assert "аргументы бери из" in _WORKER_PLANNER_PROMPT
    assert "Если обязательного входа нет, этот tool не подходит" in (
        normalized_prompt
    )
    assert "Палитра worker никогда не пуста" in _WORKER_PLANNER_PROMPT
    assert "внутренний `analyze_known_facts`" in _WORKER_PLANNER_PROMPT
    assert "действие `analyze`" not in _WORKER_PLANNER_PROMPT
    assert "Обычный текст без tool_calls\nзапрещён" in _WORKER_PLANNER_PROMPT
    assert "`finish_worker` разрешён на любом шаге" in _WORKER_PLANNER_PROMPT
    assert "`result_schema` рядом с description" in _WORKER_PLANNER_PROMPT
    assert "одним batch-вызовом" in _WORKER_PLANNER_PROMPT
    assert "не вызывай `read_previous_result` повторно" in (
        _WORKER_PLANNER_PROMPT
    )
    assert "scrollable" not in _WORKER_PLANNER_PROMPT


class _ObserverModel:
    def __init__(self, responses=None):
        self.messages = []
        self.responses = list(responses or [])
        self.last_observation = None

    def invoke(self, messages):
        self.messages.append(messages)
        payload = next(
            json.loads(message.content)
            for message in messages
            if str(getattr(message, "content", "")).lstrip().startswith("{")
        )
        prior_ids = [
            tool_call_id
            for observation in payload.get("prior_state", [])
            for tool_call_id in observation.get("accepted_tool_call_ids", [])
        ]
        current_ids = [
            str(result.get("tool_call_id") or "")
            for result in payload.get("tool_results", [])
            if not result.get("is_error")
            and result.get("name") != "analyze_known_facts"
            and str(result.get("tool_call_id") or "")
        ]
        prior_facts = [
            fact
            for observation in payload.get("prior_state", [])
            for fact in observation.get("facts", [])
        ]
        current_evidence_ids = [
            str(result.get("evidence_id") or "")
            for result in payload.get("tool_results", [])
            if not result.get("is_error")
            and result.get("name") != "analyze_known_facts"
            and str(result.get("evidence_id") or "")
        ]
        response = (
            self.responses.pop(0)
            if self.responses
            else Observation(
                status="complete",
                accepted_tool_call_ids=list(
                    dict.fromkeys([*prior_ids, *current_ids])
                ),
                facts=[
                    *prior_facts,
                    *(
                        [
                            {
                                "text": "Превью результата получено.",
                                "evidence_ids": current_evidence_ids,
                            }
                        ]
                        if current_evidence_ids
                        else []
                    ),
                ],
            )
        )
        observation = (
            response
            if isinstance(response, ObservationContract)
            else ObservationContract.model_validate(response)
        )
        if observation.status == "complete" and not observation.accepted_tool_call_ids:
            observation = observation.model_copy(
                update={
                    "accepted_tool_call_ids": list(
                        dict.fromkeys([*prior_ids, *current_ids])
                    )
                }
            )
        accepted_evidence_ids = list(
            dict.fromkeys(
                [
                    str(result.get("evidence_id") or "")
                    for result in payload.get("tool_results", [])
                    if str(result.get("tool_call_id") or "")
                    in observation.accepted_tool_call_ids
                    and str(result.get("evidence_id") or "")
                ]
                + [
                    evidence_id
                    for fact in prior_facts
                    for evidence_id in fact.get("evidence_ids", [])
                ]
            )
        )
        if accepted_evidence_ids and any(
            not fact.evidence_ids for fact in observation.facts
        ):
            observation = observation.model_copy(
                update={
                    "facts": [
                        fact.model_copy(
                            update={"evidence_ids": accepted_evidence_ids}
                        )
                        if not fact.evidence_ids
                        else fact
                        for fact in observation.facts
                    ]
                }
            )
        self.last_observation = observation
        return observation


class _WorkerModel:
    def __init__(
        self,
        responses,
        *,
        observer_responses=None,
    ):
        self.responses = list(responses)
        self.bound_tools = []
        self.messages = []
        self.observer = _ObserverModel(observer_responses)
        self.structured_methods = []

    def bind_tools(self, tools):
        self.bound_tools = list(tools)
        return self

    def with_structured_output(self, schema, method=None):
        self.structured_methods.append((schema, method))
        if schema is ObservationContract:
            return self.observer
        raise AssertionError(f"Unexpected structured schema: {schema}")

    def invoke(self, messages, **kwargs):
        del kwargs
        if messages and "Ты observer многошагового агента" in str(
            messages[0].content
        ):
            return self.observer.invoke(messages)
        self.messages.append(messages)
        return self.responses.pop(0)


def test_worker_operation_contexts_are_isolated_by_role():
    from agents.contracts import (
        WorkerRequestParts,
    )

    def lookup():
        return {"value": 1}

    tool_call = AIMessage(
        content="",
        tool_calls=[
            {
                "name": "lookup",
                "args": {},
                "id": "lookup-role-context",
                "type": "tool_call",
            }
        ],
    )
    model = _WorkerModel([tool_call, _finish_message("Готово.")])
    planner_rule = "PLANNER_ONLY_EXACT_CALL"
    observer_rule = "OBSERVER_ONLY_ACCEPTANCE"

    run_worker_graph(
        task=WorkerRequestParts(
            current_task="Прочитай значение.",
            operation_execution_context=planner_rule,
            operation_completeness_context=observer_rule,
        ),
        system_prompt="Базовый prompt.",
        model=model,
        tools={"lookup": _as_tool(lookup)},
    )

    planner_systems = [str(messages[0].content) for messages in model.messages]
    assert planner_systems
    assert all(planner_rule in text for text in planner_systems)
    assert all(observer_rule not in text for text in planner_systems)
    planner_human_text = "\n".join(
        str(message.content)
        for messages in model.messages
        for message in messages[1:]
        if message.__class__.__name__ == "HumanMessage"
    )
    assert observer_rule not in planner_human_text

    observer_system = str(model.observer.messages[0][0].content)
    assert observer_rule in observer_system
    assert planner_rule not in observer_system


def test_worker_original_task_is_exact_and_separate_from_current_task():
    from agents.contracts import (
        WorkerRequestParts,
        parse_worker_request,
    )

    def lookup():
        return {"value": "confirmed"}

    current_task = "Прочитай назначенный S2T-срез."
    original_task = (
        "Для `source_stage_731.id` → `target_core_842.id` "
        "используй файл `Load Contract 917.xlsx`."
    )
    planner_rule = "PLANNER_ONLY_RULE_917"
    observer_rule = "OBSERVER_ONLY_RULE_842"
    request = WorkerRequestParts(
        current_task=current_task,
        original_task=original_task,
        operation_execution_context=planner_rule,
        operation_completeness_context=observer_rule,
    )
    model = _WorkerModel(
        [
            AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "lookup",
                        "args": {},
                        "id": "call-original-task-envelope",
                        "type": "tool_call",
                    }
                ],
            ),
            _finish_message("Готово."),
        ]
    )

    parts = parse_worker_request(request)
    result = run_worker_graph(
        task=request,
        system_prompt="Системный контекст",
        model=model,
        tools={"lookup": _as_tool(lookup)},
    )

    assert parts.current_task == current_task
    assert parts.original_task == original_task
    assert parts.operation_execution_context == planner_rule
    assert parts.operation_completeness_context == observer_rule
    assert result.status == "complete"

    for planner_messages in model.messages:
        human_messages = [
            message
            for message in planner_messages
            if message.__class__.__name__ == "HumanMessage"
        ]
        assert json.loads(human_messages[0].content) == {
            "original_task": original_task
        }
        assert human_messages[1].content == current_task
        assert original_task not in str(human_messages[1].content)

    observer_payload = json.loads(model.observer.messages[0][-1].content)
    assert observer_payload["user_request"] == current_task
    assert "original_task" not in observer_payload
    observer_system = str(model.observer.messages[0][0].content)
    assert "`user_request` (единственную текущую task)" in observer_system
    assert "соседнего шага не" in observer_system


def test_worker_presents_independent_endpoint_after_lazy_result_context():
    from agents.contracts import PreviousResultReference, WorkerRequestParts

    calls = []
    previous_table = "mart_alpha_731"
    current_table = "mart_beta_842"
    column_name = "status_code"
    current_task = (
        f"Построй полный upstream lineage `{current_table}.{column_name}` "
        "до конечных источников."
    )

    def trace_transformation_path(
        table_name: str,
        column_name: str,
        direction: str,
    ):
        calls.append((table_name, column_name, direction))
        return {
            "table_name": table_name,
            "column_name": column_name,
            "direction": direction,
            "returned_paths": 1,
            "text_diagram": f"raw_origin.{column_name} -> {table_name}.{column_name}",
        }

    model = _WorkerModel(
        [
            AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "trace_transformation_path",
                        "args": {
                            "table_name": current_table,
                            "column_name": column_name,
                            "direction": "upstream",
                        },
                        "id": "call-current-lineage",
                        "type": "tool_call",
                    }
                ],
            ),
            _finish_message("Текущий lineage прочитан."),
        ]
    )
    task = WorkerRequestParts(
        current_task=current_task,
        previous_results=[
            PreviousResultReference(
                result_id="result_previous_lineage",
                description=(
                    "trace_transformation_path: args="
                    + json.dumps(
                        {
                            "table_name": previous_table,
                            "column_name": column_name,
                            "direction": "upstream",
                        },
                        ensure_ascii=False,
                        separators=(",", ":"),
                    )
                ),
            )
        ],
    )

    result = run_worker_graph(
        task=task,
        system_prompt="Системный контекст",
        model=model,
        tools=(_as_tool(trace_transformation_path),),
    )

    assert result.status == "complete"
    assert calls == [(current_table, column_name, "upstream")]
    first_planner_humans = [
        message
        for message in model.messages[0]
        if message.__class__.__name__ == "HumanMessage"
    ]
    assert json.loads(first_planner_humans[0].content)["previous_results"][0][
        "result_id"
    ] == "result_previous_lineage"
    assert first_planner_humans[-1].content == current_task
    assert previous_table not in first_planner_humans[-1].content
    assert "Результаты прошлых workers" not in first_planner_humans[-1].content
    planner_system = str(model.messages[0][0].content)
    assert "Сами ссылки не делают текущую task зависимой" in planner_system
    assert "другого объекта" in planner_system


@pytest.mark.parametrize(
    "encoded",
    [
        "not-json",
        json.dumps({"original_task": 731}),
        json.dumps({"original_task": "Исходная.", "extra": "forbidden"}),
    ],
)
def test_explicit_legacy_worker_adapter_rejects_non_exact_json(encoded):
    from agents.contracts import (
        WORKER_ORIGINAL_TASK_MARKER,
        parse_legacy_worker_request,
    )

    with pytest.raises(ValueError, match="Legacy worker envelope"):
        parse_legacy_worker_request(
            "Прочитай назначенный срез."
            + WORKER_ORIGINAL_TASK_MARKER
            + encoded
        )


def test_explicit_legacy_worker_adapter_composes_all_suffixes():
    from agents.contracts import (
        WORKER_OPERATION_COMPLETENESS_MARKER,
        WORKER_OPERATION_EXECUTION_MARKER,
        WORKER_ORIGINAL_TASK_MARKER,
        WORKER_PREVIOUS_RESULTS_MARKER,
        parse_legacy_worker_request,
    )

    current_task = "Прочитай следующий зависимый срез."
    original_task = "Сопоставь `source_611.id` → `target_722.id`."
    parts = parse_legacy_worker_request(
        current_task
        + WORKER_ORIGINAL_TASK_MARKER
        + json.dumps({"original_task": original_task}, ensure_ascii=False)
        + WORKER_OPERATION_EXECUTION_MARKER
        + "EXECUTION_RULE"
        + WORKER_OPERATION_COMPLETENESS_MARKER
        + "COMPLETENESS_RULE"
        + WORKER_PREVIOUS_RESULTS_MARKER
        + "\n"
        + json.dumps(
            {
                "previous_results": [
                    {
                        "result_id": "result_611",
                        "description": "Предыдущий exact result.",
                    }
                ]
            },
            ensure_ascii=False,
        )
    )

    assert parts.current_task == current_task
    assert parts.original_task == original_task
    assert parts.operation_execution_context == "EXECUTION_RULE"
    assert parts.operation_completeness_context == "COMPLETENESS_RULE"
    assert parts.previous_results is not None
    assert [item.result_id for item in parts.previous_results] == ["result_611"]


@pytest.mark.parametrize(
    "marker",
    [
        "\n\nУстойчивые правила контекста:\n",
        "\n\nРезультаты прошлых workers.",
        "\n\nИсходная задача coordinator (immutable):\n",
        "\n\nOperation-skill текущей задачи:\n",
        "\n\nOperation-skill проверки полноты:\n",
    ],
)
def test_literal_worker_request_preserves_reserved_marker_phrases(marker):
    from agents.contracts import parse_worker_request

    literal_task = f"Проверь точный текст `{marker}literal_value` без разбора."
    parts = parse_worker_request(literal_task)

    assert parts.current_task == literal_task
    assert parts.original_task == ""
    assert parts.operation_execution_context == ""
    assert parts.operation_completeness_context == ""
    assert parts.previous_results is None


def test_typed_worker_request_round_trip_preserves_empty_references():
    from agents.contracts import WorkerRequestParts, parse_worker_request

    request = WorkerRequestParts(
        current_task="Прочитай буквальную task.",
        original_task="Исходная task.",
        previous_results=[],
    )

    assert parse_worker_request(request) is request
    assert request.previous_results == []


def test_direct_worker_treats_legacy_marker_text_as_literal_task():
    from agents.contracts import parse_worker_request

    leaked_context = "RAW_CONVERSATION_CONTEXT_MUST_NOT_REACH_PLANNER"

    def lookup():
        return {"value": "confirmed"}

    model = _WorkerModel(
        [
            AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "lookup",
                        "args": {},
                        "id": "call-sanitized",
                        "type": "tool_call",
                    }
                ],
            ),
            _finish_message("Готово."),
        ]
    )
    raw_task = (
        "Прочитай значение."
        "\n\nУстойчивые правила контекста:\n"
        + leaked_context
    )

    parts = parse_worker_request(raw_task)
    result = run_worker_graph(
        task=raw_task,
        system_prompt="Системный контекст",
        model=model,
        tools={"lookup": _as_tool(lookup)},
    )

    assert parts.current_task == raw_task
    assert not hasattr(parts, "stable_context")
    assert result.status == "complete"
    assert leaked_context in str(model.messages)
    assert leaked_context in str(model.observer.messages)
    assert all(
        leaked_context not in str(messages[0].content)
        for messages in model.messages
    )
    assert leaked_context not in str(model.observer.messages[0][0].content)


class _BoundToolChoiceModel:
    def __init__(self, parent, tool_choice=None):
        self.parent = parent
        self.tool_choice = tool_choice

    def invoke(self, messages, **kwargs):
        del kwargs
        return self.parent.invoke_bound(self.tool_choice, messages)


class _ToolChoiceFallbackModel:
    def __init__(self):
        self.forced_lookup_calls = 0
        self.regular_calls = 0
        self.observer = _ObserverModel()

    def bind_tools(self, tools, tool_choice=None):
        del tools
        return _BoundToolChoiceModel(self, tool_choice)

    def with_structured_output(self, schema, method=None):
        assert method == "function_calling"
        if schema is ObservationContract:
            return self.observer
        raise AssertionError(f"Unexpected structured schema: {schema}")

    def invoke(self, messages, **kwargs):
        del kwargs
        if messages and "Ты observer многошагового агента" in str(
            messages[0].content
        ):
            return AIMessage(content="Значение подтверждено.")
        raise AssertionError("Unexpected unbound planner call")

    def invoke_bound(self, tool_choice, messages):
        del messages
        if tool_choice == "lookup":
            self.forced_lookup_calls += 1
            raise RuntimeError("malformed forced tool call")

        self.regular_calls += 1
        if self.regular_calls == 1:
            return AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "lookup",
                        "args": {},
                        "id": "call-fallback",
                        "type": "tool_call",
                    }
                ],
            )
        return _finish_message("Подтверждено через fallback.")


class _SplitToolCallModel:
    def __init__(self):
        self.observer = _ObserverModel()
        self.invocations = []

    def bind_tools(self, tools, tool_choice=None):
        del tools
        return _BoundToolChoiceModel(self, tool_choice)

    def with_structured_output(self, schema, method=None):
        assert method == "function_calling"
        if schema is ObservationContract:
            return self.observer
        raise AssertionError(f"Unexpected structured schema: {schema}")

    def invoke_bound(self, tool_choice, messages):
        self.invocations.append((tool_choice, list(messages)))
        if tool_choice == "select_worker_tool":
            return AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "select_worker_tool",
                        "args": {"tool_name": "lookup"},
                        "id": "select-lookup",
                        "type": "tool_call",
                    }
                ],
            )
        if tool_choice == "lookup":
            return AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "lookup",
                        "args": {"table_name": "orders"},
                        "id": "call-lookup",
                        "type": "tool_call",
                    }
                ],
            )
        if tool_choice == "finish_worker":
            return _finish_message("Чтение завершено.")
        raise AssertionError(f"Unexpected tool choice: {tool_choice}")


def test_worker_can_select_tool_and_build_arguments_in_separate_calls():
    from agents.contracts import WorkerRequestParts

    executed_arguments = []

    def lookup(table_name: str):
        executed_arguments.append(table_name)
        return {"table_name": table_name}

    model = _SplitToolCallModel()
    current_task = "Прочитай назначенную таблицу."
    original_task = "Прочитай таблицу `orders` для проверки нового ID 731."
    result = run_worker_graph(
        task=WorkerRequestParts(
            current_task=current_task,
            original_task=original_task,
        ),
        system_prompt="Системный контекст",
        model=model,
        tools=(_as_tool(lookup),),
        max_steps=2,
        split_tool_call_planning=True,
    )

    assert executed_arguments == ["orders"]
    assert result.answer == "Чтение завершено."
    invoked_choices = [choice for choice, _ in model.invocations]
    assert invoked_choices == [
        "select_worker_tool",
        "lookup",
        "finish_worker",
    ]

    selector_messages = model.invocations[0][1]
    argument_messages = model.invocations[1][1]
    assert "Доступные операции" in str(selector_messages[0].content)
    assert "Операция уже выбрана: `lookup`" in str(
        argument_messages[0].content
    )
    selector_humans = [
        message
        for message in selector_messages
        if message.__class__.__name__ == "HumanMessage"
    ]
    assert [message.content for message in selector_humans] == [current_task]
    argument_humans = [
        message
        for message in argument_messages
        if message.__class__.__name__ == "HumanMessage"
    ]
    assert json.loads(argument_humans[0].content) == {
        "original_task": original_task
    }
    assert argument_humans[1].content == current_task
    observer_payload = json.loads(model.observer.messages[0][-1].content)
    assert observer_payload["user_request"] == current_task
    assert "original_task" not in observer_payload
    assert all(
        not (
            isinstance(message, AIMessage)
            and any(
                call.get("name") == "select_worker_tool"
                for call in message.tool_calls
            )
        )
        for message in argument_messages
    )


def test_worker_repairs_observer_without_repeating_data_tool():
    tool_calls = []
    stages = []

    def stage_scope(stage):
        stages.append(stage)
        return nullcontext()

    def lookup():
        tool_calls.append("lookup")
        return {"value": 42}

    model = _WorkerModel(
        [
            AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "lookup",
                        "args": {},
                        "id": "call-lookup",
                        "type": "tool_call",
                    }
                ],
            ),
            _finish_message("Значение: 42."),
        ],
        observer_responses=[
            {
                "summary": "Невалидная отрицательная observation.",
                "goal_satisfied": False,
                "problem": None,
            },
            Observation(
                summary="Значение 42 подтверждено.",
                goal_satisfied=True,
                important_facts=["Значение: 42."],
            ),
        ],
    )

    with patch("agents.chat_graph.llm_stage", side_effect=stage_scope):
        result = run_worker_graph(
            task="Получи значение.",
            system_prompt="Системный контекст",
            model=model,
            tools=(_as_tool(lookup),),
            max_steps=2,
        )

    assert result.answer == "Значение: 42."
    assert tool_calls == ["lookup"]
    assert len(model.observer.messages) == 2
    repair_messages = model.observer.messages[1]
    assert "Data tool уже выполнен" in repair_messages[-1].content
    assert "не требуй его повторного" in repair_messages[-1].content
    assert stages == [
        "worker_planner",
        "observer",
        "observer",
        "finish_worker",
    ]


def test_worker_accepts_complete_evidence_despite_provider_reroute_defaults():
    tool_calls = []

    def lookup():
        tool_calls.append("lookup")
        return {"data_type": "uuid"}

    model = _WorkerModel(
        [
            AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "lookup",
                        "args": {},
                        "id": "call-exact-read",
                        "type": "tool_call",
                    }
                ],
            ),
            _finish_message("Точный тип прочитан."),
        ],
        observer_responses=[
            {
                "status": "complete",
                "gap": None,
                "accepted_tool_call_ids": ["call-exact-read"],
                "facts": [],
                "limitations": [],
                "reroute_reason": "missing_capability",
                "required_capabilities": ["sql_read"],
            }
        ],
    )

    result = run_worker_graph(
        task="Прочитай точный тип.",
        system_prompt="Системный контекст",
        model=model,
        tools=(_as_tool(lookup),),
        max_steps=2,
    )

    assert result.answer == "Точный тип прочитан."
    assert result.accepted_tool_call_ids == ["call-exact-read"]
    assert [item.name for item in result.display_items] == ["lookup"]
    assert tool_calls == ["lookup"]
    assert len(model.observer.messages) == 1


def test_worker_repairs_provider_markup_before_executing_tool():
    tool_calls = []

    def lookup(data_type=None):
        tool_calls.append(data_type)
        return {"data_type": "uuid"}

    model = _WorkerModel(
        [
            AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "lookup",
                        "args": {
                            "data_type": (
                                "}}!#native#!#tool_call_id-00001"
                                "#!#/native#!#native_result!#{"
                            )
                        },
                        "id": "call-invalid",
                        "type": "tool_call",
                    }
                ],
            ),
            AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "lookup",
                        "args": {},
                        "id": "call-valid",
                        "type": "tool_call",
                    }
                ],
            ),
            _finish_message("Тип uuid подтверждён."),
        ]
    )

    result = run_worker_graph(
        task="Получи тип данных.",
        system_prompt="Системный контекст",
        model=model,
        tools=(_as_tool(lookup),),
        max_steps=2,
    )

    assert result.answer == "Тип uuid подтверждён."
    assert tool_calls == [None]
    assert len(model.observer.messages) == 1
    repair_prompt = str(model.messages[1][-1].content)
    assert "служебную разметку LLM-провайдера" in repair_prompt
    assert "Неизвестные необязательные аргументы полностью опусти" in (
        repair_prompt
    )
    assert "call-invalid" not in str(result)


def test_worker_rejects_unknown_accepted_tool_call_id_without_repeating_tool():
    tool_calls = []

    def lookup():
        tool_calls.append("lookup")
        return {"value": 42}

    model = _WorkerModel(
        [
            AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "lookup",
                        "args": {},
                        "id": "call-lookup",
                        "type": "tool_call",
                    }
                ],
            ),
            _finish_message("Значение: 42."),
        ],
        observer_responses=[
            Observation(
                summary="Значение получено.",
                goal_satisfied=True,
                accepted_tool_call_ids=["call-unknown"],
            ),
            Observation(
                summary="Значение 42 подтверждено.",
                goal_satisfied=True,
                accepted_tool_call_ids=["call-lookup"],
            ),
        ],
    )

    result = run_worker_graph(
        task="Получи значение.",
        system_prompt="Системный контекст",
        model=model,
        tools=(_as_tool(lookup),),
        max_steps=2,
    )

    assert result.accepted_tool_call_ids == ["call-lookup"]
    assert tool_calls == ["lookup"]
    assert len(model.observer.messages) == 2
    assert "accepted_tool_call_ids" in model.observer.messages[1][-1].content


def test_worker_raises_after_five_observer_retries_without_repeating_tool():
    tool_calls = []

    def lookup():
        tool_calls.append("lookup")
        return {"value": 42}

    invalid_observation = {
        "summary": "Невалидная отрицательная observation.",
        "goal_satisfied": False,
        "problem": None,
    }
    model = _WorkerModel(
        [
            AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "lookup",
                        "args": {},
                        "id": "call-lookup",
                        "type": "tool_call",
                    }
                ],
            ),
            _finish_message("Значение 42 получено."),
        ],
        observer_responses=[invalid_observation] * 6,
    )

    with pytest.raises(WorkerResponseError, match="6 попыток"):
        run_worker_graph(
            task="Получи значение.",
            system_prompt="Системный контекст",
            model=model,
            tools=(_as_tool(lookup),),
            max_steps=2,
        )
    assert tool_calls == ["lookup"]
    assert len(model.observer.messages) == 6


def test_worker_rejects_tool_batch_larger_than_remaining_budget():
    calls = []

    def first_lookup():
        calls.append("first")
        return {"value": 1}

    def second_lookup():
        calls.append("second")
        return {"value": 2}

    model = _WorkerModel(
        [
            AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "first_lookup",
                        "args": {},
                        "id": "call-first",
                        "type": "tool_call",
                    },
                    {
                        "name": "second_lookup",
                        "args": {},
                        "id": "call-second",
                        "type": "tool_call",
                    },
                ],
            )
        ]
    )

    with pytest.raises(WorkerResponseError, match="запрошено 2, доступно 1"):
        run_worker_graph(
            task="Получи два значения.",
            system_prompt="Системный контекст",
            model=model,
            tools=(_as_tool(first_lookup), _as_tool(second_lookup)),
            max_steps=1,
        )

    assert calls == []


def test_public_worker_keeps_prior_evidence_after_observer_exhaustion():
    from agents.worker import resolve_worker_display_refs, worker_chat

    tool_calls = []

    def accepted_lookup():
        tool_calls.append("accepted")
        return {"rows": [{"value": 42}]}

    def unobserved_lookup():
        tool_calls.append("unobserved")
        return {"rows": [{"value": 99}]}

    tools = (_as_tool(accepted_lookup), _as_tool(unobserved_lookup))
    model = _WorkerModel(
        [
            AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "accepted_lookup",
                        "args": {},
                        "id": "call-accepted",
                        "type": "tool_call",
                    }
                ],
            ),
            AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "unobserved_lookup",
                        "args": {},
                        "id": "call-unobserved",
                        "type": "tool_call",
                    }
                ],
            ),
        ],
        observer_responses=[
            Observation(
                status="continue",
                gap="Нужно проверить второе значение.",
                accepted_tool_call_ids=["call-accepted"],
                important_facts=["Первое значение равно 42."],
            ),
            *[
                {
                    "status": "continue",
                    "gap": None,
                    "accepted_tool_call_ids": ["call-accepted"],
                }
                for _ in range(6)
            ],
        ],
    )
    route = ToolRoute(
        tools=["accepted_lookup", "unobserved_lookup"],
        skills=[],
        schemas=[],
    )

    with (
        patch("agents.worker.chat_model", model),
        patch("agents.worker.get_worker_tools", return_value=tools),
        patch("agents.worker.select_chat_route", return_value=route),
    ):
        outcome = worker_chat("Получи и проверь два значения.")

    assert tool_calls == ["accepted", "unobserved"]
    assert len(model.observer.messages) == 7
    assert outcome.status == "partial"
    assert outcome.stop_reason == "observer_error"
    assert outcome.unmet_requirements == [
        "Observer не смог вернуть валидную структуру после 6 попыток; "
        "data tool не повторялся."
    ]
    assert [fact.text for fact in outcome.facts] == [
        "Первое значение равно 42."
    ]
    assert len(outcome.evidence) == 1
    evidence = outcome.evidence[0]
    assert evidence.tool_name == "accepted_lookup"
    assert evidence.evidence_id == outcome.facts[0].evidence_ids[0]
    assert '"value": 42' in evidence.preview
    assert '"value": 99' not in evidence.preview
    assert evidence.display_ref is not None
    retained = resolve_worker_display_refs([evidence.display_ref])
    assert [item.tool_call_id for item in retained] == ["call-accepted"]


def test_public_worker_keeps_accepted_evidence_after_invalid_finish():
    from agents.worker import resolve_worker_display_refs, worker_chat

    calls = []

    def accepted_lookup():
        calls.append("accepted")
        return {"rows": [{"value": 42}]}

    tools = (_as_tool(accepted_lookup),)
    model = _WorkerModel(
        [
            AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "accepted_lookup",
                        "args": {},
                        "id": "call-accepted-finish",
                        "type": "tool_call",
                    }
                ],
            ),
            AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "finish_worker",
                        "args": {},
                        "id": "finish-invalid",
                        "type": "tool_call",
                    }
                ],
            ),
        ]
    )
    route = ToolRoute(tools=["accepted_lookup"], skills=[], schemas=[])

    with (
        patch("agents.worker.chat_model", model),
        patch("agents.worker.get_worker_tools", return_value=tools),
        patch("agents.worker.select_chat_route", return_value=route),
    ):
        outcome = worker_chat("Получи значение.")

    assert calls == ["accepted"]
    assert outcome.status == "partial"
    assert outcome.stop_reason == "tool_error"
    assert outcome.unmet_requirements == [
        "finish_worker вернул невалидные аргументы"
    ]
    assert len(outcome.evidence) == 1
    evidence = outcome.evidence[0]
    assert evidence.tool_name == "accepted_lookup"
    assert '"value": 42' in evidence.preview
    assert evidence.display_ref is not None
    retained = resolve_worker_display_refs([evidence.display_ref])
    assert [item.tool_call_id for item in retained] == [
        "call-accepted-finish"
    ]


def test_worker_rejects_complete_when_exact_lineage_scope_was_shortened():
    calls = []

    def trace_neo4j_lineage(
        column_reference: str,
        direction: str = "both",
        max_depth: int = 1,
    ):
        calls.append(
            {
                "column_reference": column_reference,
                "direction": direction,
                "max_depth": max_depth,
            }
        )
        return {
            "rows": [] if max_depth == 1 else [{"target_table": "branch::1"}],
            "max_depth": max_depth,
        }

    full_table = "s_grnplm_as_t_didsd_700_db_stg.a_000025_t_loanscontract"
    model = _WorkerModel(
        [
            AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "trace_neo4j_lineage",
                        "args": {
                            "column_reference": (
                                "a_000025_t_loanscontract.c_closedate"
                            ),
                            "direction": "downstream",
                        },
                        "id": "call-shortened",
                        "type": "tool_call",
                    }
                ],
            ),
            AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "trace_neo4j_lineage",
                        "args": {
                            "column_reference": f"{full_table}.c_closedate",
                            "direction": "downstream",
                            "max_depth": 5,
                        },
                        "id": "call-exact",
                        "type": "tool_call",
                    }
                ],
            ),
            _finish_message("Полный downstream lineage получен."),
        ],
        observer_responses=[
            Observation(
                status="continue",
                gap=(
                    f"Точный column_reference={full_table}.c_closedate и "
                    "требуемая полнота ещё не подтверждены."
                ),
                accepted_tool_call_ids=[],
            ),
            Observation(
                summary="Точный транзитивный lineage получен.",
                goal_satisfied=True,
                accepted_tool_call_ids=["call-exact"],
            ),
        ],
    )

    result = run_worker_graph(
        task=(
            "Выполни reverse lineage для "
            f"{full_table}.c_closedate и перечисли все зависимые transformations."
        ),
        system_prompt="Системный контекст",
        model=model,
        tools=(_as_tool(trace_neo4j_lineage),),
        max_steps=3,
    )

    assert [item["column_reference"] for item in calls] == [
        "a_000025_t_loanscontract.c_closedate",
        f"{full_table}.c_closedate",
    ]
    assert result.status == "complete"
    assert result.accepted_tool_call_ids == ["call-exact"]
    assert [item.tool_call_id for item in result.display_items] == ["call-exact"]
    first_observation = result.cycle_history[0].observation
    assert first_observation.status == "continue"
    assert full_table in str(first_observation.gap)
    assert "column_reference" in str(first_observation.gap)


def test_worker_keeps_valid_lineage_while_fetching_transformation_rules():
    executed = []
    reference = "schema.source_table.c_closedate"

    def trace_neo4j_lineage(
        column_reference: str,
        direction: str = "both",
        max_depth: int = 1,
    ):
        executed.append(("trace_neo4j_lineage", column_reference, max_depth))
        return {
            "rows": [
                {
                    "transformation_id": 118,
                    "source_table": "branch::1",
                    "target_table": "target_table",
                }
            ],
            "column_reference": column_reference,
            "direction": direction,
            "max_depth": max_depth,
        }

    def run_sql(query: str):
        executed.append(("run_sql", query))
        if "WHERE transformation_id" in query:
            return {
                "error": "SQL query failed",
                "error_message": "no such column: transformation_id",
                "query": query,
            }
        return {
            "rows": [
                {
                    "id": 118,
                    "transformation_rule": "UNION ALL",
                }
            ]
        }

    model = _WorkerModel(
        [
            AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "trace_neo4j_lineage",
                        "args": {
                            "column_reference": reference,
                            "direction": "downstream",
                            "max_depth": 5,
                        },
                        "id": "call-lineage",
                        "type": "tool_call",
                    }
                ],
            ),
            AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "run_sql",
                        "args": {
                            "query": (
                                "SELECT transformation_rule "
                                "FROM s2t_transformations "
                                "WHERE transformation_id IN (118)"
                            )
                        },
                        "id": "call-rules-wrong-column",
                        "type": "tool_call",
                    }
                ],
            ),
            AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "run_sql",
                        "args": {
                            "query": (
                                "SELECT id, transformation_rule "
                                "FROM s2t_transformations WHERE id IN (118)"
                            )
                        },
                        "id": "call-rules",
                        "type": "tool_call",
                    }
                ],
            ),
            _finish_message("Lineage и зависимые правила получены."),
        ],
        observer_responses=[
            Observation(
                status="continue",
                gap=(
                    "Lineage получен, но фактические transformation rules "
                    "для найденных записей ещё не прочитаны."
                ),
                accepted_tool_call_ids=["call-lineage"],
                facts=[{"text": "Найдена transformation 118."}],
            ),
            Observation(
                status="continue",
                gap=(
                    "Текущий SQL завершился ошибкой; повтори чтение правила "
                    "по фактической схеме источника."
                ),
                accepted_tool_call_ids=["call-lineage"],
                facts=[{"text": "Найдена transformation 118."}],
            ),
            Observation(
                status="complete",
                accepted_tool_call_ids=["call-lineage", "call-rules"],
                facts=[
                    {"text": "Transformation 118 использует UNION ALL."}
                ],
            ),
        ],
    )

    result = run_worker_graph(
        task=(
            f"Выполни reverse lineage для {reference} и перечисли "
            "downstream transformations."
        ),
        system_prompt="Системный контекст",
        model=model,
        tools=(
            _as_tool(trace_neo4j_lineage),
            _as_tool(run_sql),
        ),
        max_steps=3,
    )

    assert result.status == "complete"
    assert result.accepted_tool_call_ids == ["call-lineage", "call-rules"]
    assert result.cycle_history[0].observation.status == "continue"
    assert result.cycle_history[0].observation.accepted_tool_call_ids == [
        "call-lineage"
    ]
    assert "transformation rules" in str(
        result.cycle_history[0].observation.gap
    )
    assert result.cycle_history[1].observation.status == "continue"
    assert result.cycle_history[1].observation.accepted_tool_call_ids == [
        "call-lineage"
    ]
    assert "фактической схеме" in str(
        result.cycle_history[1].observation.gap
    )
    assert [item[0] for item in executed] == [
        "trace_neo4j_lineage",
        "run_sql",
        "run_sql",
    ]


def test_worker_fetches_lineage_rules_by_ids_without_free_sql():
    executed = []
    reference = "schema.source_table.c_closedate"

    def trace_neo4j_lineage(
        column_reference: str,
        direction: str = "both",
        max_depth: int = 1,
    ):
        executed.append(("trace_neo4j_lineage", column_reference, max_depth))
        return {
            "rows": [
                {
                    "transformation_id": 118,
                    "source_table": "branch::1",
                    "target_table": "target_table",
                }
            ],
            "column_reference": column_reference,
            "direction": direction,
            "max_depth": max_depth,
        }

    def get_s2t_rules_by_ids(transformation_ids: list[int]):
        executed.append(("get_s2t_rules_by_ids", transformation_ids))
        return {
            "rows": [
                {
                    "id": 118,
                    "transformation_rule": "UNION ALL",
                }
            ],
            "requested_ids": transformation_ids,
            "missing_ids": [],
        }

    model = _WorkerModel(
        [
            AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "trace_neo4j_lineage",
                        "args": {
                            "column_reference": reference,
                            "direction": "downstream",
                            "max_depth": 5,
                        },
                        "id": "call-lineage",
                        "type": "tool_call",
                    }
                ],
            ),
            AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "get_s2t_rules_by_ids",
                        "args": {"transformation_ids": [118]},
                        "id": "call-rules",
                        "type": "tool_call",
                    }
                ],
            ),
            _finish_message("Lineage и зависимое правило получены."),
        ],
        observer_responses=[
            Observation(
                status="continue",
                gap=(
                    "Lineage получен, но правила найденных transformations "
                    "ещё не прочитаны доступным tool."
                ),
                accepted_tool_call_ids=["call-lineage"],
                facts=[{"text": "Найдена transformation 118."}],
            ),
            Observation(
                summary="Lineage и правило подтверждены.",
                goal_satisfied=True,
                accepted_tool_call_ids=["call-lineage", "call-rules"],
                facts=[
                    {"text": "Transformation 118 использует UNION ALL."}
                ],
            ),
        ],
    )

    result = run_worker_graph(
        task=(
            f"Выполни reverse lineage для {reference} и перечисли "
            "downstream transformations."
        ),
        system_prompt="Системный контекст",
        model=model,
        tools=(
            _as_tool(trace_neo4j_lineage),
            _as_tool(get_s2t_rules_by_ids),
        ),
        max_steps=2,
    )

    assert result.status == "complete"
    assert result.accepted_tool_call_ids == ["call-lineage", "call-rules"]
    assert result.cycle_history[0].observation.status == "continue"
    assert "доступным tool" in str(
        result.cycle_history[0].observation.gap
    )
    assert result.cycle_history[1].observation.status == "complete"
    assert executed == [
        ("trace_neo4j_lineage", reference, 5),
        ("get_s2t_rules_by_ids", [118]),
    ]


def test_worker_rejects_empty_s2t_result_with_swapped_role_filter():
    executed = []

    def list_s2t_transformations(
        source_table: str | None = None,
        target_table: str | None = None,
        source_field: str | None = None,
        target_field: str | None = None,
    ):
        executed.append(
            {
                "source_table": source_table,
                "target_table": target_table,
                "source_field": source_field,
                "target_field": target_field,
            }
        )
        return {
            "rows": []
        }

    def list_s2t_table_mapping(source_table: str, target_table: str):
        executed.append(
            {
                "source_table": source_table,
                "target_table": target_table,
            }
        )
        return {
            "rows": [
                {
                    "source_table": source_table,
                    "source_field": "src_id",
                    "target_table": target_table,
                    "target_field": "optn_id",
                }
            ]
        }

    model = _WorkerModel(
        [
            AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "list_s2t_transformations",
                        "args": {
                            "source_table": "b3050000420005_paymentdetails",
                            "target_field": "t_optn",
                        },
                        "id": "call-wrong-role",
                        "type": "tool_call",
                    }
                ],
            ),
            AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "list_s2t_table_mapping",
                        "args": {
                            "source_table": "b3050000420005_paymentdetails",
                            "target_table": "t_optn",
                        },
                        "id": "call-correct-role",
                        "type": "tool_call",
                    }
                ],
            ),
            _finish_message("Полный mapping получен."),
        ],
        observer_responses=[
            Observation(
                status="continue",
                gap=(
                    "Пустой результат получен с перепутанной ролью; "
                    "нужен точный target_table=t_optn."
                ),
                accepted_tool_call_ids=[],
            ),
            Observation(
                goal_satisfied=True,
                accepted_tool_call_ids=["call-correct-role"],
            ),
        ],
    )

    result = run_worker_graph(
        task=(
            "Покажи полный маппинг b3050000420005_paymentdetails -> "
            "t_optn: source column -> target column."
        ),
        system_prompt="Системный контекст",
        model=model,
        tools=(
            _as_tool(list_s2t_transformations),
            _as_tool(list_s2t_table_mapping),
        ),
        max_steps=2,
    )

    assert result.status == "complete"
    assert result.accepted_tool_call_ids == ["call-correct-role"]
    assert [item.tool_call_id for item in result.display_items] == [
        "call-correct-role"
    ]
    assert result.cycle_history[0].observation.status == "continue"
    assert "target_table=t_optn" in str(
        result.cycle_history[0].observation.gap
    )
    assert executed == [
        {
            "source_table": "b3050000420005_paymentdetails",
            "target_table": None,
            "source_field": None,
            "target_field": "t_optn",
        },
        {
            "source_table": "b3050000420005_paymentdetails",
            "target_table": "t_optn",
        },
    ]


def test_worker_requires_target_roles_for_exact_loaded_field():
    executed = []
    target_table = "b700000025_agr_cred::subquery::v_agr_cred1"

    def search_s2t_transformations(needle: str):
        executed.append(("search_s2t_transformations", needle))
        return {"rows": []}

    def list_s2t_transformations(
        target_table: str | None = None,
        target_field: str | None = None,
    ):
        executed.append(
            ("list_s2t_transformations", target_table, target_field)
        )
        return {
            "rows": [
                {
                    "target_table": target_table,
                    "target_field": target_field,
                    "source_field": "ctl_action",
                    "transformation_rule": "CASE ... END",
                }
            ]
        }

    full_reference = f"{target_table}.del_dt"
    model = _WorkerModel(
        [
            AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "search_s2t_transformations",
                        "args": {"needle": full_reference},
                        "id": "call-search",
                        "type": "tool_call",
                    }
                ],
            ),
            AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "list_s2t_transformations",
                        "args": {
                            "target_table": target_table,
                            "target_field": "del_dt",
                        },
                        "id": "call-list",
                        "type": "tool_call",
                    }
                ],
            ),
            _finish_message("Mappings получены."),
        ],
        observer_responses=[
            Observation(
                status="continue",
                gap=(
                    "Подстрочный поиск не подтвердил точные target_table и "
                    "target_field исходной task."
                ),
                accepted_tool_call_ids=[],
            ),
            Observation(
                goal_satisfied=True,
                accepted_tool_call_ids=["call-list"],
            ),
        ],
    )

    result = run_worker_graph(
        task=f"Найди все S2T mappings, которые загружают {full_reference}.",
        system_prompt="Системный контекст",
        model=model,
        tools=(
            _as_tool(search_s2t_transformations),
            _as_tool(list_s2t_transformations),
        ),
        max_steps=2,
    )

    assert result.status == "complete"
    assert result.accepted_tool_call_ids == ["call-list"]
    assert [item.tool_call_id for item in result.display_items] == ["call-list"]
    assert executed == [
        ("search_s2t_transformations", full_reference),
        ("list_s2t_transformations", target_table, "del_dt"),
    ]


def test_worker_requires_dependent_value_in_current_tool_filter():
    from agents.contracts import (
        PreviousResultReference,
        WorkerRequestParts,
        parse_worker_request,
    )

    queries = []
    selected_target = "t_rate_rule_param"

    def run_sql(query: str):
        queries.append(query)
        return {
            "rows": [
                {
                    "distinct_source_tables": (
                        5 if selected_target in query else 236
                    )
                }
            ]
        }

    wrong_query = (
        "SELECT COUNT(DISTINCT source_table) AS distinct_source_tables "
        "FROM s2t_transformations WHERE source_table IS NOT NULL"
    )
    correct_query = wrong_query + f" AND target_table = '{selected_target}'"
    model = _WorkerModel(
        [
            AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "run_sql",
                        "args": {"query": wrong_query},
                        "id": "call-global-count",
                        "type": "tool_call",
                    }
                ],
            ),
            AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "run_sql",
                        "args": {"query": correct_query},
                        "id": "call-filtered-count",
                        "type": "tool_call",
                    }
                ],
            ),
            _finish_message("Зависимый count получен."),
        ],
        observer_responses=[
            Observation(
                status="continue",
                gap=(
                    f"Глобальный count не применяет target_table="
                    f"{selected_target} из прошлого outcome."
                ),
                accepted_tool_call_ids=[],
            ),
            Observation(
                goal_satisfied=True,
                accepted_tool_call_ids=["call-filtered-count"],
            ),
        ],
    )
    reference = PreviousResultReference(
        result_id="result-first",
        description=(
            "run_sql: target_table с максимальным числом строк: "
            f"{selected_target}: 110"
        ),
    )
    task = WorkerRequestParts(
        current_task=(
            "Используя target_table, полученную на предыдущем шаге, посчитай "
            "число различных непустых source_table в s2t_transformations."
        ),
        previous_results=[reference],
    )
    request_parts = parse_worker_request(task)
    assert request_parts.current_task.startswith("Используя target_table")
    assert [
        item.model_dump(mode="json", exclude_none=True)
        for item in (request_parts.previous_results or [])
    ] == [reference.model_dump(mode="json", exclude_none=True)]

    result = run_worker_graph(
        task=task,
        system_prompt="Системный контекст",
        model=model,
        tools=(_as_tool(run_sql),),
        max_steps=2,
    )

    assert result.status == "complete"
    assert result.accepted_tool_call_ids == ["call-filtered-count"]
    assert result.cycle_history[0].observation.status == "continue"
    assert selected_target in str(result.cycle_history[0].observation.gap)
    assert "schema.previous.table" not in str(
        result.cycle_history[0].observation.gap
    )
    assert queries == [wrong_query, correct_query]


def test_worker_reroutes_when_description_is_claimed_as_s2t_rule():
    calls = []

    def semantic_search_descriptions(query: str):
        calls.append(query)
        return {
            "rows": [
                {
                    "column_name": "DEL_DT",
                    "description": "Дата удаления",
                }
            ]
        }

    model = _WorkerModel(
        [
            AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "semantic_search_descriptions",
                        "args": {"query": "дата удаления записи"},
                        "id": "call-description",
                        "type": "tool_call",
                    }
                ],
            )
        ],
        observer_responses=[
            Observation(
                status="reroute",
                gap=(
                    "Описание поля не подтверждает transformation_rule; "
                    "нужен другой источник данных."
                ),
                accepted_tool_call_ids=[],
            )
        ],
    )

    result = run_worker_graph(
        task=(
            "Найди техническое поле для даты удаления записи и "
            "соответствующее S2T-правило."
        ),
        system_prompt="Системный контекст",
        model=model,
        tools=(_as_tool(semantic_search_descriptions),),
        max_steps=2,
    )

    assert calls == ["дата удаления записи"]
    assert result.status == "reroute"
    assert "transformation_rule" in str(result.gap)
    assert result.accepted_tool_call_ids == []
    assert result.display_items == []


def test_worker_uses_function_calling_for_observer_contracts():
    def lookup():
        return {"value": 42}

    model = _WorkerModel(
        [
            AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "lookup",
                        "args": {},
                        "id": "call-lookup",
                        "type": "tool_call",
                    }
                ],
            ),
            _finish_message("Значение: 42."),
        ]
    )

    run_worker_graph(
        task="Получи значение.",
        system_prompt="Системный контекст",
        model=model,
        tools=(_as_tool(lookup),),
        max_steps=2,
    )

    assert model.structured_methods == [
        (ObservationContract, "function_calling"),
    ]


def test_structured_observer_trims_only_surrounding_call_name_spaces():
    from agents.chat_graph import _with_structured_output

    class _RawRunnable:
        def invoke(self, messages, **kwargs):
            del messages, kwargs
            return {
                "raw": AIMessage(
                    content="",
                    tool_calls=[
                        {
                            "name": " Observation ",
                            "args": {
                                "status": "complete",
                                "gap": None,
                                "accepted_tool_call_ids": [],
                                "facts": [],
                                "limitations": [],
                                "reroute_reason": "missing_capability",
                                "required_capabilities": ["sql_read"],
                            },
                            "id": "observation-1",
                            "type": "tool_call",
                        }
                    ],
                ),
                "parsed": None,
                "parsing_error": ValueError("unknown tool type"),
            }

    class _RawModel:
        def with_structured_output(
            self,
            schema,
            *,
            method=None,
            include_raw=False,
        ):
            assert schema is ObservationContract
            assert method == "function_calling"
            assert include_raw is True
            return _RawRunnable()

    observer = _with_structured_output(_RawModel(), ObservationContract)

    result = observer.invoke([])

    assert result == ObservationContract(status="complete")


def test_worker_without_tools_is_observed_before_returning_answer():
    candidate_answer = "Первая пара\nВторая пара"
    model = _WorkerModel(
        [
            AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "analyze_known_facts",
                        "args": {"answer": candidate_answer},
                        "id": "call-analysis",
                        "type": "tool_call",
                    }
                ],
            ),
            _finish_message(candidate_answer),
        ],
        observer_responses=[
            Observation(
                summary="Обе пары точно перенесены из task.",
                goal_satisfied=True,
                important_facts=["Ответ содержит две пары."],
            )
        ],
    )

    result = run_worker_graph(
        task=(
            "Верни две строки из уже известных фактов: "
            "Первая пара; Вторая пара."
        ),
        system_prompt="Системный контекст",
        model=model,
        tools=(),
        max_steps=2,
    )

    assert result.answer == candidate_answer
    assert result.status == "complete"
    assert result.gap is None
    assert result.display_items == []
    assert len(result.cycle_history) == 1
    cycle = result.cycle_history[0]
    assert cycle.tool_calls == [
        {
            "name": "analyze_known_facts",
            "args": {"answer": candidate_answer},
        }
    ]
    assert len(cycle.tool_results) == 1
    assert cycle.tool_results[0]["name"] == "analyze_known_facts"
    assert cycle.observation.status == "complete"
    observer_payload = json.loads(model.observer.messages[0][-1].content)
    assert observer_payload["tool_calls"][0]["name"] == "analyze_known_facts"
    assert observer_payload["tool_results"][0]["name"] == "analyze_known_facts"
    planner_system = str(model.messages[0][0].content)
    assert "Доступные worker tools:\nanalyze_known_facts" in planner_system
    assert "Палитра worker никогда не пуста" in planner_system
    observer_system = str(model.observer.messages[0][0].content)
    assert "не является новым evidence" in observer_system
    assert observer_payload["candidate_answer"] == ""


def test_worker_keeps_text_preview_and_returns_full_successful_result():
    tail_marker = "FULL_RESULT_TAIL"

    def long_result():
        return {
            "payload": ("x" * 200) + tail_marker,
            "truncated": True,
        }

    model = _WorkerModel(
        [
            AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "long_result",
                        "args": {},
                        "id": "call-long",
                        "type": "tool_call",
                    }
                ],
            ),
            _finish_message("Результат получен."),
        ]
    )

    result = run_worker_graph(
        task="Получи длинный результат",
        system_prompt="Системный контекст",
        model=model,
        tools=(_as_tool(long_result),),
        max_steps=2,
        tool_message_preview_chars=50,
    )

    assert result.answer == "Результат получен."
    assert len(model.messages) == 2
    assert len(result.display_items) == 1
    assert result.display_items[0].name == "long_result"
    assert tail_marker in result.display_items[0].content
    assert result.display_items[0].truncated is True
    assert len(result.cycle_history) == 1
    cycle = result.cycle_history[0]
    assert cycle.cycle == 1
    assert cycle.routing_attempt == 1
    assert cycle.tool_calls == [{"name": "long_result", "args": {}}]
    assert cycle.tool_results[0]["name"] == "long_result"
    assert len(cycle.tool_results[0]["content"]) <= 50
    assert tail_marker not in cycle.tool_results[0]["content"]
    assert cycle.observation.facts[0].text == "Превью результата получено."
    assert cycle.observation.facts[0].evidence_ids == [
        cycle.tool_results[0]["evidence_id"]
    ]

    llm_prompts = [*model.messages, *model.observer.messages]
    assert all(
        tail_marker not in str(message.content)
        for prompt in llm_prompts
        for message in prompt
    )
    preview_messages = [
        message
        for prompt in llm_prompts
        for message in prompt
        if isinstance(message, ToolMessage)
    ]
    assert preview_messages
    assert all(isinstance(message.content, str) for message in preview_messages)
    assert all(len(message.content) <= 50 for message in preview_messages)


def test_worker_decodes_packed_display_content_and_names_model_preview_rows():
    repeated_value = "exact transformation rule " + ("x" * 500)
    packed_payload = {
        "row_format": "arrays_in_column_order",
        "columns": ["source_table", "transformation_rule", "row_num"],
        "dictionaries": {
            "source_table": ["raw.payment"],
            "transformation_rule": [repeated_value],
        },
        "returned_rows": 50,
        "truncated": False,
        "rows": [[0, 0, index] for index in range(50)],
    }

    def packed_result():
        return packed_payload

    model = _WorkerModel(
        [
            AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "packed_result",
                        "args": {},
                        "id": "call-packed",
                        "type": "tool_call",
                    }
                ],
            ),
            _finish_message("Packed result получен."),
        ]
    )

    preview_limit = 10_000
    result = run_worker_graph(
        task="Получи полный packed result.",
        system_prompt="Системный контекст",
        model=model,
        tools=(_as_tool(packed_result),),
        max_steps=2,
        tool_message_preview_chars=preview_limit,
    )

    item = result.display_items[0]
    decoded_content = json.loads(item.content)
    compact_preview = json.loads(item.preview)
    assert len(item.preview) < preview_limit
    assert len(item.content) > preview_limit
    assert item.truncated is False
    assert "row_format" not in decoded_content
    assert "dictionaries" not in decoded_content
    assert decoded_content["columns"] == packed_payload["columns"]
    assert decoded_content["rows"] == [
        ["raw.payment", repeated_value, index]
        for index in range(50)
    ]
    assert compact_preview["row_format"] == (
        "named_records_with_dictionary_refs"
    )
    assert compact_preview["dictionaries"]["transformation_rule"] == [
        repeated_value
    ]
    assert compact_preview["rows"] == [
        {
            "source_table": "raw.payment",
            "transformation_rule": {"dictionary_ref": 0},
            "row_num": index,
        }
        for index in range(50)
    ]


def test_worker_returns_all_successful_tool_results_for_coordinator():
    def first_result():
        return {"rows": [{"value": "first"}]}

    def second_result():
        return {"rows": [{"value": "second"}]}

    model = _WorkerModel(
        [
            AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "first_result",
                        "args": {},
                        "id": "call-first",
                        "type": "tool_call",
                    },
                    {
                        "name": "second_result",
                        "args": {},
                        "id": "call-second",
                        "type": "tool_call",
                    },
                ],
            ),
            _finish_message("Готово."),
        ]
    )

    result = run_worker_graph(
        task="Получи результаты",
        system_prompt="Системный контекст",
        model=model,
        tools=(_as_tool(first_result), _as_tool(second_result)),
        max_steps=3,
    )

    assert [item.name for item in result.display_items] == [
        "first_result",
        "second_result",
    ]
    assert "first" in result.display_items[0].content
    assert "second" in result.display_items[1].content


def test_worker_graph_materializes_sqlite_tool_rows_in_active_store():
    from agents.tools.saved_results import (
        query_saved_result,
        saved_result_store_scope,
    )

    def lookup_rows():
        return {
            "total": 2,
            "rows": [
                {"name": "first", "score": 1},
                {"name": "second", "score": 2},
            ],
        }

    model = _WorkerModel(
        [
            AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "list_s2t_transformations",
                        "args": {},
                        "id": "call-sqlite",
                        "type": "tool_call",
                    }
                ],
            ),
            _finish_message("Строки получены."),
        ]
    )

    with saved_result_store_scope() as store:
        result = run_worker_graph(
            task="Получи строки",
            system_prompt="Системный контекст",
            model=model,
            tools=(
                _as_tool(
                    lookup_rows,
                    name="list_s2t_transformations",
                ),
            ),
            max_steps=2,
        )

        descriptors = store.descriptors()
        assert len(descriptors) == 1
        descriptor = descriptors[0]
        assert descriptor.source_tool == "list_s2t_transformations"
        assert descriptor.row_count == 2
        assert descriptor.truncated is False
        assert "saved_result" in result.display_items[0].content
        assert result.display_items[0].result_ref == descriptor.result_ref
        assert "saved_result" in result.cycle_history[0].tool_results[0][
            "content"
        ]

        queried = query_saved_result.invoke(
            {
                "result_ref": descriptor.result_ref,
                "query": "SELECT name FROM result WHERE score = 2",
            }
        )
        assert queried["rows"] == [{"name": "second"}]


def test_worker_binds_referenced_saved_result_schema_into_selected_tool():
    from agents.contracts import WorkerRequestParts
    from agents.tools.saved_results import saved_result_store_scope
    from agents.worker import worker_chat

    route = ToolRoute(
        tools=["query_saved_result"],
        skills=[],
        schemas=[],
    )
    graph_result = WorkerRunResult(
        answer="Найдено: 1.",
        display_items=[],
        goal_satisfied=True,
    )

    with saved_result_store_scope() as store:
        descriptor = store.save_payload(
            source_tool="run_sql",
            payload={
                "columns": ["target_table", "row_count"],
                "rows": [{"target_table": "t_example", "row_count": 1}],
            },
        )
        assert descriptor is not None
        previous_result = store.register_previous_result(
            source_tool="run_sql",
            source_tool_call_id="call-source",
            content=json.dumps(
                {"rows": [{"target_table": "t_example", "row_count": 1}]}
            ),
            description="Исходный табличный результат",
            dataset_ref=descriptor.result_ref,
        )
        task = WorkerRequestParts(
            current_task="Отфильтруй сохранённый результат.",
            previous_results=[previous_result],
        )

        with (
            patch("agents.worker.select_chat_route", return_value=route) as router,
            patch(
                "agents.worker.run_worker_graph",
                return_value=graph_result,
            ) as run_graph,
        ):
            result = worker_chat(task)

    assert result.summary == "Найдено: 1."
    assert result.datasets == []
    available_tools = router.call_args.kwargs["available_tools"]
    routed_tool = next(
        item for item in available_tools if item.name == "query_saved_result"
    )
    assert descriptor.result_ref in routed_tool.description
    assert '"target_table" TEXT' in routed_tool.description
    selected_tool = run_graph.call_args.kwargs["tools"][0]
    assert selected_tool.name == "query_saved_result"
    assert descriptor.result_ref in selected_tool.description


def test_worker_exposes_only_saved_results_accepted_by_observer():
    from agents.contracts import EvidenceFact
    from agents.tools.saved_results import (
        get_active_saved_result_store,
        read_previous_result,
        saved_result_store_scope,
    )
    from agents.worker import worker_chat

    route = ToolRoute(tools=["run_sql"], skills=[], schemas=[])

    def run_graph(**kwargs):
        del kwargs
        store = get_active_saved_result_store()
        assert store is not None
        store.save_payload(
            source_tool="run_sql",
            source_tool_call_id="call-wrong",
            payload={"rows": [{"value": "wrong"}]},
        )
        correct_descriptor = store.save_payload(
            source_tool="run_sql",
            source_tool_call_id="call-correct",
            payload={"rows": [{"value": "correct"}]},
        )
        assert correct_descriptor is not None
        return WorkerRunResult(
            answer="Получен correct.",
            goal_satisfied=True,
            display_items=[
                WorkerDisplayItem(
                    name="run_sql",
                    content=json.dumps({"rows": [{"value": "correct"}]}),
                    evidence_id="evidence-correct",
                    tool_call_id="call-correct",
                    result_ref=correct_descriptor.result_ref,
                    arguments={"query": "SELECT value FROM result"},
                )
            ],
            facts=[
                EvidenceFact(
                    text="Получено значение correct.",
                    evidence_ids=["evidence-correct"],
                )
            ],
            accepted_tool_call_ids=["call-correct"],
        )

    with (
        saved_result_store_scope(),
        patch("agents.worker.select_chat_route", return_value=route),
        patch("agents.worker.run_worker_graph", side_effect=run_graph),
    ):
        result = worker_chat("Получи корректное значение.")
        assert len(result.previous_results) == 1
        reference = result.previous_results[0]
        assert set(reference.model_dump()) == {
            "result_id",
            "description",
            "result_schema",
            "source_evidence_ids",
        }
        assert reference.source_evidence_ids == ["evidence-correct"]
        assert reference.description == (
            'run_sql: args={"query":"SELECT value FROM result"}'
        )
        assert "correct" not in reference.description
        assert reference.result_schema is not None
        assert reference.result_schema.row_count == 1
        assert [
            (column.name, column.sqlite_type)
            for column in reference.result_schema.columns
        ] == [("value", "TEXT")]
        resolved = read_previous_result.invoke(
            {"result_id": reference.result_id}
        )
        assert resolved["result"]["rows"] == [{"value": "correct"}]
        assert resolved["source_evidence_ids"] == ["evidence-correct"]

    assert len(result.datasets) == 1
    descriptor = result.datasets[0]
    assert descriptor.source_tool_call_id == "call-correct"
    assert "source_tool_call_id" not in descriptor.model_dump()


def test_worker_repairs_plain_planner_finish_to_native_finish_call():
    def lookup():
        return {"value": "confirmed"}

    model = _WorkerModel(
        [
            AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "lookup",
                        "args": {},
                        "id": "call-lookup",
                        "type": "tool_call",
                    }
                ],
            ),
            AIMessage(content="Подтверждённое значение: confirmed."),
            _finish_message("Подтверждённое значение: confirmed."),
        ]
    )

    result = run_worker_graph(
        task="Получи значение",
        system_prompt="Системный контекст",
        model=model,
        tools=(_as_tool(lookup),),
        max_steps=2,
    )

    assert result.answer == "Подтверждённое значение: confirmed."
    assert [item.name for item in result.display_items] == ["lookup"]
    assert len(model.messages) == 3
    assert "Обычный текст planner недопустим" in str(
        model.messages[2][-1].content
    )


def test_worker_keeps_evidence_when_plain_text_repair_has_no_native_call():
    def lookup():
        return {"value": "confirmed"}

    model = _WorkerModel(
        [
            AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "lookup",
                        "args": {},
                        "id": "call-lookup",
                        "type": "tool_call",
                    }
                ],
            ),
            AIMessage(content="Подтверждённое значение: confirmed."),
            AIMessage(content="Подтверждённое значение: confirmed."),
        ]
    )

    result = run_worker_graph(
        task="Получи значение",
        system_prompt="Системный контекст",
        model=model,
        tools=(_as_tool(lookup),),
        max_steps=2,
    )

    assert result.gap is not None
    assert "native data-tool call" in result.gap
    assert result.stop_reason == "tool_error"
    assert result.accepted_tool_call_ids == ["call-lookup"]
    assert [item.tool_call_id for item in result.display_items] == [
        "call-lookup"
    ]


def test_worker_repairs_plain_text_before_first_data_tool_call():
    def lookup():
        return {"value": "confirmed"}

    model = _WorkerModel(
        [
            AIMessage(content="Сначала я выполню поиск."),
            AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "lookup",
                        "args": {},
                        "id": "call-lookup",
                        "type": "tool_call",
                    }
                ],
            ),
            _finish_message("Значение получено."),
        ]
    )

    result = run_worker_graph(
        task="Получи значение",
        system_prompt="Системный контекст",
        model=model,
        tools=(_as_tool(lookup),),
        max_steps=2,
    )

    assert result.answer == "Значение получено."
    assert [item.name for item in result.display_items] == ["lookup"]
    assert result.status == "complete"
    assert len(model.messages) == 3
    assert "Сначала я выполню поиск." not in str(model.messages[1])


def test_worker_marks_native_finish_before_first_data_tool_as_no_results():
    def lookup():
        return {"value": "confirmed"}

    model = _WorkerModel(
        [_finish_message("Данных для проверки недостаточно.")]
    )

    result = run_worker_graph(
        task="Получи значение",
        system_prompt="Системный контекст",
        model=model,
        tools=(_as_tool(lookup),),
        max_steps=2,
    )

    assert result.answer == "Данных для проверки недостаточно."
    assert result.display_items == []
    assert result.status == "complete"
    assert result.gap == (
        "Worker завершил task без принятого data-tool результата."
    )
    assert result.stop_reason == "no_results"
    assert result.unmet_requirements == [result.gap]
    assert len(model.messages) == 1


def test_public_worker_maps_early_finish_to_failed_no_results():
    from agents.worker import worker_chat

    gap = "Worker завершил task без принятого data-tool результата."
    graph_result = WorkerRunResult(
        answer="Данных для проверки недостаточно.",
        gap=gap,
        stop_reason="no_results",
        unmet_requirements=[gap],
    )
    route = ToolRoute(tools=["list_files"], skills=[], schemas=[])

    with (
        patch("agents.worker.select_chat_route", return_value=route),
        patch("agents.worker.run_worker_graph", return_value=graph_result),
    ):
        outcome = worker_chat("Получи список файлов")

    assert outcome.status == "failed"
    assert outcome.stop_reason == "no_results"
    assert outcome.unmet_requirements == [gap]
    assert outcome.evidence == []


def test_worker_does_not_force_one_tool_when_finish_is_also_allowed():
    def lookup():
        return {"value": "confirmed"}

    model = _ToolChoiceFallbackModel()

    result = run_worker_graph(
        task="Получи значение",
        system_prompt="Системный контекст",
        model=model,
        tools=(_as_tool(lookup),),
        max_steps=2,
    )

    assert result.answer == "Подтверждено через fallback."
    assert [item.name for item in result.display_items] == ["lookup"]
    assert model.forced_lookup_calls == 0
    assert model.regular_calls == 2


def test_worker_planner_keeps_only_latest_tool_exchange():
    def first_result():
        return {"value": "first"}

    def second_result():
        return {"value": "second"}

    model = _WorkerModel(
        [
            AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "first_result",
                        "args": {},
                        "id": "call-first",
                        "type": "tool_call",
                    }
                ],
            ),
            AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "second_result",
                        "args": {},
                        "id": "call-second",
                        "type": "tool_call",
                    }
                ],
            ),
            _finish_message("Готово."),
        ],
        observer_responses=[
            Observation(
                summary="Получен только первый результат.",
                goal_satisfied=False,
                problem="Второй результат ещё не получен.",
                accepted_tool_call_ids=["call-first"],
            ),
            Observation(
                summary="Получены оба результата.",
                goal_satisfied=True,
                accepted_tool_call_ids=["call-first", "call-second"],
            ),
        ],
    )

    result = run_worker_graph(
        task="Получи два результата",
        system_prompt="Системный контекст",
        model=model,
        tools=(_as_tool(first_result), _as_tool(second_result)),
        max_steps=3,
    )

    assert [item.name for item in result.display_items] == [
        "first_result",
        "second_result",
    ]
    second_prompt_tool_ids = [
        message.tool_call_id
        for message in model.messages[1]
        if isinstance(message, ToolMessage)
    ]
    final_prompt_tool_ids = [
        message.tool_call_id
        for message in model.messages[2]
        if isinstance(message, ToolMessage)
    ]
    assert second_prompt_tool_ids == ["call-first"]
    assert final_prompt_tool_ids == ["call-second"]


def test_worker_planner_keeps_only_latest_cumulative_observation():
    from agents.chat_graph import _runtime_context

    first = Observation(
        summary="Первый результат неполон.",
        goal_satisfied=False,
        problem="Не найден источник.",
    )
    latest = Observation(
        summary="Источник найден, правило ещё отсутствует.",
        goal_satisfied=False,
        problem="Не найдено правило преобразования.",
        important_facts=["Источник: source_contracts."],
    )

    context = _runtime_context({"observations": [first, latest]})

    assert context is not None
    assert "Observation для шага 2" in context
    assert "Источник найден, правило ещё отсутствует." not in context
    assert "Не найдено правило преобразования." in context
    assert "Источник: source_contracts." in context
    assert "Observation для шага 1" not in context
    assert "Первый результат неполон." not in context
    assert "Не найден источник." not in context


def test_worker_observer_evaluates_current_result_with_prior_state():
    calls = []

    def find_source():
        calls.append("source")
        return {"source_table": "source_contracts"}

    def find_rule():
        calls.append("rule")
        return {"transformation_rule": "source.c_closedate"}

    model = _WorkerModel(
        [
            AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "find_source",
                        "args": {},
                        "id": "call-source",
                        "type": "tool_call",
                    }
                ],
            ),
            AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "find_rule",
                        "args": {},
                        "id": "call-rule",
                        "type": "tool_call",
                    }
                ],
            ),
            _finish_message("Источник и правило подтверждены."),
        ],
        observer_responses=[
            Observation(
                summary="Источник: source_contracts.",
                goal_satisfied=False,
                problem="Правило преобразования ещё не подтверждено.",
                important_facts=["Источник: source_contracts."],
                accepted_tool_call_ids=["call-source"],
            ),
            Observation(
                summary=(
                    "Источник: source_contracts. Правило: "
                    "source.c_closedate."
                ),
                goal_satisfied=True,
                important_facts=[
                    "Источник: source_contracts.",
                    "Правило: source.c_closedate.",
                ],
                accepted_tool_call_ids=["call-source", "call-rule"],
            ),
        ],
    )

    result = run_worker_graph(
        task="Найди источник и правило преобразования c_closedate.",
        system_prompt="Системный контекст",
        model=model,
        tools=(_as_tool(find_source), _as_tool(find_rule)),
        max_steps=3,
    )

    assert result.status == "complete"
    assert calls == ["source", "rule"]
    assert len(model.observer.messages) == 2
    second_observer_prompt = model.observer.messages[1]
    second_payload = json.loads(second_observer_prompt[-1].content)
    assert len(second_payload["prior_state"]) == 1
    prior_fact = second_payload["prior_state"][0]["facts"][0]
    assert prior_fact["text"] == "Источник: source_contracts."
    assert len(prior_fact["evidence_ids"]) == 1
    assert prior_fact["evidence_ids"][0].startswith("evidence_")
    observer_system_prompt = " ".join(
        str(second_observer_prompt[0].content).split()
    )
    assert "`prior_state` и `accepted_evidence` накоплены ранее" in (
        observer_system_prompt
    )
    assert "не требуй их повторно" in observer_system_prompt


def test_worker_rejects_legacy_display_selection_but_keeps_evidence():
    def lookup():
        return {"rows": [{"value": 1}]}

    model = _WorkerModel(
        [
            AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "lookup",
                        "args": {},
                        "id": "call-real",
                        "type": "tool_call",
                    }
                ],
            ),
            _finish_message(
                "Готово.",
                extra_args={"display_tool_call_ids": ["call-real"]},
            ),
        ]
    )

    result = run_worker_graph(
        task="Получи значение",
        system_prompt="Системный контекст",
        model=model,
        tools=(_as_tool(lookup),),
        max_steps=2,
    )

    assert result.gap == "finish_worker вернул невалидные аргументы"
    assert result.stop_reason == "tool_error"
    assert result.accepted_tool_call_ids == ["call-real"]
    assert [item.tool_call_id for item in result.display_items] == ["call-real"]


def test_worker_asks_llm_to_finish_after_step_limit():
    def lookup():
        return {"rows": [{"value": 1}]}

    repeated_call = AIMessage(
        content="",
        tool_calls=[
            {
                "name": "lookup",
                "args": {},
                "id": "call-repeated",
                "type": "tool_call",
            }
        ],
    )
    model = _WorkerModel(
        [
            AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "lookup",
                        "args": {},
                        "id": "call-first",
                        "type": "tool_call",
                    }
                ],
            ),
            repeated_call,
            _finish_message("Значение получено."),
        ]
    )

    result = run_worker_graph(
        task="Получи значение",
        system_prompt="Системный контекст",
        model=model,
        tools=(_as_tool(lookup),),
        max_steps=1,
    )

    assert result.answer == "Значение получено."
    assert len(model.messages) == 3
    assert "Больше не вызывай data tools" in str(
        model.messages[2][-1].content
    )


def test_worker_returns_unsatisfied_status_after_step_limit():
    def lookup():
        return {"rows": []}

    model = _WorkerModel(
        [
            AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "lookup",
                        "args": {},
                        "id": "call-empty",
                        "type": "tool_call",
                    }
                ],
            ),
            _finish_message("Данные не найдены."),
        ],
        observer_responses=[
            Observation(
                summary="Результат пуст.",
                goal_satisfied=False,
                problem="Task ожидала значение, но tool вернул пустой результат.",
            )
        ],
    )

    result = run_worker_graph(
        task="Получи значение.",
        system_prompt="Системный контекст",
        model=model,
        tools=(_as_tool(lookup),),
        max_steps=1,
    )

    assert result.status == "complete"
    assert result.gap == (
        "Task ожидала значение, но tool вернул пустой результат."
    )


def test_worker_llm_handles_tool_error_without_backend_branch():
    def lookup(item: str):
        return {
            "error": "Источник недоступен",
            "rows": [],
            "item": item,
        }

    model = _WorkerModel(
        [
            AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "lookup",
                        "args": {"item": "значение"},
                        "id": "call-lookup",
                        "type": "tool_call",
                    }
                ],
            ),
            _finish_message(
                "Источник недоступен; значение проверить не удалось."
            ),
        ]
    )

    result = run_worker_graph(
        task="Проверь значение в источнике.",
        system_prompt="Системный контекст",
        model=model,
        tools=(_as_tool(lookup),),
        max_steps=3,
    )

    assert result.answer == (
        "Источник недоступен; значение проверить не удалось."
    )
    assert result.display_items == []
    assert len(model.messages) == 2
    assert "Источник недоступен" in str(model.messages[1])


def test_worker_llm_can_correct_its_tool_call_after_observation():
    executed_queries = []

    def run_sql(query: str):
        executed_queries.append(query)
        return {"rows": [{"target_table": "t_rate_rule_param", "row_count": 55}]}

    wrong_sql = "SELECT source_table, COUNT(*) FROM s2t_transformations GROUP BY source_table"
    correct_sql = (
        "SELECT target_table, COUNT(*) AS row_count FROM s2t_transformations "
        "GROUP BY target_table ORDER BY row_count DESC LIMIT 1"
    )
    model = _WorkerModel(
        [
            AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "run_sql",
                        "args": {"query": wrong_sql},
                        "id": "call-wrong",
                        "type": "tool_call",
                    }
                ],
            ),
            AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "run_sql",
                        "args": {"query": correct_sql},
                        "id": "call-correct",
                        "type": "tool_call",
                    }
                ],
            ),
            _finish_message("Максимум: t_rate_rule_param, 55 строк."),
        ],
        observer_responses=[
            Observation(
                summary="Получена агрегация по source_table вместо target_table.",
                goal_satisfied=False,
                problem="Task требует агрегацию по target_table.",
            ),
            Observation(
                summary="Получена требуемая агрегация по target_table.",
                goal_satisfied=True,
            ),
        ],
    )

    result = run_worker_graph(
        task="Найди target_table с наибольшим числом строк.",
        system_prompt="Системный контекст",
        model=model,
        tools=(_as_tool(run_sql),),
        max_steps=2,
    )

    assert result.answer == "Максимум: t_rate_rule_param, 55 строк."
    assert executed_queries == [wrong_sql, correct_sql]
    assert [item.name for item in result.display_items] == ["run_sql"]
    assert result.display_items[0].tool_call_id == "call-correct"
    assert result.display_items[0].arguments == {"query": correct_sql}
    assert "t_rate_rule_param" in result.display_items[0].content


def test_worker_repairs_finish_attempt_after_observer_reports_semantic_mismatch():
    selected_fields = []

    def lookup(field_name: str):
        selected_fields.append(field_name)
        return {field_name: "value"}

    model = _WorkerModel(
        [
            AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "lookup",
                        "args": {"field_name": "source_table"},
                        "id": "call-wrong-role",
                        "type": "tool_call",
                    }
                ],
            ),
            _finish_message("Ошибочно считаю source_table целевой таблицей."),
            AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "lookup",
                        "args": {"field_name": "target_table"},
                        "id": "call-correct-role",
                        "type": "tool_call",
                    }
                ],
            ),
            _finish_message("Получено значение target_table."),
        ],
        observer_responses=[
            Observation(
                summary="Tool получил значение поля source_table.",
                goal_satisfied=False,
                problem="Task просит target_table, но tool получил source_table.",
            ),
            Observation(
                summary="Tool получил требуемое значение поля target_table.",
                goal_satisfied=True,
                accepted_tool_call_ids=["call-correct-role"],
            ),
        ],
    )

    result = run_worker_graph(
        task="Получи target_table.",
        system_prompt="Системный контекст",
        model=model,
        tools=(_as_tool(lookup),),
        max_steps=2,
    )

    assert result.answer == "Получено значение target_table."
    assert selected_fields == ["source_table", "target_table"]
    assert len(model.observer.messages) == 2
    assert result.status == "complete"
    assert result.gap is None
    assert [item.tool_call_id for item in result.display_items] == [
        "call-correct-role"
    ]
    repair_prompt = str(model.messages[2][-1].content)
    assert "завершать worker сейчас запрещено" in repair_prompt


def test_worker_graph_returns_reroute_after_current_palette_cannot_repair():
    def list_names():
        return {"rows": [{"table_name": "t_example"}]}

    model = _WorkerModel(
        [
            AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "list_names",
                        "args": {},
                        "id": "call-list-names",
                        "type": "tool_call",
                    }
                ],
            ),
        ],
        observer_responses=[
            Observation(
                summary="Получен только список имён без агрегирования.",
                goal_satisfied=False,
                problem=(
                    "Task требует сравнить количества строк, но текущий tool "
                    "возвращает только имена; нужна возможность произвольной "
                    "агрегации данных."
                ),
                reroute_required=True,
            ),
        ],
    )

    result = run_worker_graph(
        task="Найди таблицу с максимальным числом строк.",
        system_prompt="Системный контекст",
        model=model,
        tools=(_as_tool(list_names),),
        max_steps=3,
    )

    assert result.status == "reroute"
    assert result.gap == (
        "Task требует сравнить количества строк, но текущий tool возвращает "
        "только имена; нужна возможность произвольной агрегации данных."
    )
    assert result.display_items == []
    assert len(model.messages) == 1
    observer_payload = str(model.observer.messages[0][-1].content)
    assert '"available_tools"' in observer_payload
    assert '"name": "list_names"' in observer_payload


def test_worker_reroute_retains_accepted_evidence_for_partial_outcome():
    from agents.worker import resolve_worker_display_refs, worker_chat

    def lookup(table_name: str):
        return {"rows": [{"table_name": table_name, "row_count": 55}]}

    model = _WorkerModel(
        [
            AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "lookup",
                        "args": {"table_name": "t_example"},
                        "id": "call-accepted-before-reroute",
                        "type": "tool_call",
                    }
                ],
            )
        ],
        observer_responses=[
            Observation(
                status="reroute",
                gap="Нужен другой tool для проверки контрольной суммы.",
                accepted_tool_call_ids=["call-accepted-before-reroute"],
                reroute_reason="missing_capability",
                required_capabilities=["sql_read"],
            )
        ],
    )

    graph_result = run_worker_graph(
        task="Получи число строк и проверь контрольную сумму.",
        system_prompt="Системный контекст",
        model=model,
        tools=(_as_tool(lookup),),
        max_steps=2,
    )

    assert graph_result.status == "reroute"
    assert graph_result.accepted_tool_call_ids == [
        "call-accepted-before-reroute"
    ]
    assert len(graph_result.display_items) == 1
    accepted_item = graph_result.display_items[0]
    assert accepted_item.name == "lookup"
    assert accepted_item.tool_call_id == "call-accepted-before-reroute"
    assert accepted_item.arguments == {"table_name": "t_example"}
    assert '"row_count": 55' in accepted_item.content
    assert accepted_item.evidence_id

    route = ToolRoute(tools=[], skills=[], schemas=[])
    with (
        patch("agents.worker.WORKER_MAX_REROUTES", 0),
        patch("agents.worker.select_chat_route", return_value=route),
        patch(
            "agents.worker.run_worker_graph",
            return_value=graph_result,
        ),
    ):
        outcome = worker_chat(
            "Получи число строк и проверь контрольную сумму."
        )

    assert outcome.status == "partial"
    assert outcome.unmet_requirements == [
        "Нужен другой tool для проверки контрольной суммы."
    ]
    assert len(outcome.evidence) == 1
    assert outcome.evidence[0].evidence_id == accepted_item.evidence_id
    assert outcome.evidence[0].tool_name == "lookup"
    assert outcome.evidence[0].compact_args == {
        "table_name": "t_example"
    }
    display_ref = outcome.evidence[0].display_ref
    assert display_ref is not None
    assert resolve_worker_display_refs([display_ref]) == [accepted_item]


def test_native_finish_after_semantic_mismatch_does_not_invent_reroute():
    lookup_calls = []

    def lookup():
        lookup_calls.append(True)
        return {"value": "wrong"}

    model = _WorkerModel(
        [
            AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "lookup",
                        "args": {},
                        "id": "call-lookup",
                        "type": "tool_call",
                    }
                ],
            ),
            _finish_message("Текущего результата достаточно."),
            AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "lookup",
                        "args": {},
                        "id": "call-lookup-retry",
                        "type": "tool_call",
                    }
                ],
            ),
            _finish_message("Нужное значение не подтверждено."),
        ],
        observer_responses=[
            Observation(
                summary="Получен неподходящий результат.",
                goal_satisfied=False,
                problem="Нужное значение не подтверждено.",
            ),
            Observation(
                summary="Повторно получен неподходящий результат.",
                goal_satisfied=False,
                problem="Нужное значение не подтверждено.",
            ),
        ],
    )

    result = run_worker_graph(
        task="Получи нужное значение.",
        system_prompt="Системный контекст",
        model=model,
        tools=(_as_tool(lookup),),
        max_steps=2,
    )

    assert result.answer == "Нужное значение не подтверждено."
    assert result.status == "complete"
    assert result.gap == "Нужное значение не подтверждено."
    assert len(lookup_calls) == 2


def test_worker_loop_does_not_rewrite_llm_tool_arguments_in_python():
    executed_queries = []

    def run_sql(query: str):
        executed_queries.append(query)
        return {"rows": [{"source_count": 5}]}

    wrong_sql = "SELECT COUNT(*) AS source_count FROM target_tables"
    correct_sql = (
        "SELECT COUNT(DISTINCT source_table) AS source_count "
        "FROM s2t_transformations "
        "WHERE target_table = 't_rate_rule_param' "
        "AND source_table IS NOT NULL AND TRIM(source_table) <> ''"
    )
    model = _WorkerModel(
        [
            AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "run_sql",
                        "args": {"query": wrong_sql},
                        "id": "call-wrong-table",
                        "type": "tool_call",
                    }
                ],
            ),
            AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "run_sql",
                        "args": {"query": correct_sql},
                        "id": "call-correct-table",
                        "type": "tool_call",
                    }
                ],
            ),
            _finish_message("Уникальных source_table: 5."),
        ],
        observer_responses=[
            Observation(
                summary="Подсчитаны строки target_tables, а не source_table S2T.",
                goal_satisfied=False,
                problem="Запрос выполнен не по s2t_transformations.",
            ),
            Observation(
                summary="Подсчитаны distinct непустые source_table S2T.",
                goal_satisfied=True,
            ),
        ],
    )

    result = run_worker_graph(
        task=(
            "В s2t_transformations для target_table=t_rate_rule_param "
            "посчитай distinct непустых source_table."
        ),
        system_prompt="Системный контекст",
        model=model,
        tools=(_as_tool(run_sql),),
        max_steps=2,
    )

    assert result.answer == "Уникальных source_table: 5."
    assert executed_queries == [wrong_sql, correct_sql]


def test_public_worker_contract_exposes_evidence_and_opaque_runtime_refs(
    caplog,
):
    from agents.contracts import parse_worker_request
    from agents.worker import (
        WorkerOutcome,
        resolve_worker_display_refs,
        worker_chat,
    )

    hidden_full_result = "FULL_RESULT_MUST_NOT_LEAVE_WORKER"
    graph_result = WorkerRunResult(
        answer="Готово.",
        display_items=[
            WorkerDisplayItem(
                name="list_files",
                content=hidden_full_result,
                evidence_id="evidence-files",
                tool_call_id="call-files",
                arguments={"limit": 3},
                preview="ограниченное превью",
                truncated=True,
            )
        ],
        cycle_history=[
            WorkerCycleTrace(
                cycle=1,
                tool_calls=[{"name": "list_files", "args": {}}],
                tool_results=[
                    {
                        "name": "list_files",
                        "tool_call_id": "call-files",
                        "content": "ограниченное превью",
                        "status": "success",
                        "is_error": False,
                    }
                ],
                observation=Observation(
                    summary="Файлы получены.",
                    goal_satisfied=False,
                    problem="Нужна дополнительная проверка.",
                    important_facts=["Найдено 3 файла."],
                ),
            )
        ],
        goal_satisfied=False,
        problem="Нужна дополнительная проверка.",
        facts=[
            {
                "text": "Найдено 3 файла.",
                "evidence_ids": ["evidence-files"],
            }
        ],
        accepted_tool_call_ids=["call-files"],
    )
    route = ToolRoute(
        tools=["list_files"],
        skills=["Excel и описания"],
        schemas=["Excel-маппинги"],
    )
    caplog.set_level("INFO", logger="agents.worker")
    with (
        patch("agents.worker.select_chat_route", return_value=route) as router,
        patch(
            "agents.worker.run_worker_graph",
            return_value=graph_result,
        ) as run_graph,
        patch("agents.worker.record_worker_route") as route_recorder,
        patch("agents.worker.record_worker_observation") as observation_recorder,
        patch("agents.worker.load_skills", wraps=load_skills),
        patch("agents.worker.load_schemas", wraps=load_schemas),
    ):
        result = worker_chat("  Покажи файлы  ")

    assert isinstance(result, WorkerOutcome)
    assert result.summary == (
        "Готово.\n"
        "Причина незавершённости: Нужна дополнительная проверка."
    )
    assert not hasattr(result, "gap")
    assert result.facts[0].text == "Найдено 3 файла."
    assert len(result.evidence) == 1
    assert result.evidence[0].evidence_id == "evidence-files"
    assert result.evidence[0].tool_name == "list_files"
    assert result.evidence[0].compact_args == {"limit": 3}
    assert result.evidence[0].preview == "ограниченное превью"
    assert result.evidence[0].truncated is True
    assert hidden_full_result not in result.model_dump_json()
    assert "display_ref" not in result.model_dump_json()
    refs = [
        item.display_ref for item in result.evidence if item.display_ref
    ]
    assert resolve_worker_display_refs(refs) == graph_result.display_items
    assert resolve_worker_display_refs(refs) == []
    routed_request = parse_worker_request(router.call_args.args[0])
    assert routed_request.current_task == "Покажи файлы"
    assert "history" not in router.call_args.kwargs
    assert router.call_args.kwargs["available_tools"] == tuple(
        tool
        for tool in get_worker_tools()
        if tool.name not in {"read_previous_result", "query_saved_result"}
    )
    assert result.previous_results == []
    graph_request = parse_worker_request(run_graph.call_args.kwargs["task"])
    assert graph_request.current_task == "Покажи файлы"
    assert "## Актуальная схема SQLite" not in run_graph.call_args.kwargs[
        "system_prompt"
    ]
    assert "history" not in run_graph.call_args.kwargs
    assert "file_id" not in run_graph.call_args.kwargs
    route_recorder.assert_called_once_with(
        worker_task="Покажи файлы",
        routing_attempt=1,
        tools=["list_files"],
        skills=["Excel и описания"],
        schemas=["Excel-маппинги"],
        gap=None,
    )
    observation_recorder.assert_called_once()
    observation_call = observation_recorder.call_args.kwargs
    assert observation_call["worker_task"] == "Покажи файлы"
    assert observation_call["cycle"] == 1
    assert observation_call["routing_attempt"] == 1
    assert observation_call["observation"]["status"] == "continue"
    assert observation_call["observation"]["gap"] == (
        "Нужна дополнительная проверка."
    )
    assert "Worker route:" in caplog.text
    assert "Worker observation:" in caplog.text
    assert '"status": "continue"' in caplog.text


def test_public_worker_allows_empty_palette_and_returns_observation():
    from agents.worker import worker_chat

    observation = Observation(
        summary="Форматирование известных фактов выполнено.",
        goal_satisfied=True,
    )
    graph_result = WorkerRunResult(
        answer="Готовый форматированный ответ",
        display_items=[],
        cycle_history=[
            WorkerCycleTrace(
                cycle=1,
                tool_calls=[],
                tool_results=[],
                observation=observation,
            )
        ],
        goal_satisfied=True,
    )
    route = ToolRoute(tools=[], skills=[], schemas=[])

    with (
        patch("agents.worker.select_chat_route", return_value=route),
        patch(
            "agents.worker.run_worker_graph",
            return_value=graph_result,
        ) as run_graph,
    ):
        result = worker_chat("Отформатируй уже известные пары")

    assert result.summary == graph_result.answer
    assert result.evidence == []
    assert result.datasets == []
    assert [
        tool.name for tool in run_graph.call_args.kwargs["tools"]
    ] == ["analyze_known_facts"]


def test_public_worker_adds_previous_result_reader_outside_router():
    from agents.contracts import WorkerRequestParts
    from agents.tools.saved_results import saved_result_store_scope
    from agents.worker import worker_chat

    route = ToolRoute(
        tools=["list_s2t_transformations"],
        skills=[],
        schemas=[],
    )
    graph_result = WorkerRunResult(
        answer="Прошлый результат доступен.",
        display_items=[],
        goal_satisfied=True,
    )

    with saved_result_store_scope() as store:
        reference = store.register_previous_result(
            source_tool="semantic_search_descriptions",
            source_tool_call_id="call-semantic",
            content=json.dumps({"rows": [{"column_name": "c_debtlimit"}]}),
            description="semantic_search_descriptions: найден кандидат колонки",
        )
        task = WorkerRequestParts(
            current_task="Получи S2T для найденной колонки.",
            previous_results=[reference],
        )
        with (
            patch("agents.worker.select_chat_route", return_value=route) as router,
            patch(
                "agents.worker.run_worker_graph",
                return_value=graph_result,
            ) as run_graph,
        ):
            result = worker_chat(task)

    assert result.summary == "Прошлый результат доступен."
    assert "read_previous_result" not in {
        tool.name for tool in router.call_args.kwargs["available_tools"]
    }
    assert [
        tool.name for tool in run_graph.call_args.kwargs["tools"]
    ] == ["list_s2t_transformations", "read_previous_result"]


def test_public_worker_reroutes_original_task_after_observer_request(
    monkeypatch,
):
    from agents.contracts import parse_worker_request
    from agents.worker import (
        WORKER_CAPABILITY_REROUTE_EXPERIMENT_ENV,
        worker_chat,
    )

    monkeypatch.setenv(WORKER_CAPABILITY_REROUTE_EXPERIMENT_ENV, "1")

    routes = [
        ToolRoute(
            tools=["list_s2t_transformations"],
            skills=["S2T-строки"],
            schemas=[],
        ),
        ToolRoute(
            tools=["list_s2t_transformations", "trace_transformation_path"],
            skills=[],
            schemas=["S2T-маппинг"],
        ),
    ]
    graph_results = [
        WorkerRunResult(
            answer="Нужна другая палитра.",
            cycle_history=[
                WorkerCycleTrace(
                    cycle=1,
                    tool_calls=[
                        {"name": "list_s2t_transformations", "args": {}}
                    ],
                    tool_results=[],
                    observation=Observation(
                        summary="Агрегирование не выполнено.",
                        goal_satisfied=False,
                        problem="Текущий tool не строит многошаговый путь с rules.",
                        reroute_required=True,
                    ),
                )
            ],
            goal_satisfied=False,
            problem="Текущий tool не строит многошаговый путь с rules.",
            reroute_required=True,
            stop_reason="missing_capability",
            reroute_reason="missing_capability",
            required_capabilities=["graph_read"],
            unmet_requirements=[
                "Текущий tool не строит многошаговый путь с rules."
            ],
        ),
        WorkerRunResult(
            answer="Максимум: t_example, 55 строк.",
            cycle_history=[
                WorkerCycleTrace(
                    cycle=1,
                    tool_calls=[{"name": "trace_transformation_path", "args": {}}],
                    tool_results=[],
                    observation=Observation(
                        summary="Максимум найден.",
                        goal_satisfied=True,
                        important_facts=["t_example: 55 строк."],
                    ),
                )
            ],
            goal_satisfied=True,
        ),
    ]

    with (
        patch(
            "agents.worker.select_chat_route",
            side_effect=routes,
        ) as router,
        patch(
            "agents.worker.run_worker_graph",
            side_effect=graph_results,
        ) as run_graph,
    ):
        result = worker_chat("  Найди target_table с максимумом строк  ")

    assert result.summary == "Максимум: t_example, 55 строк."
    assert router.call_count == 2
    assert parse_worker_request(
        router.call_args_list[0].args[0]
    ).current_task == "Найди target_table с максимумом строк"
    assert "reroute_context" not in router.call_args_list[0].kwargs
    assert parse_worker_request(
        router.call_args_list[1].args[0]
    ).current_task == "Найди target_table с максимумом строк"
    reroute_context = router.call_args_list[1].kwargs["reroute_context"]
    assert reroute_context == {
        "gap": "Текущий tool не строит многошаговый путь с rules.",
        "reason": "missing_capability",
        "required_capabilities": ["graph_read"],
        "previous_tool_palettes": [["list_s2t_transformations"]],
        "attempt": 1,
    }
    selected_tool_names = [
        [tool.name for tool in item.kwargs["tools"]]
        for item in run_graph.call_args_list
    ]
    assert selected_tool_names[0] == ["list_s2t_transformations"]
    assert set(selected_tool_names[1]) == {
        "list_s2t_transformations",
        "trace_transformation_path",
    }


def test_public_worker_adds_only_required_graph_capability_on_first_reroute(
    monkeypatch,
):
    from agents.tools import WORKER_GENERAL_FALLBACK_TOOL_NAMES
    from agents.worker import (
        WORKER_CAPABILITY_REROUTE_EXPERIMENT_ENV,
        worker_chat,
    )

    monkeypatch.setenv(WORKER_CAPABILITY_REROUTE_EXPERIMENT_ENV, "1")

    routes = [
        ToolRoute(
            tools=["read_s2t_by_target_table"],
            skills=[],
            schemas=[],
        ),
        ToolRoute(
            tools=[
                "read_s2t_by_target_table",
                "run_cypher",
            ],
            skills=[],
            schemas=[],
        ),
    ]
    graph_results = [
        WorkerRunResult(
            answer="Точных контрактов недостаточно.",
            goal_satisfied=False,
            reroute_required=True,
            problem="Нужен нестандартный срез данных.",
            stop_reason="missing_capability",
            reroute_reason="missing_capability",
            required_capabilities=["graph_read"],
            unmet_requirements=["Нужен нестандартный срез данных."],
        ),
        WorkerRunResult(answer="Срез получен."),
    ]

    with (
        patch(
            "agents.worker.select_chat_route",
            side_effect=routes,
        ) as router,
        patch(
            "agents.worker.run_worker_graph",
            side_effect=graph_results,
        ) as run_graph,
    ):
        result = worker_chat("Получи нестандартный срез S2T-данных")

    assert result.summary == "Срез получен."
    assert router.call_count == 2
    routed_names = [
        {tool.name for tool in call.kwargs["available_tools"]}
        for call in router.call_args_list
    ]
    assert routed_names[0].isdisjoint(WORKER_GENERAL_FALLBACK_TOOL_NAMES)
    assert "read_s2t_by_target_table" in routed_names[0]
    assert "run_cypher" not in routed_names[0]
    assert "run_cypher" in routed_names[1]
    assert (
        routed_names[1] & WORKER_GENERAL_FALLBACK_TOOL_NAMES
    ) == {"run_cypher"}
    assert router.call_args_list[0].kwargs["catalog_stage"] == "specialized_only"
    assert router.call_args_list[1].kwargs["catalog_stage"] == (
        "capability_expansion"
    )
    assert "reroute_context" not in router.call_args_list[0].kwargs
    assert router.call_args_list[1].kwargs["reroute_context"]["attempt"] == 1
    assert router.call_args_list[1].kwargs["reroute_context"][
        "required_capabilities"
    ] == ["graph_read"]
    assert "run_cypher" in {
        tool.name for tool in run_graph.call_args_list[1].kwargs["tools"]
    }


@pytest.mark.parametrize("flag_value", [None, "0"])
def test_default_reroute_opens_general_fallback_only_after_two_failures(
    monkeypatch,
    flag_value,
):
    from agents.tools import WORKER_GENERAL_FALLBACK_TOOL_NAMES
    from agents.worker import (
        WORKER_CAPABILITY_REROUTE_EXPERIMENT_ENV,
        worker_chat,
    )

    if flag_value is None:
        monkeypatch.delenv(
            WORKER_CAPABILITY_REROUTE_EXPERIMENT_ENV,
            raising=False,
        )
    else:
        monkeypatch.setenv(
            WORKER_CAPABILITY_REROUTE_EXPERIMENT_ENV,
            flag_value,
        )
    routes = [
        ToolRoute(
            tools=["read_s2t_by_target_table"],
            skills=[],
            schemas=[],
        ),
        ToolRoute(
            tools=[
                "read_s2t_by_target_table",
                "trace_transformation_path",
            ],
            skills=[],
            schemas=[],
        ),
        ToolRoute(
            tools=[
                "read_s2t_by_target_table",
                "trace_transformation_path",
                "run_cypher",
            ],
            skills=[],
            schemas=[],
        ),
    ]
    graph_results = [
        WorkerRunResult(
            answer="Нужна другая специализированная операция.",
            goal_satisfied=False,
            reroute_required=True,
            problem="Нужно проверить полный путь.",
            stop_reason="missing_capability",
            reroute_reason="missing_capability",
            required_capabilities=["graph_read"],
        ),
        WorkerRunResult(
            answer="Специализированной палитры недостаточно.",
            goal_satisfied=False,
            reroute_required=True,
            problem="Нужен общий graph reader.",
            stop_reason="missing_capability",
            reroute_reason="missing_capability",
            required_capabilities=["graph_read"],
        ),
        WorkerRunResult(answer="Готово."),
    ]

    with (
        patch(
            "agents.worker.select_chat_route",
            side_effect=routes,
        ) as router,
        patch(
            "agents.worker.run_worker_graph",
            side_effect=graph_results,
        ),
    ):
        result = worker_chat("Получи нестандартный срез")

    assert result.status == "complete"
    catalogs = [
        {tool.name for tool in call.kwargs["available_tools"]}
        for call in router.call_args_list
    ]
    assert catalogs[0].isdisjoint(WORKER_GENERAL_FALLBACK_TOOL_NAMES)
    assert catalogs[1].isdisjoint(WORKER_GENERAL_FALLBACK_TOOL_NAMES)
    assert WORKER_GENERAL_FALLBACK_TOOL_NAMES.issubset(catalogs[2])
    assert [
        call.kwargs["catalog_stage"] for call in router.call_args_list
    ] == ["specialized_only", "specialized_only", "general_fallback"]
    assert router.call_args_list[1].kwargs["reroute_context"] == {
        "gap": "Нужно проверить полный путь.",
        "previous_tool_palettes": [["read_s2t_by_target_table"]],
        "attempt": 1,
    }
    assert router.call_args_list[2].kwargs["reroute_context"] == {
        "gap": "Нужен общий graph reader.",
        "previous_tool_palettes": [
            ["read_s2t_by_target_table"],
            [
                "read_s2t_by_target_table",
                "trace_transformation_path",
            ],
        ],
        "attempt": 2,
    }
    assert all(
        "required_capabilities" not in call.kwargs.get(
            "reroute_context", {}
        )
        and "reason" not in call.kwargs.get("reroute_context", {})
        for call in router.call_args_list
    )


def test_public_worker_can_execute_repeated_reroute_palette(monkeypatch):
    from agents.worker import (
        WORKER_CAPABILITY_REROUTE_EXPERIMENT_ENV,
        worker_chat,
    )

    monkeypatch.setenv(WORKER_CAPABILITY_REROUTE_EXPERIMENT_ENV, "1")

    repeated_route = ToolRoute(
        tools=["list_s2t_transformations"],
        skills=["S2T-строки"],
        schemas=[],
    )
    graph_results = [
        WorkerRunResult(
            answer="Нужно исправить запрос.",
            goal_satisfied=False,
            problem="SQL не учитывает нужный фильтр.",
            reroute_required=True,
            stop_reason="wrong_arguments",
            reroute_reason="wrong_arguments",
            unmet_requirements=["Нужно исправить фильтр SQL."],
        ),
        WorkerRunResult(
            answer="Максимум: t_example, 55 строк.",
            goal_satisfied=True,
        ),
    ]

    with (
        patch(
            "agents.worker.select_chat_route",
            return_value=repeated_route,
        ) as router,
        patch(
            "agents.worker.run_worker_graph",
            side_effect=graph_results,
        ) as run_graph,
    ):
        result = worker_chat("Найди target_table с максимумом строк")

    assert result.summary == "Максимум: t_example, 55 строк."
    assert router.call_count == 2
    assert run_graph.call_count == 2
    assert [
        [tool.name for tool in item.kwargs["tools"]]
        for item in run_graph.call_args_list
    ] == [["list_s2t_transformations"], ["list_s2t_transformations"]]
    second_system_prompt = run_graph.call_args_list[1].kwargs["system_prompt"]
    assert "<reroute_feedback>" in second_system_prompt
    assert "SQL не учитывает нужный фильтр." in second_system_prompt
    assert "палитры требуемых возможностей" in second_system_prompt
    assert router.call_args_list[1].kwargs["reroute_context"] == {
        "gap": "SQL не учитывает нужный фильтр.",
        "reason": "wrong_arguments",
        "required_capabilities": [],
        "previous_tool_palettes": [["list_s2t_transformations"]],
        "attempt": 1,
    }


def test_wrong_arguments_retains_previously_expanded_general_tool(
    monkeypatch,
):
    from agents.worker import (
        WORKER_CAPABILITY_REROUTE_EXPERIMENT_ENV,
        worker_chat,
    )

    monkeypatch.setenv(WORKER_CAPABILITY_REROUTE_EXPERIMENT_ENV, "1")

    routes = [
        ToolRoute(
            tools=["read_s2t_by_target_table"],
            skills=[],
            schemas=[],
        ),
        ToolRoute(
            tools=["read_s2t_by_target_table", "run_sql"],
            skills=[],
            schemas=[],
        ),
        ToolRoute(
            tools=["read_s2t_by_target_table", "run_sql"],
            skills=[],
            schemas=[],
        ),
    ]
    graph_results = [
        WorkerRunResult(
            answer="Нужен SQL.",
            goal_satisfied=False,
            reroute_required=True,
            problem="Нужен нестандартный SQL срез.",
            stop_reason="missing_capability",
            reroute_reason="missing_capability",
            required_capabilities=["sql_read"],
        ),
        WorkerRunResult(
            answer="Исправь SQL args.",
            goal_satisfied=False,
            reroute_required=True,
            problem="SQL потерял фильтр.",
            stop_reason="wrong_arguments",
            reroute_reason="wrong_arguments",
        ),
        WorkerRunResult(answer="Готово."),
    ]

    with (
        patch(
            "agents.worker.select_chat_route",
            side_effect=routes,
        ) as router,
        patch(
            "agents.worker.run_worker_graph",
            side_effect=graph_results,
        ),
    ):
        result = worker_chat("Получи SQL-срез с фильтром")

    assert result.status == "complete"
    assert router.call_args_list[2].kwargs["catalog_stage"] == (
        "reroute_palette"
    )
    assert "run_sql" in {
        tool.name for tool in router.call_args_list[2].kwargs["available_tools"]
    }
    assert router.call_args_list[2].kwargs["reroute_context"]["reason"] == (
        "wrong_arguments"
    )
    assert router.call_args_list[2].kwargs["reroute_context"][
        "required_capabilities"
    ] == []
