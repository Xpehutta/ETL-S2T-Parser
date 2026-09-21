from copy import deepcopy
from contextlib import nullcontext
from unittest.mock import patch

import pytest
from langchain_core.messages import AIMessage, HumanMessage, SystemMessage

from agents.chat_graph import WorkerDisplayItem, WorkerRunResult
from agents.coordinator import CoordinatorAnswer


class _SupervisorModel:
    def __init__(self, responses):
        self.responses = list(responses)
        self.bound_tools = None
        self.messages = []

    def bind_tools(self, tools):
        self.bound_tools = list(tools)
        return self

    def invoke(self, messages, **kwargs):
        del kwargs
        self.messages.append(list(messages))
        return self.responses.pop(0)


def _delegate_message(
    resolved_references="",
    *,
    context="",
    call_id="delegate-1",
    extra_args=None,
):
    args = {"resolved_references": resolved_references, "context": context}
    args.update(extra_args or {})
    return AIMessage(
        content="",
        tool_calls=[
            {
                "name": "delegate_to_coordinator",
                "args": args,
                "id": call_id,
                "type": "tool_call",
            }
        ],
    )


def _supervisor_patches(model):
    return (
        patch("agents.supervisor.chat_model", model),
        patch("agents.supervisor.get_callback_handler", return_value=None),
        patch(
            "agents.supervisor.langfuse_trace_context",
            return_value=nullcontext(),
        ),
    )


def _conversation(model, invocation_index=0):
    messages = model.messages[invocation_index]
    assert isinstance(messages[0], SystemMessage)
    return [
        (
            "user" if isinstance(message, HumanMessage) else "assistant",
            message.content,
        )
        for message in messages[1:]
    ]


def test_supervisor_graph_routes_coordinator_directly_to_end():
    from agents.supervisor import build_supervisor_graph

    model = _SupervisorModel([])
    graph = build_supervisor_graph(model)
    graph_view = graph.get_graph()

    assert {"supervisor", "coordinator"}.issubset(graph_view.nodes)
    assert "limit" not in graph_view.nodes
    edges = {(edge.source, edge.target) for edge in graph_view.edges}
    assert ("__start__", "supervisor") in edges
    assert ("coordinator", "__end__") in edges
    assert ("coordinator", "supervisor") not in edges


def test_supervisor_graph_keeps_operation_routing_after_supervisor(
    monkeypatch,
):
    from agents.sql_risk_scope_contract import (
        OPERATION_SQL_RISK_SCOPE_EVIDENCE_EXPERIMENT_ENV,
    )
    from agents.supervisor import build_supervisor_graph

    monkeypatch.setenv(
        OPERATION_SQL_RISK_SCOPE_EVIDENCE_EXPERIMENT_ENV,
        "1",
    )
    graph = build_supervisor_graph(_SupervisorModel([]))
    graph_view = graph.get_graph()

    assert "operation_router" not in graph_view.nodes
    edges = {(edge.source, edge.target) for edge in graph_view.edges}
    assert ("__start__", "supervisor") in edges
    assert ("supervisor", "coordinator") in edges
    assert ("__start__", "coordinator") not in edges


def test_operation_scope_request_still_uses_supervisor_handoff(monkeypatch):
    from agents.sql_risk_scope_contract import (
        OPERATION_SQL_RISK_SCOPE_EVIDENCE_EXPERIMENT_ENV,
    )
    from agents.supervisor import supervisor_chat

    monkeypatch.setenv(
        OPERATION_SQL_RISK_SCOPE_EVIDENCE_EXPERIMENT_ENV,
        "1",
    )
    model = _SupervisorModel([_delegate_message()])
    model_patch, callback_patch, trace_patch = _supervisor_patches(model)
    with (
        model_patch,
        callback_patch,
        trace_patch,
        patch(
            "agents.supervisor.coordinator_chat",
            return_value=CoordinatorAnswer(
                answer="JOIN может условно размножить строки.",
                display_refs=[],
            ),
        ) as coordinator,
    ):
        result = supervisor_chat(
            "Оцени cardinality для src_alpha → tgt_beta."
        )

    assert result.answer == "JOIN может условно размножить строки."
    assert len(model.messages) == 1
    assert "верхний supervisor" in model.messages[0][0].content
    coordinator.assert_called_once()
    assert coordinator.call_args.args == (
        "Оцени cardinality для src_alpha → tgt_beta.",
    )
    assert coordinator.call_args.kwargs["context"] == ""
    assert "operation_route" not in coordinator.call_args.kwargs


def test_non_scope_request_is_delegated_without_stale_preroute(
    monkeypatch,
):
    from agents.sql_risk_scope_contract import (
        OPERATION_SQL_RISK_SCOPE_EVIDENCE_EXPERIMENT_ENV,
    )
    from agents.supervisor import supervisor_chat

    monkeypatch.setenv(
        OPERATION_SQL_RISK_SCOPE_EVIDENCE_EXPERIMENT_ENV,
        "1",
    )
    model = _SupervisorModel([_delegate_message()])
    model_patch, callback_patch, trace_patch = _supervisor_patches(model)
    with (
        model_patch,
        callback_patch,
        trace_patch,
        patch(
            "agents.supervisor.coordinator_chat",
            return_value=CoordinatorAnswer(answer="Данные прочитаны."),
        ) as coordinator,
    ):
        result = supervisor_chat("Покажи сохранённые данные")

    assert result.answer == "Данные прочитаны."
    assert len(model.messages) == 1
    coordinator.assert_called_once()
    assert coordinator.call_args.args == ("Покажи сохранённые данные",)
    assert coordinator.call_args.kwargs["context"] == ""
    assert "operation_route" not in coordinator.call_args.kwargs


def test_supervisor_prompt_keeps_decision_and_handoff_llm_driven():
    from agents.supervisor import _SUPERVISOR_PROMPT

    normalized_prompt = " ".join(_SUPERVISOR_PROMPT.split()).lower()
    assert "реши, ответить сразу или вызвать" in normalized_prompt
    assert "будет передан coordinator программно и дословно" in normalized_prompt
    assert "не добавляй новых целей" in normalized_prompt.lower()
    assert "не пересказывай, не сокращай" in normalized_prompt
    assert "не превращай его в план" in normalized_prompt
    assert "если `recent_history` пуст, оба поля" in normalized_prompt
    assert "при единственном однозначном референте" in normalized_prompt
    assert "«в ней» = таблица x" in normalized_prompt
    assert "относится к `resolved_references`, а не к context" in normalized_prompt
    assert "только компактные устойчивые правила" in normalized_prompt
    assert "в context остались только повторно применимые договорённости" in normalized_prompt


def test_supervisor_answers_directly_when_coordinator_is_not_needed():
    from agents.supervisor import supervisor_chat

    model = _SupervisorModel([AIMessage(content="Здравствуйте!")])
    stages = []

    def stage_scope(stage):
        stages.append(stage)
        return nullcontext()

    model_patch, callback_patch, trace_patch = _supervisor_patches(model)
    with (
        model_patch,
        callback_patch,
        trace_patch,
        patch("agents.supervisor.llm_stage", side_effect=stage_scope),
        patch("agents.supervisor.coordinator_chat") as coordinator,
    ):
        result = supervisor_chat("Привет")

    assert result == WorkerRunResult(answer="Здравствуйте!", display_items=[])
    coordinator.assert_not_called()
    assert len(model.bound_tools) == 1
    parameters = model.bound_tools[0]["function"]["parameters"]
    assert set(parameters["properties"]) == {"resolved_references", "context"}
    assert parameters["required"] == ["resolved_references", "context"]
    assert parameters["properties"]["context"]["maxLength"] == 4000
    assert (
        parameters["properties"]["resolved_references"]["maxLength"]
        == 4000
    )
    references_description = parameters["properties"]["resolved_references"][
        "description"
    ]
    context_description = parameters["properties"]["context"]["description"]
    assert "разрешения ссылок current_query" in references_description
    assert "пустая строка" in references_description
    assert "разовые объекты, ID, числа, результаты" in context_description
    assert parameters["additionalProperties"] is False
    assert stages == ["supervisor"]


def test_supervisor_retries_empty_decision_without_changing_history_payload():
    from agents.supervisor import supervisor_chat

    history = [
        {"role": "user", "content": "Работаем только с подтверждёнными данными."},
        {"role": "assistant", "content": "Принято."},
    ]
    model = _SupervisorModel(
        [
            AIMessage(content="   "),
            _delegate_message(),
        ]
    )
    model_patch, callback_patch, trace_patch = _supervisor_patches(model)
    with (
        model_patch,
        callback_patch,
        trace_patch,
        patch(
            "agents.supervisor.coordinator_chat",
            return_value=CoordinatorAnswer(
                answer="Тест-протокол сформирован.",
                display_refs=[],
            ),
        ) as coordinator,
    ):
        result = supervisor_chat(
            "Составь стандартный тест-протокол",
            history=history,
        )

    assert result.answer == "Тест-протокол сформирован."
    assert len(model.messages) == 2
    assert _conversation(model) == [
        (item["role"], item["content"]) for item in history
    ] + [("user", "Составь стандартный тест-протокол")]
    assert _conversation(model, 1) == _conversation(model)
    assert "предыдущий вызов не вернул" in model.messages[1][0].content.lower()
    coordinator.assert_called_once_with(
        "Составь стандартный тест-протокол",
        context="",
    )


def test_supervisor_rejects_repeated_empty_decision_before_direct_route():
    from agents.supervisor import supervisor_chat

    model = _SupervisorModel(
        [
            AIMessage(content=""),
            AIMessage(content=[]),
        ]
    )
    model_patch, callback_patch, trace_patch = _supervisor_patches(model)
    with (
        model_patch,
        callback_patch,
        trace_patch,
        patch("agents.supervisor.record_supervisor_decision") as record_decision,
        patch("agents.supervisor.coordinator_chat") as coordinator,
    ):
        try:
            supervisor_chat("Проверь данные")
        except RuntimeError as exc:
            assert "повторно вернул пустой ответ" in str(exc)
        else:
            raise AssertionError("Repeated empty supervisor response must fail")

    assert len(model.messages) == 2
    record_decision.assert_not_called()
    coordinator.assert_not_called()


def test_supervisor_keeps_last_six_history_messages_without_mutating_input():
    from agents.supervisor import supervisor_chat

    model = _SupervisorModel([AIMessage(content="Ответ без чтения данных.")])
    history = [
        {
            "role": "user" if index % 2 == 0 else "assistant",
            "content": f"history-{index}",
            "ui_only": f"metadata-{index}",
        }
        for index in range(8)
    ]
    original_history = deepcopy(history)
    model_patch, callback_patch, trace_patch = _supervisor_patches(model)
    with (
        model_patch,
        callback_patch,
        trace_patch,
        patch("agents.supervisor.coordinator_chat") as coordinator,
    ):
        result = supervisor_chat("  Текущий запрос отдельно  ", history=history)

    assert result.answer == "Ответ без чтения данных."
    coordinator.assert_not_called()
    assert len(model.messages) == 1
    assert isinstance(model.messages[0][0], SystemMessage)
    assert isinstance(model.messages[0][1], HumanMessage)
    assert _conversation(model) == [
        (item["role"], item["content"]) for item in history[-6:]
    ] + [("user", "Текущий запрос отдельно")]
    assert history == original_history


def test_supervisor_exposes_history_to_direct_answer_model_without_coordinator():
    from agents.supervisor import supervisor_chat

    history = [
        {"role": "user", "content": "Ранее мы договорились отвечать кратко."},
        {"role": "assistant", "content": "Хорошо."},
    ]
    model = _SupervisorModel([AIMessage(content="Краткий ответ.")])
    model_patch, callback_patch, trace_patch = _supervisor_patches(model)
    with (
        model_patch,
        callback_patch,
        trace_patch,
        patch("agents.supervisor.coordinator_chat") as coordinator,
    ):
        result = supervisor_chat("Что ты умеешь?", history=history)

    assert result == WorkerRunResult(answer="Краткий ответ.", display_items=[])
    assert _conversation(model) == [
        (item["role"], item["content"]) for item in history
    ] + [("user", "Что ты умеешь?")]
    coordinator.assert_not_called()


def test_supervisor_keeps_assistant_only_assumption_non_authoritative():
    from agents.supervisor import supervisor_chat

    history = [
        {
            "role": "user",
            "content": "Не выбирай объект за меня: я назову его позже.",
        },
        {
            "role": "assistant",
            "content": "Буду считать, что выбран объект assistant_choice.",
        },
    ]
    model = _SupervisorModel(
        [AIMessage(content="Какой именно объект вы выбираете?")]
    )
    model_patch, callback_patch, trace_patch = _supervisor_patches(model)
    with (
        model_patch,
        callback_patch,
        trace_patch,
        patch("agents.supervisor.coordinator_chat") as coordinator,
    ):
        result = supervisor_chat("Прочитай его.", history=history)

    assert result.answer == "Какой именно объект вы выбираете?"
    coordinator.assert_not_called()
    assert _conversation(model) == [
        ("user", history[0]["content"]),
        ("assistant", history[1]["content"]),
        ("user", "Прочитай его."),
    ]


def test_supervisor_preserves_latest_user_confirmed_reference():
    from agents.supervisor import supervisor_chat

    history = [
        {"role": "user", "content": "Рабочий объект = first_choice."},
        {"role": "assistant", "content": "Принял first_choice."},
        {
            "role": "user",
            "content": "Отменяю прежнее правило: рабочий объект = final_choice.",
        },
        {"role": "assistant", "content": "Принял final_choice."},
    ]
    model = _SupervisorModel(
        [_delegate_message(context="Рабочий объект = final_choice.")]
    )
    model_patch, callback_patch, trace_patch = _supervisor_patches(model)
    with (
        model_patch,
        callback_patch,
        trace_patch,
        patch(
            "agents.supervisor.coordinator_chat",
            return_value=CoordinatorAnswer(answer="Готово."),
        ) as coordinator,
    ):
        result = supervisor_chat("Прочитай рабочий объект.", history=history)

    assert result.answer == "Готово."
    coordinator.assert_called_once_with(
        "Прочитай рабочий объект.",
        context="Рабочий объект = final_choice.",
    )
    assert _conversation(model) == [
        (item["role"], item["content"]) for item in history
    ] + [("user", "Прочитай рабочий объект.")]


def test_supervisor_preserves_ambiguous_user_history_for_clarification():
    from agents.supervisor import supervisor_chat

    history = [
        {
            "role": "user",
            "content": "Рассматриваю alpha и beta; конкретный объект не выбран.",
        },
        {"role": "assistant", "content": "Выбор ещё не сделан."},
    ]
    model = _SupervisorModel(
        [AIMessage(content="Уточните, нужен alpha или beta?")]
    )
    model_patch, callback_patch, trace_patch = _supervisor_patches(model)
    with (
        model_patch,
        callback_patch,
        trace_patch,
        patch("agents.supervisor.coordinator_chat") as coordinator,
    ):
        result = supervisor_chat("Прочитай его.", history=history)

    assert result.answer == "Уточните, нужен alpha или beta?"
    coordinator.assert_not_called()
    assert _conversation(model) == [
        (item["role"], item["content"]) for item in history
    ] + [("user", "Прочитай его.")]


def test_supervisor_historyless_request_is_one_user_turn():
    from agents.supervisor import supervisor_chat

    model = _SupervisorModel([_delegate_message()])
    model_patch, callback_patch, trace_patch = _supervisor_patches(model)
    with (
        model_patch,
        callback_patch,
        trace_patch,
        patch(
            "agents.supervisor.coordinator_chat",
            return_value=CoordinatorAnswer(answer="Готово."),
        ) as coordinator,
    ):
        result = supervisor_chat("Прочитай self_contained_object.")

    assert result.answer == "Готово."
    coordinator.assert_called_once_with(
        "Прочитай self_contained_object.",
        context="",
    )
    assert _conversation(model) == [
        ("user", "Прочитай self_contained_object.")
    ]


def test_supervisor_self_contained_delegate_does_not_forward_raw_history():
    from agents.supervisor import supervisor_chat

    history = [
        {"role": "user", "content": "HISTORY_USER_SENTINEL"},
        {"role": "assistant", "content": "HISTORY_ASSISTANT_SENTINEL"},
    ]
    model = _SupervisorModel([_delegate_message()])
    model_patch, callback_patch, trace_patch = _supervisor_patches(model)
    with (
        model_patch,
        callback_patch,
        trace_patch,
        patch(
            "agents.supervisor.coordinator_chat",
            return_value=CoordinatorAnswer(answer="Три файла.", display_refs=[]),
        ) as coordinator,
    ):
        result = supervisor_chat("Сколько файлов загружено?", history=history)

    assert result.answer == "Три файла."
    coordinator.assert_called_once_with(
        "Сколько файлов загружено?",
        context="",
    )
    delegated_task = coordinator.call_args.args[0]
    delegated_context = coordinator.call_args.kwargs["context"]
    assert "HISTORY_USER_SENTINEL" not in delegated_task
    assert "HISTORY_ASSISTANT_SENTINEL" not in delegated_task
    assert delegated_context == ""


def test_supervisor_discards_handoff_fields_when_history_is_empty():
    from agents.supervisor import supervisor_chat

    model = _SupervisorModel(
        [
            _delegate_message(
                "file_id=9102 и comparison key=order_id.",
                context="Проверить tgt_pk по ключу order_id.",
            )
        ]
    )
    model_patch, callback_patch, trace_patch = _supervisor_patches(model)
    with (
        model_patch,
        callback_patch,
        trace_patch,
        patch(
            "agents.supervisor.coordinator_chat",
            return_value=CoordinatorAnswer(
                answer="Тест-протокол сформирован.",
                display_refs=[],
            ),
        ) as coordinator,
        patch("agents.supervisor.record_supervisor_decision") as record_decision,
    ):
        result = supervisor_chat(
            "Составь протокол для file_id=9102, ключ order_id",
            history=[],
        )

    assert result.answer == "Тест-протокол сформирован."
    coordinator.assert_called_once_with(
        "Составь протокол для file_id=9102, ключ order_id",
        context="",
    )
    record_decision.assert_called_once_with(
        route="delegate",
        resolved_references="",
        context="",
    )


def test_historyless_supervisor_discards_non_string_handoff_fields():
    from agents.supervisor import supervisor_chat

    model = _SupervisorModel(
        [
            _delegate_message(
                [],
                context={},
            )
        ]
    )
    model_patch, callback_patch, trace_patch = _supervisor_patches(model)
    with (
        model_patch,
        callback_patch,
        trace_patch,
        patch(
            "agents.supervisor.coordinator_chat",
            return_value=CoordinatorAnswer(answer="Данные прочитаны."),
        ) as coordinator,
    ):
        result = supervisor_chat("Покажи сохранённые данные", history=[])

    assert result.answer == "Данные прочитаны."
    coordinator.assert_called_once_with(
        "Покажи сохранённые данные",
        context="",
    )


@pytest.mark.parametrize(
    "content",
    [
        '{"resolved_references": "", "context": ""}',
        '```json\n{"resolved_references": [], "context": []}\n```',
    ],
)
def test_supervisor_recovers_pseudo_delegate_json_from_local_model(content):
    from agents.supervisor import supervisor_chat

    model = _SupervisorModel([AIMessage(content=content)])
    model_patch, callback_patch, trace_patch = _supervisor_patches(model)
    with (
        model_patch,
        callback_patch,
        trace_patch,
        patch(
            "agents.supervisor.coordinator_chat",
            return_value=CoordinatorAnswer(answer="Данные прочитаны."),
        ) as coordinator,
    ):
        result = supervisor_chat("Покажи сохранённые данные")

    assert result.answer == "Данные прочитаны."
    coordinator.assert_called_once_with(
        "Покажи сохранённые данные",
        context="",
    )


def test_supervisor_keeps_unrelated_json_as_direct_answer():
    from agents.supervisor import supervisor_chat

    model = _SupervisorModel([AIMessage(content='{"status": "ok"}')])
    model_patch, callback_patch, trace_patch = _supervisor_patches(model)
    with (
        model_patch,
        callback_patch,
        trace_patch,
        patch("agents.supervisor.coordinator_chat") as coordinator,
    ):
        result = supervisor_chat("Верни JSON")

    assert result.answer == '{"status": "ok"}'
    coordinator.assert_not_called()


def test_supervisor_delegates_whole_goal_and_returns_coordinator_result():
    from agents.supervisor import supervisor_chat

    resolved_references = "«в нём» = файл с file_id=42."
    common_context = (
        "Под словом «листы» в этом диалоге всегда понимаются листы Excel."
    )
    model = _SupervisorModel(
        [_delegate_message(resolved_references, context=common_context)]
    )
    coordinator_result = CoordinatorAnswer(
        answer="В файле два листа: S2T и Дополнительные объекты.",
        display_refs=["ref-sheets"],
    )
    resolved_items = [
        WorkerDisplayItem(
            name="list_sheets",
            content='["S2T", "Дополнительные объекты"]',
        )
    ]
    history = [
        {"role": "user", "content": "Открой файл 42"},
        {"role": "assistant", "content": "Файл выбран."},
    ]
    model_patch, callback_patch, trace_patch = _supervisor_patches(model)
    with (
        model_patch,
        callback_patch,
        trace_patch,
        patch(
            "agents.supervisor.coordinator_chat",
            return_value=coordinator_result,
        ) as coordinator,
        patch(
            "agents.supervisor.resolve_worker_display_refs",
            return_value=resolved_items,
        ) as resolve_refs,
    ):
        result = supervisor_chat(
            "Какие в нём листы и сколько их?",
            history=history,
            session_id="session-1",
        )

    assert result == WorkerRunResult(
        answer=coordinator_result.answer,
        display_items=resolved_items,
    )
    coordinator.assert_called_once_with(
        "Какие в нём листы и сколько их?\n\n"
        "Однозначно разрешённые ссылки из истории:\n"
        "«в нём» = файл с file_id=42.",
        context=common_context,
    )
    resolve_refs.assert_called_once_with(["ref-sheets"])
    assert _conversation(model) == [
        (item["role"], item["content"]) for item in history
    ] + [("user", "Какие в нём листы и сколько их?")]
    assert len(model.messages) == 1


def test_supervisor_data_path_forwards_only_resolved_history_and_bounded_context():
    from agents.coordinator import COORDINATOR_CONTEXT_MAX_CHARS
    from agents.supervisor import supervisor_chat

    history = [
        {"role": "user", "content": "Работаем с таблицей source.orders."},
        {
            "role": "assistant",
            "content": "В ней есть 100 строк; неподтверждённый комментарий.",
        },
    ]
    resolved_references = "  «в ней» = таблица source.orders.  "
    context = "П" * COORDINATOR_CONTEXT_MAX_CHARS
    model = _SupervisorModel(
        [
            _delegate_message(
                resolved_references,
                context=context,
            )
        ]
    )
    model_patch, callback_patch, trace_patch = _supervisor_patches(model)
    with (
        model_patch,
        callback_patch,
        trace_patch,
        patch(
            "agents.supervisor.coordinator_chat",
            return_value=CoordinatorAnswer(
                answer="В таблице 100 строк.",
                display_refs=[],
            ),
        ) as coordinator,
    ):
        result = supervisor_chat("  Посчитай строки в ней  ", history=history)

    assert result.answer == "В таблице 100 строк."
    assert _conversation(model) == [
        (item["role"], item["content"]) for item in history
    ] + [("user", "Посчитай строки в ней")]
    coordinator.assert_called_once_with(
        "Посчитай строки в ней\n\n"
        "Однозначно разрешённые ссылки из истории:\n"
        "«в ней» = таблица source.orders.",
        context="П" * COORDINATOR_CONTEXT_MAX_CHARS,
    )
    delegated_task = coordinator.call_args.args[0]
    assert "неподтверждённый комментарий" not in delegated_task


def test_supervisor_native_handoff_accepts_bounded_string_fields():
    from agents.coordinator import COORDINATOR_CONTEXT_MAX_CHARS
    from agents.supervisor import _parse_delegate_handoff

    resolved_references = "Р" * COORDINATOR_CONTEXT_MAX_CHARS
    context = "К" * COORDINATOR_CONTEXT_MAX_CHARS

    assert _parse_delegate_handoff(
        _delegate_message(resolved_references, context=context)
    ) == (resolved_references, context)


@pytest.mark.parametrize(
    ("field_name", "field_value"),
    [
        ("resolved_references", None),
        ("resolved_references", 42),
        ("context", ["rule"]),
    ],
)
def test_supervisor_native_handoff_rejects_non_string_fields(
    field_name,
    field_value,
):
    from agents.supervisor import _parse_delegate_handoff

    decision = _delegate_message()
    decision.tool_calls[0]["args"][field_name] = field_value

    with pytest.raises(RuntimeError, match=f"не-string поле .*{field_name}"):
        _parse_delegate_handoff(decision)


@pytest.mark.parametrize("field_name", ["resolved_references", "context"])
def test_supervisor_native_handoff_rejects_overlong_fields(field_name):
    from agents.coordinator import COORDINATOR_CONTEXT_MAX_CHARS
    from agents.supervisor import _parse_delegate_handoff

    decision = _delegate_message()
    decision.tool_calls[0]["args"][field_name] = (
        "X" * (COORDINATOR_CONTEXT_MAX_CHARS + 1)
    )

    with pytest.raises(RuntimeError, match=f"лимит поля .*{field_name}"):
        _parse_delegate_handoff(decision)


def test_supervisor_native_handoff_rejects_non_mapping_arguments():
    from agents.supervisor import _parse_delegate_handoff

    decision = _delegate_message()
    decision.tool_calls[0]["args"] = []

    with pytest.raises(RuntimeError, match="не-object arguments"):
        _parse_delegate_handoff(decision)


def test_supervisor_native_handoff_rejects_wrong_or_multiple_tool_calls():
    from agents.supervisor import _parse_delegate_handoff

    wrong_name = _delegate_message()
    wrong_name.tool_calls[0]["name"] = "unexpected_delegate"
    with pytest.raises(RuntimeError, match="ровно один native call"):
        _parse_delegate_handoff(wrong_name)

    multiple = _delegate_message()
    multiple.tool_calls.append(deepcopy(multiple.tool_calls[0]))
    with pytest.raises(RuntimeError, match="ровно один native call"):
        _parse_delegate_handoff(multiple)


def test_supervisor_ignores_unexpected_llm_task_rewrite():
    from agents.supervisor import supervisor_chat

    decision = _delegate_message(
        extra_args={
            "task": "Покажи только один файл",
            "tools": ["list_files"],
        },
    )
    model = _SupervisorModel([decision])
    model_patch, callback_patch, trace_patch = _supervisor_patches(model)
    with (
        model_patch,
        callback_patch,
        trace_patch,
        patch(
            "agents.supervisor.coordinator_chat",
            return_value=CoordinatorAnswer(
                answer="Найдено три файла.",
                display_refs=[],
            ),
        ) as coordinator,
    ):
        result = supervisor_chat("Покажи файлы")

    assert result.answer == "Найдено три файла."
    coordinator.assert_called_once_with("Покажи файлы", context="")
    assert len(model.messages) == 1


def test_supervisor_passes_current_query_verbatim_instead_of_llm_rewrite():
    from agents.supervisor import supervisor_chat

    decision = _delegate_message(
        extra_args={"task": "Проверь две таблицы через SQLite."},
    )
    model = _SupervisorModel([decision])
    model_patch, callback_patch, trace_patch = _supervisor_patches(model)
    query = "Через Neo4j найди путь от source_a до target_b. Не используй SQLite."
    with (
        model_patch,
        callback_patch,
        trace_patch,
        patch(
            "agents.supervisor.coordinator_chat",
            return_value=CoordinatorAnswer(answer="Путь найден.", display_refs=[]),
        ) as coordinator,
    ):
        result = supervisor_chat(
            query,
            history=[{"role": "assistant", "content": "Прошлый ответ."}],
        )

    assert result.answer == "Путь найден."
    coordinator.assert_called_once_with(
        query,
        context="",
    )


def test_supervisor_prompt_is_generic_and_preserves_semantics():
    from agents.supervisor import _SUPERVISOR_PROMPT, _delegate_tool_schema

    normalized_prompt = " ".join(_SUPERVISOR_PROMPT.split()).lower()
    assert "программно и дословно" in normalized_prompt
    assert "не пересказывай, не сокращай, не исправляй" in normalized_prompt
    assert "устойчивые правила и устоявшиеся идеи" in normalized_prompt
    assert "исходных ролях user/assistant" in normalized_prompt
    assert "только user может выбрать или подтвердить" in normalized_prompt
    assert "имеет приоритет" in normalized_prompt
    assert "разрешения ссылок current_query" in str(_delegate_tool_schema()).lower()
    assert "не помещай в context" in normalized_prompt
    assert "current_query или его сокращённый пересказ" in normalized_prompt
    assert "конкретные объекты, id, имена, числа и результаты" in normalized_prompt
    for domain_detail in ("Neo4j", "SQLite", "s2t_transformations", "file_id"):
        assert domain_detail not in _SUPERVISOR_PROMPT
