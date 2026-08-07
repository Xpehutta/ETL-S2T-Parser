import json
from unittest.mock import MagicMock, patch

import pytest
from langchain_core.messages import AIMessage
from langchain_core.tools import StructuredTool

from agents.agent import (
    _select_chat_route,
    agent_chat,
    get_header_decision,
)
from agents.tools import get_tools, load_skills
from agents.tools.routing import (
    SKILL_CATALOG,
    ToolRoute,
    ToolRoutingError,
)


def _decision(kind, **payload):
    return AIMessage(content=json.dumps({"kind": kind, **payload}, ensure_ascii=False))


def _as_tool(function, name=None):
    tool_name = name or function.__name__
    return StructuredTool.from_function(
        func=function,
        name=tool_name,
        description=f"Test tool {tool_name}",
    )


@pytest.fixture
def mock_llm_success():
    with (
        patch(
            "agents.agent.predict_header_row",
            side_effect=RuntimeError("CatBoost unavailable"),
        ),
        patch("agents.agent.call_header_model_with_retry") as mock_call,
    ):
        mock_call.return_value = (
            '{"header_start_row": 0, "header_rows": 1, '
            '"nested": false, "explanation": "Test decision"}'
        )
        yield mock_call


def test_get_header_decision_single_row_header(mock_llm_success):
    preview_rows = [
        ["Name", "Age", "City"],
        ["John", 30, "New York"],
        ["Jane", 25, "London"],
    ]
    assert get_header_decision("Sheet1", preview_rows) == (0, 1, False)


def test_get_header_decision_limits_preview_to_four_rows():
    with (
        patch(
            "agents.agent.predict_header_row",
            side_effect=RuntimeError("CatBoost unavailable"),
        ),
        patch("agents.agent.call_header_model_with_retry") as mock_call,
    ):
        mock_call.return_value = (
            '{"header_start_row": 0, "header_rows": 1, "nested": false}'
        )
        get_header_decision("SheetLimited", [[f"row-{i}"] for i in range(6)])
        user_prompt = mock_call.call_args.args[1]
        assert "row-3" in user_prompt
        assert "row-4" not in user_prompt


def test_get_header_decision_multi_row_header():
    with (
        patch(
            "agents.agent.predict_header_row",
            side_effect=RuntimeError("CatBoost unavailable"),
        ),
        patch("agents.agent.call_header_model_with_retry") as mock_call,
    ):
        mock_call.return_value = (
            '{"header_start_row": 0, "header_rows": 2, "nested": true}'
        )
        preview_rows = [
            ["Name", "Name", "Age", "Age"],
            ["First", "Last", "Years", "Months"],
            ["John", "Doe", 30, 360],
        ]
        assert get_header_decision("Sheet2", preview_rows) == (0, 2, True)


def test_get_header_decision_propagates_llm_failure():
    preview_rows = [["Column A", "Column B"], ["Data 1", "Data 2"]]
    with (
        patch(
            "agents.agent.predict_header_row",
            side_effect=RuntimeError("CatBoost unavailable"),
        ),
        patch(
            "agents.agent.call_header_model_with_retry",
            side_effect=Exception("API error"),
        ),
    ):
        with pytest.raises(Exception, match="API error"):
            get_header_decision("Sheet3", preview_rows)


def test_get_header_decision_uses_catboost_before_llm():
    preview_rows = [["report title"], ["Name"], ["Alice"]]
    with (
        patch("agents.agent.predict_header_row", return_value=1),
        patch("agents.agent.call_header_model_with_retry") as mock_llm,
    ):
        assert get_header_decision("SheetCatBoost", preview_rows) == (1, 1, False)

    mock_llm.assert_not_called()


def test_agent_chat_delegates_to_native_tool_graph():
    history = [{"role": "user", "content": "Контекст"}]
    with (
        patch("agents.agent.run_agent_graph", return_value="ok") as run_graph,
        patch(
            "agents.agent._get_langfuse_callbacks",
            return_value=["callback"],
        ),
        patch(
            "agents.agent._select_chat_route",
            return_value=ToolRoute(
                tools=["list_sheets"],
                skills=["Excel и описания"],
            ),
        ) as route_agent,
        patch(
            "agents.agent.load_skills",
            wraps=load_skills,
        ) as load_runtime_skills,
    ):
        out = agent_chat(
            "Покажи листы",
            file_id=42,
            history=history,
            session_id="chat-session-1",
            user_id="user-1",
        )

    assert out == "ok"
    kwargs = run_graph.call_args.kwargs
    assert kwargs["user_query"] == "Покажи листы"
    assert kwargs["model"] is not None
    assert kwargs["tools"]
    assert kwargs["file_id"] == 42
    assert kwargs["history"] == [{"role": "user", "content": "Контекст"}]
    assert kwargs["session_id"] == "chat-session-1"
    assert kwargs["user_id"] == "user-1"
    assert kwargs["callbacks"] == ["callback"]
    assert [tool.name for tool in kwargs["tools"]] == ["list_sheets"]
    assert kwargs["trace_tags"] == ["chat"]
    assert kwargs["trace_metadata"] == {"file_id": 42}
    assert "read-only" in kwargs["system_prompt"]
    assert "# Навыки агента" not in kwargs["system_prompt"]
    assert "## Excel и описания" in kwargs["system_prompt"]
    assert "## Neo4j" not in kwargs["system_prompt"]
    assert len(kwargs["system_prompt"]) < 7000
    route_args = route_agent.call_args
    assert route_args.args == ("Покажи листы", history)
    assert route_args.kwargs["model"] is not None
    assert route_args.kwargs["available_tools"] == get_tools()
    assert route_args.kwargs["callbacks"] == ["callback"]
    load_runtime_skills.assert_called_once_with(("Excel и описания",))


class _ToolRouterModel:
    def __init__(self, result):
        self.result = result
        self.messages = None
        self.config = None

    def invoke(self, messages, config=None):
        self.messages = messages
        self.config = config
        if isinstance(self.result, Exception):
            raise self.result
        return self.result


class _SequenceToolRouterModel:
    def __init__(self, results):
        self.results = list(results)
        self.calls = []
        self.bound_tools = []
        self.tool_choice = None

    def bind_tools(self, tools, tool_choice=None):
        self.bound_tools = list(tools)
        self.tool_choice = tool_choice
        return self

    def invoke(self, messages, config=None):
        self.calls.append((messages, config))
        result = self.results.pop(0)
        if isinstance(result, Exception):
            raise result
        return result


def test_chat_tool_router_parses_raw_llm_json_selection():
    cases = [
        (["run_sql"], ["SQLite SQL"], ["run_sql"], ["SQLite SQL"]),
        (
            ["run_sql"],
            ["S2T-строки"],
            ["run_sql"],
            ["S2T-строки"],
        ),
        (
            ["search_excel_values", "get_excel_row"],
            ["Excel и описания"],
            ["search_excel_values", "get_excel_row"],
            ["Excel и описания"],
        ),
        (
            ["list_files", "list_files"],
            ["Excel и описания", "Excel и описания"],
            ["list_files"],
            ["Excel и описания"],
        ),
    ]
    for tool_names, skill_names, expected_tools, expected_skills in cases:
        model = _ToolRouterModel(
            AIMessage(
                content=json.dumps(
                    {"tools": tool_names, "skills": skill_names},
                    ensure_ascii=False,
                )
            )
        )

        result = _select_chat_route(
            "Запрос без эвристически значимых слов",
            model=model,
            available_tools=get_tools(),
            callbacks=["router-callback"],
        )

        assert result.tools == expected_tools
        assert result.skills == expected_skills
        assert model.config == {"callbacks": ["router-callback"]}

    schema = ToolRoute.model_json_schema()
    assert schema["required"] == ["tools", "skills"]
    assert "reason" not in schema["properties"]


def test_router_skill_catalog_matches_lazy_runtime_sections():
    for skill_name in SKILL_CATALOG:
        assert f"## {skill_name}" in load_skills((skill_name,))


def test_chat_tool_router_passes_query_history_and_catalog_to_llm():
    history = [
        {
            "role": "assistant",
            "content": "```sql\nselect count(*) from files\n```",
        }
    ]
    model = _ToolRouterModel(
        AIMessage(
            content=(
                '{"tools":["run_sql"],'
                '"skills":["SQLite SQL","S2T-строки"]}'
            )
        )
    )

    route = _select_chat_route(
        "Выполни этот запрос",
        history,
        model=model,
        available_tools=get_tools(),
    )
    assert route.tools == ["run_sql"]
    assert route.skills == ["SQLite SQL", "S2T-строки"]

    payload = json.loads(model.messages[1].content)
    assert payload["current_query"] == "Выполни этот запрос"
    assert payload["recent_history"] == history
    assert {item["name"] for item in payload["available_tools"]} == {
        tool.name for tool in get_tools()
    }
    assert all(item["description"] for item in payload["available_tools"])
    run_sql_description = next(
        item["description"]
        for item in payload["available_tools"]
        if item["name"] == "run_sql"
    )
    assert "составленный агентом" in run_sql_description
    assert "выбирай list_s2t_table_names" in run_sql_description
    assert "а не этот tool" in run_sql_description
    table_names_description = next(
        item["description"]
        for item in payload["available_tools"]
        if item["name"] == "list_s2t_table_names"
    )
    assert "уникальные имена S2T-таблиц" in table_names_description
    assert "детерминированный инструмент" in table_names_description
    sql_graph_description = next(
        item["description"]
        for item in payload["available_tools"]
        if item["name"] == "visualize_sql_lineage"
    )
    assert "конкретного SQL-текста" in sql_graph_description
    assert "никогда не подменяй" in sql_graph_description
    global_graph_description = next(
        item["description"]
        for item in payload["available_tools"]
        if item["name"] == "visualize_s2t_table_graph"
    )
    assert "глобальный" in global_graph_description
    assert "Не используй для конкретного" in global_graph_description
    assert sum(
        len(item["description"]) for item in payload["available_tools"]
    ) < 7000
    assert payload["available_skills"] == SKILL_CATALOG
    assert "description: это контракт" in model.messages[0].content
    assert "компактную палитру" in model.messages[0].content
    assert "обычно от одного до четырёх" in model.messages[0].content
    assert "проверенные" in model.messages[0].content
    assert "результаты предыдущих шагов" in model.messages[0].content
    assert "Не наследуй прошлый tool" in model.messages[0].content
    assert "planner сам выберет порядок" in model.messages[0].content
    assert "универсальный read-only tool" in model.messages[0].content
    assert "не добавляй универсальный tool" in model.messages[0].content
    assert "самостоятельно составить и выполнить SQL" not in model.messages[0].content
    assert "parse_sql_column_lineage" not in model.messages[0].content
    assert "Skills выбирай независимо" in model.messages[0].content


def test_planner_requires_tool_call_for_an_unfinished_tool_step():
    from agents.chat_graph import _planner_instruction

    instruction = _planner_instruction(["first_tool", "second_tool"])

    assert "Не описывай будущий вызов словами" in instruction
    assert "без tool_call не считается выполнением шага" in instruction


def test_chat_tool_router_accepts_raw_json_markdown_fence():
    model = _ToolRouterModel(
        AIMessage(
            content=(
                '```json\n{"tools":["run_sql"],'
                '"skills":["SQLite SQL"]}\n```'
            )
        )
    )

    route = _select_chat_route(
        "Выполни запрос",
        model=model,
        available_tools=get_tools(),
    )
    assert route.tools == ["run_sql"]
    assert route.skills == ["SQLite SQL"]


def test_chat_tool_router_repairs_invalid_raw_json_with_one_more_llm_call():
    model = _SequenceToolRouterModel(
        [
            AIMessage(
                content=(
                    '{"tools":["list_s2t_table_names"],'
                    '"skills":["S2T-строки"],'
                    '"description":"лишнее пояснение"}'
                )
            ),
            AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "select_tools_and_skills",
                        "args": {
                            "tools": ["list_s2t_table_names"],
                            "skills": ["S2T-строки"],
                        },
                        "id": "route-repair-1",
                        "type": "tool_call",
                    }
                ],
            ),
        ]
    )

    route = _select_chat_route(
        "Покажи пересечение ролей таблиц",
        model=model,
        available_tools=get_tools(),
        callbacks=["router-callback"],
    )

    assert route.tools == ["list_s2t_table_names"]
    assert route.skills == ["S2T-строки"]
    assert len(model.calls) == 2
    assert model.tool_choice == "select_tools_and_skills"
    assert model.bound_tools[0]["function"]["name"] == "select_tools_and_skills"
    repair_messages, repair_config = model.calls[1]
    assert repair_config == {"callbacks": ["router-callback"]}
    assert "лишнее пояснение" in str(repair_messages[-2].content)
    assert "двумя и только двумя ключами" in str(repair_messages[-1].content)
    assert "Не добавляй description" in str(repair_messages[-1].content)


def test_chat_tool_router_rejects_invalid_llm_repair():
    model = _SequenceToolRouterModel(
        [
            AIMessage(content='{"tools":["run_sql"],"skills":[],"reason":"x"}'),
            AIMessage(content='{"tools":["run_sql"],"skills":[],"reason":"y"}'),
        ]
    )

    with pytest.raises(ToolRoutingError):
        _select_chat_route(
            "Маршрутизируй",
            model=model,
            available_tools=get_tools(),
        )

    assert len(model.calls) == 2


def test_chat_tool_router_rejects_invalid_llm_plan():
    invalid_routes = [
        {"tools": ["unknown"], "skills": []},
        {"tools": [], "skills": []},
        {"tools": ["run_sql"], "skills": ["unknown"]},
        {"tools": ["run_sql"]},
        {"tools": ["run_sql"], "skills": [], "reason": "extra field"},
        {"capabilities": ["sql_query"]},
    ]
    for route in invalid_routes:
        model = _ToolRouterModel(
            AIMessage(content=json.dumps(route, ensure_ascii=False))
        )

        with pytest.raises(ToolRoutingError):
            _select_chat_route(
                "Маршрутизируй",
                model=model,
                available_tools=get_tools(),
            )


def test_chat_tool_router_surfaces_llm_failure():
    model = _ToolRouterModel(RuntimeError("router unavailable"))

    with pytest.raises(ToolRoutingError, match="tool-router"):
        _select_chat_route(
            "Маршрутизируй",
            model=model,
            available_tools=get_tools(),
        )


def test_chat_tool_router_rejects_invalid_raw_text():
    for raw_text in ("не JSON", "[]", ""):
        model = _ToolRouterModel(AIMessage(content=raw_text))

        with pytest.raises(ToolRoutingError):
            _select_chat_route(
                "Маршрутизируй",
                model=model,
                available_tools=get_tools(),
            )


class _ObserverModel:
    def __init__(self):
        self.messages = []

    def invoke(self, messages):
        self.messages.append(messages)
        return AIMessage(content="Инструмент вернул ok=true.")


class _ScriptedNativeModel:
    def __init__(
        self,
        responses,
        audit_responses=None,
    ):
        self.responses = list(responses)
        self.audit_responses = list(audit_responses or [])
        self.bound_tools = []
        self.messages = []
        self.audit_messages = []
        self.observer = _ObserverModel()

    def bind_tools(self, tools):
        self.bound_tools = list(tools)
        return self

    def invoke(self, messages, **kwargs):
        del kwargs
        if (
            messages
            and getattr(messages[0], "type", None) == "system"
            and "Ты observer многошагового агента" in str(messages[0].content)
        ):
            return self.observer.invoke(messages)
        if (
            messages
            and (
                "Аудит завершения planner" in str(messages[-1].content)
                or "Повтори аудит в последний раз" in str(messages[-1].content)
            )
        ):
            self.audit_messages.append(messages)
            if self.audit_responses:
                return self.audit_responses.pop(0)
            for message in reversed(messages[:-1]):
                if (
                    isinstance(message, AIMessage)
                    and not message.tool_calls
                    and str(message.content).strip()
                ):
                    return AIMessage(content=message.content)
            return AIMessage(content="Все части запроса подтверждены.")
        self.messages.append(messages)
        return self.responses.pop(0)


def test_fallback_observation_uses_raw_model_output_as_summary():
    from langchain_core.exceptions import OutputParserException
    from langchain_core.messages import ToolMessage

    from agents.chat_graph import _fallback_observation

    observation = _fallback_observation(
        AIMessage(
            content="",
            tool_calls=[
                {
                    "name": "ping",
                    "args": {},
                    "id": "call-1",
                    "type": "tool_call",
                }
            ],
        ),
        [ToolMessage(content='{"ok": true}', tool_call_id="call-1", name="ping")],
        OutputParserException("invalid output", llm_output="Сырой вывод observer"),
    )

    assert observation.summary == "Сырой вывод observer"
    assert observation.has_error is False
    assert observation.important_facts == []
    assert observation.limitations == []


def test_run_agent_graph_executes_native_tool_call_and_uses_observer():
    from agents.chat_graph import run_agent_graph

    calls = []

    def ping():
        calls.append(True)
        return {"ok": True}

    model = _ScriptedNativeModel(
        [
            AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "ping",
                        "args": {},
                        "id": "call-1",
                        "type": "tool_call",
                    }
                ],
            ),
            AIMessage(
                content="Фактов достаточно.",
            ),
            AIMessage(content="Готово."),
        ]
    )

    out = run_agent_graph(
        "Проверь",
        "Системный контекст",
        model,
        (_as_tool(ping),),
        max_steps=2,
    )

    assert out == "Готово."
    assert calls == [True]
    assert [tool.name for tool in model.bound_tools] == ["ping"]
    assert len(model.observer.messages) == 1
    assert any(
        "ok=true" in str(message.content)
        for prompt in model.messages
        for message in prompt
        if hasattr(message, "content")
    )


def test_responder_keeps_model_table_output_without_backend_rewrite():
    from agents.chat_graph import run_agent_graph

    def table_lookup(limit: int):
        assert limit == 2
        return {
            "columns": ["table_name"],
            "rows": [
                {"table_name": "first_table"},
                {"table_name": "second_table"},
            ],
            "returned_rows": 2,
        }

    model = _ScriptedNativeModel(
        [
            AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "table_lookup",
                        "args": {"limit": 2},
                        "id": "table-1",
                        "type": "tool_call",
                    }
                ],
            ),
            AIMessage(content="Две строки подтверждены."),
            AIMessage(
                content=(
                    "Вот две строки:\n\n"
                    "```text\n"
                    '[["first_table"],["second_table"]]\n'
                    "```\n\n"
                    "```table\n"
                    '[["table_name","count"],["first_table",1]]\n'
                    "```"
                )
            ),
        ]
    )

    out = run_agent_graph(
        "Покажи две строки",
        "Системный контекст",
        model,
        (_as_tool(table_lookup),),
        max_steps=2,
    )

    assert out == (
        "Вот две строки:\n\n"
        "```text\n"
        '[["first_table"],["second_table"]]\n'
        "```\n\n"
        "```table\n"
        '[["table_name","count"],["first_table",1]]\n'
        "```"
    )


def test_numeric_contract_corrects_single_explicit_limit():
    from agents.chat_graph import run_agent_graph

    calls = []

    def bounded_lookup(limit: int):
        calls.append(limit)
        return {
            "columns": ["value"],
            "rows": [{"value": index} for index in range(limit)],
        }

    default_call = AIMessage(
        content="",
        tool_calls=[
            {
                "name": "bounded_lookup",
                "args": {"limit": 100},
                "id": "bounded-default",
                "type": "tool_call",
            }
        ],
    )
    model = _ScriptedNativeModel(
        [
            default_call,
            AIMessage(content="Три строки подтверждены."),
            AIMessage(content="Готово."),
        ]
    )

    out = run_agent_graph(
        "Покажи 3 значения",
        "Системный контекст",
        model,
        (_as_tool(bounded_lookup),),
        max_steps=2,
    )

    assert calls == [3]
    assert out == "Готово."


def test_completion_audit_calls_tool_for_unfinished_second_step():
    from agents.chat_graph import run_agent_graph

    calls = []

    def first_tool():
        calls.append("first")
        return {"table_name": "shared"}

    def second_tool(table_name: str):
        calls.append(f"second:{table_name}")
        return {"rows": [{"table_name": table_name}]}

    model = _ScriptedNativeModel(
        [
            AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "first_tool",
                        "args": {},
                        "id": "first-1",
                        "type": "tool_call",
                    }
                ],
            ),
            AIMessage(content="Теперь покажу связи вторым инструментом."),
            AIMessage(content="Оба инструментальных шага выполнены."),
            AIMessage(content="Готово."),
        ],
        audit_responses=[
            AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "second_tool",
                        "args": {"table_name": "shared"},
                        "id": "second-1",
                        "type": "tool_call",
                    }
                ],
            )
        ],
    )

    out = run_agent_graph(
        "Сначала найди таблицу, затем покажи её связи",
        "Системный контекст",
        model,
        (_as_tool(first_tool), _as_tool(second_tool)),
        max_steps=3,
    )

    assert out == "Готово."
    assert calls == ["first", "second:shared"]
    assert len(model.audit_messages) >= 2
    assert "ответ без tool_calls завершит весь граф" in str(
        model.audit_messages[0][-1].content
    )
    assert "Фактически завершённые tools: ['first_tool']" in str(
        model.audit_messages[0][-1].content
    )
    assert "не доказывает, что следующий шаг уже выполнен" in str(
        model.audit_messages[0][-1].content
    )
    assert "Если пользователь указал N" in str(
        model.audit_messages[0][-1].content
    )


def test_error_payload_is_reported_to_planner_and_same_tool_can_retry():
    from agents.chat_graph import run_agent_graph

    calls = []

    def unstable_lookup(query: str):
        calls.append(query)
        if len(calls) == 1:
            return {"error": "temporary failure"}
        return {"rows": [{"value": 1}]}

    model = _ScriptedNativeModel(
        [
            AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "unstable_lookup",
                        "args": {"query": "SELECT 1"},
                        "id": "lookup-1",
                        "type": "tool_call",
                    }
                ],
            ),
            AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "unstable_lookup",
                        "args": {"query": "SELECT 1"},
                        "id": "lookup-2",
                        "type": "tool_call",
                    }
                ],
            ),
            AIMessage(content="Получено value=1."),
            AIMessage(content="Значение: 1."),
        ]
    )

    out = run_agent_graph(
        "Получи значение",
        "Системный контекст",
        model,
        (_as_tool(unstable_lookup),),
        max_steps=3,
    )

    assert out == "Значение: 1."
    assert calls == ["SELECT 1", "SELECT 1"]
    second_planner_prompt = "\n".join(
        str(message.content) for message in model.messages[1]
    )
    failed_tool_messages = [
        message
        for message in model.messages[1]
        if getattr(message, "type", None) == "tool"
    ]
    assert len(failed_tool_messages) == 1
    assert failed_tool_messages[0].status == "error"
    observer_prompt = "\n".join(
        str(message.content) for message in model.observer.messages[0]
    )
    assert '"is_error": true' in observer_prompt
    assert "Этот инструментальный шаг содержал ошибку" in second_planner_prompt
    assert "Повтор того же tool после ошибки разрешён" in second_planner_prompt


def test_planner_uses_bound_tool_contracts_without_tool_specific_cases():
    from agents.chat_graph import _planner_instruction

    instruction = _planner_instruction(
        ("run_sql", "trace_neo4j_lineage")
    )

    assert "description и схему аргументов" in instruction
    assert "они являются его контрактом" in instruction
    assert "передай ровно N" in instruction
    assert "SQLITE_QUERY" not in instruction
    assert "GRAPH_NEO4J" not in instruction
    assert "table_name" not in instruction


def test_long_tool_output_reaches_planner_and_responder_with_handoff(caplog):
    from langchain_core.messages import ToolMessage

    from agents.chat_graph import run_agent_graph

    raw_marker = "RAW_TOOL_OUTPUT_MARKER"
    final_response = (
        "```table\n"
        '[["target_table","count"],["t_bus_srv",5]]\n'
        "```"
    )

    def long_result():
        return {"payload": raw_marker + ("x" * 50000)}

    planner_handoff = "Проверено: ok=true; ограничений нет."
    model = _ScriptedNativeModel(
        [
            AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "long_result",
                        "args": {},
                        "id": "long-1",
                        "type": "tool_call",
                    }
                ],
            ),
            AIMessage(content=planner_handoff),
            AIMessage(content=final_response),
        ]
    )

    with caplog.at_level("INFO", logger="agents.chat_graph"):
        out = run_agent_graph(
            "Проверь длинный результат",
            "Системный контекст",
            model,
            (_as_tool(long_result),),
            max_steps=2,
        )

    assert out == final_response
    assert (
        f"Agent final response ({len(final_response)} chars):\n{final_response}"
        in caplog.text
    )
    observer_text = "\n".join(
        str(message.content) for message in model.observer.messages[0]
    )
    assert raw_marker in observer_text

    planner_messages = model.messages[1]
    planner_text = "\n".join(
        str(message.content) for message in planner_messages
    )
    assert raw_marker in planner_text
    assert "Инструмент вернул ok=true." in planner_text
    planner_tool_messages = [
        message
        for message in planner_messages
        if isinstance(message, ToolMessage)
    ]
    assert len(planner_tool_messages) == 1
    assert raw_marker in str(planner_tool_messages[0].content)

    responder_messages = model.messages[2]
    responder_text = "\n".join(
        str(message.content) for message in responder_messages
    )
    assert raw_marker in responder_text
    assert "Инструмент вернул ok=true." not in responder_text
    assert planner_handoff in responder_text
    assert any(isinstance(message, ToolMessage) for message in responder_messages)


def test_run_agent_graph_appends_sql_lineage_visualization_url():
    from agents.chat_graph import run_agent_graph

    def make_graph():
        return {
            "visualization_type": "sqlglot_graph_html",
            "visualization_url": (
                "/exports/sql-lineage/sql_lineage_0123456789abcdef.html"
            ),
        }

    model = _ScriptedNativeModel(
        [
            AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "make_graph",
                        "args": {},
                        "id": "graph-1",
                        "type": "tool_call",
                    }
                ],
            ),
            AIMessage(content="Граф построен."),
            AIMessage(content="Готово."),
        ]
    )

    out = run_agent_graph(
        "Покажи граф",
        "Системный контекст",
        model,
        (_as_tool(make_graph),),
        max_steps=2,
    )

    assert out.startswith("Готово.")
    assert (
        "[Открыть интерактивный SQL lineage-граф]"
        "(/exports/sql-lineage/sql_lineage_0123456789abcdef.html)"
    ) in out


def test_run_agent_graph_does_not_duplicate_existing_visualization_url():
    from agents.chat_graph import run_agent_graph

    url = "/exports/sql-lineage/sql_lineage_0123456789abcdef.html"

    def make_graph():
        return {
            "visualization_type": "sqlglot_graph_html",
            "visualization_url": url,
        }

    model = _ScriptedNativeModel(
        [
            AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "make_graph",
                        "args": {},
                        "id": "graph-existing-url",
                        "type": "tool_call",
                    }
                ],
            ),
            AIMessage(content="Граф построен."),
            AIMessage(content=f"[Посмотреть граф]({url})"),
        ]
    )

    out = run_agent_graph(
        "Покажи граф",
        "Системный контекст",
        model,
        (_as_tool(make_graph),),
        max_steps=2,
    )

    assert out == f"[Посмотреть граф]({url})"
    assert out.count(url) == 1


def test_run_agent_graph_appends_s2t_table_graph_visualization_url():
    from agents.chat_graph import run_agent_graph

    def make_graph():
        return {
            "visualization_type": "s2t_table_graph_html",
            "visualization_url": (
                "/exports/s2t-graphs/s2t_table_graph_0123456789abcdef.html"
            ),
            "data_url": (
                "/exports/s2t-graphs/s2t_table_graph_0123456789abcdef.json"
            ),
        }

    model = _ScriptedNativeModel(
        [
            AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "make_graph",
                        "args": {},
                        "id": "graph-1",
                        "type": "tool_call",
                    }
                ],
            ),
            AIMessage(content="Граф построен."),
            AIMessage(content="Готово."),
        ]
    )

    out = run_agent_graph(
        "Покажи граф таблиц",
        "Системный контекст",
        model,
        (_as_tool(make_graph),),
        max_steps=2,
    )

    assert out.startswith("Готово.")
    assert (
        "[Открыть интерактивный граф связей S2T-таблиц]"
        "(/exports/s2t-graphs/s2t_table_graph_0123456789abcdef.html)"
    ) in out
    assert (
        "[Открыть данные графа в JSON]"
        "(/exports/s2t-graphs/s2t_table_graph_0123456789abcdef.json)"
    ) in out


def test_run_agent_graph_executes_all_tool_calls_returned_by_planner():
    from agents.chat_graph import run_agent_graph

    calls = []

    def ping():
        calls.append("ping")
        return {"tool": "ping"}

    def pong():
        calls.append("pong")
        return {"tool": "pong"}

    model = _ScriptedNativeModel(
        [
            AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "ping",
                        "args": {},
                        "id": "call-1",
                        "type": "tool_call",
                    },
                    {
                        "name": "pong",
                        "args": {},
                        "id": "call-2",
                        "type": "tool_call",
                    },
                ],
            ),
            AIMessage(content="Оба результата получены."),
            AIMessage(content="Готово."),
        ]
    )

    out = run_agent_graph(
        "Выполни обе проверки",
        "Системный контекст",
        model,
        (_as_tool(ping), _as_tool(pong)),
        max_steps=2,
    )

    assert out == "Готово."
    assert sorted(calls) == ["ping", "pong"]
    assert len(model.observer.messages) == 1
    observer_payload = json.loads(model.observer.messages[0][-1].content)
    assert [item["name"] for item in observer_payload["tool_results"]] == [
        "ping",
        "pong",
    ]
    assert [
        json.loads(item["content"])["tool"]
        for item in observer_payload["tool_results"]
    ] == ["ping", "pong"]


def test_run_agent_graph_allows_same_tool_with_different_args():
    from agents.chat_graph import run_agent_graph

    calls = []

    def echo(value: int):
        calls.append(value)
        return {"value": value}

    model = _ScriptedNativeModel(
        [
            AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "echo",
                        "args": {"value": 1},
                        "id": "echo-1",
                        "type": "tool_call",
                    }
                ],
            ),
            AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "echo",
                        "args": {"value": 2},
                        "id": "echo-2",
                        "type": "tool_call",
                    }
                ],
            ),
            AIMessage(content="Получены value=1 и value=2."),
            AIMessage(content="Значения: 1 и 2."),
        ]
    )

    out = run_agent_graph(
        "Получи два значения",
        "Системный контекст",
        model,
        (_as_tool(echo),),
        max_steps=3,
    )

    assert out == "Значения: 1 и 2."
    assert calls == [1, 2]
    assert len(model.observer.messages) == 2


def test_run_agent_graph_enforces_max_steps_in_router():
    from agents.chat_graph import run_agent_graph

    calls = []

    def ping():
        calls.append(True)
        return {"ok": True}

    model = _ScriptedNativeModel(
        [
            AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "ping",
                        "args": {},
                        "id": "call-1",
                        "type": "tool_call",
                    }
                ],
            ),
            AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "ping",
                        "args": {},
                        "id": "call-2",
                        "type": "tool_call",
                    }
                ],
            ),
            AIMessage(content="Готово без дополнительных инструментов."),
        ]
    )

    out = run_agent_graph(
        "Проверь",
        "Системный контекст",
        model,
        (_as_tool(ping),),
        max_steps=1,
    )

    assert out == "Готово без дополнительных инструментов."
    assert calls == [True]


def test_run_agent_graph_can_use_show_plan_as_native_tool():
    from agents.chat_graph import run_agent_graph
    from agents.tools import show_plan

    model = _ScriptedNativeModel(
        [
            AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "show_plan",
                        "args": {
                            "done": "Определён текущий файл.",
                            "to_do": "Получить список трансформаций.",
                        },
                        "id": "plan-1",
                        "type": "tool_call",
                    }
                ],
            ),
            AIMessage(content="План зафиксирован."),
            AIMessage(content="Продолжаю по плану."),
        ]
    )

    out = run_agent_graph(
        "Составь план анализа",
        "Системный контекст",
        model,
        (show_plan,),
        max_steps=2,
    )

    assert out == "Продолжаю по плану."
    assert [tool.name for tool in model.bound_tools] == ["show_plan"]
    observer_payload = str(model.observer.messages[0][-1].content)
    assert "Определён текущий файл." in observer_payload
    assert "Получить список трансформаций." in observer_payload


def test_run_agent_graph_passes_history_and_active_file_context():
    from agents.chat_graph import run_agent_graph

    model = _ScriptedNativeModel(
        [
            AIMessage(content="Фактов достаточно."),
            AIMessage(content="На листе S2T."),
        ]
    )

    out = run_agent_graph(
        "А какие в нём листы?",
        "Системный контекст",
        model,
        (),
        history=[
            {"role": "user", "content": "Какие файлы загружены?"},
            {"role": "assistant", "content": "Загружен mapping.xlsx."},
        ],
        file_id=42,
    )

    assert out == "На листе S2T."
    planner_messages = model.messages[0]
    contents = [str(message.content) for message in planner_messages]
    planner_prompt = "\n".join(contents)
    assert any("file_id=42" in content for content in contents)
    assert "Не применяй его к глобальной таблице" in planner_prompt
    assert "Какие файлы загружены?" in contents
    assert "Загружен mapping.xlsx." in contents
    assert "А какие в нём листы?" in contents
    assert "доступные tools: нет" in planner_prompt
    assert "TABULAR_SQLITE" not in planner_prompt
    assert "GRAPH_NEO4J" not in planner_prompt
    assert "SQL_TEXT_LINEAGE" not in planner_prompt
    assert len(planner_messages[0].content) < 6000

    responder_messages = model.messages[1]
    responder_system_text = "\n".join(
        str(message.content)
        for message in responder_messages
        if getattr(message, "type", None) == "system"
    )
    assert "<planner_handoff>" in responder_system_text
    assert "Фактов достаточно." in responder_system_text
    assert "точные значения" in responder_system_text
    assert "Выжимка observer" not in responder_system_text
    assert "не связывай глобальный результат с активным" in responder_system_text
    assert "глобальная таблица сейчас пуста" in responder_system_text

    assert "валидный JSON-список списков" in responder_system_text
    assert "Markdown-таблица" in responder_system_text
    assert "запрещена" in responder_system_text
    assert "браузер сам отрисует блок" in responder_system_text
    assert "сохраняй все запрошенные" in responder_system_text
    assert "text_diagram без изменений" in responder_system_text
    assert "не пересказывай" in responder_system_text

    for prompt in [*model.messages, *model.observer.messages]:
        system_positions = [
            index
            for index, message in enumerate(prompt)
            if getattr(message, "type", None) == "system"
        ]
        assert system_positions == [0]


def test_run_agent_graph_rejects_duplicate_tool_names():
    from agents.chat_graph import run_agent_graph

    def ping():
        return {"ok": True}

    duplicate_tools = (_as_tool(ping), _as_tool(ping))
    model = _ScriptedNativeModel([])

    with pytest.raises(ValueError, match="уникальными"):
        run_agent_graph("q", "system", model, duplicate_tools)


def test_run_agent_graph_adds_langfuse_callback_config():
    from agents.chat_graph import run_agent_graph

    graph = MagicMock()
    graph.invoke.return_value = {"messages": [AIMessage(content="ok")]}
    handler = object()
    trace_context = MagicMock()
    trace_context.__enter__.return_value = None
    trace_context.__exit__.return_value = None

    with patch("agents.chat_graph.build_agent_graph", return_value=graph), patch(
        "agents.chat_graph.get_callback_handler", return_value=handler
    ), patch(
        "agents.chat_graph.langfuse_trace_context", return_value=trace_context
    ) as mock_trace_context:
        out = run_agent_graph(
            "q",
            "system",
            MagicMock(),
            {},
            max_steps=2,
            session_id="chat-session-1",
            user_id="user-1",
            trace_tags=["chat", "header_detection"],
            trace_metadata={"file_id": 7},
        )

    assert out == "ok"
    mock_trace_context.assert_called_once_with(
        trace_name="agent_chat",
        session_id="chat-session-1",
        user_id="user-1",
        metadata={"file_id": 7},
        tags=["chat", "header_detection"],
    )
    config = graph.invoke.call_args.kwargs["config"]
    assert config["run_name"] == "agent_chat"
    assert config["callbacks"] == [handler]
    assert config["recursion_limit"] == 16
