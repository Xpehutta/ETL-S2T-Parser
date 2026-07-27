import json
from unittest.mock import MagicMock, patch

import pytest
from langchain_core.messages import AIMessage
from langchain_core.tools import BaseTool, StructuredTool

from agents.agent import (
    CHAT_AGENT_CONTEXT,
    SQLITE_SCHEMA_CONTEXT,
    SKILLS,
    agent_chat,
    get_header_decision,
    get_model_name,
    safe_extract_json,
)
from agents.tools import get_tools_by_name


def _decision(kind, **payload):
    return AIMessage(content=json.dumps({"kind": kind, **payload}, ensure_ascii=False))


def _as_tool(function, name=None):
    tool_name = name or function.__name__
    return StructuredTool.from_function(
        func=function,
        name=tool_name,
        description=f"Test tool {tool_name}",
    )


def test_file_description_skill_is_declared():
    assert "file_description_skill" in SKILLS
    assert "files.description" in SKILLS
    assert "get_file_description" in SKILLS
    assert "update_file_description" in SKILLS


def test_table_info_update_skill_is_declared():
    assert "table_info_update_skill" in SKILLS
    assert "update_table_info_from_user_query" in SKILLS
    assert "строка файла уже существует" in SKILLS
    assert "базовое описание уже сгенерировано" in SKILLS


def test_table_description_summary_skill_is_declared():
    assert "table_description_summary_skill" in SKILLS
    assert "summarize_table_descriptions" in SKILLS
    assert "source_tables" in SKILLS
    assert "target_tables" in SKILLS
    assert "UNION ALL" in SKILLS
    assert "combined_descriptions" in SKILLS


def test_chat_agent_connects_annotated_tools_without_separate_allowlist():
    tools_by_name = get_tools_by_name()
    assert all(isinstance(tool, BaseTool) for tool in tools_by_name.values())
    assert sorted(tools_by_name) == [
        "get_file_description",
        "list_columns",
        "list_file_sheet_headers",
        "list_files",
        "list_s2t_transformations",
        "list_sheet_group_classifications",
        "list_sheets",
        "resolve_file",
        "run_cypher",
        "run_sql",
        "search_s2t_transformations",
        "show_plan",
        "summarize_s2t_tables",
        "summarize_table_descriptions",
        "trace_neo4j_lineage",
    ]


def test_chat_agent_declares_public_ddl_schema():
    assert "Публичная DDL-схема" in SQLITE_SCHEMA_CONTEXT
    assert "сгенерирован из `storage/database.py`" in SQLITE_SCHEMA_CONTEXT
    assert "sqlite_master" in SQLITE_SCHEMA_CONTEXT
    assert "list_s2t_transformations" not in SQLITE_SCHEMA_CONTEXT
    assert "export_csv" not in SQLITE_SCHEMA_CONTEXT


def test_chat_agent_loads_runtime_context_file():
    assert "Runtime Chat Agent Context" in CHAT_AGENT_CONTEXT
    assert "Flask chat-agent" in CHAT_AGENT_CONTEXT
    assert "ETL/S2T" in CHAT_AGENT_CONTEXT
    assert "Выбор между SQLite и Neo4j" in CHAT_AGENT_CONTEXT
    assert "Не используй Cypher для чтения сырых Excel-строк" in CHAT_AGENT_CONTEXT
    assert "Не используй SQL для многошагового обхода графа" in CHAT_AGENT_CONTEXT
    assert "отсутствие узла" not in CHAT_AGENT_CONTEXT.lower()


def test_s2t_summary_is_declared_as_skill():
    assert "s2t_loaded_data_summary_skill" in SKILLS
    assert "s2t_table_summary_skill" in SKILLS
    assert "s2t_transformations" in SKILLS
    assert "run_sql" in SKILLS
    assert "run_cypher" in SKILLS
    assert "summarize_s2t_tables" in SKILLS
    assert "резюме таблицы трансформаций" in SKILLS
    assert "таблицы-приёмники" in SKILLS
    assert "таблицы-источники" in SKILLS
    assert "связи source → target" in SKILLS


def test_s2t_logical_table_roles_are_in_runtime_context_not_skills():
    assert "`t_*`" in CHAT_AGENT_CONTEXT
    assert "`target_table`" in CHAT_AGENT_CONTEXT
    assert "`source_table`" in CHAT_AGENT_CONTEXT
    assert "`data.table_name`" in CHAT_AGENT_CONTEXT


@pytest.fixture
def mock_llm_success():
    with patch("agents.agent.call_header_model_with_retry") as mock_call:
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
    with patch("agents.agent.call_header_model_with_retry") as mock_call:
        mock_call.return_value = (
            '{"header_start_row": 0, "header_rows": 1, "nested": false}'
        )
        get_header_decision("SheetLimited", [[f"row-{i}"] for i in range(6)])
        user_prompt = mock_call.call_args.args[1]
        assert "row-3" in user_prompt
        assert "row-4" not in user_prompt


def test_get_header_decision_multi_row_header():
    with patch("agents.agent.call_header_model_with_retry") as mock_call:
        mock_call.return_value = (
            '{"header_start_row": 0, "header_rows": 2, "nested": true}'
        )
        preview_rows = [
            ["Name", "Name", "Age", "Age"],
            ["First", "Last", "Years", "Months"],
            ["John", "Doe", 30, 360],
        ]
        assert get_header_decision("Sheet2", preview_rows) == (0, 2, True)


def test_get_header_decision_fallback_on_llm_failure_default():
    long_text = "A" * 150
    preview_rows = [[long_text, long_text], ["Data 1", "Data 2"]]
    with patch(
        "agents.agent.call_header_model_with_retry", side_effect=Exception("API error")
    ):
        assert get_header_decision("Sheet3", preview_rows) == (0, 1, False)


def test_get_header_decision_fallback_two_short_rows():
    preview_rows = [["Column A", "Column B"], ["Data 1", "Data 2"]]
    with patch(
        "agents.agent.call_header_model_with_retry", side_effect=Exception("API error")
    ):
        assert get_header_decision("Sheet4", preview_rows) == (0, 2, True)


def test_get_model_name():
    assert get_model_name()


def test_safe_extract_json_from_fence():
    raw = 'Here:\n```json\n{"a": 1}\n```\n'
    assert safe_extract_json(raw) == '{"a": 1}'


def test_agent_chat_delegates_to_native_tool_graph():
    with patch("agents.agent.run_agent_graph", return_value="ok") as run_graph, patch(
        "agents.agent._get_langfuse_callbacks",
        return_value=["callback"],
    ):
        out = agent_chat(
            "Покажи листы",
            file_id=42,
            history=[{"role": "user", "content": "Контекст"}],
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
    assert kwargs["trace_tags"] == ["chat"]
    assert kwargs["trace_metadata"] == {"file_id": 42}
    assert "read-only" in kwargs["system_prompt"]


class _ObserverModel:
    def __init__(self):
        self.messages = []

    def invoke(self, messages):
        from agents.chat_graph import Observation

        self.messages.append(messages)
        return Observation(
            summary="Инструмент вернул ok=true.",
            important_facts=["ok=true"],
            limitations=[],
        )


class _ScriptedNativeModel:
    def __init__(self, responses):
        self.responses = list(responses)
        self.bound_tools = []
        self.messages = []
        self.observer = _ObserverModel()

    def bind_tools(self, tools):
        self.bound_tools = list(tools)
        return self

    def with_structured_output(self, schema, method=None):
        del schema, method
        return self.observer

    def invoke(self, messages, **kwargs):
        del kwargs
        self.messages.append(messages)
        return self.responses.pop(0)


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
    assert "TABULAR_SQLITE" in planner_prompt
    assert "GRAPH_NEO4J" in planner_prompt
    assert "Не вызывай Neo4j-tools для этого сценария" in planner_prompt
    assert "Не вызывай SQLite-tools для обхода графа" in planner_prompt
    assert "только колонки ETLColumn и связи" in planner_prompt
    assert "Остальные факты получай из SQLite" in planner_prompt
    assert "никогда не" in planner_prompt
    assert "SQL-фильтрации" in planner_prompt

    responder_messages = model.messages[1]
    responder_system_text = "\n".join(
        str(message.content)
        for message in responder_messages
        if getattr(message, "type", None) == "system"
    )
    assert "<planner_draft>" in responder_system_text
    assert "Фактов достаточно." in responder_system_text
    assert "Используй этот черновик как основу" in responder_system_text
    assert "не связывай глобальный результат с активным" in responder_system_text
    assert "глобальная таблица сейчас пуста" in responder_system_text


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
