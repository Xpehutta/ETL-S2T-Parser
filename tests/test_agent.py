import json
from contextlib import nullcontext
from typing import get_args
from unittest.mock import MagicMock, patch

import pytest
from langchain_core.messages import AIMessage
from langchain_core.tools import StructuredTool

from agents.agent import (
    _select_chat_route,
    agent_chat,
    get_header_decision,
)
from agents.tools import get_tools, get_worker_tools, load_schemas, load_skills
from agents.tools.context import SchemaName
from agents.tools.routing import (
    GENERAL_FALLBACK_TOOL_NAMES,
    SCHEMA_CATALOG,
    SKILL_CATALOG,
    ToolRoute,
    ToolRoutingError,
    _TOOL_ROUTING_CONTRACTS,
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


def _available_fallback_tool_names():
    available_names = {tool.name for tool in get_tools()}
    return [
        name for name in GENERAL_FALLBACK_TOOL_NAMES if name in available_names
    ]


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
                schemas=[],
            ),
        ) as route_agent,
        patch(
            "agents.agent.load_skills",
            wraps=load_skills,
        ) as load_runtime_skills,
        patch(
            "agents.agent.load_schemas",
            wraps=load_schemas,
        ) as load_runtime_schemas,
    ):
        out = agent_chat(
            "Покажи листы",
            history=history,
            session_id="chat-session-1",
            user_id="user-1",
        )

    assert out == "ok"
    kwargs = run_graph.call_args.kwargs
    assert kwargs["user_query"] == "Покажи листы"
    assert kwargs["model"] is not None
    assert kwargs["tools"]
    assert "file_id" not in kwargs
    assert kwargs["history"] == [{"role": "user", "content": "Контекст"}]
    assert kwargs["session_id"] == "chat-session-1"
    assert kwargs["user_id"] == "user-1"
    assert kwargs["callbacks"] == ["callback"]
    assert [tool.name for tool in kwargs["tools"]] == ["list_sheets"]
    assert kwargs["trace_tags"] == ["chat"]
    assert kwargs["trace_metadata"] == {}
    assert "read-only" in kwargs["system_prompt"]
    assert "# Навыки агента" not in kwargs["system_prompt"]
    assert "## Excel и описания" in kwargs["system_prompt"]
    assert "## Neo4j" not in kwargs["system_prompt"]
    assert "## Актуальная схема SQLite" not in kwargs["system_prompt"]
    assert len(kwargs["system_prompt"]) < 7000
    route_args = route_agent.call_args
    assert route_args.args == ("Покажи листы", history)
    assert route_args.kwargs["model"] is not None
    assert route_args.kwargs["available_tools"] == get_tools()
    assert route_args.kwargs["callbacks"] == ["callback"]
    load_runtime_skills.assert_called_once_with(("Excel и описания",))
    load_runtime_schemas.assert_called_once_with(())


def test_agent_chat_passes_empty_palette_to_non_tool_graph():
    with (
        patch("agents.agent.run_agent_graph", return_value="Привет") as run_graph,
        patch(
            "agents.agent._select_chat_route",
            return_value=ToolRoute(tools=[], skills=[], schemas=[]),
        ),
    ):
        result = agent_chat("Ответь одним словом: привет")

    assert result == "Привет"
    assert run_graph.call_args.kwargs["tools"] == ()
    assert "## Актуальная схема SQLite" not in (
        run_graph.call_args.kwargs["system_prompt"]
    )


def test_chat_system_prompt_includes_only_explicitly_selected_schemas():
    from agents.agent import build_chat_system_prompt

    without_sql = build_chat_system_prompt("## Neo4j\nGraph rules", "")
    with_sql = build_chat_system_prompt(
        "## S2T-строки\nS2T rules",
        load_schemas(("SQLite ETL",)),
    )

    assert "## Актуальная схема SQLite" not in without_sql
    assert "## Актуальная схема SQLite" in with_sql


class _ToolRouterModel:
    def __init__(self, result):
        self.result = result
        self.messages = None
        self.config = None
        self.structured_schema = None
        self.structured_method = None

    def with_structured_output(self, schema, method=None):
        self.structured_schema = schema
        self.structured_method = method
        return self

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
        self.structured_schema = None
        self.structured_method = None

    def with_structured_output(self, schema, method=None):
        self.structured_schema = schema
        self.structured_method = method
        return self

    def invoke(self, messages, config=None):
        self.calls.append((messages, config))
        result = self.results.pop(0)
        if isinstance(result, Exception):
            raise result
        return result


def test_chat_tool_router_validates_structured_selection():
    cases = [
        (
            ["list_s2t_transformations"],
            [],
            ["S2T-маппинг"],
            ["list_s2t_transformations"],
            [],
            ["S2T-маппинг"],
        ),
        (
            ["trace_transformation_path"],
            ["S2T-строки"],
            ["S2T-маппинг"],
            ["trace_transformation_path"],
            ["S2T-строки"],
            ["S2T-маппинг"],
        ),
        (
            ["search_excel_values", "semantic_search_descriptions"],
            ["Excel и описания"],
            [],
            ["search_excel_values", "semantic_search_descriptions"],
            ["Excel и описания"],
            [],
        ),
        (
            ["list_files", "list_files"],
            ["Excel и описания", "Excel и описания"],
            ["Excel-маппинги", "Excel-маппинги"],
            ["list_files"],
            ["Excel и описания"],
            ["Excel-маппинги"],
        ),
    ]
    for (
        tool_names,
        skill_names,
        schema_names,
        expected_tools,
        expected_skills,
        expected_schemas,
    ) in cases:
        model = _ToolRouterModel(
            {
                "tools": tool_names,
                "skills": skill_names,
                "schemas": schema_names,
            }
        )

        result = _select_chat_route(
            "Запрос без эвристически значимых слов",
            model=model,
            available_tools=get_tools(),
            callbacks=["router-callback"],
        )

        assert result.tools == expected_tools
        assert result.skills == expected_skills
        assert result.schemas == expected_schemas
        assert model.config == {"callbacks": ["router-callback"]}
        assert model.structured_schema is ToolRoute
        assert model.structured_method == "function_calling"

    schema = ToolRoute.model_json_schema()
    assert schema["required"] == ["tools", "skills", "schemas"]
    assert "reason" not in schema["properties"]
    assert "enum" not in schema["properties"]["skills"]["items"]
    assert "enum" not in schema["properties"]["schemas"]["items"]


def test_chat_tool_router_attributes_model_call_to_router_stage():
    stages = []

    def stage_scope(stage):
        stages.append(stage)
        return nullcontext()

    model = _ToolRouterModel(
        ToolRoute(tools=["list_files"], skills=[], schemas=[])
    )
    with patch(
        "agents.tools.routing.llm_stage",
        side_effect=stage_scope,
    ):
        _select_chat_route(
            "Покажи файлы",
            model=model,
            available_tools=get_tools(),
        )

    assert stages == ["router"]


def test_router_skill_catalog_matches_lazy_runtime_sections():
    for skill_name in SKILL_CATALOG:
        assert f"## {skill_name}" in load_skills((skill_name,))

    assert "Анализ трансформаций" not in SKILL_CATALOG

    comparison_route = ToolRoute(
        tools=[],
        skills=["Сравнение", "Объяснение"],
        schemas=[],
    )
    assert comparison_route.skills == ["Сравнение", "Объяснение"]

    transformation_data_route = ToolRoute(
        tools=["list_s2t_transformations"],
        skills=["S2T-строки"],
        schemas=["S2T-маппинг"],
    )
    assert transformation_data_route.skills == ["S2T-строки"]


def test_chat_tool_router_passes_query_history_and_catalog_to_llm():
    history = [
        {
            "role": "assistant",
            "content": "```sql\nselect count(*) from files\n```",
        }
    ]
    model = _ToolRouterModel(
        {
            "tools": ["run_sql"],
            "skills": ["S2T-строки"],
            "schemas": ["SQLite ETL"],
        }
    )
    route = _select_chat_route(
        "Выполни этот запрос",
        history,
        model=model,
        available_tools=get_tools(),
    )
    assert route.tools == ["run_sql"]
    assert route.skills == ["S2T-строки"]
    assert route.schemas == ["SQLite ETL"]

    payload = json.loads(model.messages[1].content)
    assert payload["current_task"] == "Выполни этот запрос"
    assert payload["recent_history"] == history
    assert {item["name"] for item in payload["available_tools"]} == {
        tool.name for tool in get_tools()
    }
    assert payload["catalog_stage"] == "unrestricted"
    assert set(_TOOL_ROUTING_CONTRACTS) == {
        tool.name for tool in get_tools()
    } | {"read_previous_result"}
    assert all(item["use_when"] for item in payload["available_tools"])
    assert all(item["not_for"] for item in payload["available_tools"])
    assert all(
        set(item)
        == {
            "name",
            "use_when",
            "not_for",
        }
        for item in payload["available_tools"]
    )
    contracts = {
        item["name"]: item for item in payload["available_tools"]
    }
    assert "analyze" not in contracts
    run_sql_contract = contracts["run_sql"]
    assert "агрегация" in run_sql_contract["use_when"].casefold()
    assert "JOIN" in run_sql_contract["use_when"]
    assert "произвольное выражение" in run_sql_contract["use_when"]
    assert "точные s2t/каталожные строки" in run_sql_contract[
        "not_for"
    ].casefold()
    assert "логические ETL-таблицы" in run_sql_contract["not_for"]
    assert "transformation_rule" in run_sql_contract["not_for"]
    assert "$$-именам" in run_sql_contract["not_for"]
    excel_value_contract = contracts["search_excel_values"]
    assert "Excel-ячейк" in excel_value_contract["use_when"]
    assert "таблицы data" in excel_value_contract["use_when"]
    assert "логические ETL-таблицы" in excel_value_contract["not_for"]
    assert "выполнение запросов" in excel_value_contract["not_for"]
    s2t_list_contract = contracts["list_s2t_transformations"]
    assert "source_table.source_field" in s2t_list_contract["use_when"]
    assert "target_table.target_field" in s2t_list_contract["use_when"]
    assert "transformation_rule" in s2t_list_contract["use_when"]
    assert "transformation_rule без обхода" in s2t_list_contract["use_when"]
    assert "file_id не применяется" in s2t_list_contract["use_when"]
    assert "Семантические кандидаты" in s2t_list_contract["not_for"]
    assert "S2T-ролью/таблицей" in s2t_list_contract["not_for"]
    assert "происхождение по цепочке" in s2t_list_contract["not_for"]
    s2t_search_contract = contracts["search_s2t_transformations"]
    assert "Подстрока с неизвестной ролью" in s2t_search_contract["use_when"]
    assert "неполное имя" in s2t_search_contract["use_when"]
    assert "технических кандидатов" in s2t_search_contract["use_when"]
    assert "один batch-вызов" in s2t_search_contract["use_when"]
    assert "последовательный перебор" in s2t_list_contract["not_for"]
    assert "Точная полная source→target-пара" in s2t_search_contract["not_for"]
    assert "несколько условий" in s2t_search_contract["not_for"]
    rules_contract = contracts["get_s2t_rules_by_ids"]
    assert "из принятого результата lineage" in rules_contract["use_when"]
    assert "Числа из task" in rules_contract["not_for"]
    path_contract = contracts["trace_transformation_path"]
    assert "точной пары table+column" in path_contract["use_when"]
    assert "конечных источников/целей" in path_contract["use_when"]
    assert "одному независимому вызову на endpoint" in path_contract["use_when"]
    assert "SQLite первичен" in path_contract["use_when"]
    assert "upstream" in path_contract["use_when"]
    assert "downstream" in path_contract["use_when"]
    assert "неизвестное имя без поиска" in path_contract["not_for"]
    previous_contract = _TOOL_ROUTING_CONTRACTS["read_previous_result"]
    assert "являются входами текущего нового" in previous_contract["use_when"]
    assert "другому объекту той же операции" in previous_contract["not_for"]
    table_names_description = contracts["list_s2t_table_names"]["use_when"]
    assert "Глобальные множества source/target-таблиц" in table_names_description
    assert "операции над ними" in table_names_description
    semantic_description = contracts["semantic_search_descriptions"]["use_when"]
    assert "неизвестном имени" in semantic_description
    assert "бизнес-смысл" in semantic_description
    assert "вероятное соответствие" in semantic_description
    assert "files/tables/columns" in semantic_description
    assert "source/target" in semantic_description
    assert "фильтры колонок" in semantic_description
    assert "Подстрока" in contracts["semantic_search_descriptions"]["not_for"]
    assert "resolve_entities" not in contracts
    list_columns_contract = contracts["list_column_catalog"]
    assert "атрибуты" in (
        list_columns_contract["use_when"]
    )
    assert "table.column" in list_columns_contract["use_when"]
    assert "scope обязателен" in list_columns_contract["use_when"]
    assert "Атрибутный отбор" in list_columns_contract["not_for"]
    filter_columns_contract = contracts["filter_column_catalog"]
    assert "data_type" in filter_columns_contract["use_when"]
    assert "Точная table.column" in filter_columns_contract["not_for"]
    search_columns_contract = contracts["search_column_catalog"]
    assert "Явная буквальная подстрока" in search_columns_contract["use_when"]
    assert "фильтры ограничивают" in search_columns_contract["use_when"]
    assert "Смысл/назначение" in search_columns_contract["not_for"]
    assert "варианты" in search_columns_contract["not_for"]
    sql_graph_contract = contracts["visualize_sql_lineage"]
    assert "Полный SQL уже явно дан" in sql_graph_contract["use_when"]
    assert "интерактивный lineage-граф" in sql_graph_contract["use_when"]
    assert "Имя без SQL" in sql_graph_contract["not_for"]
    assert "получение SQL из хранилища" in sql_graph_contract["not_for"]
    parse_column_contract = contracts["parse_sql_column_lineage"]
    assert "Полный SQL уже явно передан" in parse_column_contract["use_when"]
    assert "выходных SELECT-колонок" in parse_column_contract["use_when"]
    assert "expression и source_columns" in parse_column_contract["use_when"]
    assert "SQL отсутствует" in parse_column_contract["not_for"]
    assert "хранилища" in parse_column_contract["not_for"]
    assert "WHERE" in parse_column_contract["not_for"]
    parse_table_contract = contracts["parse_sql_table_lineage"]
    assert "Полный SQL уже явно передан" in parse_table_contract["use_when"]
    assert "только исходные и целевая таблицы" in parse_table_contract["use_when"]
    assert "SQL отсутствует" in parse_table_contract["not_for"]
    assert "колонковый lineage" in parse_table_contract["not_for"]
    column_lineage_description = contracts["trace_neo4j_lineage"]["use_when"]
    assert "Явно нужен граф Neo4j" in column_lineage_description
    assert "заданную глубину" in column_lineage_description
    assert "Обычный полный lineage сохранённых S2T" in contracts[
        "trace_neo4j_lineage"
    ]["not_for"]
    global_graph_description = contracts["visualize_s2t_table_graph"]["use_when"]
    assert "глобальный" in global_graph_description
    assert "Конкретный SQL" in contracts["visualize_s2t_table_graph"]["not_for"]
    assert sum(
        len(item["use_when"]) + len(item["not_for"])
        for item in payload["available_tools"]
    ) < 5400
    assert payload["available_skills"] == [
        {"name": name, "description": description}
        for name, description in SKILL_CATALOG.items()
    ]
    assert payload["available_schemas"] == [
        {"name": name, "description": description}
        for name, description in SCHEMA_CATALOG.items()
    ]
    assert set(SCHEMA_CATALOG) == set(get_args(SchemaName))
    assert all(SCHEMA_CATALOG.values())
    assert "произвольного run_sql" in SCHEMA_CATALOG["SQLite ETL"]
    assert "source/target-роли" in SCHEMA_CATALOG["S2T-маппинг"]
    assert "физических заголовков" in SCHEMA_CATALOG["Excel-маппинги"]
    assert "произвольного run_cypher" in SCHEMA_CATALOG["Neo4j lineage"]
    router_prompt = " ".join(model.messages[0].content.split())
    assert "необходимую planner-палитру" in router_prompt
    assert "точные имена из каталогов" in router_prompt
    assert "`not_for` — запрет" in router_prompt
    assert "все необходимые tools" in router_prompt
    assert "обязательные входы" in router_prompt
    assert "получается выбранным tool" in router_prompt
    assert "Не придумывай входы" in router_prompt
    assert "не расширяй палитру по числу попыток" in router_prompt
    assert "catalog_stage=capability_expansion" in router_prompt
    assert "следуй `reason` и `required_capabilities`" in router_prompt
    assert "Списки выбирай независимо" in router_prompt
    assert "каждый может быть пустым" in router_prompt
    assert "оставляй `tools=[]`" in router_prompt
    assert "Наличие `previous_results` само по себе не создаёт зависимость" in (
        router_prompt
    )
    assert "результат той же операции для другого объекта" in router_prompt
    assert len(model.messages[0].content) < 1850
    assert "COUNT, DISTINCT, GROUP BY" not in router_prompt
    assert "обязательно включай `run_sql`" not in router_prompt
    assert "самостоятельно составить и выполнить SQL" not in model.messages[0].content
    assert "parse_sql_column_lineage" not in model.messages[0].content


def test_chat_tool_router_marks_specialized_catalog_stage():
    model = _ToolRouterModel(
        {"tools": ["list_files"], "skills": [], "schemas": []}
    )

    route = _select_chat_route(
        "Перечисли загруженные файлы",
        model=model,
        available_tools=get_worker_tools(),
        catalog_stage="specialized_only",
    )

    assert route.tools == ["list_files"]
    payload = json.loads(model.messages[1].content)
    assert payload["catalog_stage"] == "specialized_only"
    assert all(
        not item["use_when"].startswith("Общий fallback:")
        for item in payload["available_tools"]
    )


@pytest.mark.parametrize(
    ("tool_name", "task"),
    [
        ("run_sql", "Выполни дословно данный read-only SELECT."),
        (
            "list_s2t_source_field",
            "Прочитай все downstream-цели точного source-поля из task.",
        ),
        (
            "list_s2t_target_field",
            "Прочитай все upstream-источники точного target-поля из task.",
        ),
    ],
)
def test_chat_tool_router_can_select_core_readers_from_initial_catalog(
    tool_name,
    task,
):
    model = _ToolRouterModel(
        {"tools": [tool_name], "skills": [], "schemas": []}
    )

    route = _select_chat_route(
        task,
        model=model,
        available_tools=get_worker_tools(),
        catalog_stage="specialized_only",
    )

    assert route.tools == [tool_name]
    payload = json.loads(model.messages[1].content)
    contracts = {
        item["name"]: item for item in payload["available_tools"]
    }
    assert tool_name in contracts
    assert not contracts[tool_name]["use_when"].startswith(
        "Общий fallback:"
    )


def test_chat_tool_router_marks_general_tools_in_fallback_stage():
    model = _ToolRouterModel(
        {"tools": ["run_cypher"], "skills": [], "schemas": []}
    )

    route = _select_chat_route(
        "Выполни нестандартный read-only графовый обход",
        model=model,
        available_tools=get_worker_tools(include_general=True),
        catalog_stage="general_fallback",
    )

    assert route.tools == ["run_cypher"]
    payload = json.loads(model.messages[1].content)
    contracts = {
        item["name"]: item for item in payload["available_tools"]
    }
    assert payload["catalog_stage"] == "general_fallback"
    assert contracts["run_cypher"]["use_when"].startswith("Общий fallback:")
    assert not contracts["run_sql"]["use_when"].startswith(
        "Общий fallback:"
    )
    assert not contracts["list_files"]["use_when"].startswith(
        "Общий fallback:"
    )


def test_chat_tool_router_receives_explicit_reroute_context():
    model = _ToolRouterModel(
        {
            "tools": ["list_s2t_transformations", "trace_transformation_path"],
            "skills": [],
            "schemas": ["S2T-маппинг"],
        }
    )
    reroute_context = {
        "gap": "Нужен многошаговый S2T-путь с правилами.",
        "reason": "missing_capability",
        "required_capabilities": ["graph_read"],
        "previous_tool_palettes": [["list_s2t_transformations"]],
        "attempt": 1,
    }

    route = _select_chat_route(
        "Найди target_table с максимальным числом строк.",
        model=model,
        available_tools=get_tools(),
        reroute_context=reroute_context,
    )

    assert route.tools == [
        "list_s2t_transformations",
        "trace_transformation_path",
    ]
    payload = json.loads(model.messages[1].content)
    assert payload["current_task"] == (
        "Найди target_table с максимальным числом строк."
    )
    assert payload["reroute_context"] == reroute_context
    router_prompt = " ".join(model.messages[0].content.split())
    assert "при `reroute_context`" in router_prompt.lower()
    assert "следуй `reason` и `required_capabilities`" in router_prompt.lower()
    assert "сохрани нужные прежние tools" in router_prompt.lower()
    assert "добавь нужную capability" in router_prompt.lower()


def test_chat_tool_rerouter_requires_a_tool_for_each_required_capability():
    model = _SequenceToolRouterModel(
        [
            ToolRoute(
                tools=["list_s2t_transformations", "run_sql"],
                skills=[],
                schemas=[],
            ),
            ToolRoute(
                tools=[
                    "list_s2t_transformations",
                    "run_sql",
                    "trace_transformation_path",
                ],
                skills=[],
                schemas=[],
            ),
        ]
    )
    reroute_context = {
        "gap": "Нужны нестандартный SQL-срез и графовый путь.",
        "reason": "missing_capability",
        "required_capabilities": ["sql_read", "graph_read"],
        "previous_tool_palettes": [["list_s2t_transformations"]],
        "attempt": 1,
    }

    route = _select_chat_route(
        "Получи SQL-срез и графовый путь.",
        model=model,
        available_tools=get_tools(),
        reroute_context=reroute_context,
    )

    assert route.tools == [
        "list_s2t_transformations",
        "run_sql",
        "trace_transformation_path",
    ]
    assert len(model.calls) == 2
    repair_prompt = str(model.calls[1][0][-1].content)
    assert "graph_read" in repair_prompt
    assert "для каждой требуемой capability" in repair_prompt


def test_chat_tool_router_separates_current_task_from_previous_results():
    from agents.contracts import PreviousResultReference, WorkerRequestParts

    previous = {
        "previous_results": [
            {
                "result_id": "result_previous",
                "description": "run_sql: найдено filter_value=42.",
            }
        ]
    }
    model = _ToolRouterModel(
        ToolRoute(tools=["run_sql"], skills=[], schemas=["SQLite ETL"])
    )

    route = _select_chat_route(
        WorkerRequestParts(
            current_task=(
                "Прочитай новый срез основной базы с filter_value прошлого шага."
            ),
            previous_results=[
                PreviousResultReference.model_validate(
                    previous["previous_results"][0]
                )
            ],
        ),
        model=model,
        available_tools=get_tools(),
    )

    assert route.tools == ["run_sql"]
    payload = json.loads(model.messages[-1].content)
    assert payload["current_task"] == (
        "Прочитай новый срез основной базы с filter_value прошлого шага."
    )
    assert payload["previous_results"] == previous["previous_results"]
    assert "result_previous" not in payload["current_task"]


def test_chat_tool_router_routes_only_current_task_not_original_task():
    from agents.contracts import WorkerRequestParts

    current_task = "Прочитай назначенный S2T-срез."
    original_task = (
        "Для `source_stage_731.id` → `target_core_842.id` "
        "сохрани роли и file_id=917 без переименования."
    )
    model = _ToolRouterModel(
        ToolRoute(
            tools=["list_s2t_transformations"],
            skills=[],
            schemas=[],
        )
    )

    route = _select_chat_route(
        WorkerRequestParts(
            current_task=current_task,
            original_task=original_task,
        ),
        model=model,
        available_tools=get_tools(),
    )

    assert route.tools == ["list_s2t_transformations"]
    payload = json.loads(model.messages[-1].content)
    assert payload["current_task"] == current_task
    assert "original_task" not in payload
    assert original_task not in payload["current_task"]
    assert "единственный источник выбора операции" in str(
        model.messages[0].content
    )


def test_chat_tool_router_treats_legacy_marker_text_as_literal_task():
    model = _ToolRouterModel(
        ToolRoute(tools=["list_files"], skills=[], schemas=[])
    )
    leaked_context = "RAW_CONVERSATION_CONTEXT_MUST_NOT_REACH_ROUTER"

    literal_task = (
        "Покажи файлы."
        "\n\nУстойчивые правила контекста:\n"
        + leaked_context
    )
    route = _select_chat_route(
        literal_task,
        model=model,
        available_tools=get_tools(),
    )

    assert route.tools == ["list_files"]
    payload = json.loads(model.messages[-1].content)
    assert payload["current_task"] == literal_task
    assert "stable_context" not in payload
    assert leaked_context in payload["current_task"]
    assert leaked_context not in str(model.messages[0].content)


def test_chat_tool_rerouter_repairs_unchanged_palette_by_adding_tool():
    model = _SequenceToolRouterModel(
        [
            ToolRoute(
                tools=["list_s2t_transformations"],
                skills=["S2T-строки"],
                schemas=["S2T-маппинг"],
            ),
            ToolRoute(
                tools=[
                    "list_s2t_transformations",
                    "trace_transformation_path",
                ],
                skills=["S2T-строки"],
                schemas=["S2T-маппинг"],
            ),
        ]
    )

    route = _select_chat_route(
        "Найди многошаговый путь с rules.",
        model=model,
        available_tools=get_tools(),
        reroute_context={
            "gap": "Не найден многошаговый путь с rules.",
            "previous_tool_palettes": [["list_s2t_transformations"]],
            "attempt": 1,
        },
    )

    assert route.tools == [
        "list_s2t_transformations",
        "trace_transformation_path",
    ]
    assert len(model.calls) == 2
    assert "не добавил новый tool" in str(model.calls[1][0][-1].content)


def test_chat_tool_rerouter_adds_general_fallback_after_failed_repair():
    unchanged = ToolRoute(
        tools=["list_s2t_transformations"],
        skills=["S2T-строки"],
        schemas=["S2T-маппинг"],
    )
    model = _SequenceToolRouterModel([unchanged, unchanged])

    route = _select_chat_route(
        "Сравни две независимые трансформации.",
        model=model,
        available_tools=get_tools(),
        reroute_context={
            "gap": "Текущая палитра не закрыла сравнение.",
            "previous_tool_palettes": [["list_s2t_transformations"]],
            "attempt": 1,
        },
    )

    assert route.tools == list(
        dict.fromkeys(
            ["list_s2t_transformations", *_available_fallback_tool_names()]
        )
    )
    assert route.skills == []
    assert route.schemas == []
    assert len(model.calls) == 2


def test_chat_tool_router_does_not_override_semantic_route_by_keywords():
    model = _SequenceToolRouterModel(
        [
            ToolRoute(
                tools=["semantic_search_descriptions"],
                skills=[],
                schemas=[],
            ),
            ToolRoute(
                tools=[
                    "semantic_search_descriptions",
                    "list_s2t_transformations",
                ],
                skills=[],
                schemas=["S2T-маппинг"],
            ),
        ]
    )

    route = _select_chat_route(
        "Найди по русскому бизнес-термину соответствующее S2T-правило.",
        model=model,
        available_tools=get_tools(),
    )

    assert route.tools == ["semantic_search_descriptions"]
    assert len(model.calls) == 1


def test_chat_tool_router_does_not_force_impact_tools_by_keywords():
    model = _SequenceToolRouterModel(
        [
            ToolRoute(
                tools=["trace_neo4j_lineage"],
                skills=["Neo4j"],
                schemas=["Neo4j lineage"],
            ),
            ToolRoute(
                tools=[
                    "trace_neo4j_lineage",
                    "get_s2t_rules_by_ids",
                ],
                skills=["Neo4j"],
                schemas=["Neo4j lineage", "S2T-маппинг"],
            ),
        ]
    )

    route = _select_chat_route(
        "Выполни reverse lineage и перечисли downstream transformations.",
        model=model,
        available_tools=get_tools(),
    )

    assert route.tools == ["trace_neo4j_lineage"]
    assert len(model.calls) == 1


def test_chat_tool_router_does_not_force_path_tool_by_keywords():
    model = _SequenceToolRouterModel(
        [
            ToolRoute(
                tools=["run_cypher"],
                skills=["Neo4j"],
                schemas=["Neo4j lineage"],
            ),
            ToolRoute(
                tools=["trace_neo4j_table_path"],
                skills=["Neo4j"],
                schemas=["Neo4j lineage"],
            ),
        ]
    )

    route = _select_chat_route(
        "Найди в Neo4j все пути длины 3 от таблицы schema.source "
        "до таблицы target::branch::1.",
        model=model,
        available_tools=get_tools(),
    )

    assert route.tools == ["run_cypher"]
    assert len(model.calls) == 1


def test_chat_tool_router_does_not_force_s2t_role_tool_by_keywords():
    model = _SequenceToolRouterModel(
        [
            ToolRoute(
                tools=["search_s2t_transformations"],
                skills=["S2T-строки"],
                schemas=["S2T-маппинг"],
            ),
            ToolRoute(
                tools=["list_s2t_transformations"],
                skills=["S2T-строки"],
                schemas=["S2T-маппинг"],
            ),
        ]
    )

    route = _select_chat_route(
        "Найди все S2T mappings, которые загружают "
        "target::subquery::v1.del_dt.",
        model=model,
        available_tools=get_tools(),
    )

    assert route.tools == ["search_s2t_transformations"]
    assert len(model.calls) == 1


def test_chat_tool_router_does_not_force_mapping_tool_by_keywords():
    model = _SequenceToolRouterModel(
        [
            ToolRoute(
                tools=["list_s2t_transformations"],
                skills=["S2T-строки"],
                schemas=["S2T-маппинг"],
            ),
            ToolRoute(
                tools=["list_s2t_table_mapping"],
                skills=["S2T-строки"],
                schemas=["S2T-маппинг"],
            ),
        ]
    )

    route = _select_chat_route(
        "Покажи полный маппинг source_table -> target_table: "
        "source column -> target column.",
        model=model,
        available_tools=get_tools(),
    )

    assert route.tools == ["list_s2t_transformations"]
    assert len(model.calls) == 1


def test_planner_requires_tool_call_for_an_unfinished_tool_step():
    from agents.chat_graph import _planner_instruction

    instruction = _planner_instruction(["first_tool", "second_tool"])

    assert "верни нужный native tool call" in instruction
    assert "если данных уже достаточно" in instruction


def test_worker_planner_finishes_natively_without_domain_specific_rules():
    from agents.chat_graph import _planner_instruction

    instruction = _planner_instruction(
        ["run_sql", "search_s2t_transformations"],
        worker_finish=True,
    )

    assert "заверши работу через finish_worker" in " ".join(
        instruction.split()
    )
    assert "сохраняй смысл" in instruction.lower()
    assert "точные значения task" in instruction
    assert "COUNT, DISTINCT, GROUP BY" not in instruction
    assert "target_table" not in instruction
    assert "source_table" not in instruction


def test_chat_tool_router_requests_function_calling_structured_output():
    model = _ToolRouterModel(
        ToolRoute(
            tools=["run_sql"],
            skills=[],
            schemas=["SQLite ETL"],
        )
    )

    route = _select_chat_route(
        "Выполни запрос",
        model=model,
        available_tools=get_tools(),
    )
    assert route.tools == ["run_sql"]
    assert route.skills == []
    assert model.structured_schema is ToolRoute
    assert model.structured_method == "function_calling"


def test_chat_tool_router_repairs_invalid_structured_result_with_one_more_llm_call():
    model = _SequenceToolRouterModel(
        [
            {
                "tools": ["list_s2t_table_names"],
                "skills": ["S2T-строки"],
                "schemas": [],
                "description": "лишнее пояснение",
            },
            ToolRoute(
                tools=["list_s2t_table_names"],
                skills=["S2T-строки"],
                schemas=[],
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
    assert model.structured_schema is ToolRoute
    assert model.structured_method == "function_calling"
    repair_messages, repair_config = model.calls[1]
    assert repair_config == {"callbacks": ["router-callback"]}
    repair_prompt = str(repair_messages[-1].content)
    assert "Исправь только указанное нарушение" in repair_prompt
    assert "замени его одним существующим инструментом" in repair_prompt
    assert "Не расширяй палитру" in repair_prompt


def test_chat_tool_router_does_not_override_missing_explicit_column_catalog():
    model = _SequenceToolRouterModel(
        [
            ToolRoute(
                tools=["run_sql", "list_s2t_transformations"],
                skills=["S2T-строки"],
                schemas=["SQLite ETL"],
            )
        ]
    )

    route = _select_chat_route(
        "Получи обязательные поля из публичного target_columns.",
        model=model,
        available_tools=get_tools(),
    )

    assert route.tools == ["run_sql", "list_s2t_transformations"]
    assert len(model.calls) == 1


def test_chat_tool_router_does_not_override_exact_s2t_selection():
    model = _SequenceToolRouterModel(
        [
            ToolRoute(
                tools=["search_s2t_transformations"],
                skills=["S2T-строки"],
                schemas=["SQLite ETL"],
            )
        ]
    )

    route = _select_chat_route(
        (
            "Получи строки target_columns для table_name=t_optn и сопоставь "
            "их с target_table=t_optn."
        ),
        model=model,
        available_tools=get_tools(),
    )

    assert route.tools == ["search_s2t_transformations"]
    assert len(model.calls) == 1


def test_chat_tool_router_does_not_override_valid_llm_selection():
    model = _SequenceToolRouterModel(
        [
            ToolRoute(
                tools=["list_s2t_transformations"],
                skills=["S2T-строки"],
                schemas=["S2T-маппинг"],
            ),
        ]
    )

    route = _select_chat_route(
        "Через SQLite посчитай точное число строк в s2t_transformations.",
        model=model,
        available_tools=get_tools(),
    )

    assert route.tools == ["list_s2t_transformations"]
    assert route.skills == ["S2T-строки"]
    assert len(model.calls) == 1


def test_chat_tool_router_accepts_empty_selection_when_no_data_is_needed():
    model = _SequenceToolRouterModel(
        [ToolRoute(tools=[], skills=[], schemas=[])]
    )

    route = _select_chat_route(
        "Ответь одним словом: привет",
        model=model,
        available_tools=get_tools(),
    )

    assert route.tools == []
    assert route.skills == []
    assert len(model.calls) == 1


def test_chat_tool_router_allows_empty_skills_and_schemas_with_tools():
    model = _SequenceToolRouterModel(
        [ToolRoute(tools=["run_sql"], skills=[], schemas=[])]
    )

    route = _select_chat_route(
        "Выполни уже составленный SQL с известной схемой.",
        model=model,
        available_tools=get_tools(),
    )

    assert route.tools == ["run_sql"]
    assert route.skills == []
    assert route.schemas == []


def test_chat_tool_router_selects_run_sql_for_sqlite_aggregation():
    model = _SequenceToolRouterModel(
        [
            ToolRoute(
                tools=["run_sql"],
                skills=["S2T-строки"],
                schemas=["SQLite ETL"],
            ),
        ]
    )

    route = _select_chat_route(
        "Посчитай агрегат одной SQLite-таблицы.",
        model=model,
        available_tools=get_tools(),
    )

    assert route.tools == ["run_sql"]
    assert len(model.calls) == 1


def test_chat_tool_router_allows_empty_tools_with_skill_or_schema_context():
    model = _SequenceToolRouterModel(
        [
            ToolRoute(
                tools=[],
                skills=["Neo4j"],
                schemas=["S2T-маппинг"],
            )
        ]
    )

    route = _select_chat_route(
        "Проанализируй уже переданные факты без новых вызовов данных.",
        model=model,
        available_tools=get_tools(),
    )

    assert route.tools == []
    assert route.skills == ["Neo4j"]
    assert route.schemas == ["S2T-маппинг"]


def test_chat_tool_router_uses_general_fallback_after_invalid_llm_repair():
    model = _SequenceToolRouterModel(
        [
            {
                "tools": ["run_sql"],
                "skills": [],
                "schemas": ["SQLite ETL"],
                "reason": "x",
            },
            {
                "tools": ["run_sql"],
                "skills": [],
                "schemas": ["SQLite ETL"],
                "reason": "y",
            },
        ]
    )

    route = _select_chat_route(
        "Маршрутизируй",
        model=model,
        available_tools=get_tools(),
    )

    assert route.tools == _available_fallback_tool_names()
    assert route.skills == []
    assert route.schemas == []
    assert len(model.calls) == 2


def test_chat_tool_router_falls_back_for_invalid_llm_plan():
    invalid_routes = [
        {"tools": ["unknown"], "skills": [], "schemas": []},
        {"tools": ["resolve_entities"], "skills": [], "schemas": []},
        {
            "tools": ["run_sql"],
            "skills": ["unknown"],
            "schemas": ["SQLite ETL"],
        },
        {"tools": ["run_sql"]},
        {
            "tools": ["run_sql"],
            "skills": [],
            "schemas": ["SQLite ETL"],
            "reason": "extra field",
        },
        {"tools": [], "skills": [], "schemas": ["unknown"]},
        {"capabilities": ["sql_query"]},
    ]
    for route in invalid_routes:
        model = _ToolRouterModel(route)

        fallback = _select_chat_route(
            "Маршрутизируй",
            model=model,
            available_tools=get_tools(),
        )
        assert fallback.tools == _available_fallback_tool_names()
        assert fallback.skills == []
        assert fallback.schemas == []


def test_chat_tool_router_falls_back_after_llm_failure():
    model = _ToolRouterModel(RuntimeError("router unavailable"))

    fallback = _select_chat_route(
        "Маршрутизируй",
        model=model,
        available_tools=get_tools(),
    )
    assert fallback.tools == _available_fallback_tool_names()


def test_chat_tool_router_falls_back_for_invalid_structured_value():
    for raw_text in ("не JSON", "[]", ""):
        model = _ToolRouterModel(raw_text)

        fallback = _select_chat_route(
            "Маршрутизируй",
            model=model,
            available_tools=get_tools(),
        )
        assert fallback.tools == _available_fallback_tool_names()


class _ObserverModel:
    def __init__(self):
        self.messages = []

    def invoke(self, messages):
        from agents.chat_graph import Observation

        self.messages.append(messages)
        payload = next(
            json.loads(message.content)
            for message in messages
            if str(getattr(message, "content", "")).lstrip().startswith("{")
        )
        accepted_results = [
            result
            for result in payload.get("tool_results", [])
            if not result.get("is_error")
            and result.get("name") != "analyze_known_facts"
            and str(result.get("tool_call_id") or "")
        ]
        return Observation(
            status="complete",
            accepted_tool_call_ids=[
                str(result["tool_call_id"]) for result in accepted_results
            ],
            facts=[
                {
                    "text": "Инструмент вернул ok=true.",
                    "evidence_ids": [
                        str(result["evidence_id"])
                        for result in accepted_results
                        if result.get("evidence_id")
                    ],
                }
            ] if accepted_results else [],
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

    def with_structured_output(self, schema):
        from agents.chat_graph import Observation

        assert schema is Observation
        return self.observer

    def invoke(self, messages, **kwargs):
        del kwargs
        if (
            messages
            and getattr(messages[0], "type", None) == "system"
            and "Ты observer многошагового агента" in str(messages[0].content)
        ):
            return self.observer.invoke(messages)
        self.messages.append(messages)
        return self.responses.pop(0)


def test_observation_requires_gap_for_non_complete_status():
    from agents.chat_graph import Observation

    with pytest.raises(ValueError, match="gap must describe"):
        Observation(status="continue")
    with pytest.raises(ValueError, match="gap must describe"):
        Observation(
            status="reroute",
            gap="  ",
        )

    observation = Observation(
        status="continue",
        gap="Task просит target_table, а tool вернул source_table.",
    )

    assert observation.gap == (
        "Task просит target_table, а tool вернул source_table."
    )
    assert set(Observation.model_json_schema()["required"]) == {"status"}


def test_observation_normalizes_gigachat_string_null_gap():
    from agents.chat_graph import Observation

    observation = Observation(status="complete", gap="  NuLl  ")

    assert observation.gap is None

    with pytest.raises(ValueError, match="gap must describe"):
        Observation(status="continue", gap="null")


def test_observation_carries_typed_capability_reroute_metadata():
    from agents.chat_graph import Observation

    reroute = Observation(
        status="reroute",
        gap="Нужна SQL-агрегация.",
        reroute_reason="missing_capability",
        required_capabilities=["sql_read"],
    )

    assert reroute.reroute_reason == "missing_capability"
    assert reroute.required_capabilities == ["sql_read"]
    with pytest.raises(ValueError, match="requires required_capabilities"):
        Observation(
            status="reroute",
            gap="Нужна новая палитра.",
            reroute_reason="missing_capability",
        )
    continue_observation = Observation(
        status="continue",
        gap="Исправить аргументы.",
        reroute_reason="wrong_arguments",
        required_capabilities=["sql_read"],
    )
    assert continue_observation.reroute_reason is None
    assert continue_observation.required_capabilities == []


def test_observation_normalizes_local_nested_reroute_metadata():
    from agents.chat_graph import Observation

    observation = Observation.model_validate(
        {
            "status": "reroute",
            "gap": "Нужен каталог target-колонок.",
            "reroute_reason": {
                "type": "missing_capability",
                "details": "Нужен другой инструмент.",
            },
            "required_capabilities": ["target_column_catalog_read"],
        }
    )

    assert observation.reroute_reason == "missing_capability"
    assert observation.required_capabilities == ["column_catalog_read"]


def test_native_observer_schema_is_provider_compatible_flat_object():
    from agents.chat_graph import Observation

    schema = Observation.model_json_schema()

    assert schema["type"] == "object"
    assert "oneOf" not in schema
    assert schema["properties"]["status"]["enum"] == [
        "complete",
        "continue",
        "reroute",
    ]


def test_observation_status_discards_provider_added_non_reroute_metadata():
    from agents.chat_graph import Observation

    complete = Observation.model_validate(
        {
            "status": "complete",
            "accepted_tool_call_ids": ["call-exact-read"],
            "facts": [
                {
                    "text": "Точные типы прочитаны.",
                    "evidence_ids": ["evidence-exact-read"],
                }
            ],
            "reroute_reason": "missing_capability",
            "required_capabilities": ["sql_read"],
        }
    )

    assert complete.status == "complete"
    assert complete.accepted_tool_call_ids == ["call-exact-read"]
    assert complete.reroute_reason is None
    assert complete.required_capabilities == []


def test_observation_discards_echoed_input_payload_fields():
    from agents.chat_graph import Observation

    observation = Observation.model_validate(
        {
            "status": "continue",
            "gap": "Нужно точное чтение.",
            "accepted_tool_call_ids": [],
            "facts": [],
            "limitations": [],
            "prior_state": [],
            "accepted_evidence": [],
            "tool_calls": [],
            "tool_results": [],
            "user_request": "input-only",
        }
    )

    assert observation.status == "continue"
    assert observation.gap == "Нужно точное чтение."


def test_observer_continue_for_unavailable_named_tool_becomes_reroute():
    from agents.chat_graph import Observation, _reroute_for_unavailable_tool

    observation = _reroute_for_unavailable_tool(
        Observation(
            status="continue",
            gap="Нужен list_file_sheet_headers для точных заголовков.",
        ),
        ["resolve_file"],
    )

    assert observation.status == "reroute"
    assert observation.reroute_reason == "missing_capability"
    assert observation.required_capabilities == ["excel_read"]


def test_observer_prompt_requires_semantic_task_comparison():
    from agents.chat_graph import (
        Observation,
        _OBSERVER_REPAIR_PROMPT,
        _OBSERVER_PROMPT,
        _WORKER_PLANNER_PROMPT,
    )

    normalized_observer_prompt = " ".join(_OBSERVER_PROMPT.split())
    assert len(_OBSERVER_PROMPT) < 2000
    assert "structured output" in _OBSERVER_PROMPT
    assert "без Markdown" in _OBSERVER_PROMPT
    for status in ("complete", "continue", "reroute"):
        assert f"`{status}`" in _OBSERVER_PROMPT
    assert Observation.model_json_schema()["properties"]["status"]["enum"] == [
        "complete",
        "continue",
        "reroute",
    ]
    assert "`gap`" in _OBSERVER_PROMPT
    assert "как mismatch" not in _OBSERVER_PROMPT
    assert "target_table" not in _OBSERVER_PROMPT
    assert "source_table" not in _OBSERVER_PROMPT
    assert "фильтруемыми и возвращаемыми полями" not in _OBSERVER_PROMPT
    assert "source_field" not in _OBSERVER_PROMPT
    assert "одна консолидированная строка" in _OBSERVER_PROMPT
    assert "повторно получать не требуется" not in normalized_observer_prompt
    assert "ролевой фильтр" not in normalized_observer_prompt.lower()
    assert "промежуточные узлы" not in _OBSERVER_PROMPT
    assert "impact/downstream" not in _OBSERVER_PROMPT
    assert "производный анализ" in normalized_observer_prompt
    assert "upstream сделает его" in normalized_observer_prompt
    assert "не формулируй" in normalized_observer_prompt
    assert "не повторяй одну причину" in normalized_observer_prompt.lower()
    assert "accepted_tool_call_ids" in _OBSERVER_PROMPT
    accepted_schema = Observation.model_json_schema()["properties"][
        "accepted_tool_call_ids"
    ]
    assert accepted_schema["maxItems"] == 20
    assert "семантически подтверждают" in accepted_schema["description"]
    gap_description = Observation.model_json_schema()["properties"][
        "gap"
    ]["description"]
    assert "из текущего результата и prior_state" in gap_description
    assert "Одна краткая консолидированная строка" in gap_description
    assert "одну причину и её следствия" in gap_description
    facts_schema = Observation.model_json_schema()["properties"]["facts"]
    assert "evidence_id" in facts_schema["description"]
    assert "Data tool уже выполнен" in _OBSERVER_REPAIR_PROMPT
    assert "не требуй его повторного" in _OBSERVER_REPAIR_PROMPT
    assert "тот же исходный user_request" in _OBSERVER_REPAIR_PROMPT
    assert "ни один `available_tools` не закрывает gap" in _OBSERVER_PROMPT
    assert "planner сам составил" in _OBSERVER_PROMPT
    assert "Нулевой результат подтверждает отсутствие данных только" in (
        _OBSERVER_PROMPT
    )
    assert "не подтверждает исходную операцию" in _OBSERVER_PROMPT
    assert "row_format=named_records_with_dictionary_refs" in _OBSERVER_PROMPT
    assert "явные имена полей" in _OBSERVER_PROMPT
    assert "не схлопывай отдельные rows" in _OBSERVER_PROMPT
    normalized_worker_prompt = " ".join(_WORKER_PLANNER_PROMPT.split())
    assert "при `continue` закрой `gap`" in normalized_worker_prompt
    assert "действие `analyze`" not in _WORKER_PLANNER_PROMPT
    assert "заверши работу через `finish_worker`" in normalized_worker_prompt
    assert "Обычный текст запрещён" in normalized_worker_prompt
    assert "`table.column` разделяй на `table_name` и `column_name`" in (
        normalized_worker_prompt
    )
    assert (
        "row_format=named_records_with_dictionary_refs"
        in _WORKER_PLANNER_PROMPT
    )
    assert "0-based" in _WORKER_PLANNER_PROMPT
    assert "каждая row остаётся отдельным фактом" in (
        _WORKER_PLANNER_PROMPT
    )


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
    observer_system_prompt = str(model.observer.messages[0][0].content)
    assert observer_system_prompt.startswith("Системный контекст")
    assert "structured output" in observer_system_prompt
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


def test_planner_calls_second_tool_after_observer_when_task_is_unfinished():
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
            ),
            AIMessage(content="Оба инструментальных шага выполнены."),
            AIMessage(content="Готово."),
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
    assert len(model.messages) == 4


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
    assert '"error": "temporary failure"' in second_planner_prompt
    assert "После ошибки скорректируй действие" in second_planner_prompt


def test_planner_uses_bound_tool_contracts_without_tool_specific_cases():
    from agents.chat_graph import _planner_instruction

    instruction = _planner_instruction(
        ("run_sql", "trace_neo4j_lineage")
    )

    assert "description и схему" in instruction
    assert "сохраняй смысл" in instruction.lower()
    assert "точные значения" in instruction
    assert "SQLITE_QUERY" not in instruction
    assert "GRAPH_NEO4J" not in instruction
    assert "table_name" not in instruction


def test_tool_router_prompt_is_generic_and_catalog_driven():
    from agents.tools.routing import _TOOL_ROUTER_PROMPT

    normalized_prompt = " ".join(_TOOL_ROUTER_PROMPT.split())
    assert "точные имена из каталогов" in normalized_prompt
    assert "необходимую planner-палитру" in normalized_prompt
    assert "каждый может быть пустым" in normalized_prompt
    assert "оставляй `tools=[]`" in normalized_prompt
    assert "Покрой обязательные входы" in normalized_prompt
    assert "получается выбранным tool" in normalized_prompt
    assert "`not_for` — запрет" in normalized_prompt
    assert "catalog_stage=capability_expansion" in normalized_prompt
    assert "не расширяй палитру по числу попыток" in normalized_prompt
    assert "При `wrong_arguments` палитру не меняй" in normalized_prompt
    assert "Наличие `previous_results` само по себе не создаёт зависимость" in (
        normalized_prompt
    )
    assert len(_TOOL_ROUTER_PROMPT) < 1850
    for domain_detail in (
        "trace_neo4j_table_lineage",
        "run_cypher",
        "run_sql",
        "COUNT",
        "SQLite",
        "Neo4j",
    ):
        assert domain_detail not in _TOOL_ROUTER_PROMPT


def test_worker_prompts_require_internal_analysis_tool_for_empty_route():
    from agents.chat_graph import (
        _OBSERVER_PROMPT,
        _WORKER_PLANNER_PROMPT,
    )
    from agents.tools.routing import _TOOL_ROUTER_REPAIR_PROMPT

    assert "Палитра worker никогда не пуста" in _WORKER_PLANNER_PROMPT
    assert "`analyze_known_facts`" in _WORKER_PLANNER_PROMPT
    assert "внутреннего `analyze_known_facts`" in _OBSERVER_PROMPT
    assert "списки могут быть\nпустыми" in _TOOL_ROUTER_REPAIR_PROMPT


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


def test_run_agent_graph_rejects_parallel_batch_over_remaining_limit():
    from agents.chat_graph import WorkerResponseError, run_agent_graph

    calls = []

    def ping():
        calls.append("ping")
        return {"ok": True}

    def pong():
        calls.append("pong")
        return {"ok": True}

    model = _ScriptedNativeModel(
        [
            AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "ping",
                        "args": {},
                        "id": "call-ping-over-limit",
                        "type": "tool_call",
                    },
                    {
                        "name": "pong",
                        "args": {},
                        "id": "call-pong-over-limit",
                        "type": "tool_call",
                    },
                ],
            )
        ]
    )

    with pytest.raises(WorkerResponseError, match="запрошено 2, доступно 1"):
        run_agent_graph(
            "Проверь обе операции",
            "Системный контекст",
            model,
            (_as_tool(ping), _as_tool(pong)),
            max_steps=1,
        )

    assert calls == []


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


def test_run_agent_graph_passes_history_without_implicit_file_context():
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
    )

    assert out == "На листе S2T."
    planner_messages = model.messages[0]
    contents = [str(message.content) for message in planner_messages]
    planner_prompt = "\n".join(contents)
    assert all("file_id=42" not in content for content in contents)
    assert "Какие файлы загружены?" in contents
    assert "Загружен mapping.xlsx." in contents
    assert "А какие в нём листы?" in contents
    assert "доступные tools:\nнет" in planner_prompt
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
    assert "Сохраняй требуемую полноту" in responder_system_text
    assert "s2t_transformations" not in responder_system_text
    assert "target_table" not in responder_system_text
    assert "Markdown-таблица" not in responder_system_text

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
