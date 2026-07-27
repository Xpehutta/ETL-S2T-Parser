import re

import pytest
from unittest.mock import patch
from langchain_core.tools import BaseTool

import storage.database as db_storage
from storage.database import get_db_connection, init_db


def test_sqlite_schema_cheatsheet_is_independent_from_tool_documentation():
    from agents.tools import get_sqlite_schema_cheatsheet

    text = get_sqlite_schema_cheatsheet()
    assert "Актуальная схема SQLite" in text
    assert "`files`" in text
    assert "`file_sheet_headers`" in text
    assert "`s2t_transformations`" in text
    assert "| `data` | публичная |" in text
    assert "Устаревшие catalog/lineage-таблицы удалены" in text
    assert "summarize_s2t_tables" not in text


def test_sqlite_schema_cheatsheet_is_generated_from_db_storage():
    from agents.tools import get_sqlite_schema_cheatsheet

    text = get_sqlite_schema_cheatsheet()
    for table_name, columns in db_storage.STORAGE_SCHEMA_COLUMNS.items():
        assert f"`{table_name}`" in text
        for column_name in columns:
            assert f"`{column_name}`" in text
    assert "сгенерирован из `storage/database.py`" in text
    assert "Внутренние таблицы упоминай" not in text


def test_load_chat_agent_context_is_runtime_specific():
    from agents.tools import load_chat_agent_context

    text = load_chat_agent_context()
    assert "Flask chat-agent" in text
    assert "Это не `AGENTS.md`" in text
    assert "Актуальная схема SQLite" in text


@pytest.fixture(autouse=True)
def _temp_db_path(tmp_path):
    original = db_storage.DB_PATH
    db_storage.DB_PATH = str(tmp_path / "tools_test.db")
    init_db()
    yield
    db_storage.DB_PATH = original


def test_run_sql_select():
    from agents.tools import run_sql

    conn = get_db_connection()
    conn.execute(
        "INSERT INTO files (file_id, filename, upload_time) VALUES (?, ?, ?)",
        (1, "f.xlsx", "2024-01-01"),
    )
    conn.commit()
    conn.close()

    result = run_sql.invoke(
        {"query": "SELECT file_id, filename FROM files WHERE file_id = 1"}
    )
    assert result["rows"] == [{"file_id": 1, "filename": "f.xlsx"}]
    assert result["returned_rows"] == 1
    assert result["truncated"] is False


def test_run_sql_export_csv_writes_full_result(tmp_path, monkeypatch):
    import agents.tools.sql as sql_tools
    from agents.tools import run_sql

    monkeypatch.setattr(sql_tools, "SQL_EXPORT_DIR", tmp_path / "exports")

    conn = get_db_connection()
    conn.executemany(
        "INSERT INTO files (file_id, filename, upload_time) VALUES (?, ?, ?)",
        [
            (11, "one.xlsx", "2026-01-01"),
            (12, "two.xlsx", "2026-01-02"),
        ],
    )
    conn.commit()
    conn.close()

    out = run_sql.invoke(
        {
            "query": "SELECT file_id, filename FROM files ORDER BY file_id",
            "export_csv": True,
            "preview_limit": 1,
        }
    )

    assert out["row_count"] == 2
    assert out["columns"] == ["file_id", "filename"]
    assert out["preview_rows"] == [{"file_id": 11, "filename": "one.xlsx"}]
    assert out["csv_url"].startswith("/exports/sql/sql_result_")
    csv_path = tmp_path / "exports" / out["csv_filename"]
    assert out["csv_path"] == str(csv_path)
    assert csv_path.exists()
    csv_text = csv_path.read_text(encoding="utf-8-sig")
    assert "11,one.xlsx" in csv_text
    assert "12,two.xlsx" in csv_text


def test_run_sql_invalid_returns_error_dict():
    from agents.tools import run_sql

    out = run_sql.invoke({"query": "NOT A VALID STMT"})
    assert isinstance(out, dict)
    assert "error" in out


def test_run_sql_rejects_write_queries():
    from agents.tools import run_sql

    out = run_sql.invoke(
        {"query": "INSERT INTO files (file_id, filename) VALUES ('bad', 'bad.xlsx')"}
    )
    assert isinstance(out, dict)
    assert "Only SELECT, WITH and EXPLAIN QUERY PLAN are allowed" in out["error"]

    result = run_sql.invoke(
        {"query": "SELECT file_id FROM files WHERE file_id = 'bad'"}
    )
    assert result["rows"] == []


def test_list_files_empty():
    from agents.tools import list_files

    assert list_files.invoke({}) == []


def test_list_files_returns_catalog_only():
    from agents.tools import list_files

    conn = get_db_connection()
    conn.execute(
        "INSERT INTO files (file_id, filename, upload_time, summary, description) VALUES (?, ?, ?, ?, ?)",
        (21, "summary.xlsx", "2026-01-01", "Business summary", "Short description"),
    )
    conn.execute(
        """INSERT INTO file_sheet_headers
        (file_id, sheet_id, sheet_name, skipped, header_start_row,
         header_rows_count, nested_structure, columns_count, headers_json, headers_flat)
        VALUES (?, ?, ?, 0, 0, 1, 0, 0, '[]', '')""",
        (21, 22, "Лист, который не должен попасть в каталог"),
    )
    conn.commit()
    conn.close()

    result = list_files.invoke({})
    assert result == [
        {
            "filename": "summary.xlsx",
            "description": "Short description",
            "upload_time": "2026-01-01",
        }
    ]
    assert "21" not in str(result)
    assert "Лист, который не должен попасть в каталог" not in str(result)


def test_resolve_file_uses_exact_filename_case_insensitively():
    from agents.tools import resolve_file

    conn = get_db_connection()
    conn.execute(
        "INSERT INTO files (file_id, filename, upload_time) VALUES (?, ?, ?)",
        (23, "Mapping.xlsx", "2026-01-01"),
    )
    conn.commit()
    conn.close()

    assert resolve_file.invoke({"filename": "mapping.xlsx"}) == {
        "file_id": 23,
        "filename": "Mapping.xlsx",
        "upload_time": "2026-01-01",
    }
    assert (
        resolve_file.invoke({"filename": "missing.xlsx"})["error"]
        == "Uploaded file not found"
    )


def test_registered_tools_exclude_removed_legacy_tools():
    from agents.tools import WRITE_TOOLS_BY_NAME, get_tools_by_name

    tools = get_tools_by_name()
    assert all(isinstance(tool, BaseTool) for tool in tools.values())
    assert "run_sql" in tools
    assert "run_cypher" in tools
    assert "get_file_description" in tools
    assert "search_s2t_transformations" in tools
    assert "summarize_s2t_tables" in tools
    assert "summarize_table_descriptions" in tools
    assert "list_s2t_transformations" in tools
    assert "resolve_file" in tools
    assert "show_plan" in tools
    assert "list_file_sheet_headers" in tools
    assert "trace_neo4j_lineage" in tools
    assert "update_file_description" not in tools
    assert "update_table_info_from_user_query" not in tools
    assert "update_file_description" in WRITE_TOOLS_BY_NAME
    assert "update_table_info_from_user_query" in WRITE_TOOLS_BY_NAME
    for removed in (
        "search_column_mappings",
        "list_target_table_columns",
        "mapping_overview",
        "get_lineage",
        "similarity_search",
        "find_similar_columns",
    ):
        assert removed not in tools


def test_registry_uses_the_decorated_tool_objects_directly():
    from agents.tools.registry import READ_ONLY_TOOLS, WRITE_TOOLS
    from agents.tools.files import update_file_description
    from agents.tools.neo4j import run_cypher
    from agents.tools.sql import run_sql

    assert run_sql in READ_ONLY_TOOLS
    assert run_cypher in READ_ONLY_TOOLS
    assert update_file_description in WRITE_TOOLS
    assert isinstance(run_sql, BaseTool)
    assert isinstance(run_cypher, BaseTool)
    assert isinstance(update_file_description, BaseTool)


def test_registered_tools_expose_annotation_derived_argument_schemas():
    from agents.tools import get_tools_by_name

    tools = get_tools_by_name()
    sql_schema = tools["run_sql"].args_schema.model_json_schema()
    assert sql_schema["required"] == ["query"]
    assert set(sql_schema["properties"]) == {
        "query",
        "export_csv",
        "preview_limit",
    }
    assert sql_schema["properties"]["export_csv"]["type"] == "boolean"
    assert sql_schema["properties"]["preview_limit"]["type"] == "integer"

    cypher_schema = tools["run_cypher"].args_schema.model_json_schema()
    assert cypher_schema["required"] == ["query"]
    assert set(cypher_schema["properties"]) == {
        "query",
        "parameters",
        "limit",
    }
    assert cypher_schema["properties"]["limit"]["type"] == "integer"

    files_schema = tools["list_files"].args_schema.model_json_schema()
    assert files_schema["properties"] == {}
    assert tools["list_files"].return_direct is False
    assert tools["list_files"].invoke({}) == []

    summary_schema = tools["summarize_s2t_tables"].args_schema.model_json_schema()
    assert summary_schema["properties"]["group_by"]["enum"] == ["source", "target"]
    assert set(summary_schema["properties"]) == {
        "group_by",
        "min_related_tables",
        "limit",
    }
    list_s2t_schema = tools[
        "list_s2t_transformations"
    ].args_schema.model_json_schema()
    assert set(list_s2t_schema["properties"]) == {"limit", "q"}
    search_s2t_schema = tools[
        "search_s2t_transformations"
    ].args_schema.model_json_schema()
    assert set(search_s2t_schema["properties"]) == {"needle", "limit"}
    description_schema = tools["summarize_table_descriptions"].args_schema.model_json_schema()
    assert description_schema["required"] == ["table_name"]
    assert set(description_schema["properties"]) == {"table_name", "file_id", "limit"}
    file_description_schema = tools["get_file_description"].args_schema.model_json_schema()
    assert "Числовой идентификатор загрузки" in file_description_schema[
        "properties"
    ]["file_id"]["description"]

    plan_schema = tools["show_plan"].args_schema.model_json_schema()
    assert plan_schema["required"] == ["done", "to_do"]
    assert set(plan_schema["properties"]) == {"done", "to_do"}

    lineage_schema = tools["trace_neo4j_lineage"].args_schema.model_json_schema()
    assert lineage_schema["required"] == ["table_name"]
    assert set(lineage_schema["properties"]) == {
        "table_name",
        "column_name",
        "file_id",
        "direction",
        "limit",
    }
    assert lineage_schema["properties"]["direction"]["enum"] == [
        "upstream",
        "downstream",
        "both",
    ]


@patch("agents.tools.neo4j.execute_neo4j_read")
def test_run_cypher_returns_limited_rows(mock_read):
    from agents.tools import run_cypher

    mock_read.return_value = [
        {"source": "a", "target": "b"},
        {"source": "b", "target": "s"},
        {"source": "extra", "target": "extra"},
    ]
    query = """
        MATCH (source:ETLColumn)-[mapping:TRANSFORMS_TO]->
              (target:ETLColumn)
        WHERE mapping.file_id = $file_id
        RETURN source.name AS source, target.name AS target
    """

    result = run_cypher.invoke(
        {
            "query": query,
            "parameters": {"file_id": 7},
            "limit": 2,
        }
    )

    mock_read.assert_called_once_with(
        query.strip(),
        {"file_id": 7},
        row_limit=3,
    )
    assert result["columns"] == ["source", "target"]
    assert result["rows"] == [
        {"source": "a", "target": "b"},
        {"source": "b", "target": "s"},
    ]
    assert result["returned_rows"] == 2
    assert result["truncated"] is True
    assert result["limit"] == 2


@pytest.mark.parametrize(
    "query, expected_error",
    [
        (
            "MATCH (node) DELETE node",
            "Mutating or procedural Cypher is not allowed: DELETE",
        ),
        (
            "MATCH (node) SET node.value = 1 RETURN node",
            "Mutating or procedural Cypher is not allowed: SET",
        ),
        (
            "CALL db.labels()",
            "Only MATCH, OPTIONAL MATCH, WITH, UNWIND, RETURN, SHOW, "
            "EXPLAIN and PROFILE queries are allowed",
        ),
        (
            "MATCH (node) RETURN node; MATCH (other) RETURN other",
            "Exactly one Cypher statement is allowed",
        ),
    ],
)
@patch("agents.tools.neo4j.execute_neo4j_read")
def test_run_cypher_rejects_unsafe_queries(
    mock_read,
    query,
    expected_error,
):
    from agents.tools import run_cypher

    result = run_cypher.invoke({"query": query})

    mock_read.assert_not_called()
    assert result["error"] == expected_error
    assert result["query"] == query


@patch("agents.tools.neo4j.execute_neo4j_read")
def test_run_cypher_ignores_keywords_inside_literals_comments_and_identifiers(
    mock_read,
):
    from agents.tools import run_cypher

    mock_read.return_value = [{"value": "CREATE SET DELETE CALL"}]
    query = """
        // DELETE node
        MATCH (node:`CREATE`)
        WHERE node.rule = 'CREATE SET DELETE CALL;'
        RETURN node.rule AS `SET`
    """

    result = run_cypher.invoke({"query": query})

    mock_read.assert_called_once_with(
        query.strip(),
        {},
        row_limit=21,
    )
    assert result["rows"] == [{"value": "CREATE SET DELETE CALL"}]
    assert result["truncated"] is False


@patch("agents.tools.neo4j.execute_neo4j_read")
def test_trace_neo4j_lineage_uses_exact_names_and_scope(mock_read):
    from agents.tools import trace_neo4j_lineage

    mock_read.return_value = [
        {
            "file_id": 7,
            "source_table": "a_source",
            "source_field": "client_id",
            "target_table": "b_target",
            "target_field": "client_id",
            "match_direction": "downstream",
        }
    ]

    result = trace_neo4j_lineage.invoke(
        {
            "table_name": "a_source",
            "column_name": "client_id",
            "file_id": 7,
            "direction": "downstream",
            "limit": 250,
        }
    )

    query, parameters = mock_read.call_args.args
    assert "MATCH (source:ETLProjection:ETLColumn)" in query
    assert "[mapping:TRANSFORMS_TO]" in query
    assert "source.table_name = $table_name" in query
    assert "source.name = $column_name" in query
    assert "target.table_name = $table_name" in query
    assert "target.name = $column_name" in query
    assert "transformation_rule" not in query
    assert "sheet_name" not in query
    assert parameters == {
        "table_name": "a_source",
        "column_name": "client_id",
        "file_id": 7,
        "direction": "downstream",
        "limit": 100,
    }
    assert result["returned_rows"] == 1
    assert result["direction"] == "downstream"


@patch("agents.tools.neo4j.execute_neo4j_read")
def test_trace_neo4j_lineage_rejects_empty_table_name(mock_read):
    from agents.tools import trace_neo4j_lineage

    result = trace_neo4j_lineage.invoke({"table_name": "   "})

    mock_read.assert_not_called()
    assert result == {
        "error": "table_name must be non-empty",
        "rows": [],
    }


def test_show_plan_returns_explicit_progress_without_side_effects():
    from agents.tools import show_plan

    assert show_plan.invoke(
        {
            "done": "Найден текущий файл.",
            "to_do": "Прочитать его S2T-трансформации.",
        }
    ) == {
        "done": "Найден текущий файл.",
        "to_do": "Прочитать его S2T-трансформации.",
    }


def test_all_registered_tool_descriptions_are_in_russian():
    from agents.tools import get_tools_by_name

    tools = get_tools_by_name()
    missing_russian = {
        name: tool.description
        for name, tool in tools.items()
        if not re.search(r"[А-Яа-яЁё]", tool.description or "")
    }
    assert missing_russian == {}


def test_registered_tools_use_expanded_docstrings_without_markdown_catalog():
    import agents.tools as load_skills_tools

    tools = load_skills_tools.get_tools_by_name()
    assert not hasattr(load_skills_tools, "load_tools")
    assert not (load_skills_tools.PROJECT_ROOT / "tools.md").exists()
    for tool in tools.values():
        assert len(tool.description) >= 180
        properties = tool.args_schema.model_json_schema().get("properties", {})
        assert all(property_schema.get("description") for property_schema in properties.values())


def test_tool_descriptions_separate_sqlite_and_neo4j_scenarios():
    from agents.tools import get_tools_by_name

    tools = get_tools_by_name()

    assert "основной инструмент" in tools["list_s2t_transformations"].description
    assert "таблицу трансформаций" in tools["list_s2t_transformations"].description
    assert "табличного поиска" in tools["search_s2t_transformations"].description
    assert "это сценарий Neo4j" in tools["run_sql"].description
    assert "не должны содержать" in tools["run_sql"].description
    assert "фильтр по file_id" in tools["run_sql"].description

    assert "только для сложных графовых путей" in tools["run_cypher"].description
    assert "Для обычной таблицы S2T-трансформаций" in tools["run_cypher"].description
    assert "только когда пользователь просит lineage" in tools[
        "trace_neo4j_lineage"
    ].description
    assert "только узлы ETLColumn" in tools["trace_neo4j_lineage"].description


def test_get_file_description_uses_cached_value():
    from agents.tools import get_file_description

    conn = get_db_connection()
    conn.execute(
        """
        INSERT INTO files (file_id, filename, upload_time, summary, description)
        VALUES (?, ?, ?, ?, ?)
        """,
        (31, "desc.xlsx", "2026-01-01", "Long summary", "Cached description"),
    )
    conn.commit()
    conn.close()

    out = get_file_description.invoke({"file_id": 31})

    assert out["description"] == "Cached description"
    assert out["description_present"] is True
    assert out["file"]["description"] == "Cached description"


def test_get_file_description_reports_missing_without_generation():
    from agents.tools import get_file_description

    conn = get_db_connection()
    conn.execute(
        """
        INSERT INTO files (file_id, filename, upload_time, summary, description)
        VALUES (?, ?, ?, ?, ?)
        """,
        (32, "desc_gen.xlsx", "2026-01-02", "Long summary", None),
    )
    conn.commit()
    conn.close()

    out = get_file_description.invoke({"file_id": 32})

    assert out["description"] is None
    assert out["summary"] == "Long summary"
    assert out["missing_description"] is True
    assert out["summary_present"] is True
    assert "явный запрос" in out["hint"]


def test_update_file_description_tool(mock_embeddings):
    from agents.tools import update_file_description

    conn = get_db_connection()
    conn.execute(
        "INSERT INTO files (file_id, filename, upload_time) VALUES (?, ?, ?)",
        (33, "desc_upd.xlsx", "2026-01-03"),
    )
    conn.commit()
    conn.close()

    out = update_file_description.invoke(
        {"file_id": 33, "description": "Approved description"}
    )

    assert out["updated"] is True
    assert out["description"] == "Approved description"

    conn = get_db_connection()
    row = conn.execute(
        """
        SELECT description, description_embedding
        FROM files
        WHERE file_id = ?
        """,
        (33,),
    ).fetchone()
    conn.close()
    assert row["description"] == "Approved description"
    assert row["description_embedding"] == b"embedding:Approved description"


@patch("agents.summarizer_agent.update_file_description_from_user_query")
def test_update_table_info_from_user_query_tool(mock_update):
    from agents.tools import update_table_info_from_user_query

    mock_update.return_value = "Updated from user query"
    conn = get_db_connection()
    conn.execute(
        "INSERT INTO files (file_id, filename, upload_time) VALUES (?, ?, ?)",
        (34, "table_info.xlsx", "2026-01-04"),
    )
    conn.commit()
    conn.close()

    out = update_table_info_from_user_query.invoke(
        {
            "file_id": 34,
            "user_query": "Добавь, что это витрина по кредитным договорам",
        }
    )

    assert out["updated"] is True
    assert out["description"] == "Updated from user query"
    assert out["source"] == "user_query"
    assert out["sequence"] == [
        "verified_files_row",
        "ensured_generated_description",
        "updated_description_from_user_query",
    ]
    mock_update.assert_called_once_with(
        34,
        "Добавь, что это витрина по кредитным договорам",
        save=True,
    )


def test_list_sheets_and_columns_after_store(sample_excel_bytes):
    from storage.database import get_sheet_id, store_excel_data
    from agents.tools import list_columns, list_file_sheet_headers, list_sheets

    sheets = [
        {
            "sheet_name": "Sheet1",
            "skip_reason": None,
            "header": {"start_row": 0, "row_count": 1, "nested": False},
            "columns": ["ColA"],
            "data_rows": [[1]],
        }
    ]
    fh = store_excel_data(sample_excel_bytes, "x.xlsx", "m", sheets)
    assert list_sheets.invoke({"file_id": fh}) == ["Sheet1"]
    sh = get_sheet_id(fh, "Sheet1")
    columns_result = list_columns.invoke({"sheet_id": sh})
    assert columns_result["column_count"] == 1
    assert columns_result["columns"][0]["name"] == "ColA"
    headers = list_file_sheet_headers.invoke({"file_id": fh})
    assert len(headers) == 1
    assert headers[0]["sheet_name"] == "Sheet1"
    assert headers[0]["columns_count"] == 1
    assert headers[0]["headers"][0]["flat"] == "ColA"


@patch("agents.sheet_group_classifier.invoke_llm_plain_text")
def test_list_sheet_group_classifications_tool_does_not_call_llm(mock_llm, sample_excel_bytes):
    from storage.database import store_excel_data
    from agents.tools import list_sheet_group_classifications

    sheets = [
        {
            "sheet_name": "Unknown metadata sheet",
            "skip_reason": None,
            "header": {"start_row": 0, "row_count": 1, "nested": False},
            "columns": ["source column"],
            "data_rows": [["v"]],
        }
    ]
    fh = store_excel_data(
        sample_excel_bytes,
        "unknown.xlsx",
        "m",
        sheets,
    )

    out = list_sheet_group_classifications.invoke({"file_id": fh})

    mock_llm.assert_not_called()
    assert out["subagent"]["use_llm"] is False
    assert out["verification"]["status"] == "warning"
    assert out["verification"]["unmatched_sheets"] == ["Unknown metadata sheet"]


def test_search_s2t_transformations_uses_sql_table():
    from agents.tools import search_s2t_transformations

    conn = get_db_connection()
    conn.execute(
        "INSERT INTO files (file_id, filename, upload_time) VALUES (?, ?, ?)",
        (41, "s2t_search.xlsx", "2026-01-01"),
    )
    conn.execute(
        """INSERT INTO s2t_transformations
        (id, file_id, sheet_id, sheet_name, row_num, target_table, target_field,
         source_table, source_field, transformation_rule)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            42,
            41,
            43,
            "s2t",
            0,
            "t_agr_cred",
            "agr_cred_id",
            "B700000025_AGR_CRED",
            "C_ID",
            "c_id",
        ),
    )
    conn.commit()
    conn.close()

    data = search_s2t_transformations.invoke(
        {"needle": "B700000025_AGR_CRED"}
    )
    assert data["searched_table"] == "s2t_transformations"
    assert data["scope"] == "global"
    assert "file_id" not in data
    assert "file" not in data
    assert data["total"] == 1
    assert data["rows"][0]["source_table"] == "B700000025_AGR_CRED"
    assert "table_transformation_sql" not in data["rows"][0]


def test_summarize_s2t_tables_groups_shared_targets_by_source():
    from agents.tools import summarize_s2t_tables

    conn = get_db_connection()
    conn.execute(
        "INSERT INTO files (file_id, filename, upload_time) VALUES (?, ?, ?)",
        (51, "summary.xlsx", "2026-01-01"),
    )
    conn.executemany(
        """INSERT INTO s2t_transformations
        (id, file_id, sheet_id, sheet_name, row_num, target_table, target_field,
         source_table, source_field, transformation_rule)
        VALUES (?, 51, 52, 'S2T', ?, ?, ?, ?, ?, ?)""",
        [
            (53, 1, "t_target_a", "a_id", "src_shared", "id", "direct"),
            (54, 2, "t_target_b", "b_id", "src_shared", "id", "direct"),
            (55, 3, "t_target_a", "a_code", "src_single", "code", ""),
        ],
    )
    conn.commit()
    conn.close()

    result = summarize_s2t_tables.invoke(
        {
            "group_by": "source",
            "min_related_tables": 2,
        }
    )

    assert result["group_by"] == "source"
    assert result["scope"] == "global"
    assert "file_id" not in result
    assert result["group_count"] == 1
    assert result["groups"] == [
        {
            "table_name": "src_shared",
            "mapping_count": 2,
            "field_count": 1,
            "related_table_count": 2,
            "mappings_with_rule": 2,
            "related_tables": ["t_target_a", "t_target_b"],
            "rule_coverage": 1.0,
        }
    ]


def test_summarize_table_descriptions_combines_roles_and_preserves_duplicates():
    from agents.tools import summarize_table_descriptions

    conn = get_db_connection()
    conn.executemany(
        "INSERT INTO files (file_id, filename, upload_time) VALUES (?, ?, ?)",
        [
            (71, "current.xlsx", "2026-01-01"),
            (72, "other.xlsx", "2026-01-02"),
        ],
    )
    conn.executemany(
        """INSERT INTO source_tables
        (id, file_id, sheet_id, sheet_name, row_num, table_name, description)
        VALUES (?, ?, ?, ?, ?, ?, ?)""",
        [
            (701, 71, 711, "Sources", 0, "t_shared", "Описание источника"),
            (702, 71, 711, "Sources", 1, "t_shared", "Описание источника"),
        ],
    )
    conn.executemany(
        """INSERT INTO target_tables
        (id, file_id, sheet_id, sheet_name, row_num, table_name, description)
        VALUES (?, ?, ?, ?, ?, ?, ?)""",
        [
            (703, 71, 712, "Targets", 0, " T_SHARED ", "Описание приёмника"),
            (704, 72, 721, "Targets", 0, "t_shared", "Описание другого файла"),
        ],
    )
    conn.commit()
    conn.close()

    result = summarize_table_descriptions.invoke(
        {"table_name": "t_shared", "file_id": 71}
    )

    assert result["searched_tables"] == ["source_tables", "target_tables"]
    assert result["total_matches"] == 3
    assert result["returned_matches"] == 3
    assert result["role_counts"] == {"source": 2, "target": 1}
    assert result["descriptions_present"] == 3
    assert [item["description"] for item in result["combined_descriptions"]] == [
        "Описание источника",
        "Описание источника",
        "Описание приёмника",
    ]
    assert [item["table_role"] for item in result["matches"]] == [
        "source",
        "source",
        "target",
    ]


def test_summarize_table_descriptions_does_not_guess_similar_name():
    from agents.tools import summarize_table_descriptions

    conn = get_db_connection()
    conn.execute(
        """INSERT INTO source_tables
        (id, file_id, sheet_id, sheet_name, row_num, table_name, description)
        VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (705, 71, 711, "Sources", 0, "t_credit", "Кредитная таблица"),
    )
    conn.commit()
    conn.close()

    result = summarize_table_descriptions.invoke({"table_name": "t_credi"})

    assert result["total_matches"] == 0
    assert result["matches"] == []
    assert result["combined_descriptions"] == []


def test_list_s2t_transformations_is_non_terminal_preview():
    from agents.tools import list_s2t_transformations

    conn = get_db_connection()
    conn.execute(
        "INSERT INTO files (file_id, filename, upload_time) VALUES (?, ?, ?)",
        (61, "direct.xlsx", "2026-01-01"),
    )
    conn.executemany(
        """INSERT INTO s2t_transformations
        (id, file_id, sheet_id, sheet_name, row_num, target_table, target_field)
        VALUES (?, 61, 62, 'S2T', ?, 't_target', ?)""",
        [(100 + index, index, f"column_{index}") for index in range(25)],
    )
    conn.commit()
    conn.close()

    conn = get_db_connection()
    conn.execute(
        "INSERT INTO files (file_id, filename, upload_time) VALUES (?, ?, ?)",
        (63, "active-empty.xlsx", "2026-01-02"),
    )
    conn.commit()
    conn.close()

    result = list_s2t_transformations.invoke({"limit": 1000})
    assert result["total"] == 25
    assert result["limit"] == 20
    assert len(result["rows"]) == 20
    assert result["scope"] == "global"
    assert "file_id" not in result
    assert "file" not in result


def test_list_s2t_transformations_empty_result_is_global_not_file_error():
    from agents.tools import list_s2t_transformations

    result = list_s2t_transformations.invoke({})

    assert result == {
        "scope": "global",
        "total": 0,
        "limit": 20,
        "rows": [],
    }


def test_list_columns_by_sheet_name(sample_excel_bytes):
    from storage.database import store_excel_data
    from agents.tools import list_columns

    sheets = [
        {
            "sheet_name": "OnlyOne",
            "skip_reason": None,
            "header": {"start_row": 0, "row_count": 1, "nested": False},
            "columns": ["Z"],
            "data_rows": [["v"]],
        }
    ]
    store_excel_data(sample_excel_bytes, "y.xlsx", "m", sheets)
    resolved = list_columns.invoke({"sheet_id": "OnlyOne"})
    assert resolved["column_count"] == 1
    assert resolved["columns"][0]["name"] == "Z"
