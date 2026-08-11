import logging

import pytest
from unittest.mock import patch
from storage.database import init_db, get_db_connection
import io


def test_console_streams_are_reconfigured_to_utf8():
    from services.logging_setup import _configure_utf8_console_streams

    class FakeStream:
        def __init__(self):
            self.calls = []

        def reconfigure(self, **kwargs):
            self.calls.append(kwargs)

    stdout = FakeStream()
    stderr = FakeStream()
    _configure_utf8_console_streams((stdout, stderr, object()))

    assert stdout.calls == [
        {"encoding": "utf-8", "errors": "backslashreplace"}
    ]
    assert stderr.calls == [
        {"encoding": "utf-8", "errors": "backslashreplace"}
    ]


def test_file_logging_is_utf8_rotating_and_idempotent(tmp_path):
    from services.logging_setup import configure_logging

    root_logger = logging.getLogger()
    original_level = root_logger.level
    original_handlers = list(root_logger.handlers)
    log_path = tmp_path / "agent.log"
    try:
        assert configure_logging(
            log_path,
            level="INFO",
            max_bytes=1024,
            backup_count=2,
        ) == log_path.resolve()
        configure_logging(
            log_path,
            level="INFO",
            max_bytes=1024,
            backup_count=2,
        )
        handlers = [
            handler
            for handler in root_logger.handlers
            if getattr(handler, "_etls2t_log_path", None) == str(log_path.resolve())
        ]
        assert len(handlers) == 1

        logging.getLogger("tests.file_logging").info("Проверка UTF-8 лога")
        handlers[0].flush()
        assert "Проверка UTF-8 лога" in log_path.read_text(encoding="utf-8")
    finally:
        for handler in list(root_logger.handlers):
            if handler not in original_handlers:
                root_logger.removeHandler(handler)
                handler.close()
        root_logger.setLevel(original_level)


@pytest.fixture(autouse=True)
def mock_graph_sync():
    with patch(
        "services.analysis.try_sync_file_graph",
        side_effect=lambda file_id: ({"file_id": int(file_id)}, None),
    ) as analysis_sync:
        yield {"analysis": analysis_sync}


# Override DB_PATH for the test session (will be set per test)
@pytest.fixture(autouse=True)
def use_temp_db(tmp_path):
    import app as app_module
    import storage.database as db_storage
    original_path = db_storage.DB_PATH
    db_storage.DB_PATH = str(tmp_path / "app_test.db")
    init_db()
    with app_module.analysis_progress_lock:
        app_module.analysis_progress.clear()
    yield
    with app_module.analysis_progress_lock:
        app_module.analysis_progress.clear()
    db_storage.DB_PATH = original_path

def test_index(client):
    response = client.get('/')
    assert response.status_code == 200
    assert b'AI Excel Parser' in response.data
    body = response.data.decode("utf-8")
    assert "void loadTransformations();" in body
    assert 'id="clearAllDataBtn"' in body
    assert "fetch('/storage', { method: 'DELETE' })" in body
    assert "clearTransformationsBtn" not in body
    assert 'id="includeHiddenRows"' in body
    assert "Учитывать скрытые строки" in body
    assert "formData.append('include_hidden_rows', String(includeHiddenRows.checked))" in body


def test_chat_app_single_user_no_session_cookie(client):
    response = client.get('/chat_app')
    assert response.status_code == 200
    assert b'ETL S2T Agent' in response.data
    assert 'Set-Cookie' not in response.headers


def test_chat_app_has_loading_indicators(client):
    response = client.get('/chat_app')
    assert response.status_code == 200
    body = response.data.decode("utf-8")
    assert "loading-spinner" in body
    assert "message-loading" in body
    assert 'role="status"' in body
    assert "showLoadingMessage" in body
    assert "chatInput.disabled = value" in body
    assert "analysisProgress" in body
    assert "startProgressPolling" in body
    assert "/analysis_progress/" in body
    assert "\\/exports\\/sql\\/" in body
    assert "sql-lineage|s2t-graphs" in body
    assert "sql-lineage-visualization" in body
    assert 'sandbox="allow-scripts"' in body
    assert "renderCompactTableBlock" in body
    assert "const matrix = JSON.parse(inner)" in body
    assert "return tableHtml(matrix[0], matrix.slice(1))" in body
    assert "window.sessionStorage" in body
    assert "clearChatHistoryBtn" in body
    assert 'id="viewAllTransformationsBtn"' in body
    assert "showAllTransformations" in body
    assert "fetch('/transformations?full=true')" in body
    assert "transformationsTableHtml" in body
    assert "max-height: min(520px, 58vh)" in body
    assert "overflow: auto" in body
    assert "position: sticky" in body
    assert 'aria-label="Полная таблица трансформаций с прокруткой"' in body
    assert 'aria-label="Таблица с прокруткой"' in body
    assert "sessionStorage.getItem(CHAT_SESSION_ID_STORAGE_KEY)" in body
    assert "JSON.stringify({ query, file_id: currentFileId, history, session_id: currentSessionId })" in body
    assert 'id="clearAllDataBtn"' in body
    assert "Очистить все данные" in body
    assert "fetch('/storage', { method: 'DELETE' })" in body
    assert "clearTransformationsBtn" not in body
    assert 'id="includeHiddenRows"' in body
    assert "Учитывать скрытые строки" in body
    assert "includeHiddenRows.disabled = value" in body
    assert "formData.append('include_hidden_rows', String(includeHiddenRows.checked))" in body


def test_sql_lineage_export_route(client, tmp_path, monkeypatch):
    import agents.tools.sql_lineage as sql_lineage_module

    monkeypatch.setattr(sql_lineage_module, "SQL_LINEAGE_EXPORT_DIR", tmp_path)
    (tmp_path / "sql_lineage_test.html").write_text(
        "<!doctype html><title>Graph</title>",
        encoding="utf-8",
    )

    response = client.get("/exports/sql-lineage/sql_lineage_test.html")

    assert response.status_code == 200
    assert response.mimetype == "text/html"
    assert b"<title>Graph</title>" in response.data


def test_s2t_table_graph_export_route(client, tmp_path, monkeypatch):
    import agents.tools.s2t_graph as graph_module

    monkeypatch.setattr(graph_module, "S2T_TABLE_GRAPH_EXPORT_DIR", tmp_path)
    (tmp_path / "s2t_table_graph_test.html").write_text(
        "<!doctype html><title>S2T Graph</title>",
        encoding="utf-8",
    )
    (tmp_path / "s2t_table_graph_test.json").write_text(
        '{"edges": []}',
        encoding="utf-8",
    )

    html_response = client.get(
        "/exports/s2t-graphs/s2t_table_graph_test.html"
    )
    json_response = client.get(
        "/exports/s2t-graphs/s2t_table_graph_test.json"
    )

    assert html_response.status_code == 200
    assert html_response.mimetype == "text/html"
    assert b"<title>S2T Graph</title>" in html_response.data
    assert json_response.status_code == 200
    assert json_response.mimetype == "application/json"


@patch('app.parse_excel_with_decisions')
@patch('app.store_excel_data')
@patch('services.analysis.summarize_file')
@patch('services.analysis.try_generate_description')
def test_upload(mock_generate_description, mock_summarize, mock_store, mock_parse, client, sample_excel_bytes, mock_embeddings, mock_graph_sync):
    mock_parse.return_value = [{
        "sheet_name": "Sheet1",
        "skip_reason": None,
        "header": {"start_row": 0, "row_count": 1, "nested": False},
        "columns": ["Name"],
        "data_rows": [],
    }]
    mock_store.return_value = 101
    mock_summarize.return_value = "Test summary"
    mock_generate_description.return_value = ("Test description", None)
    data = {
        'file': (io.BytesIO(sample_excel_bytes), 'test.xlsx'),
        'include_hidden_rows': 'true',
    }
    response = client.post('/upload', data=data, content_type='multipart/form-data')
    assert response.status_code == 200
    json_data = response.get_json()
    assert json_data['filename'] == 'test.xlsx'
    assert 'file_id' in json_data
    assert 'summary' in json_data
    assert json_data['summary_error'] is None
    assert json_data['description'] == 'Test description'
    assert json_data['description_error'] is None
    assert json_data['s2t_transformations_count'] == 0
    assert json_data['s2t_transformations_error'] is None
    assert json_data['s2t_extraction_report']['status'] == 'ok'
    assert json_data["sheets"][0]["header"]["row_count"] == 1
    assert json_data["sheets"][0]["data_preview"] == []
    assert "data_rows" not in json_data["sheets"][0]
    assert json_data["graph_sync_report"] == {"file_id": 101}
    assert json_data["graph_sync_error"] is None
    mock_graph_sync["analysis"].assert_called_once_with(101)
    assert mock_parse.call_args.kwargs["include_hidden_rows"] is True


@patch('app.parse_excel_with_decisions')
@patch('app.store_excel_data')
@patch('services.analysis.summarize_file')
@patch('services.analysis.try_generate_description')
def test_upload_records_analysis_progress(mock_generate_description, mock_summarize, mock_store, mock_parse, client, sample_excel_bytes, mock_embeddings):
    mock_parse.return_value = [{
        "sheet_name": "Sheet1",
        "skip_reason": None,
        "header": {"start_row": 0, "row_count": 1, "nested": False},
        "columns": ["Name"],
        "data_rows": [],
    }]
    mock_store.return_value = 102
    mock_summarize.return_value = "Test summary"
    mock_generate_description.return_value = ("Test description", None)
    data = {
        'file': (io.BytesIO(sample_excel_bytes), 'test.xlsx'),
        'upload_id': 'upload-progress-1',
    }

    response = client.post('/upload', data=data, content_type='multipart/form-data')

    assert response.status_code == 200
    progress_response = client.get('/analysis_progress/upload-progress-1')
    assert progress_response.status_code == 200
    progress = progress_response.get_json()
    assert progress["status"] == "done"
    assert progress["phase"] == "done"
    assert progress["percent"] == 100
    assert progress["file_id"] == 102
    assert progress["filename"] == "test.xlsx"
    assert progress["s2t_transformations_error"] is None
    assert progress["history"]


def test_analysis_progress_missing(client):
    response = client.get('/analysis_progress/missing-upload')

    assert response.status_code == 404
    assert response.get_json()["error"] == "Progress not found"


@patch('app.parse_excel_with_decisions')
@patch('app.store_excel_data')
@patch('services.analysis.summarize_file')
@patch('services.analysis.try_generate_description')
def test_upload_returns_summary_error(mock_generate_description, mock_summarize, mock_store, mock_parse, client, sample_excel_bytes, mock_embeddings):
    mock_parse.return_value = [{
        "sheet_name": "Sheet1",
        "skip_reason": None,
        "header": {"start_row": 0, "row_count": 1, "nested": False},
        "columns": ["Name"],
        "data_rows": [],
    }]
    mock_store.return_value = 103
    mock_summarize.side_effect = OSError("getaddrinfo failed")
    mock_generate_description.return_value = (None, "getaddrinfo failed")
    data = {
        'file': (io.BytesIO(sample_excel_bytes), 'test.xlsx')
    }
    response = client.post('/upload', data=data, content_type='multipart/form-data')
    assert response.status_code == 200
    json_data = response.get_json()
    assert json_data['summary'] is None
    assert "getaddrinfo failed" in json_data['summary_error']
    assert json_data['description'] is None
    assert "getaddrinfo failed" in json_data['description_error']
    assert json_data['s2t_transformations_count'] == 0
    assert json_data['s2t_transformations_error'] is None

@patch("app.agent_chat")
def test_chat_success(mock_agent, client):
    mock_agent.return_value = "Answer text"
    response = client.post("/chat", json={"query": "List files"})
    assert response.status_code == 200
    assert response.get_json() == {"answer": "Answer text"}
    mock_agent.assert_called_once_with("List files")


@patch("app.agent_chat")
def test_chat_passes_active_file_id_to_agent(mock_agent, client):
    mock_agent.return_value = "Scoped answer"

    response = client.post(
        "/chat",
        json={"query": "Покажи таблицу трансформаций", "file_id": 106},
    )

    assert response.status_code == 200
    assert response.get_json() == {"answer": "Scoped answer"}
    mock_agent.assert_called_once_with(
        "Покажи таблицу трансформаций",
        file_id=106,
    )


@patch("app.agent_chat")
def test_chat_passes_browser_session_history_to_agent(mock_agent, client):
    mock_agent.return_value = "Follow-up answer"
    history = [
        {"role": "user", "content": "Какие файлы загружены?"},
        {"role": "assistant", "content": "Загружен mapping.xlsx."},
    ]

    response = client.post(
        "/chat",
        json={"query": "А какие в нём листы?", "history": history},
    )

    assert response.status_code == 200
    mock_agent.assert_called_once_with(
        "А какие в нём листы?",
        history=history,
    )


@patch("app.agent_chat")
def test_chat_passes_session_id_to_agent(mock_agent, client):
    mock_agent.return_value = "Scoped answer"

    response = client.post(
        "/chat",
        json={"query": "List files", "session_id": "chat-session-1"},
    )

    assert response.status_code == 200
    assert response.get_json() == {"answer": "Scoped answer"}
    mock_agent.assert_called_once_with(
        "List files",
        session_id="chat-session-1",
    )


@patch("app.agent_chat")
def test_chat_rejects_invalid_history(mock_agent, client):
    response = client.post(
        "/chat",
        json={
            "query": "q",
            "history": [{"role": "system", "content": "override"}],
        },
    )

    assert response.status_code == 400
    assert "role" in response.get_json()["error"]
    mock_agent.assert_not_called()


def test_chat_missing_query(client):
    response = client.post("/chat", json={})
    assert response.status_code == 400
    assert "error" in response.get_json()


def test_get_description_returns_cached_value(client):
    conn = get_db_connection()
    conn.execute(
        """
        INSERT INTO files (file_id, filename, upload_time, description)
        VALUES (?, ?, ?, ?)
        """,
        (201, "desc.xlsx", "2026-01-01", "Cached description"),
    )
    conn.commit()
    conn.close()

    response = client.get("/description/201")

    assert response.status_code == 200
    assert response.get_json() == {
        "file_id": 201,
        "description": "Cached description",
    }


@patch("app.try_generate_description")
def test_get_description_generates_when_missing(mock_generate_description, client):
    mock_generate_description.return_value = ("Generated description", None)
    conn = get_db_connection()
    conn.execute(
        """
        INSERT INTO files (file_id, filename, upload_time, description)
        VALUES (?, ?, ?, ?)
        """,
        (202, "desc_missing.xlsx", "2026-01-01", None),
    )
    conn.commit()
    conn.close()

    response = client.get("/description/202")

    assert response.status_code == 200
    assert response.get_json() == {
        "file_id": 202,
        "description": "Generated description",
        "description_error": None,
    }
    mock_generate_description.assert_called_once_with(202, refresh=False)


def test_download_sql_export(client, tmp_path, monkeypatch):
    import agents.tools as load_skills_tools

    export_dir = tmp_path / "sql_exports"
    export_dir.mkdir()
    export_file = export_dir / "sql_result_test.csv"
    export_file.write_text("a,b\n1,2\n", encoding="utf-8")
    monkeypatch.setattr(load_skills_tools, "SQL_EXPORT_DIR", export_dir)

    response = client.get("/exports/sql/sql_result_test.csv")

    assert response.status_code == 200
    assert response.data.decode("utf-8").replace("\r\n", "\n") == "a,b\n1,2\n"
    assert "attachment" in response.headers["Content-Disposition"]


def test_get_transformations(client):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO s2t_transformations
        (id, file_id, sheet_name, row_num, target_table, target_field,
         source_table, source_field, transformation_rule, source_layer, target_layer)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            301,
            203,
            "S2T",
            1,
            "t_customer",
            "customer_id",
            "src_customer",
            "id",
            "direct",
            "B",
            "T",
        ),
    )
    conn.commit()
    conn.close()

    response = client.get("/transformations/203")

    assert response.status_code == 200
    body = response.get_json()
    assert body["total"] == 1
    assert body["rows"][0]["target_table"] == "t_customer"
    assert body["rows"][0]["target_field"] == "customer_id"
    assert body["rows"][0]["source_table"] == "src_customer"
    assert body["rows"][0]["source_field"] == "id"
    assert body["rows"][0]["source_layer"] == "B"
    assert body["rows"][0]["target_layer"] == "T"
    assert body["rows"][0]["transformation_rule"] == "direct"
    assert "target_type" not in body["rows"][0]
    assert "target_table_description" not in body["rows"][0]
    assert "table_transformation_sql" not in body["rows"][0]


def test_get_all_transformations_can_return_full_global_table(client):
    conn = get_db_connection()
    conn.executemany(
        """
        INSERT INTO s2t_transformations
        (id, file_id, sheet_name, row_num, target_table)
        VALUES (?, ?, ?, ?, ?)
        """,
        [(304, 206, "S2T", 1, "t_first"), (305, 207, "S2T", 2, "t_second")],
    )
    conn.commit()
    conn.close()

    response = client.get("/transformations?full=true")

    assert response.status_code == 200
    body = response.get_json()
    assert body["scope"] == "global"
    assert body["total"] == 2
    assert body["limit"] is None
    assert [row["file_id"] for row in body["rows"]] == [206, 207]


def test_get_transformations_filter(client):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.executemany(
        """
        INSERT INTO s2t_transformations
        (id, file_id, sheet_name, row_num, target_table, target_field,
         source_table, source_field)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (302, 205, "S2T", 1, "t_customer", "customer_id", "src_customer", "id"),
            (303, 205, "S2T", 2, "t_order", "order_id", "src_order", "id"),
        ],
    )
    conn.commit()
    conn.close()

    response = client.get("/transformations/205?q=order&limit=1")

    assert response.status_code == 200
    body = response.get_json()
    assert body["total"] == 1
    assert body["limit"] == 1
    assert body["rows"][0]["target_table"] == "t_order"

    response = client.get("/transformations/205?q=src_order")

    assert response.status_code == 200
    body = response.get_json()
    assert body["total"] == 1
    assert body["rows"][0]["source_table"] == "src_order"


def test_delete_transformations(client):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.executemany(
        """
        INSERT INTO s2t_transformations
        (id, file_id, sheet_name, row_num, target_table)
        VALUES (?, ?, ?, ?, ?)
        """,
        [
            (304, 207, "S2T", 1, "t1"),
            (305, 207, "S2T", 2, "t2"),
            (306, 208, "S2T", 1, "t3"),
        ],
    )
    conn.commit()
    conn.close()

    response = client.delete("/transformations/207")

    assert response.status_code == 200
    assert response.get_json()["deleted"] == 2

    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM s2t_transformations ORDER BY id")
    rows = [row["id"] for row in cursor.fetchall()]
    conn.close()
    assert rows == [306]


@patch("app.clear_graph_projection", return_value={"nodes": 4})
def test_delete_all_storage_clears_sqlite_neo4j_and_memory(
    mock_clear_graph,
    client,
):
    import app as app_module

    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO files (file_id, filename, upload_time)
        VALUES (401, 'all.xlsx', '2026-07-27')
        """
    )
    cursor.execute(
        """
        INSERT INTO additional_objects
        (id, file_id, sheet_name, row_num, name, sql)
        VALUES (407, 401, 'Additional objects', 0, 'view_a', 'SELECT 1')
        """
    )
    cursor.execute(
        """
        INSERT INTO pxf_to_a
        (id, file_id, sheet_name, row_num, external_a_table)
        VALUES (408, 401, 'pxf_to_a', 0, 'ext_a')
        """
    )
    cursor.execute(
        """
        INSERT INTO file_sheet_headers
        (file_id, sheet_name, skipped, columns_count)
        VALUES (401, 'S2T', 0, 1)
        """
    )
    cursor.execute(
        """
        INSERT INTO data
        (id, file_id, table_name, row_num, column_id, value)
        VALUES (403, 401, 'S2T', 0, 1, 'value')
        """
    )
    cursor.execute(
        """
        INSERT INTO source_tables
        (id, file_id, sheet_name, row_num, table_name)
        VALUES (404, 401, 'Source', 0, 'src')
        """
    )
    cursor.execute(
        """
        INSERT INTO target_tables
        (id, file_id, sheet_name, row_num, table_name)
        VALUES (405, 401, 'Target', 0, 'tgt')
        """
    )
    cursor.execute(
        """
        INSERT INTO s2t_transformations
        (id, file_id, sheet_name, row_num, target_table)
        VALUES (406, 401, 'S2T', 0, 'tgt')
        """
    )
    conn.commit()
    conn.close()
    with app_module.analysis_progress_lock:
        app_module.analysis_progress["upload-401"] = {"status": "done"}

    response = client.delete("/storage")

    assert response.status_code == 200
    assert response.get_json() == {
        "status": "ok",
        "sqlite_deleted": {
            "files": 1,
            "file_sheet_headers": 1,
            "source_tables": 1,
            "target_tables": 1,
            "additional_objects": 1,
            "pxf_to_a": 1,
            "s2t_transformations": 1,
            "data": 1,
        },
        "neo4j_deleted": {"nodes": 4},
        "memory_deleted": {
            "progress_entries": 1,
        },
        "warnings": [],
    }
    mock_clear_graph.assert_called_once_with()
    conn = get_db_connection()
    try:
        for table_name in (
            "files",
            "file_sheet_headers",
            "source_tables",
            "target_tables",
            "additional_objects",
            "pxf_to_a",
            "s2t_transformations",
            "data",
        ):
            assert conn.execute(
                f'SELECT COUNT(*) FROM "{table_name}"'
            ).fetchone()[0] == 0
    finally:
        conn.close()
    with app_module.analysis_progress_lock:
        assert app_module.analysis_progress == {}


@patch("app.clear_graph_projection", side_effect=RuntimeError("Neo4j unavailable"))
def test_delete_all_storage_clears_sqlite_when_neo4j_fails(
    mock_clear_graph,
    client,
):
    conn = get_db_connection()
    conn.execute(
        """
        INSERT INTO files (file_id, filename, upload_time)
        VALUES (410, 'keep.xlsx', '2026-07-27')
        """
    )
    conn.commit()
    conn.close()

    response = client.delete("/storage")

    assert response.status_code == 200
    assert response.get_json() == {
        "status": "partial",
        "sqlite_deleted": {
            "files": 1,
            "file_sheet_headers": 0,
            "source_tables": 0,
            "target_tables": 0,
            "additional_objects": 0,
            "pxf_to_a": 0,
            "s2t_transformations": 0,
            "data": 0,
        },
        "neo4j_deleted": {
            "nodes": 0,
            "skipped": True,
            "error": "Neo4j unavailable",
        },
        "memory_deleted": {"progress_entries": 0},
        "warnings": [
            {
                "storage": "neo4j",
                "error": "Neo4j unavailable",
            }
        ],
    }
    mock_clear_graph.assert_called_once_with()
    conn = get_db_connection()
    try:
        assert conn.execute(
            "SELECT COUNT(*) FROM files"
        ).fetchone()[0] == 0
    finally:
        conn.close()


def test_refresh_transformations_endpoint_is_removed(client):
    response = client.post("/transformations/211/refresh")

    assert response.status_code == 404


def test_classify_sheet_groups_endpoint_without_llm(client):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO files (file_id, filename, upload_time, model_used) VALUES (?, ?, ?, ?)",
        (212, "groups.xlsx", "2026-01-01", "model"),
    )
    cursor.execute(
        """
        INSERT INTO file_sheet_headers
        (file_id, sheet_name, skipped, header_start_row,
         header_rows_count, nested_structure, columns_count, headers_json)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (212, "S2T", 0, 0, 1, 0, 0, "[]"),
    )
    conn.commit()
    conn.close()

    response = client.get("/sheet_groups/212/classify")

    assert response.status_code == 200
    body = response.get_json()
    assert body["sheet_count"] == 1
    assert body["subagent"]["name"] == "sheet_group_resolver_subagent"
    assert body["verification"]["status"] == "passed"
    assert body["classifications"][0]["group"] == "s2t"
    assert "layer" not in body["classifications"][0]
