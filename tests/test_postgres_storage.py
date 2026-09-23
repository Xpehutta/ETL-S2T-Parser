from __future__ import annotations

import sqlite3
import sys
from pathlib import Path
from typing import Any

import pytest

from agents.tools import context as context_tools
from agents.tools.context import get_sqlite_schema_cheatsheet
from agents.tools.sql import run_sql
from storage import database as db_storage
from storage import s2t as s2t_storage
from storage.postgres import (
    DatabaseRow,
    PostgresConnection,
    PostgresCursor,
    configured_postgres_schema,
    configured_statement_timeout_ms,
    connect_postgres,
    convert_qmark_placeholders,
    is_postgres_error,
    postgres_enabled,
    postgres_sql,
)
from storage import postgres_schema
from scripts import migrate_sqlite_to_postgres as migration
from scripts.migrate_sqlite_to_postgres import _source_manifest


class _SQLitePostgresCursor:
    def __init__(self, connection: "_SQLitePostgresConnection") -> None:
        self.connection = connection
        self.raw_cursor: sqlite3.Cursor | None = None

    @property
    def description(self):
        return self.raw_cursor.description if self.raw_cursor is not None else None

    @property
    def rowcount(self):
        return self.raw_cursor.rowcount if self.raw_cursor is not None else -1

    @property
    def lastrowid(self):
        return self.raw_cursor.lastrowid if self.raw_cursor is not None else None

    def execute(self, statement, params=None):
        text = str(statement).strip()
        if text.upper().startswith("TRUNCATE TABLE"):
            for table_name in postgres_schema.postgres_table_names():
                self.connection.raw.execute(f'DELETE FROM "{table_name}"')
            self.raw_cursor = None
            return self
        if text.upper().startswith("SELECT SETVAL("):
            self.raw_cursor = self.connection.raw.execute("SELECT 1")
            return self
        self.raw_cursor = self.connection.raw.execute(text, params or ())
        return self

    def executemany(self, statement, params):
        self.raw_cursor = self.connection.raw.executemany(str(statement), params)
        return self

    def fetchone(self):
        return self.raw_cursor.fetchone() if self.raw_cursor is not None else None

    def fetchall(self):
        return self.raw_cursor.fetchall() if self.raw_cursor is not None else []

    def fetchmany(self, size=1):
        return self.raw_cursor.fetchmany(size) if self.raw_cursor is not None else []


class _SQLitePostgresConnection:
    def __init__(self) -> None:
        self.raw = sqlite3.connect(":memory:")
        self.raw.row_factory = sqlite3.Row
        cursor = self.raw.cursor()
        db_storage._create_current_tables(cursor)
        db_storage._migrate_graph_sync_schema(cursor)
        db_storage._create_indexes(cursor)
        self.raw.commit()
        self.closed = False
        self.rolled_back = False

    def cursor(self):
        return _SQLitePostgresCursor(self)

    def execute(self, statement, params=None):
        return self.cursor().execute(statement, params)

    def executemany(self, statement, params):
        return self.cursor().executemany(statement, params)

    def commit(self):
        self.raw.commit()

    def rollback(self):
        self.rolled_back = True
        self.raw.rollback()

    def close(self):
        self.closed = True


def test_qmark_conversion_skips_literals_identifiers_and_comments():
    sql = (
        "SELECT ?, '?', \"?\", $$?$$, $tag$?$tag$ "
        "-- ?\n/* ? */ WHERE value = ?"
    )
    assert convert_qmark_placeholders(
        "SELECT 'it''s?', \"quoted\"\"?\", ?"
    ) == "SELECT 'it''s?', \"quoted\"\"?\", %s"

    assert convert_qmark_placeholders(sql) == (
        "SELECT %s, '?', \"?\", $$?$$, $tag$?$tag$ "
        "-- ?\n/* ? */ WHERE value = %s"
    )


def test_postgres_sql_translates_supported_legacy_functions():
    assert postgres_sql("SELECT IFNULL(a, ?), INSTR(b, ?) FROM t") == (
        "SELECT COALESCE(a, %s), STRPOS(b, %s) FROM t"
    )


def test_postgres_sql_can_preserve_native_json_question_operator():
    assert postgres_sql(
        "SELECT payload ? 'key' FROM events",
        convert_placeholders=False,
    ) == "SELECT payload ? 'key' FROM events"


def test_postgres_configuration_validation(monkeypatch):
    assert postgres_enabled("postgres://example/db") is True
    assert postgres_enabled("postgresql+psycopg://example/db") is True
    assert postgres_enabled("sqlite:///db") is False

    monkeypatch.setenv("POSTGRES_STATEMENT_TIMEOUT_MS", "2500")
    assert configured_statement_timeout_ms() == 2500
    monkeypatch.setenv("POSTGRES_STATEMENT_TIMEOUT_MS", "bad")
    with pytest.raises(ValueError, match="must be an integer"):
        configured_statement_timeout_ms()
    monkeypatch.setenv("POSTGRES_STATEMENT_TIMEOUT_MS", "0")
    with pytest.raises(ValueError, match="between"):
        configured_statement_timeout_ms()


def test_database_row_matches_used_sqlite_row_surface():
    row = DatabaseRow(("file_id", "filename"), (7, "map.xlsx"))

    assert row[0] == 7
    assert row["filename"] == "map.xlsx"
    assert dict(row) == {"file_id": 7, "filename": "map.xlsx"}
    assert tuple(row.keys()) == ("file_id", "filename")
    assert row[:] == (7, "map.xlsx")
    assert list(row) == [7, "map.xlsx"]
    assert len(row) == 2
    assert row.values() == (7, "map.xlsx")
    assert list(row.items()) == [("file_id", 7), ("filename", "map.xlsx")]


def test_postgres_cursor_reads_psycopg_column_names_without_tuple_fallback():
    class Column:
        name = "answer"

    class RawCursor:
        description = (Column(),)

        def fetchone(self):
            return (42,)

    row = PostgresCursor(RawCursor()).fetchone()

    assert row is not None
    assert row["answer"] == 42


def test_postgres_cursor_and_connection_adapter_surface(monkeypatch):
    class RawCursor:
        description = (("answer",),)
        rowcount = 3

        def __init__(self):
            self.calls = []
            self.rows = [(1,), (2,)]

        def execute(self, query, params=None):
            self.calls.append((query, params))
            return self

        def executemany(self, query, params):
            self.calls.append((query, list(params)))
            return self

        def fetchone(self):
            return self.rows.pop(0) if self.rows else None

        def fetchall(self):
            rows, self.rows = self.rows, []
            return rows

        def fetchmany(self, size=1):
            rows, self.rows = self.rows[:size], self.rows[size:]
            return rows

        def __iter__(self):
            return iter(self.rows)

    raw_cursor = RawCursor()
    cursor = PostgresCursor(raw_cursor)
    assert cursor.execute("PRAGMA query_only = ON") is cursor
    cursor.execute("SELECT ?", (1,))
    cursor.execute("SELECT payload ? 'key'")
    cursor.executemany("INSERT INTO t VALUES (?)", [(1,), (2,)])
    assert raw_cursor.calls[0] == ("SELECT %s", (1,))
    assert raw_cursor.calls[1] == ("SELECT payload ? 'key'", None)
    assert cursor.rowcount == 3
    assert cursor.lastrowid is None
    assert cursor.description == (("answer",),)
    assert cursor.fetchone()["answer"] == 1
    assert [row[0] for row in cursor.fetchmany(1)] == [2]
    assert cursor.fetchone() is None
    raw_cursor.rows = [(3,), (4,)]
    assert [row[0] for row in cursor.fetchall()] == [3, 4]
    raw_cursor.rows = [(5,)]
    assert [row[0] for row in cursor] == [5]
    assert cursor.calls is raw_cursor.calls

    class RawConnection:
        def __init__(self):
            self.cursor_value = RawCursor()
            self.commits = 0
            self.rollbacks = 0
            self.closed = False

        def cursor(self):
            return self.cursor_value

        def commit(self):
            self.commits += 1

        def rollback(self):
            self.rollbacks += 1

        def close(self):
            self.closed = True

    monkeypatch.setenv("POSTGRES_STATEMENT_TIMEOUT_MS", "1234")
    raw = RawConnection()
    connection = PostgresConnection(raw, schema="etl")
    connection.execute("SELECT ?", (1,))
    connection.executemany("SELECT ?", [(1,)])
    connection.create_function("ignored")
    connection.set_authorizer("ignored")
    connection.set_read_only()
    connection.commit()
    connection.rollback()
    connection.close()
    assert raw.commits == 1
    assert raw.rollbacks == 1
    assert raw.closed is True
    assert any("SET TRANSACTION READ ONLY" in call[0] for call in raw.cursor_value.calls)


def test_postgres_connection_context_and_factory(monkeypatch):
    class RawCursor:
        def __init__(self, fail=False):
            self.fail = fail
            self.calls = []

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def execute(self, query, params=None):
            del params
            if self.fail:
                raise RuntimeError("set path failed")
            self.calls.append(query)

    class RawConnection:
        def __init__(self, fail=False):
            self.cursor_value = RawCursor(fail)
            self.commits = 0
            self.rollbacks = 0
            self.closed = False

        def cursor(self):
            return self.cursor_value

        def commit(self):
            self.commits += 1

        def rollback(self):
            self.rollbacks += 1

        def close(self):
            self.closed = True

    import psycopg

    raw = RawConnection()
    captured = []
    monkeypatch.setattr(psycopg, "connect", lambda url: captured.append(url) or raw)
    connection = connect_postgres("postgresql+psycopg://example/db", schema="etl")
    assert captured == ["postgresql://example/db"]
    assert raw.commits == 1
    with connection:
        pass
    assert raw.commits == 2
    assert raw.closed is True

    raw_error = RawConnection()
    wrapped = PostgresConnection(raw_error, schema="etl")
    with pytest.raises(ValueError):
        with wrapped:
            raise ValueError("boom")
    assert raw_error.rollbacks == 1

    failing = RawConnection(fail=True)
    monkeypatch.setattr(psycopg, "connect", lambda _url: failing)
    with pytest.raises(RuntimeError, match="set path failed"):
        connect_postgres("postgresql://example/db")
    assert failing.closed is True
    with pytest.raises(ValueError, match="postgresql"):
        connect_postgres("sqlite:///db")
    assert is_postgres_error(psycopg.Error("db")) is True


@pytest.mark.parametrize("schema", ["bad-name", "9schema", "public;drop"])
def test_postgres_schema_identifier_is_strict(schema):
    with pytest.raises(ValueError):
        configured_postgres_schema(schema)


def test_postgres_schema_contract_uses_native_comments_and_no_comment_table():
    table_names = postgres_schema.postgres_table_names()
    columns = postgres_schema.postgres_schema_columns()
    comments = postgres_schema.postgres_column_comments()
    statements = postgres_schema._comment_ddl()

    assert "schema_table_comments" not in table_names
    assert set(columns) == set(table_names)
    assert {
        (table_name, column_name)
        for table_name, table_columns in columns.items()
        for column_name in table_columns
    } == {
        (table_name, column_name)
        for table_name, table_comments in comments.items()
        for column_name in table_comments
    }
    assert any(statement.startswith("COMMENT ON TABLE") for statement in statements)
    assert any(statement.startswith("COMMENT ON COLUMN") for statement in statements)
    assert all("schema_table_comments" not in statement for statement in statements)


def test_init_postgres_schema_applies_ddl_comments_and_validates(monkeypatch):
    columns = postgres_schema.postgres_schema_columns()

    class FakeCursor:
        def __init__(self):
            self.statements: list[str] = []

        def execute(self, statement, params=None):
            del params
            self.statements.append(str(statement).strip())
            return self

        def fetchall(self):
            if "information_schema.columns" not in self.statements[-1]:
                return []
            return [
                {"table_name": table_name, "column_name": column_name}
                for table_name, table_columns in columns.items()
                for column_name in table_columns
            ]

    class FakeConnection:
        def __init__(self):
            self.fake_cursor = FakeCursor()
            self.committed = False
            self.closed = False

        def cursor(self):
            return self.fake_cursor

        def commit(self):
            self.committed = True

        def rollback(self):
            raise AssertionError("unexpected rollback")

        def close(self):
            self.closed = True

    connection = FakeConnection()
    monkeypatch.setattr(
        postgres_schema,
        "connect_postgres",
        lambda database_url=None, schema=None: connection,
    )

    postgres_schema.init_postgres_schema("postgresql://example/db", schema="etl")

    rendered = "\n".join(connection.fake_cursor.statements)
    assert connection.committed is True
    assert connection.closed is True
    assert "CREATE SCHEMA IF NOT EXISTS \"etl\"" in rendered
    assert "COMMENT ON TABLE \"files\"" in rendered
    assert "COMMENT ON COLUMN \"files\".\"filename\"" in rendered
    assert "INSERT INTO graph_sync_generation" in rendered


def test_init_postgres_schema_rolls_back_on_validation_error(monkeypatch):
    class FakeCursor:
        def execute(self, _statement, _params=None):
            return self

        def fetchall(self):
            return []

    class FakeConnection:
        def __init__(self):
            self.rolled_back = False
            self.closed = False

        def cursor(self):
            return FakeCursor()

        def commit(self):
            raise AssertionError("unexpected commit")

        def rollback(self):
            self.rolled_back = True

        def close(self):
            self.closed = True

    connection = FakeConnection()
    monkeypatch.setattr(postgres_schema, "connect_postgres", lambda *_a, **_k: connection)

    with pytest.raises(postgres_schema.PostgresSchemaError):
        postgres_schema.init_postgres_schema("postgresql://example/db")

    assert connection.rolled_back is True
    assert connection.closed is True


def test_database_backend_selection_is_environment_driven(monkeypatch):
    monkeypatch.delenv("DATABASE_URL", raising=False)
    assert db_storage.database_backend_name() == "sqlite"

    monkeypatch.setenv("DATABASE_URL", "postgresql://example/db")
    assert db_storage.database_backend_name() == "postgresql"


def test_database_postgres_lifecycle_delegates(monkeypatch):
    calls = []
    monkeypatch.setattr(db_storage, "is_postgres_backend", lambda: True)
    monkeypatch.setattr(postgres_schema, "init_postgres_schema", lambda: calls.append("init"))
    monkeypatch.setattr(
        postgres_schema,
        "clear_postgres_data_with_graph_snapshot",
        lambda: ({"files": 2}, {"generation": 3, "requests": []}),
    )

    db_storage.init_db()
    assert db_storage.migrate_column_catalog_schema() == {
        "changed": False,
        "tables_rebuilt": [],
    }
    assert db_storage.migrate_s2t_layer_columns() == {
        "changed": False,
        "columns_added": [],
    }
    assert db_storage.clear_all_data_with_graph_snapshot() == (
        {"files": 2},
        {"generation": 3, "requests": []},
    )
    assert calls == ["init", "init", "init"]


def test_store_excel_data_uses_postgres_returning_branch(monkeypatch):
    target = _SQLitePostgresConnection()
    monkeypatch.setattr(db_storage, "is_postgres_backend", lambda: True)
    monkeypatch.setattr(db_storage, "get_db_connection", lambda: target)

    file_id = db_storage.store_excel_data(
        "input.xlsx",
        "model",
        [
            {"sheet_name": "skipped", "header": None, "skip_reason": "empty"},
            {
                "sheet_name": "data",
                "header": {"start_row": 0, "row_count": 1, "nested": False},
                "columns": ["A"],
                "data_rows": [["value"]],
            },
        ],
    )

    assert file_id == 1
    assert target.raw.execute("SELECT filename FROM files").fetchone()[0] == "input.xlsx"
    assert target.raw.execute("SELECT COUNT(*) FROM data").fetchone()[0] == 1


def test_postgres_s2t_summary_uses_string_agg(monkeypatch):
    captured = []

    class FakeConnection:
        def execute(self, query, params):
            captured.append((query, params))
            return self

        def fetchall(self):
            return [
                DatabaseRow(
                    (
                        "table_name",
                        "layer",
                        "mapping_count",
                        "field_count",
                        "related_table_count",
                        "related_tables",
                        "mappings_with_rule",
                    ),
                    ("target", "dds", 2, 2, 2, "source_b,source_a", 1),
                )
            ]

        def close(self):
            return None

    monkeypatch.setattr(db_storage, "is_postgres_backend", lambda: True)
    monkeypatch.setattr(s2t_storage, "get_db_connection", lambda: FakeConnection())

    result = s2t_storage.summarize_s2t_transformations(file_id=7)

    assert "STRING_AGG" in captured[0][0]
    assert captured[0][1][0] == 7
    assert result["groups"][0]["related_tables"] == ["source_a", "source_b"]


def test_backfill_s2t_layers_accepts_empty_file_scope(temp_db):
    result = s2t_storage.backfill_s2t_layers(file_id=999)

    assert result == {
        "file_id": 999,
        "rows": 0,
        "updated": 0,
        "resolved_source": 0,
        "resolved_target": 0,
    }


def test_run_sql_uses_backend_read_only_transaction(monkeypatch):
    class Cursor:
        description = (("answer",),)

        def execute(self, query):
            assert query == "SELECT 42 AS answer"

        def fetchmany(self, _size):
            return [(42,)]

    class Connection:
        def __init__(self):
            self.read_only = False
            self.closed = False

        def set_read_only(self):
            self.read_only = True

        def cursor(self):
            return Cursor()

        def close(self):
            self.closed = True

    connection = Connection()
    monkeypatch.setattr(db_storage, "get_db_connection", lambda: connection)

    result = run_sql.invoke({"query": "SELECT 42 AS answer"})

    assert result["rows"] == [{"answer": 42}]
    assert connection.read_only is True
    assert connection.closed is True


def test_new_backend_validation_guards(temp_db, monkeypatch):
    with pytest.raises(ValueError):
        postgres_schema._identifier("bad-name")
    with pytest.raises(ValueError):
        db_storage._sql_identifier("bad-name")

    assert s2t_storage.list_s2t_transformations(columns=["missing"])["error"]
    assert s2t_storage.list_s2t_transformations(columns=[])["error"]
    assert s2t_storage.list_s2t_table_names("missing")["error"]
    assert s2t_storage.summarize_s2t_transformations("missing")["error"]
    monkeypatch.setattr(s2t_storage, "S2T_RECORD_FIELDS", ("source_table",))
    assert s2t_storage.summarize_s2t_transformations()["missing_fields"]
    assert run_sql.invoke({"query": ""})["error"] == "query must be non-empty"

    class BrokenConnection:
        def set_read_only(self):
            raise RuntimeError("unexpected")

        def close(self):
            return None

    monkeypatch.setattr(db_storage, "get_db_connection", lambda: BrokenConnection())
    assert run_sql.invoke({"query": "SELECT 1"}) == {
        "error": "SQL query failed",
        "query": "SELECT 1",
    }


def test_context_loader_empty_and_unknown_branches(monkeypatch):
    assert context_tools._format_backtick_list(()) == ""
    assert context_tools.load_schemas(["unknown"]) == ""
    monkeypatch.setattr(context_tools, "_prompt_text", lambda _name: "")
    assert context_tools.load_operation_skills([], stage="plan") == ""

    monkeypatch.setattr(
        context_tools,
        "_prompt_text",
        lambda name: "intro\n## Selected\nbody" if name == "skills.md" else "## Other\n### План данных\nbody",
    )
    assert context_tools.load_skills(["Selected"]) == "intro\n\n## Selected\nbody"
    assert context_tools.load_operation_skills(["missing"], stage="plan") == ""

    monkeypatch.setattr(
        context_tools,
        "_prompt_text",
        lambda _name: "## Empty\n### План данных\n",
    )
    assert context_tools.load_operation_skills(["Empty"], stage="plan") == ""

    monkeypatch.setattr(context_tools, "_prompt_text", lambda _name: "intro only")
    assert context_tools.load_skills([]) == "intro only"


def test_schema_cheatsheet_describes_native_postgres_comments(monkeypatch):
    monkeypatch.setattr(db_storage, "is_postgres_backend", lambda: True)

    text = get_sqlite_schema_cheatsheet()

    assert "## Актуальная схема PostgreSQL" in text
    assert "COMMENT ON" in text
    assert "`schema_table_comments`" not in text
    assert "`pg_catalog`" in text


def test_sqlite_migration_manifest_covers_every_postgres_table(temp_db):
    manifest = _source_manifest(temp_db)

    assert set(manifest) == set(postgres_schema.postgres_table_names())
    assert all(item["rows"] >= 0 for item in manifest.values())
    assert all(len(item["sha256"]) == 64 for item in manifest.values())


def test_sqlite_to_postgres_migration_copies_and_verifies(temp_db, monkeypatch):
    temp_db.execute(
        "INSERT INTO files (filename, model_used, upload_time) VALUES (?, ?, ?)",
        ("input.xlsx", "model", "2026-09-23"),
    )
    temp_db.commit()
    source_path = Path(temp_db.execute("PRAGMA database_list").fetchone()[2])
    target = _SQLitePostgresConnection()
    monkeypatch.setattr(migration, "init_postgres_schema", lambda *_a, **_k: None)
    monkeypatch.setattr(migration, "connect_postgres", lambda *_a, **_k: target)

    result = migration.migrate(
        source_path,
        "postgresql://example/db",
        schema="etl",
        replace=False,
        batch_size=2,
    )

    assert result["verified"] is True
    assert result["native_comments"] is True
    assert target.raw.execute("SELECT filename FROM files").fetchone()[0] == "input.xlsx"
    assert target.closed is True


def test_sqlite_to_postgres_migration_rejects_nonempty_target(temp_db, monkeypatch):
    source_path = Path(temp_db.execute("PRAGMA database_list").fetchone()[2])
    target = _SQLitePostgresConnection()
    target.raw.execute(
        "INSERT INTO files (filename, model_used, upload_time) VALUES (?, ?, ?)",
        ("existing.xlsx", "model", "2026-09-23"),
    )
    target.raw.commit()
    monkeypatch.setattr(migration, "init_postgres_schema", lambda *_a, **_k: None)
    monkeypatch.setattr(migration, "connect_postgres", lambda *_a, **_k: target)

    with pytest.raises(RuntimeError, match="not empty"):
        migration.migrate(
            source_path,
            "postgresql://example/db",
            schema="public",
            replace=False,
            batch_size=10,
        )

    assert target.rolled_back is True
    assert target.closed is True


def test_sqlite_to_postgres_migration_rolls_back_digest_mismatch(temp_db, monkeypatch):
    source_path = Path(temp_db.execute("PRAGMA database_list").fetchone()[2])
    target = _SQLitePostgresConnection()
    original_digest = migration._table_digest

    def mismatched_digest(connection, table_name, columns):
        digest = original_digest(connection, table_name, columns)
        return "0" * 64 if connection is target and table_name == "files" else digest

    monkeypatch.setattr(migration, "init_postgres_schema", lambda *_a, **_k: None)
    monkeypatch.setattr(migration, "connect_postgres", lambda *_a, **_k: target)
    monkeypatch.setattr(migration, "_table_digest", mismatched_digest)

    with pytest.raises(RuntimeError, match="verification differs"):
        migration.migrate(
            source_path,
            "postgresql://example/db",
            schema="public",
            replace=False,
            batch_size=100,
        )

    assert target.rolled_back is True


def test_clear_postgres_data_preserves_graph_clear_snapshot(monkeypatch):
    target = _SQLitePostgresConnection()
    target.raw.execute(
        "INSERT INTO files (file_id, filename) VALUES (7, 'input.xlsx')"
    )
    target.raw.execute(
        "INSERT INTO s2t_transformations (file_id, target_table) VALUES (8, 't')"
    )
    target.raw.commit()
    monkeypatch.setattr(postgres_schema, "connect_postgres", lambda: target)

    deleted, snapshot = postgres_schema.clear_postgres_data_with_graph_snapshot()

    assert deleted["files"] == 1
    assert deleted["s2t_transformations"] == 1
    assert snapshot["generation"] == 1
    assert [item["file_id"] for item in snapshot["requests"]] == [7, 8]


def test_clear_postgres_data_rolls_back_failure(monkeypatch):
    class BrokenCursor:
        def execute(self, *_args, **_kwargs):
            raise RuntimeError("database unavailable")

    class BrokenConnection:
        def __init__(self):
            self.rolled_back = False
            self.closed = False

        def cursor(self):
            return BrokenCursor()

        def rollback(self):
            self.rolled_back = True

        def close(self):
            self.closed = True

    connection = BrokenConnection()
    monkeypatch.setattr(postgres_schema, "connect_postgres", lambda: connection)

    with pytest.raises(RuntimeError, match="unavailable"):
        postgres_schema.clear_postgres_data_with_graph_snapshot()

    assert connection.rolled_back is True
    assert connection.closed is True


def test_migration_helpers_reject_bad_source_and_render_dry_run(
    temp_db,
    monkeypatch,
    capsys,
):
    assert migration._json_value(memoryview(b"abc")) == {"bytes_hex": "616263"}
    with pytest.raises(ValueError):
        migration._identifier("bad-name")

    temp_db.execute("DROP TABLE files")
    temp_db.commit()
    with pytest.raises(RuntimeError, match="does not contain"):
        migration._source_manifest(temp_db)

    db_storage._create_current_tables(temp_db.cursor())
    db_storage._migrate_graph_sync_schema(temp_db.cursor())
    temp_db.commit()
    source_path = Path(temp_db.execute("PRAGMA database_list").fetchone()[2])
    monkeypatch.setattr(
        sys,
        "argv",
        ["migrate_sqlite_to_postgres.py", "--source", str(source_path), "--dry-run"],
    )
    assert migration.main() == 0
    assert '"dry_run": true' in capsys.readouterr().out


def test_migration_main_runs_configured_copy(temp_db, monkeypatch, capsys):
    source_path = Path(temp_db.execute("PRAGMA database_list").fetchone()[2])
    captured = {}

    def fake_migrate(source, database_url, **kwargs):
        captured.update(
            source=source,
            database_url=database_url,
            **kwargs,
        )
        return {"verified": True}

    monkeypatch.setattr(migration, "migrate", fake_migrate)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "migrate_sqlite_to_postgres.py",
            "--source",
            str(source_path),
            "--database-url",
            "postgresql://example/db",
            "--schema",
            "etl",
            "--replace",
        ],
    )

    assert migration.main() == 0
    assert captured["database_url"] == "postgresql://example/db"
    assert captured["replace"] is True
    assert '"verified": true' in capsys.readouterr().out


def test_migration_main_rejects_invalid_cli_inputs(temp_db, monkeypatch):
    missing_path = Path(temp_db.execute("PRAGMA database_list").fetchone()[2]).with_name(
        "missing.db"
    )
    monkeypatch.setattr(
        sys,
        "argv",
        ["migrate_sqlite_to_postgres.py", "--source", str(missing_path)],
    )
    with pytest.raises(SystemExit, match="source not found"):
        migration.main()

    source_path = Path(temp_db.execute("PRAGMA database_list").fetchone()[2])
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "migrate_sqlite_to_postgres.py",
            "--source",
            str(source_path),
            "--batch-size",
            "0",
        ],
    )
    with pytest.raises(SystemExit, match="batch-size"):
        migration.main()

    monkeypatch.setattr(
        sys,
        "argv",
        ["migrate_sqlite_to_postgres.py", "--source", str(source_path)],
    )
    monkeypatch.delenv("DATABASE_URL", raising=False)
    with pytest.raises(SystemExit, match="DATABASE_URL"):
        migration.main()
