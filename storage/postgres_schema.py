"""PostgreSQL DDL, native object comments, and schema lifecycle."""

from __future__ import annotations

import re
from typing import Any, Optional

from .postgres import connect_postgres, configured_postgres_schema


class PostgresSchemaError(RuntimeError):
    """Raised when an existing PostgreSQL schema is incompatible."""


_COMMON_COLUMN_COMMENTS = {
    "id": "Стабильный числовой идентификатор записи.",
    "file_id": "Идентификатор загрузки из таблицы files.",
    "filename": "Исходное имя загруженного Excel-файла.",
    "model_used": "Модель или способ, использованный при обработке файла.",
    "upload_time": "Время регистрации загрузки.",
    "summary": "Сохранённое краткое резюме файла.",
    "description": "Бизнес-описание объекта.",
    "description_embedding": "Сериализованный embedding описания в формате bytea.",
    "sheet_name": "Имя листа Excel, из которого получена запись.",
    "row_num": "Номер исходной строки данных на листе.",
    "table_name": "Техническое имя логической ETL-таблицы.",
    "column_name": "Техническое имя колонки.",
    "data_type": "Тип данных, сохранённый в каталоге.",
    "primary_key": "Признак принадлежности колонки первичному ключу.",
    "not_null": "Признак обязательности значения колонки.",
    "target_field": "Целевое поле S2T-преобразования.",
    "source_field": "Исходное поле S2T-преобразования.",
    "target_table": "Целевая логическая таблица S2T-преобразования.",
    "source_table": "Исходная логическая таблица S2T-преобразования.",
    "transformation_rule": "Полный текст правила или SQL преобразования.",
    "source_layer": "Детерминированный ETL-слой исходного объекта.",
    "target_layer": "Детерминированный ETL-слой целевого объекта.",
    "updated_at": "Время последнего изменения состояния.",
}

_COLUMN_OVERRIDES = {
    ("file_sheet_headers", "skipped"): "Признак пропуска листа при обработке.",
    ("file_sheet_headers", "skip_reason"): "Причина пропуска листа.",
    ("file_sheet_headers", "header_start_row"): "Нулевая строка начала заголовка.",
    ("file_sheet_headers", "header_rows_count"): "Число строк многоуровневого заголовка.",
    ("file_sheet_headers", "nested_structure"): "Признак многоуровневой структуры заголовка.",
    ("file_sheet_headers", "columns_count"): "Число распознанных колонок листа.",
    ("file_sheet_headers", "headers_json"): "Полная распознанная структура заголовков в JSON.",
    ("additional_objects", "name"): "Точное имя дополнительного SQL-объекта.",
    ("additional_objects", "sql"): "Полный сохранённый SQL дополнительного объекта.",
    ("pxf_to_a", "external_a_table"): "Имя внешней A-таблицы.",
    ("pxf_to_a", "materialized_storage"): "Имя материализованного хранилища.",
    ("pxf_to_a", "replica_table"): "Имя таблицы-реплики.",
    ("pxf_to_a", "sod"): "Сохранённый атрибут SOD.",
    ("data", "table_name"): "Имя листа Excel; историческое имя физической колонки.",
    ("data", "column_id"): "Однобазный номер колонки внутри листа.",
    ("data", "value"): "Полное исходное строковое значение Excel-ячейки.",
    ("graph_sync_generation", "singleton_id"): "Фиксированный ключ единственной строки состояния.",
    ("graph_sync_generation", "generation"): "Текущая версия полной Neo4j-проекции.",
    ("graph_sync_outbox", "generation"): "Версия проекции, к которой относится запрос.",
    ("graph_sync_outbox", "desired_revision"): "Требуемая ревизия проекции файла.",
    ("graph_sync_outbox", "applied_revision"): "Последняя применённая ревизия проекции файла.",
    ("graph_sync_outbox", "attempts"): "Число попыток применить текущую ревизию.",
    ("graph_sync_outbox", "last_error"): "Последняя ошибка синхронизации Neo4j.",
    ("graph_sync_outbox", "applied_at"): "Время успешного применения ревизии.",
    ("embedding_index_metadata", "index_name"): "Стабильное имя embedding-индекса.",
    ("embedding_index_metadata", "model_name"): "Имя embedding-модели.",
    ("embedding_index_metadata", "model_revision"): "Ревизия embedding-модели.",
    ("embedding_index_metadata", "profile_id"): "Идентификатор профиля кодирования.",
    ("embedding_index_metadata", "query_prefix"): "Префикс запросов embedding-профиля.",
    ("embedding_index_metadata", "document_prefix"): "Префикс документов embedding-профиля.",
    ("embedding_index_metadata", "normalize_embeddings"): "Признак L2-нормализации векторов.",
    ("embedding_index_metadata", "dimension"): "Размерность сохранённых векторов.",
}

_TABLE_COMMENT_OVERRIDES = {
    "graph_sync_outbox": "очередь запросов синхронизации PostgreSQL с Neo4j",
}

_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _identifier(value: str) -> str:
    if not _IDENTIFIER_RE.fullmatch(str(value)):
        raise ValueError(f"Invalid PostgreSQL identifier: {value!r}")
    return f'"{value}"'


def _literal(value: str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _column_comment(table_name: str, column_name: str) -> str:
    return _COLUMN_OVERRIDES.get(
        (table_name, column_name),
        _COMMON_COLUMN_COMMENTS.get(
            column_name,
            f"Поле {column_name} таблицы {table_name}.",
        ),
    )


def postgres_table_names() -> tuple[str, ...]:
    from .database import CORE_TABLES

    return tuple(name for name in CORE_TABLES if name != "schema_table_comments")


def postgres_schema_columns() -> dict[str, tuple[str, ...]]:
    from .database import STORAGE_SCHEMA_COLUMNS

    return {
        name: tuple(STORAGE_SCHEMA_COLUMNS[name])
        for name in postgres_table_names()
    }


def postgres_column_comments() -> dict[str, dict[str, str]]:
    return {
        table_name: {
            column_name: _column_comment(table_name, column_name)
            for column_name in columns
        }
        for table_name, columns in postgres_schema_columns().items()
    }


def _text_fields(fields: tuple[str, ...], indent: str = "            ") -> str:
    return (",\n" + indent).join(f"{_identifier(field)} TEXT" for field in fields)


def _column_fields(fields: tuple[str, ...], indent: str = "            ") -> str:
    integers = {"primary_key", "not_null"}
    return (",\n" + indent).join(
        f"{_identifier(field)} {'INTEGER' if field in integers else 'TEXT'}"
        for field in fields
    )


def _table_ddl() -> list[str]:
    from .database import (
        ADDITIONAL_OBJECT_FIELDS,
        PXF_TO_A_FIELDS,
        S2T_RECORD_FIELDS,
        SOURCE_COLUMN_FIELDS,
        SOURCE_TABLE_FIELDS,
        TARGET_COLUMN_FIELDS,
        TARGET_TABLE_FIELDS,
    )

    statements = [
        """
        CREATE TABLE IF NOT EXISTS files (
            file_id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
            filename TEXT,
            model_used TEXT,
            upload_time TEXT,
            summary TEXT,
            description TEXT,
            description_embedding BYTEA
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS file_sheet_headers (
            file_id BIGINT NOT NULL,
            sheet_name TEXT NOT NULL,
            skipped INTEGER DEFAULT 0,
            skip_reason TEXT,
            header_start_row INTEGER,
            header_rows_count INTEGER,
            nested_structure INTEGER,
            columns_count INTEGER DEFAULT 0,
            headers_json TEXT,
            PRIMARY KEY (file_id, sheet_name)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS data (
            id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
            file_id BIGINT,
            table_name TEXT,
            row_num INTEGER,
            column_id INTEGER,
            value TEXT
        )
        """,
    ]
    for table_name, fields in (
        ("source_tables", SOURCE_TABLE_FIELDS),
        ("target_tables", TARGET_TABLE_FIELDS),
    ):
        statements.append(
            f"""
            CREATE TABLE IF NOT EXISTS {_identifier(table_name)} (
                id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                file_id BIGINT,
                sheet_name TEXT,
                row_num INTEGER,
                {_text_fields(tuple(fields), '                ')},
                description_embedding BYTEA
            )
            """
        )
    for table_name, fields in (
        ("source_columns", SOURCE_COLUMN_FIELDS),
        ("target_columns", TARGET_COLUMN_FIELDS),
    ):
        statements.append(
            f"""
            CREATE TABLE IF NOT EXISTS {_identifier(table_name)} (
                id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                file_id BIGINT,
                sheet_name TEXT,
                row_num INTEGER,
                {_column_fields(tuple(fields), '                ')},
                description_embedding BYTEA
            )
            """
        )
    for table_name, fields in (
        ("additional_objects", ADDITIONAL_OBJECT_FIELDS),
        ("pxf_to_a", PXF_TO_A_FIELDS),
    ):
        statements.append(
            f"""
            CREATE TABLE IF NOT EXISTS {_identifier(table_name)} (
                id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                file_id BIGINT,
                sheet_name TEXT,
                row_num INTEGER,
                {_text_fields(tuple(fields), '                ')}
            )
            """
        )
    statements.extend(
        [
            f"""
            CREATE TABLE IF NOT EXISTS s2t_transformations (
                id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                file_id BIGINT,
                sheet_name TEXT,
                row_num INTEGER,
                {_text_fields(tuple(S2T_RECORD_FIELDS), '                ')}
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS graph_sync_generation (
                singleton_id INTEGER PRIMARY KEY CHECK (singleton_id = 1),
                generation BIGINT NOT NULL CHECK (generation >= 0)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS graph_sync_outbox (
                file_id BIGINT PRIMARY KEY,
                generation BIGINT NOT NULL DEFAULT 0,
                desired_revision BIGINT NOT NULL DEFAULT 0,
                applied_revision BIGINT NOT NULL DEFAULT 0,
                attempts INTEGER NOT NULL DEFAULT 0,
                last_error TEXT,
                updated_at TEXT NOT NULL,
                applied_at TEXT
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS embedding_index_metadata (
                index_name TEXT PRIMARY KEY,
                model_name TEXT NOT NULL,
                model_revision TEXT NOT NULL,
                profile_id TEXT NOT NULL,
                query_prefix TEXT NOT NULL,
                document_prefix TEXT NOT NULL,
                normalize_embeddings INTEGER NOT NULL,
                dimension INTEGER NOT NULL,
                updated_at TEXT NOT NULL,
                CHECK (normalize_embeddings IN (0, 1)),
                CHECK (dimension > 0)
            )
            """,
        ]
    )
    return statements


def _index_ddl() -> tuple[str, ...]:
    return (
        "CREATE INDEX IF NOT EXISTS idx_data_file_sheet_row ON data(file_id, table_name, row_num)",
        "CREATE INDEX IF NOT EXISTS idx_data_table_name ON data(table_name)",
        "CREATE INDEX IF NOT EXISTS idx_file_sheet_headers_file ON file_sheet_headers(file_id)",
        "CREATE INDEX IF NOT EXISTS idx_s2t_transformations_file ON s2t_transformations(file_id)",
        "CREATE INDEX IF NOT EXISTS idx_s2t_transformations_target ON s2t_transformations(target_table, target_field)",
        "CREATE INDEX IF NOT EXISTS idx_s2t_transformations_source ON s2t_transformations(source_table, source_field)",
        "CREATE INDEX IF NOT EXISTS idx_source_tables_file ON source_tables(file_id)",
        "CREATE INDEX IF NOT EXISTS idx_source_tables_name ON source_tables(table_name)",
        "CREATE INDEX IF NOT EXISTS idx_target_tables_file ON target_tables(file_id)",
        "CREATE INDEX IF NOT EXISTS idx_target_tables_name ON target_tables(table_name)",
        "CREATE INDEX IF NOT EXISTS idx_source_columns_file ON source_columns(file_id)",
        "CREATE INDEX IF NOT EXISTS idx_source_columns_identity ON source_columns(file_id, table_name, column_name)",
        "CREATE INDEX IF NOT EXISTS idx_target_columns_file ON target_columns(file_id)",
        "CREATE INDEX IF NOT EXISTS idx_target_columns_identity ON target_columns(file_id, table_name, column_name)",
        "CREATE INDEX IF NOT EXISTS idx_additional_objects_file ON additional_objects(file_id)",
        "CREATE INDEX IF NOT EXISTS idx_pxf_to_a_file ON pxf_to_a(file_id)",
        "CREATE INDEX IF NOT EXISTS idx_graph_sync_outbox_pending ON graph_sync_outbox(desired_revision, applied_revision)",
    )


def _function_ddl() -> tuple[str, ...]:
    return (
        """
        CREATE OR REPLACE FUNCTION casefold_contains(value TEXT, part TEXT)
        RETURNS INTEGER LANGUAGE SQL IMMUTABLE PARALLEL SAFE AS $$
            SELECT CASE
                WHEN value IS NULL OR part IS NULL THEN 0
                WHEN STRPOS(LOWER(value), LOWER(part)) > 0 THEN 1
                ELSE 0
            END
        $$
        """,
        """
        CREATE OR REPLACE FUNCTION casefold_equal(left_value TEXT, right_value TEXT)
        RETURNS INTEGER LANGUAGE SQL IMMUTABLE PARALLEL SAFE AS $$
            SELECT CASE
                WHEN left_value IS NULL OR right_value IS NULL THEN 0
                WHEN LOWER(BTRIM(left_value)) = LOWER(BTRIM(right_value)) THEN 1
                ELSE 0
            END
        $$
        """,
        """
        CREATE OR REPLACE FUNCTION header_name(headers_json TEXT, requested_column_id INTEGER)
        RETURNS TEXT LANGUAGE SQL IMMUTABLE PARALLEL SAFE AS $$
            SELECT COALESCE(
                NULLIF(item.element ->> 'flat', ''),
                (
                    SELECT STRING_AGG(part.value, ' > ' ORDER BY part.ordinality)
                    FROM JSONB_ARRAY_ELEMENTS_TEXT(
                        CASE
                            WHEN JSONB_TYPEOF(item.element -> 'path') = 'array'
                            THEN item.element -> 'path'
                            ELSE '[]'::JSONB
                        END
                    ) WITH ORDINALITY AS part(value, ordinality)
                    WHERE BTRIM(part.value) <> ''
                )
            )
            FROM JSONB_ARRAY_ELEMENTS(
                COALESCE(NULLIF(headers_json, ''), '[]')::JSONB
            ) WITH ORDINALITY AS item(element, ordinality)
            WHERE COALESCE(
                NULLIF(item.element ->> 'index', '')::INTEGER,
                item.ordinality::INTEGER - 1
            ) = requested_column_id - 1
            ORDER BY item.ordinality
            LIMIT 1
        $$
        """,
    )


def _comment_ddl() -> list[str]:
    from .database import TABLE_COMMENTS

    statements: list[str] = []
    for table_name in postgres_table_names():
        comment = _TABLE_COMMENT_OVERRIDES.get(
            table_name,
            TABLE_COMMENTS[table_name],
        )
        statements.append(
            f"COMMENT ON TABLE {_identifier(table_name)} IS "
            f"{_literal(comment)}"
        )
    for table_name, comments in postgres_column_comments().items():
        for column_name, comment in comments.items():
            statements.append(
                f"COMMENT ON COLUMN {_identifier(table_name)}."
                f"{_identifier(column_name)} IS {_literal(comment)}"
            )
    return statements


def _validate_columns(cursor: Any) -> None:
    expected = postgres_schema_columns()
    cursor.execute(
        """
        SELECT table_name, column_name
        FROM information_schema.columns
        WHERE table_schema = current_schema()
        ORDER BY table_name, ordinal_position
        """
    )
    actual: dict[str, list[str]] = {}
    for row in cursor.fetchall():
        actual.setdefault(str(row["table_name"]), []).append(str(row["column_name"]))
    mismatches = [
        f"{table_name}: expected {list(columns)}, found {actual.get(table_name, [])}"
        for table_name, columns in expected.items()
        if actual.get(table_name) != list(columns)
    ]
    if mismatches:
        raise PostgresSchemaError("; ".join(mismatches))


def init_postgres_schema(
    database_url: Optional[str] = None,
    *,
    schema: Optional[str] = None,
) -> None:
    selected_schema = configured_postgres_schema(schema)
    conn = connect_postgres(database_url, schema=selected_schema)
    try:
        cursor = conn.cursor()
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {_identifier(selected_schema)}")
        cursor.execute(f"SET search_path TO {_identifier(selected_schema)}")
        for statement in _table_ddl():
            cursor.execute(statement)
        cursor.execute(
            """
            INSERT INTO graph_sync_generation (singleton_id, generation)
            VALUES (1, 0)
            ON CONFLICT(singleton_id) DO NOTHING
            """
        )
        for statement in _index_ddl():
            cursor.execute(statement)
        for statement in _function_ddl():
            cursor.execute(statement)
        for statement in _comment_ddl():
            cursor.execute(statement)
        _validate_columns(cursor)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def clear_postgres_data_with_graph_snapshot() -> tuple[dict[str, int], dict[str, Any]]:
    from .database import USER_FACING_TABLES
    from .graph_outbox import enqueue_graph_sync

    conn = connect_postgres()
    try:
        cursor = conn.cursor()
        cursor.execute("BEGIN")
        generation_row = cursor.execute(
            "SELECT generation FROM graph_sync_generation WHERE singleton_id = 1"
        ).fetchone()
        next_generation = int(generation_row[0]) + 1 if generation_row else 1
        projection_file_ids: set[int] = set()
        for table_name in ("files", "s2t_transformations", "graph_sync_outbox"):
            projection_file_ids.update(
                int(row[0])
                for row in cursor.execute(
                    f"SELECT DISTINCT file_id FROM {_identifier(table_name)} "
                    "WHERE file_id IS NOT NULL"
                ).fetchall()
            )
        deleted = {
            table_name: int(
                cursor.execute(
                    f"SELECT COUNT(*) FROM {_identifier(table_name)}"
                ).fetchone()[0]
            )
            for table_name in USER_FACING_TABLES
        }
        cursor.execute(
            "TRUNCATE TABLE "
            + ", ".join(_identifier(name) for name in postgres_table_names())
            + " RESTART IDENTITY"
        )
        cursor.execute(
            "INSERT INTO graph_sync_generation (singleton_id, generation) VALUES (1, ?)",
            (next_generation,),
        )
        for file_id in sorted(projection_file_ids):
            enqueue_graph_sync(cursor, file_id)
        requests = [
            {
                "file_id": int(row[0]),
                "generation": int(row[1]),
                "revision": int(row[2]),
            }
            for row in cursor.execute(
                """
                SELECT file_id, generation, desired_revision
                FROM graph_sync_outbox
                ORDER BY file_id
                """
            ).fetchall()
        ]
        conn.commit()
        return deleted, {"generation": next_generation, "requests": requests}
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


__all__ = [
    "PostgresSchemaError",
    "clear_postgres_data_with_graph_snapshot",
    "init_postgres_schema",
    "postgres_column_comments",
    "postgres_schema_columns",
    "postgres_table_names",
]
