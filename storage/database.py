import json
import logging
import re
import sqlite3
from datetime import datetime
from typing import Any, Dict, List, Optional

from config.useful_columns import get_usefull_col_extraction_target


logger = logging.getLogger(__name__)


class DatabaseSchemaError(RuntimeError):
    """Raised when the configured durable database has an incompatible schema."""


DB_PATH = "excel_data.db"

FILES_COLUMNS = (
    "file_id",
    "filename",
    "model_used",
    "upload_time",
    "summary",
    "description",
    "description_embedding",
)
FILE_SHEET_HEADER_COLUMNS = (
    "file_id",
    "sheet_name",
    "skipped",
    "skip_reason",
    "header_start_row",
    "header_rows_count",
    "nested_structure",
    "columns_count",
    "headers_json",
)
EXTRACTION_METADATA_COLUMNS = (
    "id",
    "file_id",
    "sheet_name",
    "row_num",
)
SOURCE_TABLE_FIELDS = tuple(
    get_usefull_col_extraction_target("source_tables")["fields"]
)
TARGET_TABLE_FIELDS = tuple(
    get_usefull_col_extraction_target("target_tables")["fields"]
)
SOURCE_COLUMN_FIELDS = tuple(
    get_usefull_col_extraction_target("source_columns")["fields"]
)
TARGET_COLUMN_FIELDS = tuple(
    get_usefull_col_extraction_target("target_columns")["fields"]
)
ADDITIONAL_OBJECT_FIELDS = tuple(
    get_usefull_col_extraction_target("additional_objects")["fields"]
)
PXF_TO_A_FIELDS = tuple(
    get_usefull_col_extraction_target("pxf_to_a")["fields"]
)
S2T_FIELDS = tuple(
    get_usefull_col_extraction_target("s2t_transformations")["fields"]
)
S2T_LAYER_FIELDS = ("source_layer", "target_layer")
S2T_RECORD_FIELDS = S2T_FIELDS + S2T_LAYER_FIELDS
SOURCE_TABLE_COLUMNS = (
    EXTRACTION_METADATA_COLUMNS + SOURCE_TABLE_FIELDS + ("description_embedding",)
)
TARGET_TABLE_COLUMNS = (
    EXTRACTION_METADATA_COLUMNS + TARGET_TABLE_FIELDS + ("description_embedding",)
)
SOURCE_COLUMN_COLUMNS = (
    EXTRACTION_METADATA_COLUMNS
    + SOURCE_COLUMN_FIELDS
    + ("description_embedding",)
)
TARGET_COLUMN_COLUMNS = (
    EXTRACTION_METADATA_COLUMNS
    + TARGET_COLUMN_FIELDS
    + ("description_embedding",)
)
ADDITIONAL_OBJECT_COLUMNS = EXTRACTION_METADATA_COLUMNS + ADDITIONAL_OBJECT_FIELDS
PXF_TO_A_COLUMNS = EXTRACTION_METADATA_COLUMNS + PXF_TO_A_FIELDS
S2T_TRANSFORMATION_COLUMNS = EXTRACTION_METADATA_COLUMNS + S2T_RECORD_FIELDS
GRAPH_SYNC_OUTBOX_COLUMNS = (
    "file_id",
    "generation",
    "desired_revision",
    "applied_revision",
    "attempts",
    "last_error",
    "updated_at",
    "applied_at",
)
GRAPH_SYNC_GENERATION_COLUMNS = (
    "singleton_id",
    "generation",
)
EMBEDDING_INDEX_METADATA_COLUMNS = (
    "index_name",
    "model_name",
    "model_revision",
    "profile_id",
    "query_prefix",
    "document_prefix",
    "normalize_embeddings",
    "dimension",
    "updated_at",
)
SCHEMA_TABLE_COMMENT_COLUMNS = (
    "table_name",
    "comment",
)
DATA_COLUMNS = (
    "id",
    "file_id",
    "table_name",
    "row_num",
    "column_id",
    "value",
)
USER_FACING_TABLES = (
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
)
INTERNAL_TABLES = (
    "graph_sync_generation",
    "graph_sync_outbox",
    "embedding_index_metadata",
    "schema_table_comments",
)
CORE_TABLES = USER_FACING_TABLES + INTERNAL_TABLES
STORAGE_SCHEMA_TABLE_ORDER = CORE_TABLES
TABLE_COMMENTS = {
    "files": "загруженные Excel-файлы и их сохранённые описания",
    "file_sheet_headers": "листы файлов и распознанные заголовки",
    "source_tables": "исходные логические таблицы и бизнес-описания таблиц",
    "target_tables": "целевые логические таблицы и бизнес-описания таблиц",
    "source_columns": "исходные колонки, их таблицы, типы и описания полей",
    "target_columns": "целевые колонки, их таблицы, типы и описания полей",
    "additional_objects": "Additional objects с точным именем и полным SQL",
    "pxf_to_a": "соответствия external, materialized и replica-таблиц",
    "s2t_transformations": "точные source→target таблицы, поля и текст правила",
    "data": "сырые значения ячеек Excel с координатами происхождения",
    "graph_sync_generation": "текущая версия производной Neo4j-проекции",
    "graph_sync_outbox": "очередь запросов синхронизации SQLite с Neo4j",
    "embedding_index_metadata": "параметры построенных embedding-индексов",
    "schema_table_comments": "краткие назначения таблиц текущей схемы SQLite",
}
if set(TABLE_COMMENTS) != set(CORE_TABLES):
    raise RuntimeError(
        "SQLite table comments are out of sync: "
        f"missing={sorted(set(CORE_TABLES) - set(TABLE_COMMENTS))}, "
        f"extra={sorted(set(TABLE_COMMENTS) - set(CORE_TABLES))}"
    )
STORAGE_SCHEMA_COLUMNS = {
    "files": FILES_COLUMNS,
    "file_sheet_headers": FILE_SHEET_HEADER_COLUMNS,
    "source_tables": SOURCE_TABLE_COLUMNS,
    "target_tables": TARGET_TABLE_COLUMNS,
    "source_columns": SOURCE_COLUMN_COLUMNS,
    "target_columns": TARGET_COLUMN_COLUMNS,
    "additional_objects": ADDITIONAL_OBJECT_COLUMNS,
    "pxf_to_a": PXF_TO_A_COLUMNS,
    "s2t_transformations": S2T_TRANSFORMATION_COLUMNS,
    "data": DATA_COLUMNS,
    "graph_sync_generation": GRAPH_SYNC_GENERATION_COLUMNS,
    "graph_sync_outbox": GRAPH_SYNC_OUTBOX_COLUMNS,
    "embedding_index_metadata": EMBEDDING_INDEX_METADATA_COLUMNS,
    "schema_table_comments": SCHEMA_TABLE_COMMENT_COLUMNS,
}
PRE_COLUMN_CATALOG_CORE_TABLES = tuple(
    table_name
    for table_name in USER_FACING_TABLES
    if table_name not in {"source_columns", "target_columns"}
)


def _sql_identifier(value: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        raise ValueError(f"Invalid configured SQLite identifier: {value!r}")
    return f'"{value}"'


def _text_columns_sql(fields: tuple[str, ...], indent: str) -> str:
    return (",\n" + indent).join(
        f"{_sql_identifier(field)} TEXT" for field in fields
    )


def _column_catalog_fields_sql(fields: tuple[str, ...], indent: str) -> str:
    integer_fields = {"primary_key", "not_null"}
    return (",\n" + indent).join(
        f"{_sql_identifier(field)} "
        + ("INTEGER" if field in integer_fields else "TEXT")
        for field in fields
    )


def database_backend_name() -> str:
    """Return the configured durable storage backend without opening it."""
    from .postgres import postgres_enabled

    return "postgresql" if postgres_enabled() else "sqlite"


def is_postgres_backend() -> bool:
    return database_backend_name() == "postgresql"


def get_db_connection() -> Any:
    if is_postgres_backend():
        from .postgres import connect_postgres

        return connect_postgres()
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def _table_exists(cursor: sqlite3.Cursor, table_name: str) -> bool:
    cursor.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table_name,),
    )
    return cursor.fetchone() is not None


def _table_columns(cursor: sqlite3.Cursor, table_name: str) -> List[str]:
    if not _table_exists(cursor, table_name):
        return []
    cursor.execute(f"PRAGMA table_info({table_name})")
    return [str(row[1]) for row in cursor.fetchall()]


def _table_info(cursor: sqlite3.Cursor, table_name: str) -> List[sqlite3.Row]:
    if not _table_exists(cursor, table_name):
        return []
    cursor.execute(f"PRAGMA table_info({table_name})")
    return cursor.fetchall()


def _json_list(raw: Optional[str]) -> List[Any]:
    if not raw:
        return []
    try:
        parsed = json.loads(raw)
    except (TypeError, json.JSONDecodeError):
        return []
    return parsed if isinstance(parsed, list) else []


def _header_rows_to_column_rows(
    file_id: int,
    sheet_name: str,
    headers_json: Optional[str],
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for item in _json_list(headers_json):
        if not isinstance(item, dict):
            continue
        try:
            column_index = int(item.get("index"))
        except (TypeError, ValueError):
            column_index = len(rows)
        flat = str(item.get("flat") or "").strip()
        path = item.get("path")
        if not isinstance(path, list):
            path = [flat] if flat else []
        path = [str(part) for part in path if part is not None and str(part).strip()]
        if not flat and path:
            flat = " > ".join(path)
        rows.append(
            {
                "column_id": column_index + 1,
                "file_id": int(file_id),
                "sheet_name": sheet_name,
                "column_index": column_index,
                "column_name_flat": flat,
                "column_header": json.dumps(path, ensure_ascii=False, default=str),
            }
        )
    return sorted(rows, key=lambda row: row["column_index"])


def _create_current_tables(cursor: sqlite3.Cursor, suffix: str = "") -> None:
    names = {table: f"{table}{suffix}" for table in CORE_TABLES}
    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {names['files']} (
            file_id INTEGER PRIMARY KEY,
            filename TEXT,
            model_used TEXT,
            upload_time TEXT,
            summary TEXT,
            description TEXT,
            description_embedding BLOB
        )
        """
    )
    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {names['file_sheet_headers']} (
            file_id INTEGER NOT NULL,
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
        """
    )
    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {names['data']} (
            id INTEGER PRIMARY KEY,
            file_id INTEGER,
            table_name TEXT,
            row_num INTEGER,
            column_id INTEGER,
            value TEXT
        )
        """
    )
    for table_name in ("source_tables", "target_tables"):
        fields = SOURCE_TABLE_FIELDS if table_name == "source_tables" else TARGET_TABLE_FIELDS
        fields_sql = _text_columns_sql(fields, "                ")
        cursor.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {names[table_name]} (
                id INTEGER PRIMARY KEY,
                file_id INTEGER,
                sheet_name TEXT,
                row_num INTEGER,
                {fields_sql},
                description_embedding BLOB
            )
            """
        )
    for table_name in ("source_columns", "target_columns"):
        fields = (
            SOURCE_COLUMN_FIELDS
            if table_name == "source_columns"
            else TARGET_COLUMN_FIELDS
        )
        fields_sql = _column_catalog_fields_sql(fields, "                ")
        cursor.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {names[table_name]} (
                id INTEGER PRIMARY KEY,
                file_id INTEGER,
                sheet_name TEXT,
                row_num INTEGER,
                {fields_sql},
                description_embedding BLOB
            )
            """
        )
    for table_name, fields in (
        ("additional_objects", ADDITIONAL_OBJECT_FIELDS),
        ("pxf_to_a", PXF_TO_A_FIELDS),
    ):
        fields_sql = _text_columns_sql(fields, "                ")
        cursor.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {names[table_name]} (
                id INTEGER PRIMARY KEY,
                file_id INTEGER,
                sheet_name TEXT,
                row_num INTEGER,
                {fields_sql}
            )
            """
        )
    s2t_fields_sql = _text_columns_sql(S2T_RECORD_FIELDS, "            ")
    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {names['s2t_transformations']} (
            id INTEGER PRIMARY KEY,
            file_id INTEGER,
            sheet_name TEXT,
            row_num INTEGER,
            {s2t_fields_sql}
        )
        """
    )
    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {names['graph_sync_generation']} (
            singleton_id INTEGER PRIMARY KEY CHECK (singleton_id = 1),
            generation INTEGER NOT NULL CHECK (generation >= 0)
        )
        """
    )
    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {names['graph_sync_outbox']} (
            file_id INTEGER PRIMARY KEY,
            generation INTEGER NOT NULL DEFAULT 0,
            desired_revision INTEGER NOT NULL DEFAULT 0,
            applied_revision INTEGER NOT NULL DEFAULT 0,
            attempts INTEGER NOT NULL DEFAULT 0,
            last_error TEXT,
            updated_at TEXT NOT NULL,
            applied_at TEXT
        )
        """
    )
    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {names['embedding_index_metadata']} (
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
        """
    )
    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {names['schema_table_comments']} (
            table_name TEXT PRIMARY KEY,
            comment TEXT NOT NULL CHECK (trim(comment) <> '')
        )
        """
    )
    cursor.executemany(
        f"""
        INSERT INTO {names['schema_table_comments']} (table_name, comment)
        VALUES (?, ?)
        ON CONFLICT(table_name) DO UPDATE SET comment = excluded.comment
        """,
        [(table_name, TABLE_COMMENTS[table_name]) for table_name in CORE_TABLES],
    )


def _legacy_schema_recovery_hint(cursor: sqlite3.Cursor) -> str:
    """Return actionable recovery text when a pre-refactor SQLite file is detected."""
    files_cols = _table_columns(cursor, "files")
    data_cols = _table_columns(cursor, "data")
    legacy_markers: List[str] = []
    if "file_hash" in files_cols and "file_id" not in files_cols:
        legacy_markers.append("files.file_hash")
    if "sheet_hash" in data_cols or "column_hash" in data_cols:
        legacy_markers.append("data.sheet_hash/column_hash")
    if not legacy_markers:
        return ""
    backup_name = f"{DB_PATH}.legacy.bak"
    return (
        f"Обнаружена legacy-схема ({', '.join(legacy_markers)}) "
        "от прежней версии приложения. Автоматическая миграция недоступна. "
        f"Сохраните копию: mv {DB_PATH} {backup_name}, "
        "затем перезапустите приложение — база будет создана заново."
    )


def _schema_mismatches(
    cursor: sqlite3.Cursor,
    table_names: Optional[tuple[str, ...]] = None,
) -> List[str]:
    mismatches: List[str] = []
    selected = set(table_names or STORAGE_SCHEMA_COLUMNS)
    for table_name, expected_columns in STORAGE_SCHEMA_COLUMNS.items():
        if table_name not in selected:
            continue
        actual_columns = _table_columns(cursor, table_name)
        if actual_columns != list(expected_columns):
            mismatches.append(
                f"{table_name}: expected columns {list(expected_columns)}, "
                f"found {actual_columns or 'missing table'}"
            )
    integer_primary_keys = {
        "files": "file_id",
        "data": "id",
        "source_tables": "id",
        "target_tables": "id",
        "source_columns": "id",
        "target_columns": "id",
        "additional_objects": "id",
        "pxf_to_a": "id",
        "s2t_transformations": "id",
        "graph_sync_generation": "singleton_id",
        "graph_sync_outbox": "file_id",
    }
    for table_name, key_name in integer_primary_keys.items():
        if table_name not in selected:
            continue
        info = {str(row[1]): row for row in _table_info(cursor, table_name)}
        key = info.get(key_name)
        if key is None or str(key[2]).upper() != "INTEGER" or int(key[5]) != 1:
            mismatches.append(
                f"{table_name}.{key_name}: expected INTEGER PRIMARY KEY"
            )
    if "embedding_index_metadata" in selected:
        metadata_info = {
            str(row[1]): row
            for row in _table_info(cursor, "embedding_index_metadata")
        }
        index_key = metadata_info.get("index_name")
        if (
            index_key is None
            or str(index_key[2]).upper() != "TEXT"
            or int(index_key[5]) != 1
        ):
            mismatches.append(
                "embedding_index_metadata.index_name: expected TEXT PRIMARY KEY"
            )
    if "schema_table_comments" in selected:
        comments_info = {
            str(row[1]): row
            for row in _table_info(cursor, "schema_table_comments")
        }
        table_key = comments_info.get("table_name")
        if (
            table_key is None
            or str(table_key[2]).upper() != "TEXT"
            or int(table_key[5]) != 1
        ):
            mismatches.append(
                "schema_table_comments.table_name: expected TEXT PRIMARY KEY"
            )
    if "file_sheet_headers" in selected:
        headers_info = {
            str(row[1]): row for row in _table_info(cursor, "file_sheet_headers")
        }
        file_key = headers_info.get("file_id")
        name_key = headers_info.get("sheet_name")
        if (
            file_key is None
            or name_key is None
            or int(file_key[5]) != 1
            or int(name_key[5]) != 2
        ):
            mismatches.append(
                "file_sheet_headers: expected PRIMARY KEY (file_id, sheet_name)"
            )
    if "data" in selected:
        data_info = {str(row[1]): row for row in _table_info(cursor, "data")}
        column_id = data_info.get("column_id")
        if column_id is None or str(column_id[2]).upper() != "INTEGER":
            mismatches.append("data.column_id: expected INTEGER")
    return mismatches


def _create_indexes(cursor: sqlite3.Cursor) -> None:
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_data_file_sheet_row "
        "ON data(file_id, table_name, row_num)"
    )
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_data_table_name ON data(table_name)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_file_sheet_headers_file ON file_sheet_headers(file_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_s2t_transformations_file ON s2t_transformations(file_id)")
    if {"target_table", "target_field"}.issubset(S2T_FIELDS):
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_s2t_transformations_target "
            "ON s2t_transformations(target_table, target_field)"
        )
    if {"source_table", "source_field"}.issubset(S2T_FIELDS):
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_s2t_transformations_source "
            "ON s2t_transformations(source_table, source_field)"
        )
    for table_name in ("source_tables", "target_tables"):
        cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_file ON {table_name}(file_id)")
        cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_name ON {table_name}(table_name)")
    for table_name in ("source_columns", "target_columns"):
        cursor.execute(
            f"CREATE INDEX IF NOT EXISTS idx_{table_name}_file ON {table_name}(file_id)"
        )
        cursor.execute(
            f"CREATE INDEX IF NOT EXISTS idx_{table_name}_identity "
            f"ON {table_name}(file_id, table_name, column_name)"
        )
    for table_name in ("additional_objects", "pxf_to_a"):
        cursor.execute(
            f"CREATE INDEX IF NOT EXISTS idx_{table_name}_file ON {table_name}(file_id)"
        )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_graph_sync_outbox_pending "
        "ON graph_sync_outbox(desired_revision, applied_revision)"
    )


def init_db() -> None:
    """Create the current schema or reject an incompatible existing database."""
    if is_postgres_backend():
        from .postgres_schema import init_postgres_schema

        init_postgres_schema()
        logger.info("PostgreSQL database initialized with the current schema")
        return
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("BEGIN")
        existing_core_tables = [
            table_name for table_name in CORE_TABLES if _table_exists(cursor, table_name)
        ]
        if existing_core_tables:
            legacy_mismatches = _schema_mismatches(
                cursor, PRE_COLUMN_CATALOG_CORE_TABLES
            )
            if not legacy_mismatches:
                _create_current_tables(cursor)
                _migrate_column_catalog_schema(cursor)
                _migrate_graph_sync_schema(cursor)
            mismatches = _schema_mismatches(cursor)
            if mismatches:
                legacy_hint = _legacy_schema_recovery_hint(cursor)
                suffix = f" {legacy_hint}" if legacy_hint else (
                    ". Автоматическая миграция отключена; выполните явную "
                    "миграцию или используйте новую базу данных."
                )
                raise DatabaseSchemaError(
                    f"Несовместимая схема SQLite ({DB_PATH}): "
                    + "; ".join(mismatches)
                    + suffix
                )
        else:
            _create_current_tables(cursor)
            _migrate_graph_sync_schema(cursor)
        _create_indexes(cursor)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    logger.info("Database initialized with the current schema")


def _migrate_graph_sync_schema(cursor: sqlite3.Cursor) -> None:
    """Upgrade the supported pre-generation outbox without losing requests."""
    previous_columns = [
        "file_id",
        "desired_revision",
        "applied_revision",
        "attempts",
        "last_error",
        "updated_at",
        "applied_at",
    ]
    actual_columns = _table_columns(cursor, "graph_sync_outbox")
    if actual_columns == previous_columns:
        cursor.execute(
            "ALTER TABLE graph_sync_outbox "
            "ADD COLUMN generation INTEGER NOT NULL DEFAULT 0"
        )
        cursor.execute(
            """
            CREATE TABLE graph_sync_outbox_current (
                file_id INTEGER PRIMARY KEY,
                generation INTEGER NOT NULL DEFAULT 0,
                desired_revision INTEGER NOT NULL DEFAULT 0,
                applied_revision INTEGER NOT NULL DEFAULT 0,
                attempts INTEGER NOT NULL DEFAULT 0,
                last_error TEXT,
                updated_at TEXT NOT NULL,
                applied_at TEXT
            )
            """
        )
        cursor.execute(
            """
            INSERT INTO graph_sync_outbox_current
            (file_id, generation, desired_revision, applied_revision, attempts,
             last_error, updated_at, applied_at)
            SELECT file_id, generation, desired_revision, applied_revision,
                   attempts, last_error, updated_at, applied_at
            FROM graph_sync_outbox
            """
        )
        cursor.execute("DROP TABLE graph_sync_outbox")
        cursor.execute(
            "ALTER TABLE graph_sync_outbox_current RENAME TO graph_sync_outbox"
        )
    elif actual_columns != list(GRAPH_SYNC_OUTBOX_COLUMNS):
        return

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS graph_sync_generation (
            singleton_id INTEGER PRIMARY KEY CHECK (singleton_id = 1),
            generation INTEGER NOT NULL CHECK (generation >= 0)
        )
        """
    )
    cursor.execute(
        """
        INSERT INTO graph_sync_generation (singleton_id, generation)
        VALUES (1, COALESCE((SELECT MAX(generation) FROM graph_sync_outbox), 0))
        ON CONFLICT(singleton_id) DO NOTHING
        """
    )


def _migrate_column_catalog_schema(cursor: sqlite3.Cursor) -> List[str]:
    """Remove derived provenance/alias fields while preserving catalog facts."""
    rebuilt: List[str] = []
    for table_name, fields in (
        ("source_columns", SOURCE_COLUMN_FIELDS),
        ("target_columns", TARGET_COLUMN_FIELDS),
    ):
        actual_columns = _table_columns(cursor, table_name)
        current_columns = list(
            EXTRACTION_METADATA_COLUMNS + fields + ("description_embedding",)
        )
        if actual_columns == current_columns:
            continue
        base_columns = list(EXTRACTION_METADATA_COLUMNS + fields)
        supported_previous = {
            tuple(base_columns + ["metadata_source"]),
            tuple(base_columns + ["metadata_source", "description_embedding"]),
            tuple(
                base_columns
                + [
                    "metadata_source",
                    "description_embedding",
                    "description_aliases",
                ]
            ),
        }
        if tuple(actual_columns) not in supported_previous:
            continue

        replacement = f"{table_name}__catalog_schema"
        fields_sql = _column_catalog_fields_sql(fields, "                ")
        cursor.execute(
            f"""
            CREATE TABLE {_sql_identifier(replacement)} (
                id INTEGER PRIMARY KEY,
                file_id INTEGER,
                sheet_name TEXT,
                row_num INTEGER,
                {fields_sql},
                description_embedding BLOB
            )
            """
        )
        copy_columns = base_columns + ["description_embedding"]
        invalidate_embedding = "description_aliases" in actual_columns
        select_columns = [
            (
                "NULL"
                if column == "description_embedding" and invalidate_embedding
                else _sql_identifier(column)
                if column in actual_columns
                else "NULL"
            )
            for column in copy_columns
        ]
        cursor.execute(
            f"INSERT INTO {_sql_identifier(replacement)} "
            f"({', '.join(_sql_identifier(column) for column in copy_columns)}) "
            f"SELECT {', '.join(select_columns)} "
            f"FROM {_sql_identifier(table_name)}"
        )
        cursor.execute(f"DROP TABLE {_sql_identifier(table_name)}")
        cursor.execute(
            f"ALTER TABLE {_sql_identifier(replacement)} "
            f"RENAME TO {_sql_identifier(table_name)}"
        )
        rebuilt.append(table_name)
    return rebuilt


def migrate_column_catalog_schema() -> Dict[str, Any]:
    """Remove obsolete column provenance/alias fields from prior schemas."""
    if is_postgres_backend():
        from .postgres_schema import init_postgres_schema

        init_postgres_schema()
        return {"changed": False, "tables_rebuilt": []}
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("BEGIN")
        rebuilt = _migrate_column_catalog_schema(cursor)
        mismatches = _schema_mismatches(
            cursor, ("source_columns", "target_columns")
        )
        if mismatches:
            raise DatabaseSchemaError(
                "Нельзя обновить схему каталогов колонок: "
                + "; ".join(mismatches)
            )
        _create_indexes(cursor)
        conn.commit()
        return {"changed": bool(rebuilt), "tables_rebuilt": rebuilt}
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def migrate_s2t_layer_columns() -> Dict[str, Any]:
    """Explicitly add nullable source/target layer columns to the prior schema."""
    if is_postgres_backend():
        from .postgres_schema import init_postgres_schema

        init_postgres_schema()
        return {"changed": False, "columns_added": []}
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("BEGIN")
        actual_columns = _table_columns(cursor, "s2t_transformations")
        previous_columns = list(EXTRACTION_METADATA_COLUMNS + S2T_FIELDS)
        current_columns = list(S2T_TRANSFORMATION_COLUMNS)
        if actual_columns == current_columns:
            conn.commit()
            return {"changed": False, "columns_added": []}
        if actual_columns != previous_columns:
            raise DatabaseSchemaError(
                "Нельзя добавить ETL-слои: s2t_transformations не соответствует "
                "предыдущей поддерживаемой схеме"
            )
        for field in S2T_LAYER_FIELDS:
            cursor.execute(
                f"ALTER TABLE s2t_transformations ADD COLUMN {_sql_identifier(field)} TEXT"
            )
        conn.commit()
        return {"changed": True, "columns_added": list(S2T_LAYER_FIELDS)}
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def clear_all_data_with_graph_snapshot() -> tuple[Dict[str, int], Dict[str, Any]]:
    """Clear durable storage and return the graph generation to acknowledge."""
    if is_postgres_backend():
        from .postgres_schema import clear_postgres_data_with_graph_snapshot

        return clear_postgres_data_with_graph_snapshot()
    deletion_order = tuple(reversed(STORAGE_SCHEMA_TABLE_ORDER))
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("BEGIN")
        current_generation = 0
        if _table_exists(cursor, "graph_sync_generation"):
            generation_row = cursor.execute(
                """
                SELECT generation
                FROM graph_sync_generation
                WHERE singleton_id = 1
                """
            ).fetchone()
            if generation_row is not None:
                current_generation = int(generation_row[0])
        next_generation = current_generation + 1
        projection_file_ids: set[int] = set()
        for table_name in ("files", "s2t_transformations", "graph_sync_outbox"):
            if not _table_exists(cursor, table_name):
                continue
            projection_file_ids.update(
                int(row[0])
                for row in cursor.execute(
                    f"SELECT DISTINCT file_id FROM {_sql_identifier(table_name)} "
                    "WHERE file_id IS NOT NULL"
                ).fetchall()
            )
        deleted = {
            table_name: (
                int(
                    cursor.execute(
                        f"SELECT COUNT(*) FROM {_sql_identifier(table_name)}"
                    ).fetchone()[0]
                )
                if _table_exists(cursor, table_name)
                else 0
            )
            for table_name in USER_FACING_TABLES
        }
        for table_name in deletion_order:
            cursor.execute(f"DROP TABLE IF EXISTS {_sql_identifier(table_name)}")
        _create_current_tables(cursor)
        cursor.execute(
            """
            INSERT INTO graph_sync_generation (singleton_id, generation)
            VALUES (1, ?)
            """,
            (next_generation,),
        )
        _create_indexes(cursor)
        if projection_file_ids:
            from .graph_outbox import enqueue_graph_sync

            for file_id in sorted(projection_file_ids):
                enqueue_graph_sync(cursor, file_id)
        mismatches = _schema_mismatches(cursor)
        if mismatches:
            raise DatabaseSchemaError(
                "Failed to recreate the current SQLite schema: "
                + "; ".join(mismatches)
            )
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
        return deleted, {
            "generation": next_generation,
            "requests": requests,
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def clear_all_data() -> Dict[str, int]:
    """Drop all application tables and recreate the current empty schema."""
    deleted, _graph_snapshot = clear_all_data_with_graph_snapshot()
    return deleted


def store_excel_data(
    filename: str,
    model_used: str,
    sheets: List[Dict[str, Any]],
) -> int:
    """Store every parsed row of one workbook; equal facts remain separate records."""
    upload_time = datetime.now().isoformat()
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("BEGIN")
        if is_postgres_backend():
            cursor.execute(
                """
                INSERT INTO files (filename, model_used, upload_time)
                VALUES (?, ?, ?)
                RETURNING file_id
                """,
                (filename, model_used, upload_time),
            )
            current_file_id = int(cursor.fetchone()[0])
        else:
            cursor.execute(
                """
                INSERT INTO files (filename, model_used, upload_time)
                VALUES (?, ?, ?)
                """,
                (filename, model_used, upload_time),
            )
            current_file_id = int(cursor.lastrowid)

        for sheet in sheets:
            sheet_name = str(sheet["sheet_name"])
            header = sheet.get("header")
            if header is None:
                cursor.execute(
                    """
                    INSERT INTO file_sheet_headers
                    (file_id, sheet_name, skipped, skip_reason, columns_count,
                     headers_json)
                    VALUES (?, ?, 1, ?, 0, '[]')
                    """,
                    (current_file_id, sheet_name, sheet.get("skip_reason", "")),
                )
                continue

            columns = sheet.get("columns", [])
            header_rows: List[Dict[str, Any]] = []
            for index, column in enumerate(columns):
                if isinstance(column, list):
                    path = [str(part) for part in column if part is not None and str(part).strip()]
                    flat = " > ".join(path)
                else:
                    flat = str(column) if column is not None else f"Column_{index + 1}"
                    path = [flat]
                header_rows.append({"index": index, "flat": flat, "path": path})

            cursor.execute(
                """
                INSERT INTO file_sheet_headers
                (file_id, sheet_name, skipped, skip_reason, header_start_row,
                 header_rows_count, nested_structure, columns_count,
                 headers_json)
                VALUES (?, ?, 0, '', ?, ?, ?, ?, ?)
                """,
                (
                    current_file_id,
                    sheet_name,
                    header["start_row"],
                    header["row_count"],
                    1 if header["nested"] else 0,
                    len(header_rows),
                    json.dumps(header_rows, ensure_ascii=False, default=str),
                ),
            )
            rows_to_insert: List[tuple[Any, ...]] = []
            data_rows = sheet.get("data_rows", [])
            data_row_numbers = sheet.get("data_row_numbers")
            if (
                data_row_numbers is not None
                and len(data_row_numbers) != len(data_rows)
            ):
                raise ValueError(
                    f"data_row_numbers length does not match data_rows for sheet {sheet_name}"
                )
            for row_index, row in enumerate(data_rows):
                row_num = (
                    int(data_row_numbers[row_index])
                    if data_row_numbers is not None
                    else row_index
                )
                for column_index, value in enumerate(row):
                    if value is None:
                        continue
                    rows_to_insert.append(
                        (
                            current_file_id,
                            sheet_name,
                            row_num,
                            column_index + 1,
                            str(value),
                        )
                    )
            cursor.executemany(
                """
                INSERT INTO data (file_id, table_name, row_num, column_id, value)
                VALUES (?, ?, ?, ?, ?)
                """,
                rows_to_insert,
            )

        conn.commit()
        return current_file_id
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def get_file(file_id: int) -> Optional[Dict[str, Any]]:
    """Return one file record, or None when the file does not exist."""
    conn = get_db_connection()
    try:
        row = conn.execute(
            f"SELECT {', '.join(FILES_COLUMNS)} FROM files WHERE file_id = ?",
            (int(file_id),),
        ).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def update_file_summary(file_id: int, summary: str) -> None:
    conn = get_db_connection()
    try:
        conn.execute(
            "UPDATE files SET summary = ? WHERE file_id = ?",
            (summary, int(file_id)),
        )
        conn.commit()
    finally:
        conn.close()


def update_file_description(file_id: int, description: str) -> None:
    from services.embeddings import embed_document
    from storage.embedding_index import register_embedding_blobs

    description_embedding = embed_document(description)
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("BEGIN")
        register_embedding_blobs(cursor, [description_embedding])
        cursor.execute(
            """
            UPDATE files
            SET description = ?, description_embedding = ?
            WHERE file_id = ?
            """,
            (description, description_embedding, int(file_id)),
        )
        conn.commit()
    finally:
        conn.close()


def get_columns_by_sheet(file_id: int, sheet_name: str) -> List[Dict[str, Any]]:
    conn = get_db_connection()
    try:
        row = conn.execute(
            """
            SELECT headers_json
            FROM file_sheet_headers
            WHERE file_id = ?
              AND LOWER(TRIM(sheet_name)) = LOWER(TRIM(?))
            """,
            (int(file_id), str(sheet_name)),
        ).fetchone()
        if not row:
            return []
        return _header_rows_to_column_rows(
            int(file_id), str(sheet_name), row["headers_json"]
        )
    finally:
        conn.close()
