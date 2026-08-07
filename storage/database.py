import json
import logging
import re
import sqlite3
from datetime import datetime
from typing import Any, Dict, List, Optional

from config.useful_columns import get_usefull_col_extraction_target


logger = logging.getLogger(__name__)


class DatabaseSchemaError(RuntimeError):
    """Raised when an existing SQLite database does not match the current schema."""


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
ADDITIONAL_OBJECT_COLUMNS = EXTRACTION_METADATA_COLUMNS + ADDITIONAL_OBJECT_FIELDS
PXF_TO_A_COLUMNS = EXTRACTION_METADATA_COLUMNS + PXF_TO_A_FIELDS
S2T_TRANSFORMATION_COLUMNS = EXTRACTION_METADATA_COLUMNS + S2T_RECORD_FIELDS
DATA_COLUMNS = (
    "id",
    "file_id",
    "table_name",
    "row_num",
    "column_id",
    "value",
)
CORE_TABLES = (
    "files",
    "data",
    "file_sheet_headers",
    "source_tables",
    "target_tables",
    "additional_objects",
    "pxf_to_a",
    "s2t_transformations",
)
USER_FACING_TABLES = (
    "files",
    "file_sheet_headers",
    "source_tables",
    "target_tables",
    "additional_objects",
    "pxf_to_a",
    "s2t_transformations",
    "data",
)
INTERNAL_TABLES = ()
STORAGE_SCHEMA_TABLE_ORDER = USER_FACING_TABLES
STORAGE_SCHEMA_COLUMNS = {
    "files": FILES_COLUMNS,
    "file_sheet_headers": FILE_SHEET_HEADER_COLUMNS,
    "source_tables": SOURCE_TABLE_COLUMNS,
    "target_tables": TARGET_TABLE_COLUMNS,
    "additional_objects": ADDITIONAL_OBJECT_COLUMNS,
    "pxf_to_a": PXF_TO_A_COLUMNS,
    "s2t_transformations": S2T_TRANSFORMATION_COLUMNS,
    "data": DATA_COLUMNS,
}


def _sql_identifier(value: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        raise ValueError(f"Invalid configured SQLite identifier: {value!r}")
    return f'"{value}"'


def _text_columns_sql(fields: tuple[str, ...], indent: str) -> str:
    return (",\n" + indent).join(
        f"{_sql_identifier(field)} TEXT" for field in fields
    )


def get_db_connection() -> sqlite3.Connection:
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


def _schema_mismatches(cursor: sqlite3.Cursor) -> List[str]:
    mismatches: List[str] = []
    for table_name, expected_columns in STORAGE_SCHEMA_COLUMNS.items():
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
        "additional_objects": "id",
        "pxf_to_a": "id",
        "s2t_transformations": "id",
    }
    for table_name, key_name in integer_primary_keys.items():
        info = {str(row[1]): row for row in _table_info(cursor, table_name)}
        key = info.get(key_name)
        if key is None or str(key[2]).upper() != "INTEGER" or int(key[5]) != 1:
            mismatches.append(
                f"{table_name}.{key_name}: expected INTEGER PRIMARY KEY"
            )
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
    for table_name in ("additional_objects", "pxf_to_a"):
        cursor.execute(
            f"CREATE INDEX IF NOT EXISTS idx_{table_name}_file ON {table_name}(file_id)"
        )


def init_db() -> None:
    """Create the current schema or reject an incompatible existing database."""
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("BEGIN")
        existing_core_tables = [
            table_name for table_name in CORE_TABLES if _table_exists(cursor, table_name)
        ]
        if existing_core_tables:
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
        _create_indexes(cursor)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    logger.info("Database initialized with the current schema")


def migrate_s2t_layer_columns() -> Dict[str, Any]:
    """Explicitly add nullable source/target layer columns to the prior schema."""
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


def clear_all_data() -> Dict[str, int]:
    """Drop all application tables and recreate the current empty schema."""
    deletion_order = tuple(reversed(STORAGE_SCHEMA_TABLE_ORDER))
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("BEGIN")
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
        _create_indexes(cursor)
        mismatches = _schema_mismatches(cursor)
        if mismatches:
            raise DatabaseSchemaError(
                "Failed to recreate the current SQLite schema: "
                + "; ".join(mismatches)
            )
        conn.commit()
        return deleted
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def store_excel_data(
    filename: str,
    model_used: str,
    sheets: List[Dict[str, Any]],
    max_rows_per_sheet: int = 1000,
) -> int:
    """Store one workbook upload; equal files and equal rows remain separate records."""
    upload_time = datetime.now().isoformat()
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("BEGIN")
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
            for row_index, row in enumerate(data_rows[:max_rows_per_sheet]):
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
    from services.embeddings import embed_description

    description_embedding = embed_description(description)
    conn = get_db_connection()
    try:
        conn.execute(
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
            WHERE file_id = ? AND sheet_name = ? COLLATE NOCASE
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
