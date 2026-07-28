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
    "result_json",
    "description_embedding",
)
FILE_SHEET_HEADER_COLUMNS = (
    "sheet_id",
    "file_id",
    "sheet_name",
    "skipped",
    "skip_reason",
    "header_start_row",
    "header_rows_count",
    "nested_structure",
    "columns_count",
    "headers_json",
    "headers_flat",
)
EXTRACTION_METADATA_COLUMNS = (
    "id",
    "file_id",
    "sheet_id",
    "sheet_name",
    "row_num",
)
SOURCE_TABLE_FIELDS = tuple(
    get_usefull_col_extraction_target("source_tables")["fields"]
)
TARGET_TABLE_FIELDS = tuple(
    get_usefull_col_extraction_target("target_tables")["fields"]
)
S2T_FIELDS = tuple(
    get_usefull_col_extraction_target("s2t_transformations")["fields"]
)
SOURCE_TABLE_COLUMNS = (
    EXTRACTION_METADATA_COLUMNS + SOURCE_TABLE_FIELDS + ("description_embedding",)
)
TARGET_TABLE_COLUMNS = (
    EXTRACTION_METADATA_COLUMNS + TARGET_TABLE_FIELDS + ("description_embedding",)
)
S2T_TRANSFORMATION_COLUMNS = EXTRACTION_METADATA_COLUMNS + S2T_FIELDS + ("raw_json",)
DATA_COLUMNS = (
    "id",
    "sheet_id",
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
    "s2t_transformations",
)
LEGACY_TABLES = (
    "relationships",
    "embeddings",
    "column_mappings",
    "additions",
)
REMOVED_WORKBOOK_TABLES = (
    "sheets",
    "columns",
)
USER_FACING_TABLES = (
    "files",
    "file_sheet_headers",
    "source_tables",
    "target_tables",
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


def _header_rows_to_column_rows(sheet_id: int, headers_json: Optional[str]) -> List[Dict[str, Any]]:
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
                "sheet_id": int(sheet_id),
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
            result_json TEXT,
            description_embedding BLOB
        )
        """
    )
    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {names['file_sheet_headers']} (
            sheet_id INTEGER PRIMARY KEY,
            file_id INTEGER,
            sheet_name TEXT,
            skipped INTEGER DEFAULT 0,
            skip_reason TEXT,
            header_start_row INTEGER,
            header_rows_count INTEGER,
            nested_structure INTEGER,
            columns_count INTEGER DEFAULT 0,
            headers_json TEXT,
            headers_flat TEXT
        )
        """
    )
    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {names['data']} (
            id INTEGER PRIMARY KEY,
            sheet_id INTEGER,
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
                sheet_id INTEGER,
                sheet_name TEXT,
                row_num INTEGER,
                {fields_sql},
                description_embedding BLOB
            )
            """
        )
    s2t_fields_sql = _text_columns_sql(S2T_FIELDS, "            ")
    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {names['s2t_transformations']} (
            id INTEGER PRIMARY KEY,
            file_id INTEGER,
            sheet_id INTEGER,
            sheet_name TEXT,
            row_num INTEGER,
            {s2t_fields_sql},
            raw_json TEXT
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
        "file_sheet_headers": "sheet_id",
        "data": "id",
        "source_tables": "id",
        "target_tables": "id",
        "s2t_transformations": "id",
    }
    for table_name, key_name in integer_primary_keys.items():
        info = {str(row[1]): row for row in _table_info(cursor, table_name)}
        key = info.get(key_name)
        if key is None or str(key[2]).upper() != "INTEGER" or int(key[5]) != 1:
            mismatches.append(
                f"{table_name}.{key_name}: expected INTEGER PRIMARY KEY"
            )
    data_info = {str(row[1]): row for row in _table_info(cursor, "data")}
    column_id = data_info.get("column_id")
    if column_id is None or str(column_id[2]).upper() != "INTEGER":
        mismatches.append("data.column_id: expected INTEGER")
    return mismatches


def _create_indexes(cursor: sqlite3.Cursor) -> None:
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_data_sheet_row ON data(sheet_id, row_num)")
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
        for table_name in LEGACY_TABLES + REMOVED_WORKBOOK_TABLES:
            cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    logger.info("Database initialized with the current schema")


def clear_all_data() -> Dict[str, int]:
    """Delete every row from all current application tables."""
    deletion_order = (
        "data",
        "s2t_transformations",
        "source_tables",
        "target_tables",
        "file_sheet_headers",
        "files",
    )
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("BEGIN")
        deleted = {
            table_name: int(
                cursor.execute(
                    f"SELECT COUNT(*) FROM {_sql_identifier(table_name)}"
                ).fetchone()[0]
            )
            for table_name in deletion_order
        }
        for table_name in deletion_order:
            cursor.execute(f"DELETE FROM {_sql_identifier(table_name)}")
        conn.commit()
        return {
            table_name: deleted[table_name]
            for table_name in USER_FACING_TABLES
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def store_excel_data(
    file_bytes: bytes,
    filename: str,
    model_used: str,
    sheets: List[Dict[str, Any]],
    max_rows_per_sheet: int = 1000,
    file_id: Optional[int] = None,
) -> int:
    """Store one workbook upload; equal files and equal rows remain separate records."""
    _ = file_bytes
    upload_time = datetime.now().isoformat()
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("BEGIN")
        if file_id is None:
            cursor.execute(
                """
                INSERT INTO files (filename, model_used, upload_time)
                VALUES (?, ?, ?)
                """,
                (filename, model_used, upload_time),
            )
            current_file_id = int(cursor.lastrowid)
        else:
            current_file_id = int(file_id)
            existing = cursor.execute(
                "SELECT 1 FROM files WHERE file_id = ?",
                (current_file_id,),
            ).fetchone()
            if not existing:
                raise ValueError(f"File not found: {current_file_id}")
            cursor.execute(
                """
                UPDATE files
                SET filename = ?, model_used = ?, upload_time = ?
                WHERE file_id = ?
                """,
                (filename, model_used, upload_time, current_file_id),
            )
            cursor.execute(
                "DELETE FROM data WHERE sheet_id IN "
                "(SELECT sheet_id FROM file_sheet_headers WHERE file_id = ?)",
                (current_file_id,),
            )
            cursor.execute("DELETE FROM file_sheet_headers WHERE file_id = ?", (current_file_id,))
            cursor.execute("DELETE FROM source_tables WHERE file_id = ?", (current_file_id,))
            cursor.execute("DELETE FROM target_tables WHERE file_id = ?", (current_file_id,))
            cursor.execute("DELETE FROM s2t_transformations WHERE file_id = ?", (current_file_id,))

        for sheet in sheets:
            sheet_name = str(sheet["sheet_name"])
            header = sheet.get("header")
            if header is None:
                cursor.execute(
                    """
                    INSERT INTO file_sheet_headers
                    (file_id, sheet_name, skipped, skip_reason, columns_count,
                     headers_json, headers_flat)
                    VALUES (?, ?, 1, ?, 0, '[]', '')
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
                 headers_json, headers_flat)
                VALUES (?, ?, 0, '', ?, ?, ?, ?, ?, ?)
                """,
                (
                    current_file_id,
                    sheet_name,
                    header["start_row"],
                    header["row_count"],
                    1 if header["nested"] else 0,
                    len(header_rows),
                    json.dumps(header_rows, ensure_ascii=False, default=str),
                    "\n".join(row["flat"] for row in header_rows),
                ),
            )
            sheet_id = int(cursor.lastrowid)
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
                        (sheet_id, sheet_name, row_num, column_index + 1, str(value)[:1000])
                    )
            cursor.executemany(
                """
                INSERT INTO data (sheet_id, table_name, row_num, column_id, value)
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


def update_file_result_json(file_id: int, result_json: str) -> None:
    conn = get_db_connection()
    try:
        conn.execute(
            "UPDATE files SET result_json = ? WHERE file_id = ?",
            (result_json, int(file_id)),
        )
        conn.commit()
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


def _norm_header_token(value: str) -> str:
    return " ".join((value or "").split()).lower()


def _header_segments(column_name_flat: str) -> List[str]:
    if not column_name_flat:
        return []
    return [
        _norm_header_token(part)
        for part in re.split(r"\s*>\s*", column_name_flat.strip())
        if part.strip()
    ]


def get_column_id_by_name(sheet_id: int, column_name: str) -> Optional[int]:
    """Find a numeric column ID by flat or nested header name."""
    if sheet_id is None or not column_name:
        return None
    key = _norm_header_token(column_name)
    if not key:
        return None
    key_loose = key.replace("_", " ")
    segment_hits: List[tuple[int, int]] = []
    loose_segment_hits: List[tuple[int, int]] = []
    substring_hits: List[tuple[int, int]] = []

    for row in get_columns_by_sheet(int(sheet_id)):
        flat = row["column_name_flat"] or ""
        column_id = int(row["column_id"])
        column_index = int(row["column_index"])
        normalized_flat = _norm_header_token(flat)
        if normalized_flat in {key, key_loose}:
            return column_id
        segments = _header_segments(flat)
        if key in segments or key_loose in segments:
            segment_hits.append((column_index, column_id))
            continue
        if any(key_loose == segment.replace("_", " ") for segment in segments):
            loose_segment_hits.append((column_index, column_id))
            continue
        if len(key) >= 5 and (
            key in normalized_flat or key_loose in normalized_flat.replace("_", " ")
        ):
            substring_hits.append((column_index, column_id))

    for matches in (segment_hits, loose_segment_hits, substring_hits):
        if matches:
            matches.sort(key=lambda item: item[0])
            return matches[0][1]
    return None


def get_sheet_id(file_id: int, sheet_name: str) -> Optional[int]:
    conn = get_db_connection()
    try:
        row = conn.execute(
            """
            SELECT sheet_id
            FROM file_sheet_headers
            WHERE file_id = ? AND sheet_name = ?
            ORDER BY sheet_id
            LIMIT 1
            """,
            (int(file_id), sheet_name),
        ).fetchone()
        return int(row["sheet_id"]) if row else None
    finally:
        conn.close()


def get_all_files() -> List[Dict[str, Any]]:
    conn = get_db_connection()
    try:
        rows = conn.execute(
            "SELECT file_id, filename, upload_time FROM files ORDER BY upload_time DESC, file_id DESC"
        ).fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()


def get_sheets_by_file(file_id: int) -> List[str]:
    conn = get_db_connection()
    try:
        rows = conn.execute(
            "SELECT sheet_name FROM file_sheet_headers WHERE file_id = ? ORDER BY sheet_id",
            (int(file_id),),
        ).fetchall()
        return [str(row["sheet_name"]) for row in rows]
    finally:
        conn.close()


def get_columns_by_sheet(sheet_id: int) -> List[Dict[str, Any]]:
    conn = get_db_connection()
    try:
        row = conn.execute(
            "SELECT headers_json FROM file_sheet_headers WHERE sheet_id = ?",
            (int(sheet_id),),
        ).fetchone()
        if not row:
            return []
        return _header_rows_to_column_rows(int(sheet_id), row["headers_json"])
    finally:
        conn.close()
