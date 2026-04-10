import hashlib
import sqlite3
import json
import logging
from typing import List, Dict, Any
from datetime import datetime

logger = logging.getLogger(__name__)

DB_PATH = "excel_data.db"


def get_db_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def column_exists(cursor, table_name, column_name):
    cursor.execute(f"PRAGMA table_info({table_name})")
    columns = [row[1] for row in cursor.fetchall()]
    return column_name in columns


def init_db():
    conn = get_db_connection()
    cursor = conn.cursor()

    # Files table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS files (
            file_hash TEXT PRIMARY KEY,
            filename TEXT NOT NULL,
            upload_time TEXT NOT NULL,
            model_used TEXT
        )
    ''')
    if not column_exists(cursor, 'files', 'summary'):
        cursor.execute('ALTER TABLE files ADD COLUMN summary TEXT')
        logger.info("Added 'summary' column to files table")
    if not column_exists(cursor, 'files', 'result_json'):
        cursor.execute('ALTER TABLE files ADD COLUMN result_json TEXT')
        logger.info("Added 'result_json' column to files table")

    # Sheets table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS sheets (
            sheet_hash TEXT PRIMARY KEY,
            file_hash TEXT NOT NULL,
            sheet_name TEXT NOT NULL,
            header_start_row INTEGER,
            header_rows_count INTEGER,
            nested_structure INTEGER,
            FOREIGN KEY (file_hash) REFERENCES files(file_hash)
        )
    ''')

    # Columns table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS columns (
            column_hash TEXT PRIMARY KEY,
            sheet_hash TEXT NOT NULL,
            column_index INTEGER NOT NULL,
            column_header TEXT,
            column_name_flat TEXT,
            FOREIGN KEY (sheet_hash) REFERENCES sheets(sheet_hash)
        )
    ''')

    # Data values table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sheet_hash TEXT NOT NULL,
            column_hash TEXT NOT NULL,
            row_num INTEGER NOT NULL,
            value TEXT,
            FOREIGN KEY (sheet_hash) REFERENCES sheets(sheet_hash),
            FOREIGN KEY (column_hash) REFERENCES columns(column_hash)
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS source_tables (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            description TEXT,
            system_code TEXT
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS target_tables (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            description TEXT
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS column_mappings (
            id TEXT PRIMARY KEY,
            target_table_id TEXT NOT NULL,
            target_column TEXT NOT NULL,
            source_table_id TEXT NOT NULL,
            source_column TEXT,
            transformation_rule TEXT,
            data_type TEXT,
            is_primary_key INTEGER DEFAULT 0,
            FOREIGN KEY (target_table_id) REFERENCES target_tables(id),
            FOREIGN KEY (source_table_id) REFERENCES source_tables(id)
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS additions (
            id TEXT PRIMARY KEY,
            table_name TEXT,
            table_description TEXT,
            source_tables_name TEXT,
            sql TEXT,
            description TEXT
        )
    ''')

    conn.commit()
    conn.close()
    logger.info("Database initialized with all tables and columns")


def generate_file_hash(file_bytes: bytes) -> str:
    return hashlib.sha256(file_bytes).hexdigest()


def generate_sheet_hash(file_hash: str, sheet_name: str) -> str:
    return hashlib.sha256(f"{file_hash}|{sheet_name}".encode()).hexdigest()


def generate_column_hash(sheet_hash: str, column_index: int, column_header: Any) -> str:
    header_str = json.dumps(column_header, sort_keys=True) if column_header is not None else "null"
    return hashlib.sha256(f"{sheet_hash}|{column_index}|{header_str}".encode()).hexdigest()


def flatten_column_header(column_header: Any) -> str:
    if isinstance(column_header, list):
        return " > ".join(str(part) for part in column_header if part is not None)
    return str(column_header) if column_header is not None else ""

def generate_id(*parts):
    """Generate SHA256 hash from lower‑cased parts for ID."""
    combined = "|".join(str(p).lower() for p in parts if p)
    return hashlib.sha256(combined.encode()).hexdigest()[:16]

def store_excel_data(
        file_bytes: bytes,
        filename: str,
        model_used: str,
        sheets_info: List[Dict],
        data_rows_by_sheet: Dict[str, List[List[Any]]],
        max_rows_per_sheet: int = 1000
):
    file_hash = generate_file_hash(file_bytes)
    upload_time = datetime.now().isoformat()

    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        cursor.execute(
            "INSERT OR REPLACE INTO files (file_hash, filename, upload_time, model_used) VALUES (?, ?, ?, ?)",
            (file_hash, filename, upload_time, model_used)
        )

        for sheet_info in sheets_info:
            sheet_name = sheet_info["sheet_name"]
            if sheet_info.get("skipped", False):
                continue

            sheet_hash = generate_sheet_hash(file_hash, sheet_name)
            ai_dec = sheet_info.get("ai_decision", {})
            header_start_row = ai_dec.get("header_start_row", 0)
            header_rows_count = ai_dec.get("header_rows_count", 1)
            nested = 1 if ai_dec.get("nested_structure", False) else 0

            cursor.execute(
                "INSERT OR REPLACE INTO sheets (sheet_hash, file_hash, sheet_name, header_start_row, header_rows_count, nested_structure) VALUES (?, ?, ?, ?, ?, ?)",
                (sheet_hash, file_hash, sheet_name, header_start_row, header_rows_count, nested)
            )

            columns = sheet_info.get("columns", [])
            for col_idx, col_header in enumerate(columns):
                col_hash = generate_column_hash(sheet_hash, col_idx, col_header)
                col_header_json = json.dumps(col_header, ensure_ascii=False) if col_header is not None else None
                col_flat = flatten_column_header(col_header)
                cursor.execute(
                    "INSERT OR REPLACE INTO columns (column_hash, sheet_hash, column_index, column_header, column_name_flat) VALUES (?, ?, ?, ?, ?)",
                    (col_hash, sheet_hash, col_idx, col_header_json, col_flat)
                )

            data_rows = data_rows_by_sheet.get(sheet_name, [])
            for row_num, row in enumerate(data_rows[:max_rows_per_sheet]):
                for col_idx, cell_value in enumerate(row):
                    if col_idx >= len(columns):
                        continue
                    col_hash = generate_column_hash(sheet_hash, col_idx, columns[col_idx])
                    value_str = str(cell_value) if cell_value is not None else None
                    cursor.execute(
                        "INSERT INTO data (sheet_hash, column_hash, row_num, value) VALUES (?, ?, ?, ?)",
                        (sheet_hash, col_hash, row_num, value_str)
                    )

        conn.commit()
        logger.info(f"Stored data for file {filename} (hash: {file_hash})")
        return file_hash
    except Exception as e:
        logger.error(f"Failed to store data: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()


def update_file_summary(file_hash: str, summary: str):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("UPDATE files SET summary = ? WHERE file_hash = ?", (summary, file_hash))
    conn.commit()
    conn.close()


def update_file_result_json(file_hash: str, result_json: str):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("UPDATE files SET result_json = ? WHERE file_hash = ?", (result_json, file_hash))
    conn.commit()
    conn.close()