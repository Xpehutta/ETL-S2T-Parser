import pytest
import json
from db_storage import init_db, store_excel_data, get_db_connection, update_file_summary, update_file_result_json

def test_init_db(temp_db):
    # temp_db fixture provides a connection; tables should exist
    cursor = temp_db.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = [row[0] for row in cursor.fetchall()]
    assert 'files' in tables
    assert 'sheets' in tables
    assert 'columns' in tables
    assert 'data' in tables

def test_store_excel_data(temp_db, sample_excel_bytes):
    sheets_info = [{
        "sheet_name": "Sheet1",
        "skipped": False,
        "ai_decision": {"header_start_row": 0, "header_rows_count": 1, "nested_structure": False},
        "columns": ["Name", "Age"]
    }]
    data_rows_by_sheet = {
        "Sheet1": [["Alice", 30], ["Bob", 25]]
    }
    file_hash = store_excel_data(sample_excel_bytes, "test.xlsx", "GigaChat-Pro", sheets_info, data_rows_by_sheet)
    assert file_hash is not None
    # Verify data was inserted
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT filename FROM files WHERE file_hash = ?", (file_hash,))
    row = cursor.fetchone()
    assert row["filename"] == "test.xlsx"
    conn.close()

def test_update_file_summary(temp_db):
    # First insert a file manually
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("INSERT INTO files (file_hash, filename, upload_time, model_used) VALUES (?, ?, ?, ?)",
                   ("hash123", "test.xlsx", "2025-01-01", "model"))
    conn.commit()
    conn.close()
    update_file_summary("hash123", "Test summary")
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT summary FROM files WHERE file_hash = ?", ("hash123",))
    row = cursor.fetchone()
    assert row["summary"] == "Test summary"
    conn.close()