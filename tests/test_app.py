import pytest
import json
import tempfile
from unittest.mock import patch, MagicMock
from db_storage import init_db, get_db_connection
import io

# Override DB_PATH for the test session (will be set per test)
@pytest.fixture(autouse=True)
def use_temp_db():
    import db_storage
    original_path = db_storage.DB_PATH
    with tempfile.NamedTemporaryFile(suffix='.db') as tmp:
        db_storage.DB_PATH = tmp.name
        init_db()
        yield
        db_storage.DB_PATH = original_path

def test_index(client):
    response = client.get('/')
    assert response.status_code == 200
    assert b'AI Excel Parser' in response.data

@patch('app.parse_excel_with_decisions')
@patch('app.store_excel_data')
@patch('app.summarize_file')
@patch('app.update_file_result_json')
def test_upload(mock_update_json, mock_summarize, mock_store, mock_parse, client, sample_excel_bytes):
    mock_parse.return_value = (
        [{"sheet_name": "Sheet1", "skipped": False, "ai_decision": {}, "columns": ["Name"], "preview_rows": [], "first_data_rows_preview": []}],
        {"Sheet1": []}
    )
    mock_store.return_value = "fake_hash"
    mock_summarize.return_value = "Test summary"
    data = {
        'file': (io.BytesIO(sample_excel_bytes), 'test.xlsx')
    }
    response = client.post('/upload', data=data, content_type='multipart/form-data')
    assert response.status_code == 200
    json_data = response.get_json()
    assert json_data['filename'] == 'test.xlsx'
    assert 'file_hash' in json_data
    assert 'summary' in json_data

@patch('app.parse_excel_with_decisions')
@patch('app.store_excel_data')
@patch('app.summarize_file')
@patch('app.update_file_result_json')
def test_apply_corrections(mock_update_json, mock_summarize, mock_store, mock_parse, client, sample_excel_bytes):
    import app
    # Store file bytes in cache
    app.file_bytes_cache['fake_hash'] = sample_excel_bytes
    mock_parse.return_value = (
        [{"sheet_name": "Sheet1", "skipped": False, "ai_decision": {}, "columns": ["Name"], "preview_rows": []}],
        {"Sheet1": []}
    )
    mock_summarize.return_value = "Updated summary"
    # Insert a file record manually with the same fake_hash
    conn = get_db_connection()
    cursor = conn.cursor()
    # Use INSERT OR IGNORE to avoid duplicate
    cursor.execute("INSERT OR IGNORE INTO files (file_hash, filename, upload_time, model_used) VALUES (?, ?, ?, ?)",
                   ("fake_hash", "test.xlsx", "2025-01-01", "model"))
    conn.commit()
    conn.close()
    payload = {
        "file_hash": "fake_hash",
        "corrections": [
            {"sheet_name": "Sheet1", "skipped": False, "header_start_row": 0, "header_rows_count": 1}
        ]
    }
    response = client.post('/apply_corrections', json=payload)
    assert response.status_code == 200
    data = response.get_json()
    assert data['filename'] == 'test.xlsx'

def test_preview_headers(client, sample_excel_bytes):
    import app
    app.file_bytes_cache['test_hash'] = sample_excel_bytes
    payload = {
        "file_hash": "test_hash",
        "sheet_name": "Sheet1",
        "option": "1"
    }
    response = client.post('/preview_headers', json=payload)
    assert response.status_code == 200
    data = response.get_json()
    assert 'headers' in data
    assert isinstance(data['headers'], list)