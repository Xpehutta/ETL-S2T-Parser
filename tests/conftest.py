import sys
import os

# Add project root to Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import pytest
import tempfile
import sqlite3
import json
import io
from flask import Flask
from app import app as flask_app
from db_storage import init_db, get_db_connection

@pytest.fixture
def app():
    """Flask test client fixture."""
    flask_app.config['TESTING'] = True
    flask_app.config['MAX_CONTENT_LENGTH'] = 10 * 1024 * 1024
    # Use a temporary database for tests
    with tempfile.NamedTemporaryFile(suffix='.db') as tmp:
        flask_app.config['DB_PATH'] = tmp.name
        yield flask_app

@pytest.fixture
def client(app):
    return app.test_client()

@pytest.fixture
def temp_db():
    """Create a temporary SQLite database for testing."""
    with tempfile.NamedTemporaryFile(suffix='.db') as tmp:
        import db_storage
        original_path = db_storage.DB_PATH
        db_storage.DB_PATH = tmp.name
        init_db()
        yield db_storage.get_db_connection()
        db_storage.DB_PATH = original_path

@pytest.fixture
def sample_excel_bytes():
    """Return bytes of a minimal Excel file (using pandas)."""
    import pandas as pd
    import io
    df = pd.DataFrame({
        'Name': ['Alice', 'Bob'],
        'Age': [30, 25]
    })
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, sheet_name='Sheet1', index=False)
    return output.getvalue()

@pytest.fixture
def sample_excel_json():
    """Sample JSON structure as returned by /upload (mock)."""
    return {
        "filename": "test.xlsx",
        "model_used": "GigaChat-Pro",
        "file_hash": "abc123",
        "summary": "Test summary",
        "sheets": [
            {
                "sheet_name": "Sheet1",
                "skipped": False,
                "ai_decision": {
                    "header_start_row": 0,
                    "header_rows_count": 1,
                    "nested_structure": False
                },
                "columns": ["Name", "Age"],
                "first_data_rows_preview": [["Alice", 30], ["Bob", 25]],
                "preview_rows": [["Name", "Age"], ["Alice", 30], ["Bob", 25]]
            }
        ]
    }