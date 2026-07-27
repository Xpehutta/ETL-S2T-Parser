import sys
import os
import tempfile

# Add project root to Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

# app.py calls init_db() at import time; use an isolated DB so a legacy local
# excel_data.db does not break test collection.
import storage.database as db_storage

_pytest_import_db = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
_pytest_import_db.close()
db_storage.DB_PATH = _pytest_import_db.name

import pytest
import sqlite3
import json
import io
from flask import Flask
from app import app as flask_app
from storage.database import init_db, get_db_connection


@pytest.fixture
def mock_embeddings(monkeypatch):
    monkeypatch.setenv("EMBEDDING_MODEL", "test-embedding-model")
    from services import embeddings

    monkeypatch.setattr(
        embeddings,
        "embed_description",
        lambda text: f"embedding:{text}".encode("utf-8"),
    )
    monkeypatch.setattr(
        embeddings,
        "embed_descriptions",
        lambda texts: [
            f"embedding:{text}".encode("utf-8")
            for text in texts
        ],
    )


@pytest.fixture
def app(tmp_path):
    """Flask test client fixture."""
    flask_app.config['TESTING'] = True
    flask_app.config['MAX_CONTENT_LENGTH'] = 10 * 1024 * 1024
    flask_app.config['DB_PATH'] = str(tmp_path / "flask_test.db")
    yield flask_app

@pytest.fixture
def client(app):
    return app.test_client()

@pytest.fixture
def temp_db(tmp_path):
    """Create a temporary SQLite database for testing."""
    import storage.database as db_storage
    original_path = db_storage.DB_PATH
    db_storage.DB_PATH = str(tmp_path / "test.db")
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
        "file_id": "abc123",
        "summary": "Test summary",
        "description": "Test description",
        "sheets": [
            {
                "sheet_name": "Sheet1",
                "skip_reason": None,
                "header": {
                    "start_row": 0,
                    "row_count": 1,
                    "nested": False
                },
                "columns": ["Name", "Age"],
                "data_preview": [["Alice", 30], ["Bob", 25]],
            }
        ]
    }
