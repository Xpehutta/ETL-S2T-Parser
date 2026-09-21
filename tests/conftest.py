import sys
import os
import tempfile
import logging
import asyncio

# Add project root to Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

# Keep ordinary unit tests independent from developer-local ``.env`` opt-ins.
# Explicit shell/runner values win because setdefault never overwrites them.
from agents.env_flags import parse_binary_flag
from agents.experiment_flags import BINARY_EXPERIMENT_DEFAULTS

_live_agent_tests_enabled = parse_binary_flag(
    "RUN_LIVE_AGENT_SCENARIOS",
    os.getenv("RUN_LIVE_AGENT_SCENARIOS"),
    default=False,
)
if not _live_agent_tests_enabled:
    for _experiment_name, _experiment_default in BINARY_EXPERIMENT_DEFAULTS.items():
        os.environ.setdefault(
            _experiment_name,
            "1" if _experiment_default else "0",
        )

# Disable Langfuse/OTEL export noise before project modules load dotenv.
os.environ["LANGFUSE_PUBLIC_KEY"] = ""
os.environ["LANGFUSE_SECRET_KEY"] = ""
os.environ["OTEL_SDK_DISABLED"] = "true"
os.environ.setdefault("GIGACHAT_VERIFY_SSL", "0")

# app.py configures file logging at import time. Keep test-only tool calls such
# as ping/echo out of the runtime logs/agent.log.
_pytest_import_log = tempfile.NamedTemporaryFile(suffix=".log", delete=False)
_pytest_import_log.close()
os.environ["LOG_FILE"] = _pytest_import_log.name

# app.py calls init_db() at import time; use an isolated DB so a legacy local
# excel_data.db does not break test collection.
import storage.database as db_storage

_pytest_import_db = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
_pytest_import_db.close()
db_storage.DB_PATH = _pytest_import_db.name

import pytest
import httpx
import pytest_asyncio
from app import app as asgi_app
from storage.database import init_db


class _TestResponse:
    """Small Flask-response compatibility layer for the existing API tests."""

    def __init__(self, response: httpx.Response):
        self._response = response
        self.status_code = response.status_code
        self.headers = response.headers
        self.data = response.content
        self.mimetype = response.headers.get("content-type", "").split(";", 1)[0]

    def get_json(self):
        return self._response.json()

    def get_data(self, *, as_text=False):
        return self._response.text if as_text else self._response.content


class _SyncASGITestClient:
    def __init__(self, application):
        self.application = application

    def _request(self, method, url, **kwargs):
        kwargs.pop("content_type", None)
        data = kwargs.get("data")
        if isinstance(data, dict) and "file" in data:
            form = dict(data)
            stream, filename = form.pop("file")
            kwargs["data"] = form
            kwargs["files"] = {
                "file": (
                    filename,
                    stream.getvalue() if hasattr(stream, "getvalue") else stream.read(),
                    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )
            }

        async def send():
            transport = httpx.ASGITransport(app=self.application)
            async with httpx.AsyncClient(
                transport=transport,
                base_url="http://testserver",
            ) as http_client:
                return await http_client.request(method, url, **kwargs)

        return _TestResponse(asyncio.run(send()))

    def get(self, url, **kwargs):
        return self._request("GET", url, **kwargs)

    def post(self, url, **kwargs):
        return self._request("POST", url, **kwargs)

    def delete(self, url, **kwargs):
        return self._request("DELETE", url, **kwargs)


@pytest.fixture(scope="session", autouse=True)
def _cleanup_pytest_import_db():
    yield
    root_logger = logging.getLogger()
    for handler in list(root_logger.handlers):
        if getattr(handler, "_etls2t_log_path", None) == _pytest_import_log.name:
            root_logger.removeHandler(handler)
            handler.close()
    try:
        os.unlink(_pytest_import_db.name)
    except OSError:
        pass
    try:
        os.unlink(_pytest_import_log.name)
    except OSError:
        pass


@pytest.fixture(autouse=True)
def _disable_langfuse_in_tests(monkeypatch):
    monkeypatch.setenv("LANGFUSE_PUBLIC_KEY", "")
    monkeypatch.setenv("LANGFUSE_SECRET_KEY", "")
    from agents import observability

    monkeypatch.setattr(observability, "is_langfuse_configured", lambda: False)
    monkeypatch.setattr(observability, "get_langfuse_client", lambda: None)
    monkeypatch.setattr(observability, "get_callback_handler", lambda: None)


@pytest.fixture
def mock_embeddings(monkeypatch):
    monkeypatch.setenv("EMBEDDING_MODEL", "test-embedding-model")
    monkeypatch.setenv("EMBEDDING_PROFILE", "plain-normalized-v1")
    from services import embeddings

    identity = embeddings.EmbeddingIndexIdentity(
        model_name="test-embedding-model",
        model_revision="",
        profile_id="plain-normalized-v1",
        query_prefix="",
        document_prefix="",
        normalize_embeddings=True,
        dimension=2,
    )
    encode_one = lambda text: f"embedding:{text}".encode("utf-8")
    encode_many = lambda texts: [encode_one(text) for text in texts]
    monkeypatch.setattr(
        embeddings,
        "embed_description",
        encode_one,
    )
    monkeypatch.setattr(
        embeddings,
        "embed_descriptions",
        encode_many,
    )
    monkeypatch.setattr(embeddings, "embed_document", encode_one)
    monkeypatch.setattr(embeddings, "embed_documents", encode_many)
    monkeypatch.setattr(
        embeddings,
        "embedding_index_identity_for_blobs",
        lambda blobs: identity,
    )


@pytest.fixture
def app(tmp_path):
    """FastAPI application fixture with transitional config values."""
    previous_agent_mode = asgi_app.config.get('CHAT_AGENT_MODE')
    asgi_app.config['TESTING'] = True
    asgi_app.config['MAX_CONTENT_LENGTH'] = 10 * 1024 * 1024
    asgi_app.config['DB_PATH'] = str(tmp_path / "fastapi_test.db")
    asgi_app.config['CHAT_AGENT_MODE'] = 'multiagent'
    yield asgi_app
    asgi_app.config['CHAT_AGENT_MODE'] = previous_agent_mode

@pytest.fixture
def client(app):
    return _SyncASGITestClient(app)


@pytest_asyncio.fixture
async def async_client(app):
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://testserver",
    ) as http_client:
        yield http_client

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
