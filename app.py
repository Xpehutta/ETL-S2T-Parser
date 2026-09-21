from __future__ import annotations

import asyncio
import datetime
import json
import logging
import os
from contextlib import asynccontextmanager
from pathlib import Path
from threading import Lock
from typing import Any, Dict, List, Literal, Optional

import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator
from starlette.datastructures import UploadFile

from agents.agent import agent_chat, agent_chat_async, get_model_name
from agents.env_flags import read_binary_env_flag
from agents.sheet_group_classifier import classify_file_sheet_groups
from agents.supervisor import supervisor_chat, supervisor_chat_async
from processing.excel import (
    allowed_file,
    convert_to_serializable,
    parse_excel_with_decisions,
)
from services.analysis import (
    finish_analysis,
    try_generate_description,
    try_generate_summary,
    try_sync_file_graph,
    try_sync_pending_graph_projections,
)
from services.graph_sync import clear_graph_projection
from services.logging_setup import configure_logging
from storage.database import (
    clear_all_data_with_graph_snapshot,
    get_file,
    init_db,
    store_excel_data,
)
from storage.graph_outbox import get_graph_sync_state, mark_graph_syncs_applied
from storage.s2t import clear_s2t_transformations, list_s2t_transformations

LOG_FILE_PATH = configure_logging()
logger = logging.getLogger(__name__)
logger.info("File logging enabled: %s", LOG_FILE_PATH)

PROJECT_ROOT = Path(__file__).resolve().parent
MAX_UPLOAD_SIZE = 10 * 1024 * 1024
DEFAULT_CHAT_REQUEST_TIMEOUT = 900.0

CHAT_HISTORY_MAX_MESSAGES = 12
CHAT_HISTORY_MAX_MESSAGE_CHARS = 8000
CHAT_HISTORY_MAX_TOTAL_CHARS = 16000
CHAT_SESSION_ID_MAX_CHARS = 200
PROGRESS_EVENT_FIELDS = (
    "status",
    "phase",
    "percent",
    "message",
    "detail",
    "sheet_name",
    "sheet_index",
    "sheet_count",
    "total_data_row_count",
)

analysis_progress: Dict[str, Dict[str, Any]] = {}
# Upload analysis runs in worker threads. This lock therefore remains a short
# threading lock; no await or external I/O occurs while it is held.
analysis_progress_lock = Lock()

# Keep old synchronous injection points usable for integrations during the
# migration. Normal runtime requests always take the native async branch.
_DEFAULT_SYNC_AGENT_CHAT = agent_chat
_DEFAULT_SYNC_SUPERVISOR_CHAT = supervisor_chat


async def _call_agent_chat(query: str, **kwargs: Any) -> Any:
    if agent_chat is not _DEFAULT_SYNC_AGENT_CHAT:
        return await asyncio.to_thread(agent_chat, query, **kwargs)
    return await agent_chat_async(query, **kwargs)


async def _call_supervisor_chat(query: str, **kwargs: Any) -> Any:
    if supervisor_chat is not _DEFAULT_SYNC_SUPERVISOR_CHAT:
        return await asyncio.to_thread(supervisor_chat, query, **kwargs)
    return await supervisor_chat_async(query, **kwargs)


def _flask_debug_enabled() -> bool:
    """Backward-compatible flag used as the uvicorn reload fallback."""

    return read_binary_env_flag("FLASK_DEBUG", default=False)


def _uvicorn_reload_enabled() -> bool:
    if os.getenv("UVICORN_RELOAD") is not None:
        return read_binary_env_flag("UVICORN_RELOAD", default=False)
    return _flask_debug_enabled()


def _chat_request_timeout() -> float:
    raw = os.getenv("CHAT_REQUEST_TIMEOUT", "").strip()
    if not raw:
        return DEFAULT_CHAT_REQUEST_TIMEOUT
    try:
        value = float(raw)
    except ValueError as exc:
        raise ValueError("CHAT_REQUEST_TIMEOUT must be a positive number") from exc
    if value <= 0:
        raise ValueError("CHAT_REQUEST_TIMEOUT must be a positive number")
    return value


def _normalize_chat_history(value: Any) -> List[Dict[str, str]]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise ValueError("history must be an array")

    normalized: List[Dict[str, str]] = []
    for index, item in enumerate(value):
        if not isinstance(item, dict):
            raise ValueError(f"history[{index}] must be an object")
        role = item.get("role")
        content = item.get("content")
        if role not in {"user", "assistant"}:
            raise ValueError(
                f"history[{index}].role must be 'user' or 'assistant'"
            )
        if not isinstance(content, str):
            raise ValueError(f"history[{index}].content must be a string")
        text = content.strip()
        if not text:
            continue
        normalized.append(
            {
                "role": role,
                "content": text[:CHAT_HISTORY_MAX_MESSAGE_CHARS],
            }
        )

    normalized = normalized[-CHAT_HISTORY_MAX_MESSAGES:]
    while (
        len(normalized) > 1
        and sum(len(item["content"]) for item in normalized)
        > CHAT_HISTORY_MAX_TOTAL_CHARS
    ):
        normalized.pop(0)
    if normalized:
        normalized[-1]["content"] = normalized[-1]["content"][
            :CHAT_HISTORY_MAX_TOTAL_CHARS
        ]
    return normalized


def _normalize_chat_session_id(value: Any) -> Optional[str]:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError("session_id must be a string")
    text = value.strip()
    if not text:
        return None
    return text[:CHAT_SESSION_ID_MAX_CHARS]


class ChatMessage(BaseModel):
    model_config = ConfigDict(extra="ignore")

    role: Literal["user", "assistant"]
    content: str


class ChatRequest(BaseModel):
    model_config = ConfigDict(extra="ignore")

    query: str
    history: List[ChatMessage] = Field(default_factory=list)
    session_id: Optional[str] = None

    @field_validator("query", mode="before")
    @classmethod
    def validate_query(cls, value: Any) -> str:
        if not isinstance(value, str) or not value.strip():
            raise ValueError("Missing query")
        return value.strip()

    @field_validator("history", mode="before")
    @classmethod
    def validate_history(cls, value: Any) -> List[Dict[str, str]]:
        return _normalize_chat_history(value)

    @field_validator("session_id", mode="before")
    @classmethod
    def validate_session_id(cls, value: Any) -> Optional[str]:
        return _normalize_chat_session_id(value)


class ChatResponse(BaseModel):
    answer: str
    display_items: List[Dict[str, Any]] = Field(default_factory=list)


def _validation_message(error: ValidationError) -> str:
    first = error.errors(include_url=False)[0]
    context_error = (first.get("ctx") or {}).get("error")
    if context_error is not None:
        return str(context_error)
    return str(first.get("msg") or "Invalid request")


def _error(message: str, status_code: int) -> JSONResponse:
    return JSONResponse({"error": message}, status_code=status_code)


def _progress_now() -> str:
    return datetime.datetime.now().isoformat()


def _set_analysis_progress(upload_id: Optional[str], **updates: Any) -> None:
    if not upload_id:
        return
    payload = convert_to_serializable(updates)
    with analysis_progress_lock:
        current = analysis_progress.setdefault(
            upload_id,
            {
                "upload_id": upload_id,
                "status": "running",
                "phase": "queued",
                "percent": 0,
                "message": "Ожидаю начала анализа...",
                "history": [],
            },
        )
        current.update(payload)
        current["updated_at"] = _progress_now()
        event = {
            "timestamp": current["updated_at"],
            **{field: current.get(field) for field in PROGRESS_EVENT_FIELDS},
        }
        current.setdefault("history", []).append(event)
        current["history"] = current["history"][-40:]


def _get_analysis_progress(upload_id: str) -> Optional[Dict[str, Any]]:
    with analysis_progress_lock:
        progress = analysis_progress.get(upload_id)
        if progress is None:
            return None
        return json.loads(
            json.dumps(convert_to_serializable(progress), ensure_ascii=False)
        )


def _process_upload(
    file_bytes: bytes,
    filename: str,
    upload_id: Optional[str],
    include_hidden_rows: bool,
) -> Dict[str, Any]:
    _set_analysis_progress(
        upload_id,
        status="running",
        phase="parse",
        percent=5,
        message="Начинаю анализ Excel...",
        detail=filename,
    )
    sheets = parse_excel_with_decisions(
        file_bytes,
        progress_callback=lambda update: _set_analysis_progress(
            upload_id,
            **update,
        ),
        include_hidden_rows=include_hidden_rows,
    )
    _set_analysis_progress(
        upload_id,
        status="running",
        phase="store",
        percent=60,
        message="Сохраняю структуру и данные в SQLite...",
        detail=filename,
    )
    file_id = store_excel_data(filename, get_model_name(), sheets)
    return finish_analysis(
        file_id,
        filename,
        sheets,
        progress_callback=lambda update: _set_analysis_progress(
            upload_id,
            **update,
        ),
    )


def _delete_all_storage_sync() -> Dict[str, Any]:
    sqlite_deleted, graph_clear_snapshot = clear_all_data_with_graph_snapshot()
    with analysis_progress_lock:
        progress_entries = len(analysis_progress)
        analysis_progress.clear()

    warnings: List[Dict[str, str]] = []
    try:
        graph_deleted = clear_graph_projection(
            generation=int(graph_clear_snapshot["generation"]),
            requests=graph_clear_snapshot["requests"],
        )
        if not graph_deleted.get("skipped"):
            mark_graph_syncs_applied(graph_clear_snapshot["requests"])
    except Exception as exc:
        logger.warning("SQLite cleared, but Neo4j cleanup failed: %s", exc)
        graph_deleted = {"nodes": 0, "skipped": True, "error": str(exc)}
        warnings.append({"storage": "neo4j", "error": str(exc)})

    return {
        "status": "partial" if warnings else "ok",
        "sqlite_deleted": sqlite_deleted,
        "neo4j_deleted": graph_deleted,
        "memory_deleted": {"progress_entries": progress_entries},
        "warnings": warnings,
    }


def _safe_export_path(directory: Path, filename: str) -> Optional[Path]:
    base = directory.resolve()
    candidate = (base / filename).resolve()
    if candidate.parent != base or not candidate.is_file():
        return None
    return candidate


@asynccontextmanager
async def lifespan(_: FastAPI):
    recovery_report, recovery_error = await asyncio.to_thread(
        try_sync_pending_graph_projections
    )
    if recovery_error:
        logger.warning("Graph outbox recovery incomplete: %s", recovery_error)
    elif recovery_report and recovery_report.get("pending"):
        logger.info("Graph outbox recovery: %s", recovery_report)
    yield


app = FastAPI(title="ETL S2T Agent", lifespan=lifespan)
# Transitional compatibility for existing runners while the public server is
# ASGI. Runtime handlers read only CHAT_AGENT_MODE from this mapping.
app.config = {
    "MAX_CONTENT_LENGTH": MAX_UPLOAD_SIZE,
    "CHAT_AGENT_MODE": os.getenv("CHAT_AGENT_MODE", "multiagent"),
    "TESTING": False,
}
templates = Jinja2Templates(directory=str(PROJECT_ROOT / "templates"))

# Schema initialization is a process-start boundary, not request-time I/O.
init_db()


@app.get("/", response_class=HTMLResponse)
@app.get("/chat_app", response_class=HTMLResponse)
async def chat_app(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(
        request=request,
        name="chat_app.html",
        context={},
    )


@app.get("/analysis_progress/{upload_id}")
async def get_analysis_progress(upload_id: str) -> Any:
    progress = _get_analysis_progress(upload_id)
    if progress is None:
        return _error("Progress not found", 404)
    return progress


@app.post("/upload")
async def upload_file(request: Request) -> Any:
    try:
        form = await request.form()
    except Exception:
        return _error("Invalid multipart form", 400)
    file = form.get("file")
    if not isinstance(file, UploadFile):
        return _error("No file part", 400)
    filename = str(file.filename or "")
    if not filename:
        return _error("No selected file", 400)
    if not allowed_file(filename):
        return _error("File type not allowed", 400)

    upload_id_value = form.get("upload_id")
    upload_id = str(upload_id_value) if upload_id_value is not None else None
    include_hidden_rows = str(form.get("include_hidden_rows", "")).strip().casefold() in {
        "1",
        "true",
        "yes",
        "on",
    }
    _set_analysis_progress(
        upload_id,
        status="running",
        phase="received",
        percent=2,
        message="Файл получен сервером...",
        detail=filename,
    )

    file_bytes = await file.read(MAX_UPLOAD_SIZE + 1)
    await file.close()
    if len(file_bytes) > MAX_UPLOAD_SIZE:
        _set_analysis_progress(
            upload_id,
            status="error",
            phase="validate",
            percent=100,
            message="Файл превышает допустимый размер",
            detail=filename,
        )
        return _error("File too large", 413)
    if not file_bytes:
        _set_analysis_progress(
            upload_id,
            status="error",
            phase="validate",
            percent=100,
            message="Файл пустой",
            detail=filename,
        )
        return _error("Empty file", 400)

    try:
        return await asyncio.to_thread(
            _process_upload,
            file_bytes,
            filename,
            upload_id,
            include_hidden_rows,
        )
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        logger.exception("Error parsing Excel file")
        _set_analysis_progress(
            upload_id,
            status="error",
            phase="error",
            percent=100,
            message="Ошибка анализа файла",
            detail=str(exc),
        )
        return _error(f"Failed to parse Excel file: {exc}", 400)


@app.get("/summary/{file_id}")
async def get_summary(file_id: int) -> Any:
    file_record = await asyncio.to_thread(get_file, file_id)
    if file_record and file_record["summary"]:
        return {"file_id": file_id, "summary": file_record["summary"]}
    summary, summary_error = await asyncio.to_thread(try_generate_summary, file_id)
    return {"file_id": file_id, "summary": summary, "summary_error": summary_error}


@app.get("/description/{file_id}")
async def get_description(file_id: int, refresh: bool = False) -> Any:
    file_record = await asyncio.to_thread(get_file, file_id)
    if file_record and file_record["description"] and not refresh:
        return {"file_id": file_id, "description": file_record["description"]}
    description, description_error = await asyncio.to_thread(
        try_generate_description,
        file_id,
        refresh=refresh,
    )
    return {
        "file_id": file_id,
        "description": description,
        "description_error": description_error,
    }


async def _get_transformations_response(
    request: Request,
    file_id: Optional[int] = None,
) -> Any:
    try:
        full = request.query_params.get("full", "false").lower() == "true"
        raw_limit = request.query_params.get("limit", "200")
        limit = None if full else int(raw_limit)
        query = request.query_params.get("q", "").strip()
        columns = None
        if full:
            columns = [
                "file_id",
                "sheet_name",
                "row_num",
                "target_layer",
                "target_table",
                "target_field",
                "source_layer",
                "source_table",
                "source_field",
                "transformation_rule",
            ]
        return await asyncio.to_thread(
            list_s2t_transformations,
            file_id,
            limit=limit,
            q=query or None,
            columns=columns,
        )
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        logger.exception("Failed to load S2T transformations")
        return _error(str(exc), 500)


@app.get("/transformations")
async def get_all_transformations(request: Request) -> Any:
    return await _get_transformations_response(request)


@app.get("/transformations/{file_id}")
async def get_transformations(file_id: int, request: Request) -> Any:
    return await _get_transformations_response(request, file_id)


@app.delete("/transformations/{file_id}")
async def delete_transformations(file_id: int) -> Any:
    try:
        deleted = await asyncio.to_thread(clear_s2t_transformations, file_id)
        graph_sync_report, graph_sync_error = await asyncio.to_thread(
            try_sync_file_graph,
            file_id,
        )
        graph_sync_state = await asyncio.to_thread(get_graph_sync_state, file_id)
        return {
            "status": "partial" if graph_sync_error else "ok",
            "file_id": file_id,
            "deleted": deleted,
            "graph_sync_report": graph_sync_report,
            "graph_sync_error": graph_sync_error,
            "graph_sync_state": graph_sync_state,
        }
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        logger.exception("Failed to clear S2T transformations")
        return _error(str(exc), 500)


@app.delete("/storage")
async def delete_all_storage() -> Any:
    try:
        return await asyncio.to_thread(_delete_all_storage_sync)
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        logger.exception("Failed to clear SQLite application storage")
        return _error(str(exc), 500)


@app.get("/sheet_groups/{file_id}/classify")
async def classify_sheet_groups_route(file_id: int, request: Request) -> Any:
    try:
        use_llm = request.query_params.get("llm", "0").lower() in {
            "1",
            "true",
            "yes",
            "y",
        }
        return await asyncio.to_thread(
            classify_file_sheet_groups,
            file_id,
            use_llm=use_llm,
            persist_aliases=False,
        )
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        logger.exception("Failed to classify sheet groups")
        return _error(str(exc), 500)


@app.get("/exports/sql/{filename:path}")
async def download_sql_export(filename: str) -> Any:
    from agents.tools import SQL_EXPORT_DIR

    path = _safe_export_path(Path(SQL_EXPORT_DIR), filename)
    if path is None:
        return _error("Export not found", 404)
    return FileResponse(path, filename=path.name)


@app.get("/exports/sql-lineage/{filename:path}")
async def show_sql_lineage_export(filename: str) -> Any:
    from agents.tools.sql_lineage import SQL_LINEAGE_EXPORT_DIR

    path = _safe_export_path(Path(SQL_LINEAGE_EXPORT_DIR), filename)
    if path is None:
        return _error("Export not found", 404)
    return FileResponse(path, media_type="text/html")


@app.get("/exports/s2t-graphs/{filename:path}")
async def show_s2t_table_graph_export(filename: str) -> Any:
    from agents.tools.s2t_graph import S2T_TABLE_GRAPH_EXPORT_DIR

    path = _safe_export_path(Path(S2T_TABLE_GRAPH_EXPORT_DIR), filename)
    if path is None:
        return _error("Export not found", 404)
    return FileResponse(path)


@app.post("/chat", response_model=ChatResponse)
async def chat(request: Request) -> Any:
    try:
        data = await request.json()
    except Exception:
        data = {}
    if not isinstance(data, dict):
        return _error("JSON body must be an object", 400)
    try:
        payload = ChatRequest.model_validate(data)
    except ValidationError as exc:
        return _error(_validation_message(exc), 400)

    logger.info(
        "Chat request session_id=%s history_messages=%s query=%s",
        payload.session_id,
        len(payload.history),
        payload.query[:1000],
    )
    kwargs: Dict[str, Any] = {}
    if payload.history:
        kwargs["history"] = [item.model_dump() for item in payload.history]
    if payload.session_id:
        kwargs["session_id"] = payload.session_id

    try:
        async with asyncio.timeout(_chat_request_timeout()):
            agent_mode = str(
                app.config.get("CHAT_AGENT_MODE") or "multiagent"
            ).strip().lower()
            if agent_mode == "multiagent":
                chat_result = await _call_supervisor_chat(payload.query, **kwargs)
            elif agent_mode == "single_agent":
                chat_result = await _call_agent_chat(payload.query, **kwargs)
            else:
                raise ValueError(
                    "CHAT_AGENT_MODE must be 'multiagent' or 'single_agent'"
                )
        if hasattr(chat_result, "model_dump"):
            response_payload = chat_result.model_dump()
        elif isinstance(chat_result, dict):
            response_payload = chat_result
        else:
            response_payload = {"answer": str(chat_result), "display_items": []}
        return ChatResponse.model_validate(response_payload).model_dump()
    except asyncio.TimeoutError:
        logger.warning("Chat request timed out")
        return _error("Chat request timed out", 504)
    except asyncio.CancelledError:
        logger.info("Chat request cancelled by client")
        raise
    except ValueError as exc:
        return _error(str(exc), 400)
    except Exception as exc:
        logger.exception("Chat agent failed")
        return _error(str(exc), 500)


if __name__ == "__main__":
    uvicorn.run(
        "app:app",
        host=os.getenv("UVICORN_HOST", "0.0.0.0"),
        port=int(os.getenv("UVICORN_PORT", "8000")),
        reload=_uvicorn_reload_enabled(),
    )
