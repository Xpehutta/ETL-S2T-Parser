from __future__ import annotations

import asyncio
import io
from unittest.mock import AsyncMock, Mock

import pytest


@pytest.mark.asyncio
async def test_chat_awaits_native_async_supervisor(async_client, monkeypatch):
    supervisor = AsyncMock(
        return_value={"answer": "async answer", "display_items": []}
    )
    monkeypatch.setattr("app.supervisor_chat_async", supervisor)

    response = await async_client.post("/chat", json={"query": "hello"})

    assert response.status_code == 200
    assert response.json() == {
        "answer": "async answer",
        "display_items": [],
    }
    supervisor.assert_awaited_once_with("hello")


@pytest.mark.asyncio
async def test_chat_requests_overlap_without_state_leak(async_client, monkeypatch):
    both_started = asyncio.Event()
    started: list[str] = []

    async def concurrent_supervisor(query: str, **_kwargs):
        started.append(query)
        if len(started) == 2:
            both_started.set()
        await asyncio.wait_for(both_started.wait(), timeout=1)
        return {"answer": query, "display_items": []}

    monkeypatch.setattr("app.supervisor_chat_async", concurrent_supervisor)

    first, second = await asyncio.gather(
        async_client.post("/chat", json={"query": "first"}),
        async_client.post("/chat", json={"query": "second"}),
    )

    assert started == ["first", "second"]
    assert first.json()["answer"] == "first"
    assert second.json()["answer"] == "second"


@pytest.mark.asyncio
async def test_chat_timeout_returns_504(async_client, monkeypatch):
    async def slow_supervisor(_query: str, **_kwargs):
        await asyncio.sleep(1)
        return {"answer": "late", "display_items": []}

    monkeypatch.setattr("app.supervisor_chat_async", slow_supervisor)
    monkeypatch.setenv("CHAT_REQUEST_TIMEOUT", "0.01")

    response = await async_client.post("/chat", json={"query": "slow"})

    assert response.status_code == 504
    assert response.json() == {"error": "Chat request timed out"}


@pytest.mark.asyncio
async def test_chat_client_cancellation_reaches_runtime(async_client, monkeypatch):
    entered = asyncio.Event()
    cancelled = asyncio.Event()

    async def cancellable_supervisor(_query: str, **_kwargs):
        entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()

    monkeypatch.setattr("app.supervisor_chat_async", cancellable_supervisor)
    request_task = asyncio.create_task(
        async_client.post("/chat", json={"query": "cancel"})
    )
    await asyncio.wait_for(entered.wait(), timeout=1)

    request_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await request_task

    await asyncio.wait_for(cancelled.wait(), timeout=1)


@pytest.mark.asyncio
async def test_upload_rejects_oversized_body_before_processing(
    async_client,
    monkeypatch,
):
    process_upload = AsyncMock()
    monkeypatch.setattr("app._process_upload", process_upload)
    payload = b"x" * (10 * 1024 * 1024 + 1)

    response = await async_client.post(
        "/upload",
        files={"file": ("large.xlsx", payload)},
    )

    assert response.status_code == 413
    assert response.json() == {"error": "File too large"}
    process_upload.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("value", ["invalid", "0", "-1", "nan", "inf"])
async def test_chat_rejects_invalid_timeout_configuration(
    async_client,
    monkeypatch,
    value,
):
    supervisor = AsyncMock()
    monkeypatch.setattr("app.supervisor_chat_async", supervisor)
    monkeypatch.setenv("CHAT_REQUEST_TIMEOUT", value)

    response = await async_client.post("/chat", json={"query": "hello"})

    assert response.status_code == 400
    assert "positive number" in response.json()["error"]
    supervisor.assert_not_awaited()


@pytest.mark.asyncio
async def test_chat_rejects_unknown_agent_mode(async_client, app):
    app.config["CHAT_AGENT_MODE"] = "unknown"

    response = await async_client.post("/chat", json={"query": "hello"})

    assert response.status_code == 400
    assert "CHAT_AGENT_MODE" in response.json()["error"]


@pytest.mark.asyncio
async def test_chat_maps_unexpected_agent_failure_to_500(async_client, monkeypatch):
    supervisor = AsyncMock(side_effect=RuntimeError("provider unavailable"))
    monkeypatch.setattr("app.supervisor_chat_async", supervisor)

    response = await async_client.post("/chat", json={"query": "hello"})

    assert response.status_code == 500
    assert response.json() == {"error": "provider unavailable"}


@pytest.mark.asyncio
async def test_chat_accepts_pydantic_agent_response(async_client, monkeypatch):
    from app import ChatResponse

    supervisor = AsyncMock(
        return_value=ChatResponse(
            answer="typed answer",
            display_items=[{"name": "rows"}],
        )
    )
    monkeypatch.setattr("app.supervisor_chat_async", supervisor)

    response = await async_client.post("/chat", json={"query": "hello"})

    assert response.status_code == 200
    assert response.json() == {
        "answer": "typed answer",
        "display_items": [{"name": "rows"}],
    }


@pytest.mark.asyncio
async def test_chat_uses_native_async_single_agent(async_client, app, monkeypatch):
    agent = AsyncMock(return_value="single answer")
    monkeypatch.setattr("app.agent_chat_async", agent)
    app.config["CHAT_AGENT_MODE"] = "single_agent"

    response = await async_client.post(
        "/chat",
        json={
            "query": "hello",
            "history": [{"role": "user", "content": "context"}],
            "session_id": "session-1",
        },
    )

    assert response.status_code == 200
    assert response.json()["answer"] == "single answer"
    agent.assert_awaited_once_with(
        "hello",
        history=[{"role": "user", "content": "context"}],
        session_id="session-1",
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("files", "expected_error"),
    [
        (None, "No file part"),
        ({"file": ("", b"value")}, "No file part"),
        ({"file": ("mapping.txt", b"value")}, "File type not allowed"),
        ({"file": ("mapping.xlsx", b"")}, "Empty file"),
    ],
)
async def test_upload_rejects_invalid_file_inputs(
    async_client,
    monkeypatch,
    files,
    expected_error,
):
    process_upload = Mock()
    monkeypatch.setattr("app._process_upload", process_upload)

    response = await async_client.post("/upload", files=files)

    assert response.status_code == 400
    assert response.json() == {"error": expected_error}
    process_upload.assert_not_called()


@pytest.mark.asyncio
async def test_upload_maps_processing_failure_and_records_progress(
    async_client,
    monkeypatch,
):
    import app as app_module

    def fail_processing(*_args):
        raise ValueError("broken workbook")

    monkeypatch.setattr(app_module, "_process_upload", fail_processing)

    response = await async_client.post(
        "/upload",
        files={"file": ("mapping.xlsx", b"not-empty")},
        data={"upload_id": "upload-error"},
    )

    assert response.status_code == 400
    assert response.json() == {
        "error": "Failed to parse Excel file: broken workbook"
    }
    progress = app_module._get_analysis_progress("upload-error")
    assert progress is not None
    assert {
        key: progress[key]
        for key in ("status", "phase", "percent", "message", "detail")
    } == {
        "status": "error",
        "phase": "error",
        "percent": 100,
        "message": "Ошибка анализа файла",
        "detail": "broken workbook",
    }


@pytest.mark.asyncio
async def test_upload_rejects_uploadfile_without_filename():
    from starlette.datastructures import UploadFile

    from app import upload_file

    class Request:
        async def form(self):
            return {"file": UploadFile(file=io.BytesIO(b"value"), filename="")}

    response = await upload_file(Request())

    assert response.status_code == 400
    assert response.body == b'{"error":"No selected file"}'


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("recovery_result", "recovery_error"),
    [({"pending": 2}, None), (None, "neo4j unavailable")],
)
async def test_lifespan_handles_graph_recovery_outcomes(
    monkeypatch,
    recovery_result,
    recovery_error,
):
    import app as app_module

    recovery = Mock(return_value=(recovery_result, recovery_error))
    monkeypatch.setattr(app_module, "try_sync_pending_graph_projections", recovery)

    async with app_module.lifespan(app_module.app):
        pass

    recovery.assert_called_once_with()


@pytest.mark.parametrize(
    ("reload_value", "debug_value", "expected"),
    [("1", "0", True), ("0", "1", False), (None, "1", True)],
)
def test_uvicorn_reload_prefers_explicit_setting(
    monkeypatch,
    reload_value,
    debug_value,
    expected,
):
    from app import _uvicorn_reload_enabled

    if reload_value is None:
        monkeypatch.delenv("UVICORN_RELOAD", raising=False)
    else:
        monkeypatch.setenv("UVICORN_RELOAD", reload_value)
    monkeypatch.setenv("FLASK_DEBUG", debug_value)

    assert _uvicorn_reload_enabled() is expected


def test_chat_normalizers_handle_optional_and_blank_values():
    from app import _normalize_chat_history, _normalize_chat_session_id

    assert _normalize_chat_history(None) == []
    assert _normalize_chat_history(
        [{"role": "user", "content": "   "}]
    ) == []
    assert _normalize_chat_session_id(None) is None
    assert _normalize_chat_session_id("   ") is None


@pytest.mark.asyncio
async def test_upload_rejects_malformed_multipart_form():
    from app import upload_file

    class Request:
        async def form(self):
            raise ValueError("malformed")

    response = await upload_file(Request())

    assert response.status_code == 400
    assert response.body == b'{"error":"Invalid multipart form"}'


@pytest.mark.asyncio
async def test_summary_uses_cache_or_generator(async_client, monkeypatch):
    import app as app_module

    monkeypatch.setattr(
        app_module,
        "get_file",
        lambda file_id: {"summary": "cached"} if file_id == 1 else None,
    )
    generate = Mock(return_value=("generated", None))
    monkeypatch.setattr(app_module, "try_generate_summary", generate)

    cached = await async_client.get("/summary/1")
    generated = await async_client.get("/summary/2")

    assert cached.json() == {"file_id": 1, "summary": "cached"}
    assert generated.json() == {
        "file_id": 2,
        "summary": "generated",
        "summary_error": None,
    }
    generate.assert_called_once_with(2)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("path", "patched_name"),
    [
        ("/transformations", "list_s2t_transformations"),
        ("/transformations/7", "list_s2t_transformations"),
        ("/transformations/7", "clear_s2t_transformations"),
        ("/storage", "_delete_all_storage_sync"),
        ("/sheet_groups/7/classify", "classify_file_sheet_groups"),
    ],
)
async def test_async_routes_map_background_failures_to_500(
    async_client,
    monkeypatch,
    path,
    patched_name,
):
    import app as app_module

    monkeypatch.setattr(
        app_module,
        patched_name,
        Mock(side_effect=RuntimeError("background failure")),
    )
    method = async_client.delete if patched_name in {
        "clear_s2t_transformations",
        "_delete_all_storage_sync",
    } else async_client.get

    response = await method(path)

    assert response.status_code == 500
    assert response.json() == {"error": "background failure"}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "path",
    [
        "/exports/sql/missing.csv",
        "/exports/sql-lineage/missing.html",
        "/exports/s2t-graphs/missing.html",
    ],
)
async def test_export_routes_return_404_for_missing_files(async_client, path):
    response = await async_client.get(path)

    assert response.status_code == 404
    assert response.json() == {"error": "Export not found"}


@pytest.mark.asyncio
async def test_chat_rejects_malformed_json(async_client):
    response = await async_client.post(
        "/chat",
        content=b"{",
        headers={"content-type": "application/json"},
    )

    assert response.status_code == 400
    assert response.json() == {"error": "Field required"}


@pytest.mark.asyncio
async def test_chat_rejects_blank_query(async_client):
    response = await async_client.post("/chat", json={"query": "   "})

    assert response.status_code == 400
    assert response.json() == {"error": "Missing query"}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("method", "path"),
    [
        ("get", "/transformations"),
        ("delete", "/transformations/7"),
        ("delete", "/storage"),
        ("get", "/sheet_groups/7/classify"),
    ],
)
async def test_background_routes_propagate_cancellation(
    async_client,
    monkeypatch,
    method,
    path,
):
    import app as app_module

    offload = AsyncMock(side_effect=asyncio.CancelledError)
    monkeypatch.setattr(app_module, "run_sync_compat", offload)

    with pytest.raises(asyncio.CancelledError):
        await getattr(async_client, method)(path)


@pytest.mark.asyncio
async def test_upload_propagates_background_cancellation(monkeypatch):
    import app as app_module
    from starlette.datastructures import UploadFile

    class Request:
        async def form(self):
            return {
                "file": UploadFile(
                    file=io.BytesIO(b"value"),
                    filename="mapping.xlsx",
                )
            }

    offload = AsyncMock(side_effect=asyncio.CancelledError)
    monkeypatch.setattr(app_module, "run_sync_compat", offload)

    with pytest.raises(asyncio.CancelledError):
        await app_module.upload_file(Request())
