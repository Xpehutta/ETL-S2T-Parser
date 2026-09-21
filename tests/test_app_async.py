from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock

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
