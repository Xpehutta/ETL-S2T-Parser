from __future__ import annotations

import asyncio

import pytest

from agents.async_runtime import ainvoke_compat, worker_max_concurrency


class _NativeAsyncRunnable:
    def __init__(self):
        self.async_calls = 0

    async def ainvoke(self, value, **_kwargs):
        self.async_calls += 1
        return f"async:{value}"

    def invoke(self, _value, **_kwargs):
        raise AssertionError("sync invoke must not be used")


@pytest.mark.asyncio
async def test_ainvoke_compat_prefers_native_async():
    runnable = _NativeAsyncRunnable()

    result = await ainvoke_compat(runnable, "value")

    assert result == "async:value"
    assert runnable.async_calls == 1


@pytest.mark.asyncio
async def test_ainvoke_compat_enforces_timeout(monkeypatch):
    class SlowRunnable:
        async def ainvoke(self, _value, **_kwargs):
            await asyncio.sleep(1)

    monkeypatch.setenv("LLM_TIMEOUT", "0.01")

    with pytest.raises(asyncio.TimeoutError):
        await ainvoke_compat(SlowRunnable(), "value")


@pytest.mark.asyncio
async def test_ainvoke_compat_enforces_concurrency_limit(monkeypatch):
    monkeypatch.setenv("LLM_MAX_CONCURRENCY", "1")
    active = 0
    peak = 0
    first_entered = asyncio.Event()
    release_first = asyncio.Event()

    class ControlledRunnable:
        async def ainvoke(self, value, **_kwargs):
            nonlocal active, peak
            active += 1
            peak = max(peak, active)
            if value == "first":
                first_entered.set()
                await release_first.wait()
            active -= 1
            return value

    runnable = ControlledRunnable()
    first = asyncio.create_task(ainvoke_compat(runnable, "first"))
    await asyncio.wait_for(first_entered.wait(), timeout=1)
    second = asyncio.create_task(ainvoke_compat(runnable, "second"))
    await asyncio.sleep(0)
    assert not second.done()

    release_first.set()
    assert await asyncio.gather(first, second) == ["first", "second"]
    assert peak == 1


@pytest.mark.asyncio
async def test_ainvoke_compat_propagates_cancellation():
    entered = asyncio.Event()
    cancelled = asyncio.Event()

    class CancellableRunnable:
        async def ainvoke(self, _value, **_kwargs):
            entered.set()
            try:
                await asyncio.Event().wait()
            finally:
                cancelled.set()

    task = asyncio.create_task(ainvoke_compat(CancellableRunnable(), "value"))
    await asyncio.wait_for(entered.wait(), timeout=1)
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task
    await asyncio.wait_for(cancelled.wait(), timeout=1)


def test_worker_max_concurrency_is_strictly_positive(monkeypatch):
    monkeypatch.setenv("WORKER_MAX_CONCURRENCY", "3")
    assert worker_max_concurrency() == 3

    monkeypatch.setenv("WORKER_MAX_CONCURRENCY", "0")
    with pytest.raises(ValueError, match="positive integer"):
        worker_max_concurrency()
