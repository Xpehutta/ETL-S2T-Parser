from __future__ import annotations

import asyncio
import threading
import time

import pytest

from agents.async_runtime import (
    DEFAULT_WORKER_MAX_CONCURRENCY,
    active_offloaded_job_count,
    ainvoke_compat,
    ainvoke_graph_compat,
    concurrency_slot,
    offloaded_job_scope,
    run_coroutine_sync,
    worker_max_concurrency,
)


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
async def test_sync_timeout_keeps_job_and_permit_until_thread_finishes(
    monkeypatch,
):
    monkeypatch.setenv("ASYNC_COMPAT_SYNC_FALLBACK", "1")
    monkeypatch.setenv("LLM_MAX_CONCURRENCY", "1")
    entered = threading.Event()
    release = threading.Event()
    native_calls = 0

    class SlowSyncRunnable:
        def invoke(self, _value):
            entered.set()
            release.wait(timeout=2)
            return "late"

    class NativeRunnable:
        async def ainvoke(self, value):
            nonlocal native_calls
            native_calls += 1
            return value

    async with offloaded_job_scope():
        timed = asyncio.create_task(
            ainvoke_compat(SlowSyncRunnable(), "first", timeout=0.01)
        )
        deadline = time.monotonic() + 1
        while not entered.is_set() and time.monotonic() < deadline:
            await asyncio.sleep(0)
        assert entered.is_set()

        await asyncio.sleep(0.03)
        assert not timed.done()
        assert active_offloaded_job_count() == 1

        second = asyncio.create_task(
            ainvoke_compat(NativeRunnable(), "second", timeout=1)
        )
        await asyncio.sleep(0)
        assert not second.done()
        assert native_calls == 0

        release.set()
        with pytest.raises(asyncio.TimeoutError):
            await timed
        assert await second == "second"
        assert active_offloaded_job_count() == 0


@pytest.mark.asyncio
async def test_sync_timeout_keeps_saved_result_store_open(monkeypatch):
    from agents.tools.saved_results import saved_result_store_scope

    monkeypatch.setenv("ASYNC_COMPAT_SYNC_FALLBACK", "1")
    entered = threading.Event()
    release = threading.Event()

    with saved_result_store_scope() as store:
        class StoreWriter:
            def invoke(self, _value):
                entered.set()
                release.wait(timeout=2)
                return store.save_payload(
                    source_tool="run_sql",
                    payload={"rows": [{"value": "late"}]},
                )

        async with offloaded_job_scope():
            timed = asyncio.create_task(
                ainvoke_compat(StoreWriter(), "value", timeout=0.01)
            )
            deadline = time.monotonic() + 1
            while not entered.is_set() and time.monotonic() < deadline:
                await asyncio.sleep(0)
            assert entered.is_set()
            await asyncio.sleep(0.03)
            assert not timed.done()
            assert store.path.exists()

            release.set()
            with pytest.raises(asyncio.TimeoutError):
                await timed
            assert len(store.descriptors()) == 1


@pytest.mark.asyncio
async def test_request_cancellation_drains_registered_sync_job():
    entered = threading.Event()
    release = threading.Event()

    def blocking_operation():
        entered.set()
        release.wait(timeout=2)
        return "done"

    from agents.async_runtime import run_sync_compat

    async with offloaded_job_scope():
        task = asyncio.create_task(run_sync_compat(blocking_operation))
        deadline = time.monotonic() + 1
        while not entered.is_set() and time.monotonic() < deadline:
            await asyncio.sleep(0)
        assert entered.is_set()

        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()
        assert active_offloaded_job_count() == 1

        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert active_offloaded_job_count() == 0


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


@pytest.mark.asyncio
async def test_ainvoke_compat_passes_config_and_kwargs_to_native_async():
    seen = {}

    class Runnable:
        async def ainvoke(self, value, **kwargs):
            seen.update(value=value, kwargs=kwargs)
            return "done"

    result = await ainvoke_compat(
        Runnable(),
        "value",
        config={"tags": ["async"]},
        marker=7,
    )

    assert result == "done"
    assert seen == {
        "value": "value",
        "kwargs": {"config": {"tags": ["async"]}, "marker": 7},
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("with_config", [False, True])
async def test_ainvoke_compat_offloads_sync_only_runnable(with_config):
    event_loop_thread = threading.get_ident()
    seen = {}

    class SyncRunnable:
        def invoke(self, value, **kwargs):
            seen.update(value=value, kwargs=kwargs, thread=threading.get_ident())
            return "sync-result"

    config = {"run_name": "compat"} if with_config else None
    result = await ainvoke_compat(
        SyncRunnable(),
        "value",
        config=config,
        marker=3,
    )

    assert result == "sync-result"
    assert seen["value"] == "value"
    assert seen["kwargs"] == (
        {"config": config, "marker": 3} if with_config else {"marker": 3}
    )
    assert seen["thread"] != event_loop_thread


@pytest.mark.asyncio
async def test_sync_fallback_must_be_explicitly_enabled(monkeypatch):
    class SyncRunnable:
        def invoke(self, value):
            return value

    monkeypatch.setenv("ASYNC_COMPAT_SYNC_FALLBACK", "0")

    with pytest.raises(RuntimeError, match="sync fallback is disabled"):
        await ainvoke_compat(SyncRunnable(), "value")


@pytest.mark.asyncio
async def test_ainvoke_compat_rejects_object_without_invoke_methods():
    with pytest.raises(TypeError, match="neither ainvoke nor invoke"):
        await ainvoke_compat(object(), "value")


@pytest.mark.asyncio
@pytest.mark.parametrize("value", ["invalid", "0", "-1", "nan", "inf"])
async def test_ainvoke_compat_rejects_invalid_timeout_env(monkeypatch, value):
    monkeypatch.setenv("TOOL_TIMEOUT", value)

    with pytest.raises(ValueError, match="positive number"):
        await ainvoke_compat(_NativeAsyncRunnable(), "value", category="tool")


@pytest.mark.asyncio
async def test_tool_concurrency_slot_uses_tool_limit(monkeypatch):
    monkeypatch.setenv("TOOL_MAX_CONCURRENCY", "1")
    first_entered = asyncio.Event()
    release_first = asyncio.Event()

    async def occupy_slot():
        async with concurrency_slot("tool"):
            first_entered.set()
            await release_first.wait()

    first = asyncio.create_task(occupy_slot())
    await asyncio.wait_for(first_entered.wait(), timeout=1)
    second = asyncio.create_task(
        ainvoke_compat(
            _NativeAsyncRunnable(),
            "second",
            category="tool",
        )
    )
    await asyncio.sleep(0)
    assert not second.done()

    release_first.set()
    await first
    assert await second == "async:second"


@pytest.mark.asyncio
async def test_concurrency_slot_rejects_unknown_category():
    with pytest.raises(ValueError, match="Unknown async concurrency category"):
        async with concurrency_slot("database"):
            pass


@pytest.mark.parametrize("value", ["invalid", "1.5"])
def test_worker_max_concurrency_rejects_non_integer(monkeypatch, value):
    monkeypatch.setenv("WORKER_MAX_CONCURRENCY", value)

    with pytest.raises(ValueError, match="positive integer"):
        worker_max_concurrency()


def test_worker_max_concurrency_uses_default_for_missing_or_blank(monkeypatch):
    monkeypatch.delenv("WORKER_MAX_CONCURRENCY", raising=False)
    assert worker_max_concurrency() == DEFAULT_WORKER_MAX_CONCURRENCY

    monkeypatch.setenv("WORKER_MAX_CONCURRENCY", "   ")
    assert worker_max_concurrency() == DEFAULT_WORKER_MAX_CONCURRENCY


class _NativeGraph:
    def __init__(self):
        self.seen = None

    async def ainvoke(self, value, **kwargs):
        self.seen = (value, kwargs)
        return "native-graph"


@pytest.mark.asyncio
@pytest.mark.parametrize("with_config", [False, True])
async def test_ainvoke_graph_compat_uses_native_async(with_config):
    graph = _NativeGraph()
    config = {"thread_id": "test"} if with_config else None

    result = await ainvoke_graph_compat(graph, "state", config=config)

    assert result == "native-graph"
    assert graph.seen == (
        "state",
        {"config": config} if with_config else {},
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("with_config", [False, True])
async def test_ainvoke_graph_compat_offloads_sync_graph(with_config):
    event_loop_thread = threading.get_ident()
    seen = {}

    class SyncGraph:
        def invoke(self, value, **kwargs):
            seen.update(value=value, kwargs=kwargs, thread=threading.get_ident())
            return "sync-graph"

    config = {"thread_id": "test"} if with_config else None
    result = await ainvoke_graph_compat(SyncGraph(), "state", config=config)

    assert result == "sync-graph"
    assert seen["kwargs"] == ({"config": config} if with_config else {})
    assert seen["thread"] != event_loop_thread


@pytest.mark.asyncio
@pytest.mark.parametrize("returns_awaitable", [False, True])
async def test_ainvoke_graph_compat_supports_wrapped_async_method(
    returns_awaitable,
):
    class WrappedGraph:
        def ainvoke(self, value, **kwargs):
            result = f"wrapped:{value}:{kwargs.get('config', 'none')}"

            async def deferred():
                return result

            return deferred() if returns_awaitable else result

    result = await ainvoke_graph_compat(
        WrappedGraph(),
        "state",
        config="config",
    )

    assert result == "wrapped:state:config"


@pytest.mark.asyncio
async def test_graph_prefers_callable_ainvoke_before_sync_invoke():
    class WrappedGraph:
        def ainvoke(self, value, **kwargs):
            async def deferred():
                return f"async:{value}:{kwargs.get('config')}"

            return deferred()

        def invoke(self, value, **kwargs):
            raise AssertionError(f"sync invoke used for {value}: {kwargs}")

    result = await ainvoke_graph_compat(
        WrappedGraph(),
        "state",
        config="config",
    )

    assert result == "async:state:config"


@pytest.mark.asyncio
async def test_ainvoke_graph_compat_rejects_unknown_graph():
    with pytest.raises(TypeError, match="neither ainvoke nor invoke"):
        await ainvoke_graph_compat(object(), {})


def test_run_coroutine_sync_returns_result_without_running_loop():
    async def operation():
        return "done"

    assert run_coroutine_sync(operation()) == "done"


@pytest.mark.asyncio
async def test_run_coroutine_sync_rejects_active_event_loop_and_closes_coroutine():
    async def operation():
        return "never"

    awaitable = operation()

    with pytest.raises(RuntimeError, match="use the corresponding .*_async"):
        run_coroutine_sync(awaitable)

    assert awaitable.cr_frame is None
