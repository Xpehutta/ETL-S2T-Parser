"""Shared async runtime primitives for agent and ASGI execution."""

from __future__ import annotations

import asyncio
import inspect
import logging
import math
import os
from collections.abc import AsyncIterator, Awaitable
from contextlib import asynccontextmanager
from contextvars import ContextVar
from threading import Lock
from typing import Any, Callable, Optional, TypeVar
from weakref import WeakKeyDictionary

logger = logging.getLogger(__name__)

T = TypeVar("T")

DEFAULT_LLM_MAX_CONCURRENCY = 8
DEFAULT_TOOL_MAX_CONCURRENCY = 16
DEFAULT_WORKER_MAX_CONCURRENCY = 4
DEFAULT_SERVER_WORKER_MAX_CONCURRENCY = 16
DEFAULT_LLM_TIMEOUT_SECONDS = 120.0
DEFAULT_TOOL_TIMEOUT_SECONDS = 120.0

_SEMAPHORES_LOCK = Lock()
_SEMAPHORES: WeakKeyDictionary[
    asyncio.AbstractEventLoop,
    dict[str, asyncio.Semaphore],
] = WeakKeyDictionary()


class OffloadedJobRegistry:
    """Track sync compatibility work that can outlive task cancellation."""

    def __init__(self) -> None:
        self._jobs: set[asyncio.Task[Any]] = set()

    def register(self, job: asyncio.Task[Any]) -> None:
        self._jobs.add(job)
        job.add_done_callback(self._jobs.discard)

    @property
    def pending_count(self) -> int:
        return sum(not job.done() for job in self._jobs)

    async def wait(self) -> None:
        while pending := [job for job in self._jobs if not job.done()]:
            await asyncio.gather(
                *(asyncio.shield(job) for job in pending),
                return_exceptions=True,
            )


_ACTIVE_OFFLOADED_JOBS: ContextVar[Optional[OffloadedJobRegistry]] = (
    ContextVar("active_offloaded_jobs", default=None)
)


def _positive_int_env(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be a positive integer") from exc
    if value < 1:
        raise ValueError(f"{name} must be a positive integer")
    return value


def _positive_float_env(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    try:
        value = float(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be a positive number") from exc
    if not math.isfinite(value) or value <= 0:
        raise ValueError(f"{name} must be a positive number")
    return value


def worker_max_concurrency() -> int:
    """Return the bounded number of concurrently runnable DAG workers."""

    return _positive_int_env(
        "WORKER_MAX_CONCURRENCY",
        DEFAULT_WORKER_MAX_CONCURRENCY,
    )


def server_worker_max_concurrency() -> int:
    """Return the server-wide worker limit shared by concurrent requests."""

    return _positive_int_env(
        "SERVER_WORKER_MAX_CONCURRENCY",
        DEFAULT_SERVER_WORKER_MAX_CONCURRENCY,
    )


def _sync_fallback_enabled() -> bool:
    raw = os.getenv("ASYNC_COMPAT_SYNC_FALLBACK", "0").strip()
    if raw not in {"0", "1"}:
        raise ValueError("ASYNC_COMPAT_SYNC_FALLBACK must be 0 or 1")
    return raw == "1"


def _semaphore(category: str) -> asyncio.Semaphore:
    loop = asyncio.get_running_loop()
    if category == "llm":
        limit = _positive_int_env(
            "LLM_MAX_CONCURRENCY",
            DEFAULT_LLM_MAX_CONCURRENCY,
        )
    elif category == "tool":
        limit = _positive_int_env(
            "TOOL_MAX_CONCURRENCY",
            DEFAULT_TOOL_MAX_CONCURRENCY,
        )
    elif category == "worker":
        limit = server_worker_max_concurrency()
    else:
        raise ValueError(f"Unknown async concurrency category: {category}")
    key = f"{category}:{limit}"
    with _SEMAPHORES_LOCK:
        per_loop = _SEMAPHORES.setdefault(loop, {})
        return per_loop.setdefault(key, asyncio.Semaphore(limit))


@asynccontextmanager
async def concurrency_slot(category: str) -> AsyncIterator[None]:
    """Bound one LLM or tool operation without sharing locks across loops."""

    semaphore = _semaphore(category)
    async with semaphore:
        yield


@asynccontextmanager
async def offloaded_job_scope() -> AsyncIterator[OffloadedJobRegistry]:
    """Wait for all registered sync jobs before run-scoped resources close."""
    existing = _ACTIVE_OFFLOADED_JOBS.get()
    if existing is not None:
        yield existing
        return
    registry = OffloadedJobRegistry()
    token = _ACTIVE_OFFLOADED_JOBS.set(registry)
    try:
        yield registry
    finally:
        await registry.wait()
        _ACTIVE_OFFLOADED_JOBS.reset(token)


def active_offloaded_job_count() -> int:
    registry = _ACTIVE_OFFLOADED_JOBS.get()
    return registry.pending_count if registry is not None else 0


async def run_sync_compat(
    function: Callable[..., T],
    /,
    *args: Any,
    **kwargs: Any,
) -> T:
    """Offload sync work and delay cancellation until the thread has ended."""
    job = asyncio.create_task(asyncio.to_thread(function, *args, **kwargs))
    registry = _ACTIVE_OFFLOADED_JOBS.get()
    if registry is not None:
        registry.register(job)
    try:
        return await asyncio.shield(job)
    except asyncio.CancelledError as cancellation:
        while not job.done():
            try:
                await asyncio.shield(job)
            except asyncio.CancelledError:
                continue
            except BaseException:
                break
        if job.done() and not job.cancelled():
            try:
                job.result()
            except BaseException:
                pass
        raise cancellation


async def ainvoke_compat(
    runnable: Any,
    value: Any,
    *,
    config: Any = None,
    category: str = "llm",
    timeout: float | None = None,
    **kwargs: Any,
) -> Any:
    """Use native ``ainvoke`` and offload only sync-only compatibility fakes.

    Production LangChain models and tool nodes expose ``ainvoke``. The thread
    fallback is intentionally narrow so existing deterministic test doubles and
    third-party sync-only runnables keep working during the migration.
    """

    if timeout is None:
        timeout = _positive_float_env(
            "LLM_TIMEOUT" if category == "llm" else "TOOL_TIMEOUT",
            (
                DEFAULT_LLM_TIMEOUT_SECONDS
                if category == "llm"
                else DEFAULT_TOOL_TIMEOUT_SECONDS
            ),
        )

    async def call() -> Any:
        async_method = getattr(runnable, "ainvoke", None)
        if callable(async_method):
            result = (
                async_method(value, **kwargs)
                if config is None
                else async_method(value, config=config, **kwargs)
            )
            return await result if inspect.isawaitable(result) else result

        sync_method = getattr(runnable, "invoke", None)
        if not callable(sync_method):
            raise TypeError(
                f"{type(runnable).__name__} exposes neither ainvoke nor invoke"
            )
        if not _sync_fallback_enabled():
            raise RuntimeError(
                "sync fallback is disabled; set "
                "ASYNC_COMPAT_SYNC_FALLBACK=1 only for compatibility"
            )
        logger.debug(
            "Using thread fallback for sync-only runnable %s",
            type(runnable).__name__,
        )
        if config is None:
            return await run_sync_compat(sync_method, value, **kwargs)
        return await run_sync_compat(
            sync_method,
            value,
            config=config,
            **kwargs,
        )

    async with concurrency_slot(category):
        try:
            async with asyncio.timeout(timeout):
                return await call()
        except asyncio.CancelledError:
            raise


async def ainvoke_graph_compat(
    graph: Any,
    value: Any,
    *,
    config: Any = None,
) -> Any:
    """Run a compiled graph asynchronously with support for sync-only fakes."""

    async_method = getattr(graph, "ainvoke", None)
    if callable(async_method):
        result = (
            async_method(value)
            if config is None
            else async_method(value, config=config)
        )
        return await result if inspect.isawaitable(result) else result

    sync_method = getattr(graph, "invoke", None)
    if callable(sync_method):
        if not _sync_fallback_enabled():
            raise RuntimeError(
                "sync fallback is disabled; set "
                "ASYNC_COMPAT_SYNC_FALLBACK=1 only for compatibility"
            )
        if config is None:
            return await run_sync_compat(sync_method, value)
        return await run_sync_compat(sync_method, value, config=config)
    raise TypeError(f"{type(graph).__name__} exposes neither ainvoke nor invoke")


def run_coroutine_sync(awaitable: Awaitable[T]) -> T:
    """Run an async compatibility API only when no event loop is active."""

    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(awaitable)

    close = getattr(awaitable, "close", None)
    if callable(close):
        close()
    raise RuntimeError(
        "Sync compatibility API cannot run inside an event loop; "
        "use the corresponding *_async function."
    )


__all__ = [
    "DEFAULT_LLM_MAX_CONCURRENCY",
    "DEFAULT_TOOL_MAX_CONCURRENCY",
    "DEFAULT_WORKER_MAX_CONCURRENCY",
    "DEFAULT_SERVER_WORKER_MAX_CONCURRENCY",
    "OffloadedJobRegistry",
    "active_offloaded_job_count",
    "ainvoke_graph_compat",
    "ainvoke_compat",
    "concurrency_slot",
    "offloaded_job_scope",
    "run_sync_compat",
    "run_coroutine_sync",
    "server_worker_max_concurrency",
    "worker_max_concurrency",
]
