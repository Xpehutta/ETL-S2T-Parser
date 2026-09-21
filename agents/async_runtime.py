"""Shared async runtime primitives for agent and ASGI execution."""

from __future__ import annotations

import asyncio
import inspect
import logging
import math
import os
from collections.abc import AsyncIterator, Awaitable
from contextlib import asynccontextmanager
from threading import Lock
from typing import Any, TypeVar
from weakref import WeakKeyDictionary

logger = logging.getLogger(__name__)

T = TypeVar("T")

DEFAULT_LLM_MAX_CONCURRENCY = 8
DEFAULT_TOOL_MAX_CONCURRENCY = 16
DEFAULT_WORKER_MAX_CONCURRENCY = 4
DEFAULT_LLM_TIMEOUT_SECONDS = 120.0
DEFAULT_TOOL_TIMEOUT_SECONDS = 120.0

_SEMAPHORES_LOCK = Lock()
_SEMAPHORES: WeakKeyDictionary[
    asyncio.AbstractEventLoop,
    dict[str, asyncio.Semaphore],
] = WeakKeyDictionary()


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
            if config is None:
                return await async_method(value, **kwargs)
            return await async_method(value, config=config, **kwargs)

        sync_method = getattr(runnable, "invoke", None)
        if not callable(sync_method):
            raise TypeError(
                f"{type(runnable).__name__} exposes neither ainvoke nor invoke"
            )
        logger.debug(
            "Using thread fallback for sync-only runnable %s",
            type(runnable).__name__,
        )
        if config is None:
            return await asyncio.to_thread(sync_method, value, **kwargs)
        return await asyncio.to_thread(
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
    if callable(async_method) and inspect.iscoroutinefunction(async_method):
        if config is None:
            return await async_method(value)
        return await async_method(value, config=config)

    sync_method = getattr(graph, "invoke", None)
    if callable(sync_method):
        if config is None:
            return await asyncio.to_thread(sync_method, value)
        return await asyncio.to_thread(sync_method, value, config=config)

    if callable(async_method):
        result = (
            async_method(value)
            if config is None
            else async_method(value, config=config)
        )
        if inspect.isawaitable(result):
            return await result
        return result
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
    "ainvoke_graph_compat",
    "ainvoke_compat",
    "concurrency_slot",
    "run_coroutine_sync",
    "worker_max_concurrency",
]
