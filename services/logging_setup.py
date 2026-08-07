"""Application logging with a UTF-8 rotating file."""

from __future__ import annotations

import logging
import os
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, Iterable, Optional, Union


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_LOG_FILE = PROJECT_ROOT / "logs" / "agent.log"
DEFAULT_LOG_MAX_BYTES = 5 * 1024 * 1024
DEFAULT_LOG_BACKUP_COUNT = 5
LOG_FORMAT = "%(asctime)s - %(levelname)s - %(name)s - %(message)s"


def _configure_utf8_console_streams(
    streams: Optional[Iterable[Any]] = None,
) -> None:
    """Make direct prints and console logging Unicode-safe on Windows."""
    selected_streams = streams if streams is not None else (sys.stdout, sys.stderr)
    for stream in selected_streams:
        reconfigure = getattr(stream, "reconfigure", None)
        if not callable(reconfigure):
            continue
        try:
            reconfigure(encoding="utf-8", errors="backslashreplace")
        except (OSError, ValueError):
            # Captured/closed streams may reject reconfiguration. File logging
            # remains UTF-8 and the caller can still use the existing stream.
            continue


def _positive_int(value: Optional[Union[str, int]], default: int, name: str) -> int:
    if value is None or str(value).strip() == "":
        return default
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be an integer, got {value!r}") from exc
    if parsed < 1:
        raise ValueError(f"{name} must be positive, got {parsed}")
    return parsed


def _log_level(value: Optional[Union[str, int]]) -> int:
    selected = value if value is not None else os.getenv("LOG_LEVEL", "INFO")
    if isinstance(selected, int):
        return selected
    name = str(selected).strip().upper()
    level = getattr(logging, name, None)
    if not isinstance(level, int):
        raise ValueError(f"Unknown LOG_LEVEL={selected!r}")
    return level


def _log_path(value: Optional[Union[str, Path]]) -> Path:
    selected = value if value is not None else os.getenv("LOG_FILE")
    path = Path(selected) if selected else DEFAULT_LOG_FILE
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    return path.resolve()


def configure_logging(
    log_file: Optional[Union[str, Path]] = None,
    *,
    level: Optional[Union[str, int]] = None,
    max_bytes: Optional[int] = None,
    backup_count: Optional[int] = None,
) -> Path:
    """Configure console logging and one idempotent rotating file handler."""
    _configure_utf8_console_streams()
    path = _log_path(log_file)
    path.parent.mkdir(parents=True, exist_ok=True)
    selected_level = _log_level(level)
    selected_max_bytes = _positive_int(
        max_bytes if max_bytes is not None else os.getenv("LOG_MAX_BYTES"),
        DEFAULT_LOG_MAX_BYTES,
        "LOG_MAX_BYTES",
    )
    selected_backup_count = _positive_int(
        (
            backup_count
            if backup_count is not None
            else os.getenv("LOG_BACKUP_COUNT")
        ),
        DEFAULT_LOG_BACKUP_COUNT,
        "LOG_BACKUP_COUNT",
    )

    root_logger = logging.getLogger()
    root_logger.setLevel(selected_level)
    formatter = logging.Formatter(LOG_FORMAT)

    if not root_logger.handlers:
        console = logging.StreamHandler()
        console.setLevel(selected_level)
        console.setFormatter(formatter)
        root_logger.addHandler(console)

    normalized_path = str(path)
    for handler in root_logger.handlers:
        if getattr(handler, "_etls2t_log_path", None) == normalized_path:
            handler.setLevel(selected_level)
            handler.setFormatter(formatter)
            return path

    file_handler = RotatingFileHandler(
        path,
        maxBytes=selected_max_bytes,
        backupCount=selected_backup_count,
        encoding="utf-8",
    )
    file_handler.setLevel(selected_level)
    file_handler.setFormatter(formatter)
    file_handler._etls2t_log_path = normalized_path
    root_logger.addHandler(file_handler)
    return path


__all__ = ["configure_logging", "DEFAULT_LOG_FILE"]
