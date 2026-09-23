"""Small psycopg adapter for the project's DB-API-shaped storage code.

The application historically used sqlite3 directly.  This module keeps the
existing repository surface (``?`` parameters and rows addressable by both
index and name) while the durable ETL store moves to PostgreSQL.  The
run-scoped saved-result store intentionally remains SQLite.
"""

from __future__ import annotations

import os
import re
from collections.abc import Iterator, Mapping, Sequence
from typing import Any, Optional, overload


DATABASE_URL_ENV = "DATABASE_URL"
POSTGRES_SCHEMA_ENV = "POSTGRES_SCHEMA"
POSTGRES_STATEMENT_TIMEOUT_MS_ENV = "POSTGRES_STATEMENT_TIMEOUT_MS"
DEFAULT_POSTGRES_SCHEMA = "public"
DEFAULT_STATEMENT_TIMEOUT_MS = 30_000
_POSTGRES_URL_RE = re.compile(r"^postgres(?:ql)?(?:\+psycopg)?://", re.IGNORECASE)
_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def configured_database_url() -> str:
    return str(os.getenv(DATABASE_URL_ENV) or "").strip()


def postgres_enabled(database_url: Optional[str] = None) -> bool:
    value = configured_database_url() if database_url is None else str(database_url)
    return bool(_POSTGRES_URL_RE.match(value.strip()))


def configured_postgres_schema(schema: Optional[str] = None) -> str:
    value = str(schema or os.getenv(POSTGRES_SCHEMA_ENV) or DEFAULT_POSTGRES_SCHEMA)
    value = value.strip()
    if not _IDENTIFIER_RE.fullmatch(value):
        raise ValueError(f"Invalid PostgreSQL schema identifier: {value!r}")
    return value


def configured_statement_timeout_ms() -> int:
    raw = str(
        os.getenv(
            POSTGRES_STATEMENT_TIMEOUT_MS_ENV,
            str(DEFAULT_STATEMENT_TIMEOUT_MS),
        )
    ).strip()
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(
            f"{POSTGRES_STATEMENT_TIMEOUT_MS_ENV} must be an integer"
        ) from exc
    if not 1 <= value <= 900_000:
        raise ValueError(
            f"{POSTGRES_STATEMENT_TIMEOUT_MS_ENV} must be between 1 and 900000"
        )
    return value


def _dollar_quote_tag(sql: str, start: int) -> Optional[str]:
    match = re.match(r"\$[A-Za-z_][A-Za-z0-9_]*\$|\$\$", sql[start:])
    return match.group(0) if match else None


def convert_qmark_placeholders(sql: str) -> str:
    """Convert SQLite qmarks to psycopg placeholders outside SQL literals."""

    result: list[str] = []
    index = 0
    state = "normal"
    dollar_tag: Optional[str] = None
    while index < len(sql):
        char = sql[index]
        next_char = sql[index + 1] if index + 1 < len(sql) else ""
        if state == "normal":
            tag = _dollar_quote_tag(sql, index) if char == "$" else None
            if tag:
                dollar_tag = tag
                result.append(tag)
                index += len(tag)
                state = "dollar"
                continue
            if char == "'":
                state = "single"
            elif char == '"':
                state = "double"
            elif char == "-" and next_char == "-":
                result.extend((char, next_char))
                index += 2
                state = "line_comment"
                continue
            elif char == "/" and next_char == "*":
                result.extend((char, next_char))
                index += 2
                state = "block_comment"
                continue
            elif char == "?":
                result.append("%s")
                index += 1
                continue
        elif state == "single":
            if char == "'" and next_char == "'":
                result.extend((char, next_char))
                index += 2
                continue
            if char == "'":
                state = "normal"
        elif state == "double":
            if char == '"' and next_char == '"':
                result.extend((char, next_char))
                index += 2
                continue
            if char == '"':
                state = "normal"
        elif state == "line_comment":
            if char in "\r\n":
                state = "normal"
        elif state == "block_comment":
            if char == "*" and next_char == "/":
                result.extend((char, next_char))
                index += 2
                state = "normal"
                continue
        elif state == "dollar" and dollar_tag and sql.startswith(dollar_tag, index):
            result.append(dollar_tag)
            index += len(dollar_tag)
            state = "normal"
            dollar_tag = None
            continue
        result.append(char)
        index += 1
    return "".join(result)


def postgres_sql(sql: str, *, convert_placeholders: bool = True) -> str:
    """Translate the small portable subset still emitted by repositories."""

    translated = re.sub(r"\bIFNULL\s*\(", "COALESCE(", sql, flags=re.IGNORECASE)
    translated = re.sub(r"\bINSTR\s*\(", "STRPOS(", translated, flags=re.IGNORECASE)
    return (
        convert_qmark_placeholders(translated)
        if convert_placeholders
        else translated
    )


class DatabaseRow(Sequence[Any], Mapping[str, Any]):
    """Row compatible with sqlite3.Row's index and name access patterns."""

    def __init__(self, names: Sequence[str], values: Sequence[Any]) -> None:
        self._names = tuple(str(name) for name in names)
        self._values = tuple(values)
        self._positions = {name: index for index, name in enumerate(self._names)}

    @overload
    def __getitem__(self, key: int) -> Any: ...

    @overload
    def __getitem__(self, key: slice) -> tuple[Any, ...]: ...

    @overload
    def __getitem__(self, key: str) -> Any: ...

    def __getitem__(self, key: int | slice | str) -> Any:
        if isinstance(key, str):
            return self._values[self._positions[key]]
        return self._values[key]

    def __iter__(self) -> Iterator[Any]:
        return iter(self._values)

    def __len__(self) -> int:
        return len(self._values)

    def keys(self) -> tuple[str, ...]:
        return self._names

    def values(self) -> tuple[Any, ...]:
        return self._values

    def items(self) -> Iterator[tuple[str, Any]]:
        return iter(zip(self._names, self._values))


class PostgresCursor:
    def __init__(self, cursor: Any) -> None:
        self._cursor = cursor

    @property
    def description(self) -> Any:
        return self._cursor.description

    @property
    def rowcount(self) -> int:
        return int(self._cursor.rowcount)

    @property
    def lastrowid(self) -> None:
        return None

    def _names(self) -> tuple[str, ...]:
        names: list[str] = []
        for column in self._cursor.description or ():
            name = getattr(column, "name", None)
            if name is None:
                name = column[0]
            names.append(str(name))
        return tuple(names)

    def _row(self, values: Any) -> Optional[DatabaseRow]:
        if values is None:
            return None
        return DatabaseRow(self._names(), values)

    def execute(self, query: str, params: Any = None) -> "PostgresCursor":
        if str(query).strip().upper() == "PRAGMA QUERY_ONLY = ON":
            return self
        self._cursor.execute(
            postgres_sql(str(query), convert_placeholders=params is not None),
            params,
        )
        return self

    def executemany(self, query: str, params_seq: Any) -> "PostgresCursor":
        self._cursor.executemany(postgres_sql(str(query)), params_seq)
        return self

    def fetchone(self) -> Optional[DatabaseRow]:
        return self._row(self._cursor.fetchone())

    def fetchall(self) -> list[DatabaseRow]:
        names = self._names()
        return [DatabaseRow(names, row) for row in self._cursor.fetchall()]

    def fetchmany(self, size: int = 1) -> list[DatabaseRow]:
        names = self._names()
        return [DatabaseRow(names, row) for row in self._cursor.fetchmany(size)]

    def __iter__(self) -> Iterator[DatabaseRow]:
        names = self._names()
        for row in self._cursor:
            yield DatabaseRow(names, row)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._cursor, name)


class PostgresConnection:
    backend_name = "postgresql"

    def __init__(self, connection: Any, *, schema: str) -> None:
        self._connection = connection
        self.schema = schema

    def cursor(self) -> PostgresCursor:
        return PostgresCursor(self._connection.cursor())

    def execute(self, query: str, params: Any = None) -> PostgresCursor:
        return self.cursor().execute(query, params)

    def executemany(self, query: str, params_seq: Any) -> PostgresCursor:
        return self.cursor().executemany(query, params_seq)

    def commit(self) -> None:
        self._connection.commit()

    def rollback(self) -> None:
        self._connection.rollback()

    def close(self) -> None:
        self._connection.close()

    def create_function(self, *_args: Any, **_kwargs: Any) -> None:
        # PostgreSQL equivalents are installed once by postgres_schema.py.
        return None

    def set_authorizer(self, *_args: Any, **_kwargs: Any) -> None:
        return None

    def set_read_only(self) -> None:
        cursor = self.cursor()
        cursor.execute("SET TRANSACTION READ ONLY")
        cursor.execute(
            "SELECT set_config('statement_timeout', ?, true)",
            (str(configured_statement_timeout_ms()),),
        )

    def __enter__(self) -> "PostgresConnection":
        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        try:
            if exc_type is None:
                self.commit()
            else:
                self.rollback()
        finally:
            self.close()


def connect_postgres(
    database_url: Optional[str] = None,
    *,
    schema: Optional[str] = None,
) -> PostgresConnection:
    url = str(database_url or configured_database_url()).strip()
    if not postgres_enabled(url):
        raise ValueError("DATABASE_URL must be a postgresql:// URL")
    if url.lower().startswith("postgresql+psycopg://"):
        url = "postgresql://" + url.split("://", 1)[1]
    selected_schema = configured_postgres_schema(schema)
    try:
        import psycopg
    except ImportError as exc:  # pragma: no cover - exercised in deployments.
        raise RuntimeError(
            "PostgreSQL backend requires psycopg; install project dependencies"
        ) from exc
    raw = psycopg.connect(url)
    try:
        with raw.cursor() as cursor:
            cursor.execute(
                f'SET search_path TO "{selected_schema.replace(chr(34), chr(34) * 2)}"'
            )
        raw.commit()
    except Exception:
        raw.close()
        raise
    return PostgresConnection(raw, schema=selected_schema)


def is_postgres_error(exc: BaseException) -> bool:
    try:
        import psycopg
    except ImportError:
        return False
    return isinstance(exc, psycopg.Error)


__all__ = [
    "DATABASE_URL_ENV",
    "DEFAULT_POSTGRES_SCHEMA",
    "DatabaseRow",
    "POSTGRES_SCHEMA_ENV",
    "POSTGRES_STATEMENT_TIMEOUT_MS_ENV",
    "PostgresConnection",
    "configured_database_url",
    "configured_postgres_schema",
    "connect_postgres",
    "convert_qmark_placeholders",
    "is_postgres_error",
    "postgres_enabled",
    "postgres_sql",
]
