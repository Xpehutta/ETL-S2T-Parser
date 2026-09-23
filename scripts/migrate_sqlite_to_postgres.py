"""Copy the durable ETL store from SQLite to PostgreSQL with verification."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sqlite3
import sys
from pathlib import Path
from typing import Any, Iterable, Sequence

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
load_dotenv(PROJECT_ROOT / ".env", override=False)

from storage.postgres import connect_postgres, configured_postgres_schema
from storage.postgres_schema import (
    init_postgres_schema,
    postgres_schema_columns,
    postgres_table_names,
)


IDENTITY_COLUMNS = {
    "files": "file_id",
    "data": "id",
    "source_tables": "id",
    "target_tables": "id",
    "source_columns": "id",
    "target_columns": "id",
    "additional_objects": "id",
    "pxf_to_a": "id",
    "s2t_transformations": "id",
}

ORDER_COLUMNS = {
    "file_sheet_headers": ("file_id", "sheet_name"),
    "graph_sync_generation": ("singleton_id",),
    "graph_sync_outbox": ("file_id",),
    "embedding_index_metadata": ("index_name",),
    **{table: (column,) for table, column in IDENTITY_COLUMNS.items()},
}


def _identifier(value: str) -> str:
    if not value.replace("_", "a").isalnum() or value[0].isdigit():
        raise ValueError(f"Invalid project identifier: {value!r}")
    return f'"{value}"'


def _sqlite_tables(connection: sqlite3.Connection) -> set[str]:
    return {
        str(row[0])
        for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        ).fetchall()
    }


def _count(connection: Any, table_name: str) -> int:
    return int(
        connection.execute(
            f"SELECT COUNT(*) FROM {_identifier(table_name)}"
        ).fetchone()[0]
    )


def _json_value(value: Any) -> Any:
    if isinstance(value, memoryview):
        value = value.tobytes()
    if isinstance(value, (bytes, bytearray)):
        return {"bytes_hex": bytes(value).hex()}
    return value


def _table_digest(
    connection: Any,
    table_name: str,
    columns: Sequence[str],
) -> str:
    order = ORDER_COLUMNS[table_name]
    query = (
        "SELECT "
        + ", ".join(_identifier(column) for column in columns)
        + f" FROM {_identifier(table_name)} ORDER BY "
        + ", ".join(_identifier(column) for column in order)
    )
    cursor = connection.execute(query)
    digest = hashlib.sha256()
    while True:
        rows = cursor.fetchmany(1000)
        if not rows:
            break
        for row in rows:
            payload = [_json_value(row[index]) for index in range(len(columns))]
            digest.update(
                json.dumps(
                    payload,
                    ensure_ascii=False,
                    separators=(",", ":"),
                ).encode("utf-8")
            )
            digest.update(b"\n")
    return digest.hexdigest()


def _chunks(cursor: sqlite3.Cursor, size: int) -> Iterable[list[tuple[Any, ...]]]:
    while True:
        rows = cursor.fetchmany(size)
        if not rows:
            return
        yield [tuple(row) for row in rows]


def _source_manifest(connection: sqlite3.Connection) -> dict[str, Any]:
    columns_by_table = postgres_schema_columns()
    existing = _sqlite_tables(connection)
    missing = [table for table in postgres_table_names() if table not in existing]
    if missing:
        raise RuntimeError(
            "SQLite source does not contain the current schema tables: "
            + ", ".join(missing)
        )
    return {
        table: {
            "rows": _count(connection, table),
            "sha256": _table_digest(
                connection,
                table,
                columns_by_table[table],
            ),
        }
        for table in postgres_table_names()
    }


def migrate(
    source_path: Path,
    database_url: str,
    *,
    schema: str,
    replace: bool,
    batch_size: int,
) -> dict[str, Any]:
    sqlite_connection = sqlite3.connect(source_path)
    sqlite_connection.row_factory = sqlite3.Row
    try:
        source_manifest = _source_manifest(sqlite_connection)
        init_postgres_schema(database_url, schema=schema)
        postgres_connection = connect_postgres(database_url, schema=schema)
        try:
            nonempty = {
                table: _count(postgres_connection, table)
                for table in postgres_table_names()
                if table != "graph_sync_generation"
                and _count(postgres_connection, table) > 0
            }
            if nonempty and not replace:
                raise RuntimeError(
                    "PostgreSQL target is not empty; use --replace only for the "
                    "verified target schema. Non-empty tables: "
                    + ", ".join(f"{name}={count}" for name, count in nonempty.items())
                )

            cursor = postgres_connection.cursor()
            cursor.execute("BEGIN")
            cursor.execute(
                "TRUNCATE TABLE "
                + ", ".join(_identifier(table) for table in postgres_table_names())
                + " RESTART IDENTITY"
            )
            columns_by_table = postgres_schema_columns()
            for table_name in postgres_table_names():
                columns = columns_by_table[table_name]
                select_cursor = sqlite_connection.execute(
                    "SELECT "
                    + ", ".join(_identifier(column) for column in columns)
                    + f" FROM {_identifier(table_name)} ORDER BY "
                    + ", ".join(
                        _identifier(column) for column in ORDER_COLUMNS[table_name]
                    )
                )
                insert_sql = (
                    f"INSERT INTO {_identifier(table_name)} ("
                    + ", ".join(_identifier(column) for column in columns)
                    + ") VALUES ("
                    + ", ".join("?" for _ in columns)
                    + ")"
                )
                for batch in _chunks(select_cursor, batch_size):
                    cursor.executemany(insert_sql, batch)

            for table_name, column_name in IDENTITY_COLUMNS.items():
                cursor.execute(
                    "SELECT setval(pg_get_serial_sequence(?, ?), "
                    f"COALESCE(MAX({_identifier(column_name)}), 1), "
                    f"MAX({_identifier(column_name)}) IS NOT NULL) "
                    f"FROM {_identifier(table_name)}",
                    (table_name, column_name),
                )

            target_manifest = {
                table: {
                    "rows": _count(postgres_connection, table),
                    "sha256": _table_digest(
                        postgres_connection,
                        table,
                        columns_by_table[table],
                    ),
                }
                for table in postgres_table_names()
            }
            if target_manifest != source_manifest:
                raise RuntimeError(
                    "PostgreSQL verification differs from SQLite; transaction rolled back"
                )
            postgres_connection.commit()
        except Exception:
            postgres_connection.rollback()
            raise
        finally:
            postgres_connection.close()
    finally:
        sqlite_connection.close()

    return {
        "source": str(source_path.resolve()),
        "schema": schema,
        "tables": source_manifest,
        "verified": True,
        "native_comments": True,
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", default="excel_data.db")
    parser.add_argument("--database-url", default=os.getenv("DATABASE_URL", ""))
    parser.add_argument("--schema", default=os.getenv("POSTGRES_SCHEMA", "public"))
    parser.add_argument("--batch-size", type=int, default=1000)
    parser.add_argument("--replace", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    return parser


def main() -> int:
    args = _parser().parse_args()
    source_path = Path(args.source)
    if not source_path.is_file():
        raise SystemExit(f"SQLite source not found: {source_path}")
    if args.batch_size <= 0:
        raise SystemExit("--batch-size must be positive")
    schema = configured_postgres_schema(args.schema)

    if args.dry_run:
        connection = sqlite3.connect(source_path)
        connection.row_factory = sqlite3.Row
        try:
            manifest = _source_manifest(connection)
        finally:
            connection.close()
        print(
            json.dumps(
                {
                    "source": str(source_path.resolve()),
                    "schema": schema,
                    "tables": manifest,
                    "dry_run": True,
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return 0

    if not args.database_url:
        raise SystemExit("DATABASE_URL or --database-url is required")
    result = migrate(
        source_path,
        args.database_url,
        schema=schema,
        replace=bool(args.replace),
        batch_size=int(args.batch_size),
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
