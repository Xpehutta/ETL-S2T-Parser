"""Run-scoped lazy result storage with optional tabular SQLite views."""

from __future__ import annotations

import json
import logging
import sqlite3
import tempfile
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from pathlib import Path
from threading import RLock
from typing import Any, Dict, Iterable, Iterator, List, Mapping, Optional, Sequence
from uuid import uuid4

from langchain_core.messages import ToolMessage
from langchain_core.tools import BaseTool, tool

from ..contracts import (
    PreviousResultReference,
    PreviousResultSchema,
    SavedResultColumn,
    SavedResultDescriptor,
    WorkerRequestParts,
    parse_worker_request,
)
from .common import clamped_int
from .sql import (
    _readonly_sql_authorizer,
    _result_column_error,
    _row_dict,
    _validate_readonly_sql,
)

logger = logging.getLogger(__name__)

MAX_SAVED_RESULT_QUERY_ROWS = 100

# These tools read SQLite-backed project facts and return tabular rows. Results
# from graph, visualization and static SQL parsing tools are intentionally not
# materialized here.
SQLITE_RESULT_TOOL_NAMES = frozenset(
    {
        "get_excel_row",
        "get_file_description",
        "get_s2t_rules_by_ids",
        "get_source_target_column_pair",
        "filter_column_catalog",
        "list_column_catalog",
        "list_column_metadata",
        "list_columns",
        "list_file_sheet_headers",
        "list_files",
        "list_s2t_table_mapping",
        "list_s2t_occurrences",
        "list_s2t_field_mapping",
        "list_s2t_table_names",
        "list_s2t_source_field",
        "list_s2t_source_table",
        "list_s2t_target_field",
        "list_s2t_target_table",
        "list_s2t_transformations",
        "list_source_column_catalog",
        "list_target_column_catalog",
        "list_sheets",
        "query_saved_result",
        "read_s2t_by_source_table",
        "read_s2t_by_target_table",
        "read_s2t_mapping",
        "read_s2t_source_to_target",
        "resolve_file",
        "run_sql",
        "search_column_catalog",
        "search_excel_values",
        "search_s2t_transformations",
        "semantic_search_descriptions",
        "summarize_s2t_tables",
        "summarize_table_descriptions",
    }
)


def _quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _json_value(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bytes)):
        return value
    if isinstance(value, bool):
        return int(value)
    return json.dumps(value, ensure_ascii=False, default=str, separators=(",", ":"))


def _sqlite_type(values: Iterable[Any]) -> str:
    kinds = set()
    for value in values:
        if value is None:
            continue
        if isinstance(value, bool) or isinstance(value, int):
            kinds.add("INTEGER")
        elif isinstance(value, float):
            kinds.add("REAL")
        elif isinstance(value, bytes):
            kinds.add("BLOB")
        else:
            kinds.add("TEXT")
    if not kinds:
        return "TEXT"
    if kinds <= {"INTEGER", "REAL"}:
        return "REAL" if "REAL" in kinds else "INTEGER"
    return kinds.pop() if len(kinds) == 1 else "TEXT"


def _normalized_columns(
    rows: Sequence[Mapping[str, Any]],
    declared_columns: Sequence[Any],
) -> List[str]:
    columns: List[str] = []
    seen: Dict[str, str] = {}
    declared_seen: set[str] = set()
    for value in declared_columns:
        name = "" if value is None else str(value)
        if not name:
            raise ValueError("Saved result contains an empty column name")
        folded = name.casefold()
        if folded in declared_seen:
            raise ValueError(
                "Saved result contains ambiguous declared column names: "
                f"{seen[folded]!r} and {name!r}"
            )
        declared_seen.add(folded)
        seen[folded] = name
        columns.append(name)
    for row in rows:
        for value in row:
            name = "" if value is None else str(value)
            if not name:
                raise ValueError("Saved result contains an empty column name")
            folded = name.casefold()
            existing = seen.get(folded)
            if existing is not None:
                if existing != name:
                    raise ValueError(
                        "Saved result contains ambiguous column names: "
                        f"{existing!r} and {name!r}"
                    )
                continue
            seen[folded] = name
            columns.append(name)
    return columns


def _tabular_payload(payload: Any) -> Optional[Dict[str, Any]]:
    if isinstance(payload, list):
        raw_rows = payload
        declared_columns: Sequence[Any] = ()
        metadata: Mapping[str, Any] = {}
    elif isinstance(payload, Mapping):
        metadata = payload
        if isinstance(payload.get("rows"), list):
            raw_rows = payload["rows"]
        elif isinstance(payload.get("preview_rows"), list):
            raw_rows = payload["preview_rows"]
        else:
            return None
        declared_columns = (
            payload.get("columns")
            if isinstance(payload.get("columns"), list)
            else ()
        )
    else:
        return None

    rows: List[Dict[str, Any]] = []
    packed_format = (
        isinstance(metadata, Mapping)
        and metadata.get("row_format") == "arrays_in_column_order"
    )
    if packed_format:
        if not declared_columns:
            return None
        packed_columns = _normalized_columns([], declared_columns)
        raw_dictionaries = metadata.get("dictionaries", {})
        if not isinstance(raw_dictionaries, Mapping):
            return None
        dictionaries: Dict[str, List[Any]] = {}
        for key, values in raw_dictionaries.items():
            column = "" if key is None else str(key)
            if column not in packed_columns or not isinstance(values, list):
                return None
            dictionaries[column] = values
        for item in raw_rows:
            if (
                not isinstance(item, Sequence)
                or isinstance(item, (str, bytes, bytearray))
                or len(item) != len(packed_columns)
            ):
                return None
            decoded: Dict[str, Any] = {}
            for column, value in zip(packed_columns, item):
                if column in dictionaries:
                    if (
                        isinstance(value, bool)
                        or not isinstance(value, int)
                        or value < 0
                        or value >= len(dictionaries[column])
                    ):
                        return None
                    value = dictionaries[column][value]
                decoded[column] = value
            rows.append(decoded)
    else:
        for item in raw_rows:
            if isinstance(item, Mapping):
                rows.append({str(key): value for key, value in item.items()})
            else:
                rows.append({"value": item})

    columns = _normalized_columns(rows, declared_columns)
    if not columns:
        return None

    source_total: Optional[int] = None
    for key in ("total", "total_matches", "total_candidates", "row_count"):
        value = metadata.get(key)
        if isinstance(value, int) and not isinstance(value, bool) and value >= 0:
            source_total = value
            break
    truncated = bool(metadata.get("truncated"))
    input_truncated = bool(metadata.get("input_truncated"))
    if source_total is not None and source_total > len(rows):
        truncated = True
    return {
        "columns": columns,
        "rows": rows,
        "source_total": source_total,
        "truncated": truncated,
        "input_truncated": input_truncated,
    }


def _decode_tool_content(content: Any) -> Optional[Any]:
    if isinstance(content, (dict, list)):
        return content
    if not isinstance(content, str):
        return None
    try:
        return json.loads(content)
    except json.JSONDecodeError:
        return None


class SavedResultStore:
    """Run-scoped accepted results plus optional temporary SQLite relations."""

    def __init__(self) -> None:
        self._temp_dir = tempfile.TemporaryDirectory(
            prefix="etl_agent_saved_results_"
        )
        self.path = Path(self._temp_dir.name) / "saved_results.db"
        self._lock = RLock()
        self._descriptors: Dict[str, SavedResultDescriptor] = {}
        self._tables: Dict[str, str] = {}
        self._previous_results: Dict[str, Dict[str, Any]] = {}
        self._result_datasets: Dict[str, str] = {}
        sqlite3.connect(self.path).close()

    def close(self) -> None:
        with self._lock:
            self._previous_results.clear()
            self._result_datasets.clear()
            self._descriptors.clear()
            self._tables.clear()
        self._temp_dir.cleanup()

    def descriptors(self) -> List[SavedResultDescriptor]:
        with self._lock:
            return [item.model_copy(deep=True) for item in self._descriptors.values()]

    def descriptor(self, result_ref: str) -> Optional[SavedResultDescriptor]:
        with self._lock:
            item = self._descriptors.get(str(result_ref or "").strip())
            return item.model_copy(deep=True) if item is not None else None

    def descriptors_for_worker(
        self,
        worker_execution_id: str,
        result_refs: Sequence[str],
    ) -> List[SavedResultDescriptor]:
        """Return exact datasets produced by one worker execution."""
        clean_execution_id = str(worker_execution_id or "").strip()
        clean_refs = list(
            dict.fromkeys(
                clean_ref
                for value in result_refs
                if (clean_ref := str(value or "").strip())
            )
        )
        with self._lock:
            return [
                descriptor.model_copy(deep=True)
                for result_ref in clean_refs
                if (
                    (descriptor := self._descriptors.get(result_ref))
                    is not None
                    and descriptor.worker_execution_id == clean_execution_id
                )
            ]

    def register_previous_result(
        self,
        *,
        source_tool: str,
        source_tool_call_id: str,
        content: str,
        description: str,
        dataset_ref: Optional[str] = None,
        source_evidence_ids: Sequence[str] = (),
    ) -> PreviousResultReference:
        """Store one accepted tool result behind an opaque run-scoped id."""
        clean_dataset_ref = str(dataset_ref or "").strip() or None
        with self._lock:
            if (
                clean_dataset_ref is not None
                and clean_dataset_ref not in self._descriptors
            ):
                raise ValueError(
                    "previous result references an unknown saved dataset"
                )
            descriptor = (
                self._descriptors.get(clean_dataset_ref)
                if clean_dataset_ref is not None
                else None
            )
            access = get_active_worker_result_access()
            if (
                descriptor is not None
                and access is not None
                and descriptor.worker_execution_id != access.worker_execution_id
            ):
                raise ValueError(
                    "previous result cannot register a dataset owned by "
                    "another worker execution"
                )
            reference = PreviousResultReference(
                result_id=f"result_{uuid4().hex}",
                description=description,
                source_evidence_ids=list(source_evidence_ids),
                result_schema=(
                    PreviousResultSchema(
                        result_ref=descriptor.result_ref,
                        row_count=descriptor.row_count,
                        truncated=descriptor.truncated,
                        input_truncated=descriptor.input_truncated,
                        columns=[
                            column.model_copy(deep=True)
                            for column in descriptor.columns
                        ],
                    )
                    if descriptor is not None
                    else None
                ),
            )
            self._previous_results[reference.result_id] = {
                "source_tool": str(source_tool or "unknown_tool"),
                "source_tool_call_id": str(source_tool_call_id or "").strip(),
                "content": str(content or ""),
                "source_evidence_ids": list(reference.source_evidence_ids),
            }
            if clean_dataset_ref is not None:
                self._result_datasets[reference.result_id] = clean_dataset_ref
        return reference

    def read_previous_result(self, result_id: str) -> Dict[str, Any]:
        """Resolve one accepted result inside the current coordinator run."""
        clean_id = str(result_id or "").strip()
        access = get_active_worker_result_access()
        if access is not None and clean_id not in access.allowed_result_ids:
            return {
                "error": "Previous result is not allowed for this worker",
                "result_id": clean_id,
            }
        with self._lock:
            stored = self._previous_results.get(clean_id)
            payload = dict(stored) if stored is not None else None
        if payload is None:
            return {
                "error": "Previous result not found in the current coordinator run",
                "result_id": clean_id,
            }
        content = payload["content"]
        decoded = _decode_tool_content(content)
        result = {
            "result_id": clean_id,
            "source_tool": payload["source_tool"],
            "source_evidence_ids": list(
                payload.get("source_evidence_ids") or []
            ),
            "result": decoded if decoded is not None else content,
        }
        return result

    def descriptors_for_result_ids(
        self,
        result_ids: Sequence[str],
    ) -> List[SavedResultDescriptor]:
        """Return saved tabular datasets linked to accepted lazy results."""
        access = get_active_worker_result_access()
        selected_ids = [str(result_id or "").strip() for result_id in result_ids]
        if access is not None:
            selected_ids = [
                result_id
                for result_id in selected_ids
                if result_id in access.allowed_result_ids
            ]
        with self._lock:
            refs = [
                self._result_datasets.get(result_id)
                for result_id in selected_ids
            ]
            return [
                self._descriptors[result_ref].model_copy(deep=True)
                for result_ref in dict.fromkeys(refs)
                if result_ref is not None and result_ref in self._descriptors
            ]

    def save_payload(
        self,
        *,
        source_tool: str,
        source_tool_call_id: Optional[str] = None,
        payload: Any,
    ) -> Optional[SavedResultDescriptor]:
        tabular = _tabular_payload(payload)
        if tabular is None:
            return None

        rows: List[Dict[str, Any]] = tabular["rows"]
        column_names: List[str] = tabular["columns"]
        column_types = [
            _sqlite_type(row.get(name) for row in rows)
            for name in column_names
        ]
        result_ref = f"saved_{uuid4().hex}"
        access = get_active_worker_result_access()

        with self._lock:
            table_name = f"saved_result_{len(self._descriptors) + 1}"
            definitions = ", ".join(
                f"{_quote_identifier(name)} {sqlite_type}"
                for name, sqlite_type in zip(column_names, column_types)
            )
            conn = sqlite3.connect(self.path)
            try:
                conn.execute(
                    f"CREATE TABLE {_quote_identifier(table_name)} ({definitions})"
                )
                if rows:
                    placeholders = ", ".join("?" for _ in column_names)
                    insert_sql = (
                        f"INSERT INTO {_quote_identifier(table_name)} "
                        f"VALUES ({placeholders})"
                    )
                    conn.executemany(
                        insert_sql,
                        [
                            tuple(_json_value(row.get(name)) for name in column_names)
                            for row in rows
                        ],
                    )
                conn.commit()
            finally:
                conn.close()

            descriptor = SavedResultDescriptor(
                result_ref=result_ref,
                source_tool=str(source_tool or "unknown_tool"),
                source_tool_call_id=(
                    str(source_tool_call_id or "").strip() or None
                ),
                worker_execution_id=(
                    access.worker_execution_id if access is not None else None
                ),
                row_count=len(rows),
                source_total=tabular["source_total"],
                truncated=bool(tabular["truncated"]),
                input_truncated=bool(tabular["input_truncated"]),
                columns=[
                    SavedResultColumn(name=name, sqlite_type=sqlite_type)
                    for name, sqlite_type in zip(column_names, column_types)
                ],
            )
            self._tables[result_ref] = table_name
            self._descriptors[result_ref] = descriptor
            logger.info(
                "Saved SQLite tool result: ref=%s source_tool=%s rows=%s "
                "truncated=%s columns=%s",
                result_ref,
                descriptor.source_tool,
                descriptor.row_count,
                descriptor.truncated,
                [column.name for column in descriptor.columns],
            )
            return descriptor.model_copy(deep=True)

    def query(
        self,
        *,
        result_ref: str,
        query: str,
        preview_limit: int,
    ) -> Dict[str, Any]:
        clean_ref = str(result_ref or "").strip()
        text = str(query or "").strip()
        access = get_active_worker_result_access()
        if access is not None and clean_ref not in access.allowed_result_refs:
            return {
                "error": "Saved result is not allowed for this worker",
                "result_ref": clean_ref,
                "query": text,
            }
        validation_error = _validate_readonly_sql(text)
        if validation_error:
            return {
                "error": validation_error,
                "result_ref": clean_ref,
                "query": text,
            }

        with self._lock:
            descriptor = self._descriptors.get(clean_ref)
            table_name = self._tables.get(clean_ref)
        if descriptor is None or table_name is None:
            return {
                "error": "Saved result not found in the current coordinator run",
                "result_ref": clean_ref,
                "query": text,
            }

        limit = clamped_int(preview_limit, 20, 0, MAX_SAVED_RESULT_QUERY_ROWS)
        conn = sqlite3.connect(self.path)
        conn.row_factory = sqlite3.Row
        try:
            conn.execute(
                "CREATE TEMP VIEW result AS SELECT * FROM "
                + _quote_identifier(table_name)
            )
            conn.execute("PRAGMA query_only = ON")
            if hasattr(conn, "set_authorizer"):
                allowed_tables = {"result", table_name}

                def authorizer(
                    action_code: int,
                    arg1: Optional[str],
                    arg2: Optional[str],
                    database_name: Optional[str],
                    trigger_name: Optional[str],
                ) -> int:
                    base = _readonly_sql_authorizer(
                        action_code,
                        arg1,
                        arg2,
                        database_name,
                        trigger_name,
                    )
                    if base != sqlite3.SQLITE_OK:
                        return base
                    if (
                        action_code == sqlite3.SQLITE_READ
                        and str(arg1 or "") not in allowed_tables
                    ):
                        return sqlite3.SQLITE_DENY
                    return sqlite3.SQLITE_OK

                conn.set_authorizer(authorizer)

            cursor = conn.execute(text)
            columns = [item[0] for item in (cursor.description or [])]
            column_error = _result_column_error(columns)
            if column_error:
                return {
                    "error": column_error,
                    "result_ref": clean_ref,
                    "query": text,
                    "columns": columns,
                }
            fetched = cursor.fetchmany(limit + 1)
            truncated = len(fetched) > limit
            rows = [_row_dict(row, columns) for row in fetched[:limit]]
            logger.info(
                "Queried saved SQLite result: ref=%s returned_rows=%s "
                "truncated=%s",
                clean_ref,
                len(rows),
                truncated,
            )
            return {
                "result_ref": clean_ref,
                "query": text,
                "input_row_count": descriptor.row_count,
                "input_truncated": (
                    descriptor.truncated or descriptor.input_truncated
                ),
                "columns": columns,
                "rows": rows,
                "returned_rows": len(rows),
                "truncated": truncated,
                "max_inline_rows": limit,
            }
        except sqlite3.Error as exc:
            logger.exception("Saved result SQL execution failed")
            return {
                "error": "Saved result SQL query failed",
                "error_message": str(exc),
                "result_ref": clean_ref,
                "query": text,
            }
        finally:
            conn.close()


_ACTIVE_SAVED_RESULT_STORE: ContextVar[Optional[SavedResultStore]] = ContextVar(
    "active_saved_result_store",
    default=None,
)


@dataclass(frozen=True)
class WorkerResultAccess:
    """Fail-closed result capabilities for one worker execution."""

    worker_execution_id: str
    allowed_result_ids: frozenset[str]
    allowed_result_refs: frozenset[str]


_ACTIVE_WORKER_RESULT_ACCESS: ContextVar[Optional[WorkerResultAccess]] = (
    ContextVar("active_worker_result_access", default=None)
)


@contextmanager
def worker_result_access_scope(
    previous_results: Sequence[PreviousResultReference],
    *,
    worker_execution_id: Optional[str] = None,
) -> Iterator[WorkerResultAccess]:
    """Bind direct dependency references to one isolated worker execution."""
    access = WorkerResultAccess(
        worker_execution_id=(
            str(worker_execution_id or "").strip()
            or f"worker_{uuid4().hex}"
        ),
        allowed_result_ids=frozenset(
            item.result_id for item in previous_results
        ),
        allowed_result_refs=frozenset(
            item.result_schema.result_ref
            for item in previous_results
            if item.result_schema is not None
        ),
    )
    token = _ACTIVE_WORKER_RESULT_ACCESS.set(access)
    try:
        yield access
    finally:
        _ACTIVE_WORKER_RESULT_ACCESS.reset(token)


def get_active_worker_result_access() -> Optional[WorkerResultAccess]:
    return _ACTIVE_WORKER_RESULT_ACCESS.get()


@contextmanager
def saved_result_store_scope() -> Iterator[SavedResultStore]:
    """Create and clean one isolated store for a coordinator execution."""
    store = SavedResultStore()
    token = _ACTIVE_SAVED_RESULT_STORE.set(store)
    try:
        yield store
    finally:
        _ACTIVE_SAVED_RESULT_STORE.reset(token)
        store.close()


def get_active_saved_result_store() -> Optional[SavedResultStore]:
    return _ACTIVE_SAVED_RESULT_STORE.get()


def persist_sqlite_tool_message(message: ToolMessage) -> ToolMessage:
    """Materialize a successful SQLite tool message and attach its schema."""
    store = get_active_saved_result_store()
    if store is None or str(message.name or "") not in SQLITE_RESULT_TOOL_NAMES:
        return message
    if getattr(message, "status", None) == "error":
        return message

    payload = _decode_tool_content(message.content)
    if not isinstance(payload, (dict, list)):
        return message
    if isinstance(payload, dict) and payload.get("error"):
        return message
    if isinstance(payload, dict) and isinstance(payload.get("saved_result"), dict):
        return message

    try:
        descriptor = store.save_payload(
            source_tool=str(message.name or "unknown_tool"),
            source_tool_call_id=str(message.tool_call_id or "").strip() or None,
            payload=payload,
        )
    except ValueError as exc:
        logger.warning("Tool result could not be materialized safely: %s", exc)
        if not isinstance(payload, dict):
            return message
        enriched = dict(payload)
        enriched["saved_result_error"] = str(exc)
        return message.model_copy(
            update={
                "content": json.dumps(
                    enriched,
                    ensure_ascii=False,
                    default=str,
                    separators=(",", ":"),
                )
            }
        )
    if descriptor is None:
        return message

    if isinstance(payload, dict):
        enriched = dict(payload)
    else:
        enriched = {"rows": payload}
    enriched["saved_result"] = descriptor.model_dump(mode="json")
    return message.model_copy(
        update={
            "content": json.dumps(
                enriched,
                ensure_ascii=False,
                default=str,
                separators=(",", ":"),
            )
        }
    )


def _descriptor_catalog(
    descriptors: Sequence[SavedResultDescriptor],
) -> str:
    items = []
    for descriptor in descriptors:
        columns = ", ".join(
            f"{_quote_identifier(column.name)} {column.sqlite_type}"
            for column in descriptor.columns
        )
        total = (
            f", source_total={descriptor.source_total}"
            if descriptor.source_total is not None
            else ""
        )
        items.append(
            f"- result_ref={descriptor.result_ref}; "
            f"source_tool={descriptor.source_tool}; "
            f"stored_rows={descriptor.row_count}{total}; "
            f"truncated={str(descriptor.truncated).lower()}; "
            f"input_truncated={str(descriptor.input_truncated).lower()}; "
            f"schema: CREATE TABLE result ({columns})"
        )
    return "\n".join(items)


def bind_saved_result_schemas(
    tools: Sequence[BaseTool],
    task: str | WorkerRequestParts,
) -> tuple[BaseTool, ...]:
    """Expose only applicable lazy-result tools and bind tabular schemas."""
    available_tools = list(tools)
    if all(item.name != "read_previous_result" for item in available_tools):
        available_tools.append(read_previous_result)
    store = get_active_saved_result_store()
    if store is None:
        return tuple(
            item
            for item in available_tools
            if item.name not in {"read_previous_result", "query_saved_result"}
        )
    request_parts = parse_worker_request(task)
    result_ids = [
        item.result_id for item in (request_parts.previous_results or [])
    ]
    descriptors = store.descriptors_for_result_ids(result_ids)
    if not descriptors and get_active_worker_result_access() is None:
        descriptors = [
            item
            for item in store.descriptors()
            if item.result_ref in request_parts.current_task
        ]
    catalog = _descriptor_catalog(descriptors) if descriptors else ""
    bound: List[BaseTool] = []
    for item in available_tools:
        if item.name == "read_previous_result":
            if result_ids:
                bound.append(item)
            continue
        if item.name != "query_saved_result":
            bound.append(item)
            continue
        if not descriptors:
            continue
        bound.append(
            item.model_copy(
                update={
                    "description": (
                        f"{item.description}\n\n"
                        "Доступные сохранённые результаты текущего coordinator-"
                        "запуска. Для выбранного result_ref SQL видит только "
                        "таблицу `result` с указанной схемой:\n"
                        f"{catalog}"
                    )
                }
            )
        )
    return tuple(bound)


@tool(parse_docstring=True)
def read_previous_result(
    result_id: Optional[str] = None,
    result_ids: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """Прочитать один или несколько результатов прошлых workers по ID.

    Используй только точные ID из блока `previous_results`, когда краткого
    description недостаточно для текущей task. Сохраняй строки и роли
    semantic-result; если следующий search поддерживает batch, передавай ему
    различающиеся технические имена одним вызовом, а не перебирай кандидатов по
    одному. Само наличие ссылки не требует чтения: если текущая task уже содержит
    точные аргументы независимого нового data-вызова, выполняй его напрямую.
    Результат той же операции для другого endpoint не является входом текущего
    endpoint и уже доступен upstream отдельно. Если нужны несколько результатов,
    передай их одним вызовом в `result_ids`. Инструмент не читает новые внешние
    данные и лениво возвращает только принятые tool results текущего запуска.

    Args:
        result_id: Один точный непрозрачный ID; legacy-вариант одиночного чтения.
        result_ids: Несколько точных ID для одного пакетного чтения.
    """
    store = get_active_saved_result_store()
    selected_ids = list(
        dict.fromkeys(
            clean_id
            for item in ([result_id] if result_id is not None else [])
            + list(result_ids or [])
            if (clean_id := str(item or "").strip())
        )
    )
    if store is None:
        return {
            "error": "No active saved-result store",
            "result_id": selected_ids[0] if len(selected_ids) == 1 else None,
            "result_ids": selected_ids,
        }
    if not selected_ids:
        return {"error": "At least one previous result ID is required"}
    if len(selected_ids) == 1 and not result_ids:
        return store.read_previous_result(selected_ids[0])
    return {
        "results": [store.read_previous_result(item) for item in selected_ids]
    }


@tool(parse_docstring=True)
def query_saved_result(
    result_ref: str,
    query: str,
    preview_limit: int = 20,
) -> Dict[str, Any]:
    """Выполнить произвольный read-only SQL по сохранённому результату tool.

    Используй только когда текущая task требует SQL-операцию над табличным
    результатом предыдущего worker, а description этого tool содержит его точный
    result_ref и схему.
    В SQL выбранный результат всегда называется `result`; другие таблицы и
    основная SQLite-база недоступны. Если schema помечена truncated=true, запрос
    анализирует только сохранённые строки preview и не доказывает свойства
    полного исходного набора. Поддерживается ровно один SELECT, WITH или EXPLAIN
    QUERY PLAN без мутаций.

    Args:
        result_ref: Точный непрозрачный идентификатор сохранённого результата.
        query: Один read-only SQL-запрос к таблице result по переданной схеме.
        preview_limit: Максимум возвращаемых строк, от 0 до 100.
    """
    store = get_active_saved_result_store()
    if store is None:
        return {
            "error": "No active saved-result store",
            "result_ref": str(result_ref or "").strip(),
            "query": str(query or "").strip(),
        }
    return store.query(
        result_ref=result_ref,
        query=query,
        preview_limit=preview_limit,
    )


__all__ = [
    "MAX_SAVED_RESULT_QUERY_ROWS",
    "SQLITE_RESULT_TOOL_NAMES",
    "SavedResultColumn",
    "SavedResultDescriptor",
    "SavedResultStore",
    "WorkerResultAccess",
    "bind_saved_result_schemas",
    "get_active_saved_result_store",
    "get_active_worker_result_access",
    "persist_sqlite_tool_message",
    "query_saved_result",
    "read_previous_result",
    "saved_result_store_scope",
    "worker_result_access_scope",
]
