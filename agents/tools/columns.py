"""Read-only tools for structured source/target column catalogs."""

from __future__ import annotations

from typing import Any, Dict, List, Literal, Optional, Sequence, Tuple

from langchain_core.tools import tool

from .common import clamped_int, pack_tabular_rows


ColumnScope = Literal["all_tables", "source_columns", "target_columns"]

COLUMN_RESULT_FIELDS = (
    "record_id",
    "column_role",
    "file_id",
    "filename",
    "sheet_name",
    "row_num",
    "table_name",
    "column_name",
    "data_type",
    "primary_key",
    "not_null",
    "description",
)


def _catalog_branches(scope: ColumnScope) -> Tuple[Tuple[str, str], ...]:
    if scope == "source_columns":
        return (("source_columns", "source"),)
    if scope == "target_columns":
        return (("target_columns", "target"),)
    return (("source_columns", "source"), ("target_columns", "target"))


def _catalog_cte(scope: ColumnScope) -> str:
    return "\nUNION ALL\n".join(
        f"""
        SELECT catalog.id AS record_id, '{role}' AS column_role,
               catalog.file_id, files.filename, catalog.sheet_name,
               catalog.row_num, catalog.table_name, catalog.column_name,
               catalog.data_type, catalog.primary_key, catalog.not_null,
               catalog.description
        FROM {table_name} AS catalog
        LEFT JOIN files ON files.file_id = catalog.file_id
        """.strip()
        for table_name, role in _catalog_branches(scope)
    )


def _clean_exact_filter(value: Optional[str], name: str) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if len(text) > 300:
        raise ValueError(f"{name} too long")
    return text


def _subset_conditions(
    *,
    file_id: Optional[int],
    table_name: Optional[str],
    column_name: Optional[str],
    data_type: Optional[str],
    primary_key: Optional[bool],
    not_null: Optional[bool],
    needle: Optional[str] = None,
) -> Tuple[List[str], List[Any], Dict[str, Any]]:
    conditions: List[str] = []
    params: List[Any] = []
    filters: Dict[str, Any] = {}

    if file_id is not None:
        clean_file_id = int(file_id)
        if clean_file_id <= 0:
            raise ValueError("file_id must be positive")
        conditions.append("file_id = ?")
        params.append(clean_file_id)
        filters["file_id"] = clean_file_id

    for field, value in (
        ("table_name", table_name),
        ("column_name", column_name),
        ("data_type", data_type),
    ):
        clean_value = _clean_exact_filter(value, field)
        if clean_value is None:
            continue
        conditions.append(f"LOWER(TRIM({field})) = LOWER(TRIM(?))")
        params.append(clean_value)
        filters[field] = clean_value

    for field, value in (("primary_key", primary_key), ("not_null", not_null)):
        if value is None:
            continue
        conditions.append(f"{field} = ?")
        params.append(1 if value else 0)
        filters[field] = bool(value)

    if needle is not None:
        pattern = f"%{needle}%"
        conditions.append(
            "(" + " OR ".join(
                f"LOWER(COALESCE({field}, '')) LIKE LOWER(?)"
                for field in (
                    "table_name",
                    "column_name",
                    "data_type",
                    "description",
                )
            ) + ")"
        )
        params.extend([pattern] * 4)
    return conditions, params, filters


def _query_catalog(
    *,
    scope: ColumnScope,
    limit: Optional[int],
    columns: Optional[Sequence[str]],
    file_id: Optional[int],
    table_name: Optional[str],
    column_name: Optional[str],
    data_type: Optional[str],
    primary_key: Optional[bool],
    not_null: Optional[bool],
    needle: Optional[str] = None,
) -> Dict[str, Any]:
    selected = list(columns or COLUMN_RESULT_FIELDS)
    if not selected:
        return {"error": "columns must not be empty", "rows": []}
    unknown = [field for field in selected if field not in COLUMN_RESULT_FIELDS]
    if unknown:
        return {
            "error": f"unknown columns: {', '.join(unknown)}",
            "allowed_columns": list(COLUMN_RESULT_FIELDS),
            "rows": [],
        }
    selected = list(dict.fromkeys(selected))
    try:
        conditions, params, filters = _subset_conditions(
            file_id=file_id,
            table_name=table_name,
            column_name=column_name,
            data_type=data_type,
            primary_key=primary_key,
            not_null=not_null,
            needle=needle,
        )
    except (TypeError, ValueError) as exc:
        return {"error": str(exc), "rows": []}

    where_sql = "WHERE " + " AND ".join(conditions) if conditions else ""
    clean_limit = (
        clamped_int(limit, 50, 1, 100)
        if limit is not None
        else None
    )
    select_sql = ", ".join(f'"{field}"' for field in selected)
    limit_sql = "LIMIT ?" if clean_limit is not None else ""
    query = f"""
        WITH catalog AS (
            {_catalog_cte(scope)}
        ), matched AS (
            SELECT * FROM catalog
            {where_sql}
        )
        SELECT {select_sql}, COUNT(*) OVER () AS __total_matches
        FROM matched
        ORDER BY column_role, file_id, table_name, column_name, record_id
        {limit_sql}
    """
    if clean_limit is not None:
        params.append(clean_limit)

    from storage.database import get_db_connection

    conn = get_db_connection()
    try:
        fetched = conn.execute(query, params).fetchall()
    finally:
        conn.close()
    total_matches = int(fetched[0]["__total_matches"]) if fetched else 0
    rows = []
    for row in fetched:
        item = dict(row)
        item.pop("__total_matches", None)
        rows.append(item)
    return {
        "scope": scope,
        "filters": filters,
        "columns": selected,
        "total_matches": total_matches,
        "returned_rows": len(rows),
        "truncated": total_matches > len(rows),
        "rows": rows,
    }


def _list_role_column_catalog(
    *,
    scope: Literal["source_columns", "target_columns"],
    file_id: int,
    table_name: str,
    column_name: Optional[str],
    data_type: Optional[str],
    primary_key: Optional[bool],
    not_null: Optional[bool],
) -> Dict[str, Any]:
    """Read one explicit catalog role inside a required file and table."""
    try:
        clean_table = _clean_exact_filter(table_name, "table_name")
    except (TypeError, ValueError) as exc:
        return {"error": str(exc), "rows": []}
    if clean_table is None:
        return {"error": "table_name must be non-empty", "rows": []}
    return _query_catalog(
        scope=scope,
        limit=None,
        columns=None,
        file_id=file_id,
        table_name=clean_table,
        column_name=column_name,
        data_type=data_type,
        primary_key=primary_key,
        not_null=not_null,
    )


@tool(parse_docstring=True)
def list_source_column_catalog(
    file_id: int,
    table_name: str,
    column_name: Optional[str] = None,
    data_type: Optional[str] = None,
    primary_key: Optional[bool] = None,
    not_null: Optional[bool] = None,
) -> Dict[str, Any]:
    """Прочитать source-колонки одной точной таблицы конкретного файла.

    Роль source, ``file_id`` и ``table_name`` зафиксированы сигнатурой и не
    могут быть потеряны либо заменены target-каталогом. Для одной известной
    колонки передай ``column_name``; без него вернутся колонки всей таблицы.
    Фильтры типа, PK и NOT NULL ограничивают это же точное множество.

    Args:
        file_id: Положительный идентификатор явно выбранной загрузки.
        table_name: Точное полное имя исходной логической таблицы.
        column_name: Опциональное точное имя исходной колонки.
        data_type: Опциональный точный тип данных.
        primary_key: Опциональный фильтр признака первичного ключа.
        not_null: Опциональный фильтр обязательности значения.
    """
    return _list_role_column_catalog(
        scope="source_columns",
        file_id=file_id,
        table_name=table_name,
        column_name=column_name,
        data_type=data_type,
        primary_key=primary_key,
        not_null=not_null,
    )


@tool(parse_docstring=True)
def list_target_column_catalog(
    file_id: int,
    table_name: str,
    column_name: Optional[str] = None,
    data_type: Optional[str] = None,
    primary_key: Optional[bool] = None,
    not_null: Optional[bool] = None,
) -> Dict[str, Any]:
    """Прочитать target-колонки одной точной таблицы конкретного файла.

    Роль target, ``file_id`` и ``table_name`` зафиксированы сигнатурой и не
    могут быть потеряны либо заменены source-каталогом. Для одной известной
    колонки передай ``column_name``; без него вернутся колонки всей таблицы.
    Фильтры типа, PK и NOT NULL ограничивают это же точное множество.

    Args:
        file_id: Положительный идентификатор явно выбранной загрузки.
        table_name: Точное полное имя целевой логической таблицы.
        column_name: Опциональное точное имя целевой колонки.
        data_type: Опциональный точный тип данных.
        primary_key: Опциональный фильтр признака первичного ключа.
        not_null: Опциональный фильтр обязательности значения.
    """
    return _list_role_column_catalog(
        scope="target_columns",
        file_id=file_id,
        table_name=table_name,
        column_name=column_name,
        data_type=data_type,
        primary_key=primary_key,
        not_null=not_null,
    )


@tool(parse_docstring=True)
def get_source_target_column_pair(
    file_id: int,
    source_table: str,
    source_column: str,
    target_table: str,
    target_column: str,
) -> Dict[str, Any]:
    """Прочитать точную source/target-пару колонок одного файла.

    Используй для сравнения атрибутов уже известных S2T-колонок. Все пять
    идентификаторов обязательны, поэтому одна сторона, роль или ``file_id`` не
    могут исчезнуть между вызовами. Tool только возвращает две каталоговые
    записи; совместимость анализирует upstream.

    Args:
        file_id: Положительный идентификатор явно выбранной загрузки.
        source_table: Точное полное имя исходной логической таблицы.
        source_column: Точное имя исходной колонки.
        target_table: Точное полное имя целевой логической таблицы.
        target_column: Точное имя целевой колонки.
    """
    names: Dict[str, str] = {}
    try:
        for name, value in (
            ("source_table", source_table),
            ("source_column", source_column),
            ("target_table", target_table),
            ("target_column", target_column),
        ):
            clean_value = _clean_exact_filter(value, name)
            if clean_value is None:
                return {"error": f"{name} must be non-empty", "rows": []}
            names[name] = clean_value
    except (TypeError, ValueError) as exc:
        return {"error": str(exc), "rows": []}

    source = _query_catalog(
        scope="source_columns",
        limit=None,
        columns=None,
        file_id=file_id,
        table_name=names["source_table"],
        column_name=names["source_column"],
        data_type=None,
        primary_key=None,
        not_null=None,
    )
    target = _query_catalog(
        scope="target_columns",
        limit=None,
        columns=None,
        file_id=file_id,
        table_name=names["target_table"],
        column_name=names["target_column"],
        data_type=None,
        primary_key=None,
        not_null=None,
    )
    for result in (source, target):
        if result.get("error"):
            return {"error": result["error"], "rows": []}
    rows = [*source.get("rows", []), *target.get("rows", [])]
    return {
        "scope": "source_target_pair",
        "filters": {"file_id": int(file_id), **names},
        "columns": list(COLUMN_RESULT_FIELDS),
        "role_counts": {
            "source": int(source.get("total_matches") or 0),
            "target": int(target.get("total_matches") or 0),
        },
        "total_matches": int(source.get("total_matches") or 0)
        + int(target.get("total_matches") or 0),
        "returned_rows": len(rows),
        "truncated": False,
        "rows": rows,
    }


def _normalize_file_scope(
    file_scope: str,
) -> Tuple[Optional[int], str]:
    if not isinstance(file_scope, str):
        raise ValueError("file_scope must be 'all' or a positive decimal file_id")
    clean_scope = file_scope.strip()
    if clean_scope.casefold() == "all":
        return None, "all"
    if not clean_scope.isdecimal() or int(clean_scope) <= 0:
        raise ValueError("file_scope must be 'all' or a positive decimal file_id")
    clean_file_id = int(clean_scope)
    return clean_file_id, str(clean_file_id)


@tool(parse_docstring=True)
def list_column_metadata(
    file_scope: str,
    table_names: List[str],
) -> Dict[str, Any]:
    """Прочитать полную структуру точных таблиц в обеих catalog-ролях.

    ``file_scope`` — строка с decimal file_id (например ``"3"``) либо ``"all"``.
    Одним batch читает полную структуру ``table_names`` в source/target-ролях
    без лимита. Type/PK/NOT NULL-фильтров нет: фактические значения выбираются
    из результата. Description читается search/semantic tool. Формат lossless:
    позиции задаёт ``columns``, словарные значения — 0-based индексы в
    ``dictionaries``. Это transport references; каждая catalog-row сохранена.

    Args:
        file_scope: Строка ``all`` либо положительный десятичный file_id, например ``3``.
        table_names: Непустой список точных полных имён таблиц; максимум 20.
    """
    try:
        file_id, normalized_scope = _normalize_file_scope(file_scope)
        clean_tables: List[str] = []
        seen_tables = set()
        for value in table_names or []:
            clean_value = _clean_exact_filter(value, "table_name")
            if clean_value is None:
                continue
            identity = clean_value.casefold()
            if identity not in seen_tables:
                seen_tables.add(identity)
                clean_tables.append(clean_value)
        if not clean_tables:
            raise ValueError("table_names must contain at least one exact name")
        if len(clean_tables) > 20:
            raise ValueError("table_names supports at most 20 exact names")
    except (TypeError, ValueError) as exc:
        return {"error": str(exc), "rows": []}

    rows: List[Dict[str, Any]] = []
    table_counts: Dict[str, Dict[str, int]] = {}
    role_counts = {"source": 0, "target": 0}
    result_columns = [
        "record_id",
        "column_role",
        "file_id",
        "filename",
        "sheet_name",
        "row_num",
        "table_name",
        "column_name",
        "data_type",
        "primary_key",
        "not_null",
    ]
    for table_name in clean_tables:
        result = _query_catalog(
            scope="all_tables",
            limit=None,
            columns=result_columns,
            file_id=file_id,
            table_name=table_name,
            column_name=None,
            data_type=None,
            primary_key=None,
            not_null=None,
        )
        if result.get("error"):
            return {"error": result["error"], "rows": []}
        current_counts = {"source": 0, "target": 0}
        for row in result.get("rows") or []:
            item = dict(row)
            role = str(item.get("column_role") or "")
            if role in current_counts:
                current_counts[role] += 1
                role_counts[role] += 1
            rows.append(item)
        table_counts[table_name] = current_counts

    filters: Dict[str, Any] = {
        "file_scope": normalized_scope,
        "table_names": clean_tables,
    }
    packed = pack_tabular_rows(
        rows,
        columns=result_columns,
        dictionary_columns=[
            "column_role",
            "filename",
            "sheet_name",
            "table_name",
        ],
    )
    packed_rows = packed.pop("rows")
    return {
        "scope": "source_and_target_column_structure",
        "filters": filters,
        **packed,
        "role_counts": role_counts,
        "table_role_counts": table_counts,
        "total_matches": len(rows),
        "returned_rows": len(rows),
        "truncated": False,
        "rows": packed_rows,
    }


@tool(parse_docstring=True)
def list_column_catalog(
    scope: ColumnScope,
    limit: int = 50,
    columns: Optional[List[str]] = None,
    file_id: Optional[int] = None,
    table_name: Optional[str] = None,
    column_name: Optional[str] = None,
) -> Dict[str, Any]:
    """Прочитать колонки и их атрибуты по точным идентификаторам.

    Используй для известной роли и точных значений table_name/column_name либо
    для списка колонок явно выбранной таблицы или файла. Квалифицированное имя
    ``table.column`` всегда разделяй: часть до последней точки передавай в
    ``table_name``, часть после неё — в ``column_name``. Обязательный scope
    all_tables объединяет source_columns и target_columns; ролевые scope читают
    только один каталог. Выбирай all_tables только если task явно требует обе
    стороны либо не ограничивает роль колонки. Один вызов применяет одинаковые table_name и
    column_name ко всему выбранному scope: разные source/target пары получай
    отдельными вызовами, не ищи target по идентификаторам source. Для фрагмента имени или текста сначала используй
    search_column_catalog, для смысла описания — semantic_search_descriptions.
    Чтобы получить data_type, primary_key или not_null известной колонки, просто
    прочитай её этим tool: эти атрибуты входят в результат и не являются
    входными аргументами. Для отбора множества колонок по значениям атрибутов
    используй filter_column_catalog.
    Без columns возвращает все публичные поля, но никогда не возвращает BLOB.

    Args:
        scope: Обязательная область: all_tables, source_columns или target_columns; all_tables выбирай только для обеих сторон или неизвестной роли.
        limit: Максимальное число строк, от 1 до 100.
        columns: Опциональная подвыборка возвращаемых публичных полей.
        file_id: Опциональный точный идентификатор явно выбранной загрузки.
        table_name: Опциональное точное полное имя таблицы; обязательно передай, если оно явно задано task.
        column_name: Опциональное точное имя колонки без префикса table_name.
    """
    return _query_catalog(
        scope=scope,
        limit=limit,
        columns=columns,
        file_id=file_id,
        table_name=table_name,
        column_name=column_name,
        data_type=None,
        primary_key=None,
        not_null=None,
    )


@tool(parse_docstring=True)
def filter_column_catalog(
    scope: ColumnScope,
    limit: int = 50,
    columns: Optional[List[str]] = None,
    file_id: Optional[int] = None,
    table_name: Optional[str] = None,
    data_type: Optional[str] = None,
    primary_key: Optional[bool] = None,
    not_null: Optional[bool] = None,
) -> Dict[str, Any]:
    """Отобрать множество колонок по типу, PK или nullable-ограничению.

    Используй, когда task просит найти все колонки с заданным data_type,
    primary_key или not_null, опционально внутри точного file_id/table_name.
    Хотя бы один из трёх атрибутных фильтров обязателен. Этот tool не принимает
    column_name: атрибуты уже известной колонки читай через list_column_catalog.

    Args:
        scope: Обязательная область: all_tables, source_columns или target_columns.
        limit: Максимальное число строк, от 1 до 100.
        columns: Опциональная подвыборка возвращаемых публичных полей.
        file_id: Опциональный точный идентификатор явно выбранной загрузки.
        table_name: Опциональное точное полное имя таблицы для ограничения множества.
        data_type: Опциональный точный тип данных для отбора колонок.
        primary_key: Опциональный фильтр признака первичного ключа.
        not_null: Опциональный фильтр обязательности значения.
    """
    has_data_type = bool(str(data_type or "").strip())
    if not has_data_type and primary_key is None and not_null is None:
        return {
            "error": (
                "one of data_type, primary_key or not_null must be provided"
            ),
            "rows": [],
        }
    return _query_catalog(
        scope=scope,
        limit=limit,
        columns=columns,
        file_id=file_id,
        table_name=table_name,
        column_name=None,
        data_type=data_type,
        primary_key=primary_key,
        not_null=not_null,
    )


@tool(parse_docstring=True)
def search_column_catalog(
    needle: str,
    scope: ColumnScope,
    limit: int = 50,
    file_id: Optional[int] = None,
    table_name: Optional[str] = None,
    data_type: Optional[str] = None,
    primary_key: Optional[bool] = None,
    not_null: Optional[bool] = None,
) -> Dict[str, Any]:
    """Найти колонки по одной буквальной подстроке в структурной подвыборке.

    Ищет один needle как текстовый фрагмент в table_name, column_name,
    data_type и description. Используй, только когда такой фрагмент явно дан в
    task или принятом результате; scope и точные фильтры сначала ограничивают
    подвыборку. Не передавай в needle список альтернатив, OR-выражение,
    придуманные синонимы или переводы бизнес-термина. Для поиска по назначению,
    бизнес-смыслу или описанию при неизвестном точном фрагменте используй
    semantic_search_descriptions. После разрешения точных имён передавай их в
    exact catalog reader активной палитры.

    Args:
        needle: Одна непустая буквальная подстрока длиной до 300 символов.
        scope: Обязательная область: all_tables, source_columns или target_columns; all_tables выбирай только для обеих сторон или неизвестной роли.
        limit: Максимальное число строк, от 1 до 100.
        file_id: Опциональный точный идентификатор явно выбранной загрузки.
        table_name: Опциональное точное имя таблицы для ограничения поиска.
        data_type: Опциональный точный тип данных.
        primary_key: Опциональный фильтр признака первичного ключа.
        not_null: Опциональный фильтр обязательности значения.
    """
    text = str(needle or "").strip()
    if not text:
        return {"error": "needle must be non-empty", "rows": []}
    if len(text) > 300:
        return {"error": "needle too long", "rows": []}
    result = _query_catalog(
        scope=scope,
        limit=limit,
        columns=None,
        file_id=file_id,
        table_name=table_name,
        column_name=None,
        data_type=data_type,
        primary_key=primary_key,
        not_null=not_null,
        needle=text,
    )
    result["query"] = text
    return result


__all__ = [
    "COLUMN_RESULT_FIELDS",
    "filter_column_catalog",
    "get_source_target_column_pair",
    "list_column_catalog",
    "list_column_metadata",
    "list_source_column_catalog",
    "list_target_column_catalog",
    "search_column_catalog",
]
