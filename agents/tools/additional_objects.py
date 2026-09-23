"""Read-only tools for saved Additional objects metadata."""

from __future__ import annotations

from typing import Any, Dict, List, Literal, Optional, Sequence

from langchain_core.tools import tool

from .common import clamped_int


_RESULT_COLUMNS = [
    "additional_object_id",
    "file_id",
    "filename",
    "sheet_name",
    "row_num",
    "name",
    "sql",
]


def _read_rows(
    where_parts: Sequence[str],
    params: Sequence[Any],
    limit: int,
) -> Dict[str, Any]:
    from storage.database import get_db_connection

    where_sql = " AND ".join(where_parts) if where_parts else "1 = 1"
    query = f"""
        SELECT
            objects.id AS additional_object_id,
            objects.file_id,
            files.filename,
            objects.sheet_name,
            objects.row_num,
            objects.name,
            objects.sql,
            COUNT(*) OVER () AS total_matches
        FROM additional_objects AS objects
        LEFT JOIN files ON files.file_id = objects.file_id
        WHERE {where_sql}
        ORDER BY objects.file_id, objects.id
        LIMIT ?
    """
    conn = get_db_connection()
    try:
        raw_rows = [
            dict(row)
            for row in conn.execute(query, (*params, limit)).fetchall()
        ]
    finally:
        conn.close()

    total_matches = int(raw_rows[0].pop("total_matches")) if raw_rows else 0
    for row in raw_rows[1:]:
        row.pop("total_matches", None)
    return {
        "columns": list(_RESULT_COLUMNS),
        "total_matches": total_matches,
        "returned_rows": len(raw_rows),
        "truncated": total_matches > len(raw_rows),
        "rows": raw_rows,
    }


def _invalid_file_id(file_id: Optional[int]) -> Optional[Dict[str, Any]]:
    if file_id is None:
        return None
    try:
        parsed = int(file_id)
    except (TypeError, ValueError):
        return {"error": "file_id must be a positive integer", "rows": []}
    if parsed <= 0:
        return {"error": "file_id must be a positive integer", "rows": []}
    return None


@tool(parse_docstring=True)
def list_additional_objects(
    file_id: Optional[int] = None,
    name: Optional[str] = None,
    additional_object_id: Optional[int] = None,
    sheet_name: Optional[str] = None,
    row_num: Optional[int] = None,
    limit: int = 20,
) -> Dict[str, Any]:
    """Получить Additional objects по точным структурным фильтрам.

    Возвращает исходные строки без дедупликации: идентификатор объекта, file_id,
    файл, лист, номер строки, имя и полный сохранённый SQL. Используй для списка
    объектов файла или точного объекта; подстроку в name/sql ищи через
    search_additional_objects. Все переданные фильтры объединяются через AND.

    Args:
        file_id: Точный идентификатор загруженного файла.
        name: Точное имя Additional object без учёта регистра и внешних пробелов.
        additional_object_id: Точный внутренний идентификатор сохранённой строки.
        sheet_name: Точное имя исходного Excel-листа без учёта регистра.
        row_num: Точный номер исходной строки листа.
        limit: Максимальное число строк, от 1 до 100.
    """
    error = _invalid_file_id(file_id)
    if error is not None:
        return error

    where_parts: List[str] = []
    params: List[Any] = []
    filters: Dict[str, Any] = {}
    if file_id is not None:
        parsed_file_id = int(file_id)
        where_parts.append("objects.file_id = ?")
        params.append(parsed_file_id)
        filters["file_id"] = parsed_file_id
    if name is not None:
        clean_name = str(name).strip()
        if not clean_name:
            return {"error": "name must be non-empty", "rows": []}
        where_parts.append("LOWER(TRIM(objects.name)) = LOWER(TRIM(?))")
        params.append(clean_name)
        filters["name"] = clean_name
    if additional_object_id is not None:
        parsed_object_id = int(additional_object_id)
        if parsed_object_id <= 0:
            return {
                "error": "additional_object_id must be a positive integer",
                "rows": [],
            }
        where_parts.append("objects.id = ?")
        params.append(parsed_object_id)
        filters["additional_object_id"] = parsed_object_id
    if sheet_name is not None:
        clean_sheet = str(sheet_name).strip()
        if not clean_sheet:
            return {"error": "sheet_name must be non-empty", "rows": []}
        where_parts.append("LOWER(TRIM(objects.sheet_name)) = LOWER(TRIM(?))")
        params.append(clean_sheet)
        filters["sheet_name"] = clean_sheet
    if row_num is not None:
        parsed_row_num = int(row_num)
        where_parts.append("objects.row_num = ?")
        params.append(parsed_row_num)
        filters["row_num"] = parsed_row_num

    result = _read_rows(
        where_parts,
        params,
        clamped_int(limit, 20, 1, 100),
    )
    result["filters"] = filters
    return result


@tool(parse_docstring=True)
def search_additional_objects(
    needle: str,
    file_id: Optional[int] = None,
    search_in: Literal["all", "name", "sql"] = "all",
    limit: int = 20,
) -> Dict[str, Any]:
    """Найти Additional objects по подстроке в имени или полном SQL.

    Используй, когда точное имя объекта неизвестно или нужно найти SQL-конструкцию.
    file_id ограничивает поиск конкретной загрузкой. Для точных file/name/id/row
    фильтров используй list_additional_objects. Исходные дубликаты сохраняются.

    Args:
        needle: Непустая буквальная подстрока длиной не более 300 символов.
        file_id: Опциональный точный идентификатор загруженного файла.
        search_in: all для name и sql, name только для имени, sql только для SQL.
        limit: Максимальное число строк, от 1 до 100.
    """
    text = str(needle or "").strip()
    if not text:
        return {"error": "needle must be non-empty", "rows": []}
    if len(text) > 300:
        return {"error": "needle too long", "query": text, "rows": []}
    error = _invalid_file_id(file_id)
    if error is not None:
        return error

    where_parts: List[str] = []
    params: List[Any] = []
    if search_in == "name":
        where_parts.append("LOWER(COALESCE(objects.name, '')) LIKE '%' || LOWER(?) || '%'")
        params.append(text)
        searched_columns = ["name"]
    elif search_in == "sql":
        where_parts.append("LOWER(COALESCE(objects.sql, '')) LIKE '%' || LOWER(?) || '%'")
        params.append(text)
        searched_columns = ["sql"]
    else:
        where_parts.append(
            "(LOWER(COALESCE(objects.name, '')) LIKE '%' || LOWER(?) || '%' "
            "OR LOWER(COALESCE(objects.sql, '')) LIKE '%' || LOWER(?) || '%')"
        )
        params.extend((text, text))
        searched_columns = ["name", "sql"]
    if file_id is not None:
        parsed_file_id = int(file_id)
        where_parts.append("objects.file_id = ?")
        params.append(parsed_file_id)

    result = _read_rows(
        where_parts,
        params,
        clamped_int(limit, 20, 1, 100),
    )
    result.update(
        {
            "query": text,
            "file_id": int(file_id) if file_id is not None else None,
            "searched_columns": searched_columns,
        }
    )
    return result


__all__ = ["list_additional_objects", "search_additional_objects"]
