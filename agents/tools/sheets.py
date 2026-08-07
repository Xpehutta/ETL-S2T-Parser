"""Tools for workbook sheets, headers, groups, and columns."""

import json
from typing import Any, Dict, List

from langchain_core.tools import tool

@tool(parse_docstring=True)
def list_sheets(file_id: int) -> List[str]:
    """Получить полный список имён Excel-листов одной сохранённой загрузки.

    Используй для прямого вопроса «какие листы есть в этом файле» после того,
    как file_id получен из UI или resolve_file. Не используй для поиска колонок,
    групп листов, строк Excel или S2T-трансформаций. Инструмент не выбирает
    последний файл и не принимает логическое имя ETL-таблицы вместо file_id.

    Читает file_sheet_headers и возвращает только реальные имена листов, включая
    пропущенные при анализе листы, если они были сохранены в метаданных. Пустой
    список означает отсутствие сохранённых строк file_sheet_headers для этого
    file_id; сам по себе он не различает отсутствующий файл и файл без листов.

    Args:
        file_id: Числовой идентификатор загрузки из UI или resolve_file.
    """
    from storage.database import get_db_connection
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT sheet_name FROM file_sheet_headers WHERE file_id = ? ORDER BY sheet_name", (file_id,))
    rows = cursor.fetchall()
    conn.close()
    return [row["sheet_name"] for row in rows]

@tool(parse_docstring=True)
def list_file_sheet_headers(file_id: int) -> List[Dict[str, Any]]:
    """Получить подробные метаданные листов и распознанных Excel-заголовков.

    Для каждого листа возвращает статус пропуска, положение и глубину
    заголовка, число колонок, плоские названия и разобранные пути колонок из
    file_sheet_headers.headers_json. Используй, когда нужны именно результаты
    определения заголовков или причина пропуска листа. Для одних только имён
    листов предпочитай list_sheets, а для значений строк —
    search_excel_values/get_excel_row.

    Инструмент ничего не переопределяет и не запускает CatBoost или LLM повторно:
    он показывает уже сохранённое решение. Пустой список означает, что для
    переданного file_id метаданные листов не найдены.

    Args:
        file_id: Числовой идентификатор загрузки из UI или resolve_file.
    """
    from storage.database import get_db_connection

    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT file_id, sheet_name, skipped, skip_reason,
               header_start_row, header_rows_count, nested_structure,
               columns_count, headers_json
        FROM file_sheet_headers
        WHERE file_id = ?
        ORDER BY sheet_name
        """,
        (file_id,),
    )
    rows = []
    for row in cursor.fetchall():
        item = dict(row)
        try:
            item["headers"] = json.loads(item.get("headers_json") or "[]")
        except json.JSONDecodeError:
            item["headers"] = []
        rows.append(item)
    conn.close()
    return rows


@tool(parse_docstring=True)
def list_sheet_group_classifications(file_id: int) -> Dict[str, Any]:
    """Получить сохранённо-детерминированную классификацию групп Excel-листов.

    Показывает сопоставление листов с группами из sheet_groups.json, направления
    ETL-слоёв, несопоставленные листы и проверочный отчёт. Инструментальный вызов
    не обращается к LLM и не записывает новые алиасы. Используй для диагностики
    маршрутизации sheet skills: почему лист отнесён к s2t, source_tables,
    target_tables, additional_objects или pxf_to_a и какие листы остались без
    группы. Не используй как повторный запуск извлечения полезных колонок.

    Несопоставленный лист является фактическим результатом текущих алиасов и
    детерминированного сопоставления, а не разрешением planner-у придумать группу.

    Args:
        file_id: Числовой идентификатор загрузки, листы которой нужно классифицировать.
    """
    from agents.sheet_group_classifier import classify_file_sheet_groups

    return classify_file_sheet_groups(file_id, use_llm=False, persist_aliases=False)


# ----------------------------------------------------------------------
# Tool: list_columns
# ----------------------------------------------------------------------
@tool(parse_docstring=True)
def list_columns(file_id: int, sheet_name: str) -> Dict[str, Any]:
    """Получить распознанные колонки одного Excel-листа в сохранённом порядке.

    Принимает фактическое имя листа либо имя/алиас группы из sheet_groups.json.
    Сначала ищет точное имя внутри указанного файла, затем разрешает алиас группы
    в фактическое имя листа. Если одной группе соответствуют несколько листов,
    возвращает кандидатов и не выбирает один из них самовольно.

    Используй для вопроса о структуре конкретного Excel-листа или чтобы получить
    реальные column_id/имена перед дальнейшим файловым анализом. Не используй
    для колонок логической ETL-таблицы вида t_*: такие колонки ищутся в
    s2t_transformations, а не в физической схеме SQLite. Пустой columns означает,
    что у найденного листа нет сохранённых заголовков.

    Args:
        file_id: Числовой идентификатор загрузки из UI или resolve_file.
        sheet_name: Фактическое имя листа либо группа/алиас из sheet_groups.json.
    """
    from config.sheet_groups import find_sheet_group_alias
    from storage.database import get_columns_by_sheet, get_db_connection

    requested = str(sheet_name or "").strip()
    if not requested:
        return {"error": "sheet_name must be non-empty", "columns": []}

    conn = get_db_connection()
    try:
        stored = [
            str(row["sheet_name"])
            for row in conn.execute(
                """
                SELECT sheet_name
                FROM file_sheet_headers
                WHERE file_id = ?
                ORDER BY sheet_name
                """,
                (int(file_id),),
            ).fetchall()
        ]
    finally:
        conn.close()

    exact = [name for name in stored if name.casefold() == requested.casefold()]
    matches = exact
    if not matches:
        requested_group = find_sheet_group_alias(requested)
        if requested_group:
            matches = [
                name
                for name in stored
                if (
                    (actual_group := find_sheet_group_alias(name))
                    and actual_group["group"] == requested_group["group"]
                )
            ]
    if not matches:
        return {
            "error": "Sheet not found",
            "file_id": int(file_id),
            "sheet_name": requested,
            "columns": [],
        }
    if len(matches) > 1:
        return {
            "error": "Multiple sheets match this name or group",
            "file_id": int(file_id),
            "sheet_name": requested,
            "matches": matches,
            "columns": [],
        }

    resolved_name = matches[0]
    rows = get_columns_by_sheet(file_id, resolved_name)
    columns = [
        {
            "column_id": row["column_id"],
            "name": row["column_name_flat"],
            "index": row["column_index"],
        }
        for row in rows
    ]
    return {
        "file_id": int(file_id),
        "sheet_name": resolved_name,
        "columns": columns,
        "column_count": len(columns),
    }
