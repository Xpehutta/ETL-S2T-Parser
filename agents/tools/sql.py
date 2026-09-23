"""Read-only SQL tool and CSV export support."""

import csv
import datetime
import logging
import re
import sqlite3
from typing import Any, Dict, List, Optional

from langchain_core.tools import tool
from .common import PROJECT_ROOT, clamped_int

logger = logging.getLogger(__name__)

SQL_EXPORT_DIR = PROJECT_ROOT / "exports" / "sql_exports"
SQL_EXPORT_URL_PREFIX = "/exports/sql"
MAX_INLINE_SQL_ROWS = 100
SQL_FETCH_BATCH_SIZE = 1000


def _result_column_error(columns: List[str]) -> Optional[str]:
    """Return an explicit error for names sqlite3.Row cannot represent safely."""
    seen: Dict[str, str] = {}
    for column in columns:
        folded = column.casefold()
        if folded in seen:
            return (
                "SQL result contains ambiguous column names: "
                f"{seen[folded]!r} and {column!r}; assign unique aliases"
            )
        seen[folded] = column
    return None


def _row_dict(row: Any, columns: List[str]) -> Dict[str, Any]:
    return {column: row[index] for index, column in enumerate(columns)}


def _write_sql_export_cursor(
    query: str,
    cursor: Any,
    columns: List[str],
    preview_limit: int,
) -> Dict[str, Any]:
    """Потоково сохранить SQL-результат в CSV без загрузки всей выборки в память."""
    SQL_EXPORT_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    filename = f"sql_result_{stamp}.csv"
    path = SQL_EXPORT_DIR / filename
    temporary_path = path.with_name(f".{filename}.tmp")

    row_count = 0
    preview_rows: List[Dict[str, Any]] = []
    try:
        with temporary_path.open("w", newline="", encoding="utf-8-sig") as file:
            writer = csv.DictWriter(file, fieldnames=columns, extrasaction="ignore")
            writer.writeheader()
            while True:
                batch = cursor.fetchmany(SQL_FETCH_BATCH_SIZE)
                if not batch:
                    break
                for row in batch:
                    item = _row_dict(row, columns)
                    writer.writerow(item)
                    row_count += 1
                    if len(preview_rows) < preview_limit:
                        preview_rows.append(item)
        temporary_path.replace(path)
    except Exception:
        temporary_path.unlink(missing_ok=True)
        raise

    return {
        "query": query,
        "row_count": row_count,
        "columns": columns,
        "preview_limit": preview_limit,
        "preview_rows": preview_rows,
        "csv_filename": filename,
        "csv_path": str(path),
        "csv_url": f"{SQL_EXPORT_URL_PREFIX}/{filename}",
    }


def _readonly_sql_authorizer(
    action_code: int,
    arg1: Optional[str],
    arg2: Optional[str],
    database_name: Optional[str],
    trigger_name: Optional[str],
) -> int:
    del arg1, arg2, database_name, trigger_name
    denied_actions = {
        sqlite3.SQLITE_INSERT,
        sqlite3.SQLITE_UPDATE,
        sqlite3.SQLITE_DELETE,
        sqlite3.SQLITE_CREATE_INDEX,
        sqlite3.SQLITE_CREATE_TABLE,
        sqlite3.SQLITE_CREATE_TEMP_INDEX,
        sqlite3.SQLITE_CREATE_TEMP_TABLE,
        sqlite3.SQLITE_CREATE_TEMP_TRIGGER,
        sqlite3.SQLITE_CREATE_TEMP_VIEW,
        sqlite3.SQLITE_CREATE_TRIGGER,
        sqlite3.SQLITE_CREATE_VIEW,
        sqlite3.SQLITE_DROP_INDEX,
        sqlite3.SQLITE_DROP_TABLE,
        sqlite3.SQLITE_DROP_TEMP_INDEX,
        sqlite3.SQLITE_DROP_TEMP_TABLE,
        sqlite3.SQLITE_DROP_TEMP_TRIGGER,
        sqlite3.SQLITE_DROP_TEMP_VIEW,
        sqlite3.SQLITE_DROP_TRIGGER,
        sqlite3.SQLITE_DROP_VIEW,
        sqlite3.SQLITE_ALTER_TABLE,
        sqlite3.SQLITE_REINDEX,
        sqlite3.SQLITE_ANALYZE,
        sqlite3.SQLITE_ATTACH,
        sqlite3.SQLITE_DETACH,
        sqlite3.SQLITE_TRANSACTION,
        sqlite3.SQLITE_SAVEPOINT,
    }
    return sqlite3.SQLITE_DENY if action_code in denied_actions else sqlite3.SQLITE_OK


def _validate_readonly_sql(query: str) -> Optional[str]:
    text = query.strip()
    if not text:
        return "query must be non-empty"
    if not re.match(r"(?is)^(select|with|explain(?:\s+query\s+plan)?)\b", text):
        return "Only SELECT, WITH and EXPLAIN QUERY PLAN are allowed"
    return None


@tool(parse_docstring=True)
def run_sql(
    query: str,
    export_csv: bool = False,
    preview_limit: int = 20,
) -> Dict[str, Any]:
    """Выполнить составленный агентом или переданный read-only SQL по ETL-хранилищу.

    Используй для read-only агрегаций, JOIN, UNION, подзапросов, оконных функций,
    произвольных выражений и других срезов, которых нет в готовых tools.
    Точные S2T-строки получай специализированными ролевыми tools; этот tool
    оставляй только нестандартным срезам, которых их схемы не выражают.
    Для стандартных списков source/target, их пересечения, объединения и разности
    выбирай list_s2t_table_names, а не этот tool.
    Если нестандартная аналитика всё же требует пересечения двух множеств, SQL
    должен явно доказать присутствие значения с обеих сторон через INNER JOIN,
    EXISTS, INTERSECT или GROUP BY с HAVING. UNION ALL с сортировкой по COUNT без
    HAVING не доказывает пересечение и может вернуть значение только из одного
    множества.
    Доступная пользовательская схема включает files, file_sheet_headers,
    source_tables, target_tables, source_columns, target_columns,
    additional_objects, pxf_to_a,
    s2t_transformations и data. Логические ETL-таблицы вида t_* не являются
    физическими таблицами хранилища: не выполняй для них системные команды и не пиши `FROM t_*`;
    ищи их имена в source_table/target_table и связанных строках.

    Таблица s2t_transformations глобальная: запросы к ней не должны содержать
    неявный фильтр по file_id или последней загрузке.
    Правила найденного lineage дочитывает сам trace_neo4j_lineage с
    include_transformation_rules=true. Не составляй SQL по transformation_id:
    числа из task или имён объектов не подтверждают происхождение ID.
    Условия по атрибутам S2T-строк применяй непосредственно к
    s2t_transformations. Не связывай её с каталогами или сырыми данными, если
    задача не требует сведений из этих таблиц.
    Не используй для самостоятельного построения lineage, путей и цепочек:
    это сценарий Neo4j. Не используй также для разбора переданного пользователем
    SQL-текста без выполнения: для этого предназначены parse_sql_column_lineage
    и parse_sql_table_lineage. Поддерживаются SELECT, WITH и EXPLAIN.
    Без CSV-экспорта возвращается не более MAX_INLINE_SQL_ROWS строк. При
    export_csv=True полный результат сохраняется в CSV, а модели возвращается
    только preview. Пустой rows означает пустой результат выполненного запроса,
    а не автоматически отсутствие соответствующего факта в других таблицах.
    query должен содержать настоящий SQL-текст: не передавай символы `\\n` как
    буквальные разделители вместо пробелов или реальных переносов строк.

    Args:
        query: Полный текст одного read-only SQL-запроса с реальными пробелами
            или переносами строк, без буквальных JSON-последовательностей `\\n`.
        export_csv: Сохранить полный результат в CSV.
        preview_limit: Число первых строк в preview, от 0 до 100.
    """
    from storage.database import get_db_connection
    from storage.postgres import is_postgres_error

    text = (query or "").strip()
    validation_error = _validate_readonly_sql(text)
    if validation_error:
        return {"error": validation_error, "query": text}

    conn = get_db_connection()
    try:
        if hasattr(conn, "set_read_only"):
            conn.set_read_only()
        else:
            conn.execute("PRAGMA query_only = ON")
        if hasattr(conn, "set_authorizer") and not hasattr(conn, "set_read_only"):
            conn.set_authorizer(_readonly_sql_authorizer)

        cursor = conn.cursor()
        cursor.execute(text)
        columns = [
            str(getattr(item, "name", None) or item[0])
            for item in (cursor.description or [])
        ]
        column_error = _result_column_error(columns)
        if column_error:
            return {
                "error": column_error,
                "query": text,
                "columns": columns,
            }

        if export_csv:
            preview_count = clamped_int(preview_limit, 20, 0, 100)
            return _write_sql_export_cursor(text, cursor, columns, preview_count)

        rows = cursor.fetchmany(MAX_INLINE_SQL_ROWS + 1)
        truncated = len(rows) > MAX_INLINE_SQL_ROWS
        visible_rows = rows[:MAX_INLINE_SQL_ROWS]
        return {
            "query": text,
            "columns": columns,
            "rows": [_row_dict(row, columns) for row in visible_rows],
            "returned_rows": len(visible_rows),
            "truncated": truncated,
            "max_inline_rows": MAX_INLINE_SQL_ROWS,
        }
    except Exception as exc:
        logger.exception("SQL execution failed")
        if isinstance(exc, sqlite3.Error) or is_postgres_error(exc):
            error = (
                "Exactly one SQL statement is allowed"
                if "one statement at a time" in str(exc).casefold()
                else "SQL query failed"
            )
            return {
                "error": error,
                "error_message": str(exc),
                "query": text,
            }
        return {"error": "SQL query failed", "query": text}
    finally:
        conn.close()
