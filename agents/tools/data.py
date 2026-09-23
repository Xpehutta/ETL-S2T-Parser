"""Tools for cell-level evidence and semantic description search."""

from __future__ import annotations

import json
import math
from array import array
from typing import Any, Dict, List, Literal, Optional

from langchain_core.tools import tool

from .common import clamped_int


def _header_name(headers_json: Optional[str], column_id: int) -> Optional[str]:
    try:
        headers = json.loads(headers_json or "[]")
    except (TypeError, json.JSONDecodeError):
        return None
    index = int(column_id) - 1
    for position, item in enumerate(headers):
        if not isinstance(item, dict):
            continue
        try:
            item_index = int(item.get("index", position))
        except (TypeError, ValueError):
            item_index = position
        if item_index != index:
            continue
        flat = str(item.get("flat") or "").strip()
        if flat:
            return flat
        path = item.get("path")
        if isinstance(path, list):
            return " > ".join(
                str(part) for part in path if part is not None and str(part).strip()
            ) or None
    return None


@tool(parse_docstring=True)
def search_excel_values(
    needle: str,
    file_id: Optional[int] = None,
    sheet_name: Optional[str] = None,
    column_name: Optional[str] = None,
    limit: int = 20,
) -> Dict[str, Any]:
    """Найти исходные значения ячеек в сохранённых строках Excel.

    Используй, когда пользователь ищет конкретное значение, код, имя объекта
    или текст внутри загруженных листов. Инструмент читает публичную таблицу
    data и возвращает доказуемое место значения: файл, лист, row_num,
    вычисленный номер строки Excel, column_id и настоящее имя колонки из
    file_sheet_headers.headers_json. Это только поиск буквального значения в
    исходных Excel-ячейках таблицы data: не поиск S2T-маппингов, не semantic
    search по описаниям и не доступ к физическим строкам логических ETL-таблиц.

    Поиск needle выполняется как Unicode-подстрока без учёта регистра по уже
    сохранённым значениям data. Фильтры sheet_name и column_name, напротив,
    требуют точного совпадения без учёта регистра; file_id ограничивает только
    файловый поиск и допустим здесь. Не используй tool для ответа «какие
    трансформации относятся к таблице»: там нужны search_s2t_transformations или
    read-only SQL по метаданным. Не передавай сюда transformation_rule и не
    используй найденные в нём имена для проверки строк внешней БД: tool не
    выполняет SQL. Если rows пуст, совпадение не найдено среди сохранённых ячеек с
    указанными фильтрами; это не доказывает отсутствие значения в исходном файле
    за пределами реально сохранённых данных.

    Args:
        needle: Непустая подстрока значения ячейки; сравнение без учёта регистра.
        file_id: Опциональный идентификатор конкретной загрузки.
        sheet_name: Опциональное точное имя листа без учёта регистра.
        column_name: Опциональное точное плоское имя колонки без учёта регистра.
        limit: Максимальное число совпадений, от 1 до 100.
    """
    text = str(needle or "").strip()
    if not text:
        return {"error": "needle must be non-empty", "rows": []}
    if len(text) > 500:
        return {"error": "needle too long", "rows": []}

    clean_limit = clamped_int(limit, 20, 1, 100)
    clean_file_id = int(file_id) if file_id is not None else None
    clean_sheet = str(sheet_name).strip() if sheet_name else None
    clean_column = str(column_name).strip() if column_name else None
    clauses = ["casefold_contains(data.value, ?) = 1"]
    params: List[Any] = [text]
    if clean_file_id is not None:
        clauses.append("headers.file_id = ?")
        params.append(clean_file_id)
    if clean_sheet:
        clauses.append("casefold_equal(headers.sheet_name, ?) = 1")
        params.append(clean_sheet)
    if clean_column:
        clauses.append(
            "casefold_equal(header_name(headers.headers_json, data.column_id), ?) = 1"
        )
        params.append(clean_column)
    params.append(clean_limit)

    from storage.database import get_db_connection

    conn = get_db_connection()
    try:
        conn.create_function(
            "casefold_contains",
            2,
            lambda value, part: int(
                str(part).casefold() in str(value).casefold()
            ) if value is not None and part is not None else 0,
        )
        conn.create_function(
            "casefold_equal",
            2,
            lambda left, right: int(
                str(left).strip().casefold() == str(right).strip().casefold()
            ) if left is not None and right is not None else 0,
        )
        conn.create_function("header_name", 2, _header_name)
        rows = conn.execute(
            f"""
            SELECT
                files.file_id,
                files.filename,
                headers.sheet_name,
                data.row_num,
                COALESCE(headers.header_start_row, 0)
                    + COALESCE(headers.header_rows_count, 0)
                    + data.row_num + 1 AS excel_row_number,
                data.column_id,
                header_name(headers.headers_json, data.column_id) AS column_name,
                data.value,
                COUNT(*) OVER () AS total_matches
            FROM data
            JOIN file_sheet_headers AS headers
              ON headers.file_id = data.file_id
             AND LOWER(headers.sheet_name) = LOWER(data.table_name)
            JOIN files ON files.file_id = headers.file_id
            WHERE {' AND '.join(clauses)}
            ORDER BY files.file_id, headers.sheet_name,
                     data.row_num, data.column_id, data.id
            LIMIT ?
            """,
            params,
        ).fetchall()
    finally:
        conn.close()

    total = int(rows[0]["total_matches"]) if rows else 0
    result_rows = []
    for row in rows:
        item = dict(row)
        item.pop("total_matches", None)
        result_rows.append(item)
    return {
        "query": text,
        "filters": {
            "file_id": clean_file_id,
            "sheet_name": clean_sheet,
            "column_name": clean_column,
        },
        "total_matches": total,
        "returned_rows": len(result_rows),
        "rows": result_rows,
    }


@tool(parse_docstring=True)
def get_excel_row(
    file_id: int,
    sheet_name: str,
    row_num: int,
    include_empty: bool = False,
    limit: int = 100,
) -> Dict[str, Any]:
    """Восстановить одну исходную строку листа по файлу, имени и row_num.

    Используй после search_excel_values либо при проверке происхождения записи
    S2T, каталога или structured metadata. Возвращает значения вместе с
    настоящими именами колонок и вычисленным однобазным номером строки Excel.
    row_num — сохранённый номер строки данных из таблицы data, а не номер строки
    интерфейса и не id записи.

    Передавай file_id, фактическое sheet_name и row_num из предыдущего
    результата; не угадывай другую загрузку или видимый номер Excel. Tool не
    ищет ближайшую строку. По умолчанию пропускает колонки без сохранённого
    значения, но include_empty=True восстанавливает их по распознанной структуре
    заголовков. Ошибка Stored row not found относится только к указанному файлу,
    листу и row_num.

    Args:
        file_id: Числовой идентификатор загрузки.
        sheet_name: Фактическое имя листа из результата поиска.
        row_num: Сохранённый нулевой номер строки данных в листе.
        include_empty: Включить распознанные колонки без сохранённого значения.
        limit: Максимальное число возвращаемых колонок, от 1 до 300.
    """
    clean_file_id = int(file_id)
    clean_sheet_name = str(sheet_name or "").strip()
    clean_row_num = int(row_num)
    clean_limit = clamped_int(limit, 100, 1, 300)

    from storage.database import get_columns_by_sheet, get_db_connection

    conn = get_db_connection()
    try:
        sheet = conn.execute(
            """
            SELECT files.file_id, files.filename, headers.sheet_name,
                   headers.header_start_row,
                   headers.header_rows_count
            FROM file_sheet_headers AS headers
            JOIN files ON files.file_id = headers.file_id
            WHERE headers.file_id = ?
              AND LOWER(TRIM(headers.sheet_name)) = LOWER(TRIM(?))
            """,
            (clean_file_id, clean_sheet_name),
        ).fetchone()
        values = conn.execute(
            """
            SELECT column_id, value
            FROM data
            WHERE file_id = ?
              AND LOWER(TRIM(table_name)) = LOWER(TRIM(?))
              AND row_num = ?
            ORDER BY column_id, id
            """,
            (clean_file_id, clean_sheet_name, clean_row_num),
        ).fetchall()
    finally:
        conn.close()
    if not sheet:
        return {
            "error": "Sheet not found",
            "file_id": clean_file_id,
            "sheet_name": clean_sheet_name,
            "cells": [],
        }
    if not values:
        return {
            "error": "Stored row not found",
            "file_id": clean_file_id,
            "sheet_name": clean_sheet_name,
            "row_num": clean_row_num,
            "cells": [],
        }

    value_by_column = {int(row["column_id"]): row["value"] for row in values}
    columns = get_columns_by_sheet(clean_file_id, str(sheet["sheet_name"]))
    cells = []
    for column in columns:
        column_id = int(column["column_id"])
        value = value_by_column.get(column_id)
        if value is None and not include_empty:
            continue
        cells.append(
            {
                "column_id": column_id,
                "column_index": int(column["column_index"]),
                "column_name": column["column_name_flat"],
                "value": value,
            }
        )
    total_cells = len(cells)
    cells = cells[:clean_limit]
    return {
        "file_id": int(sheet["file_id"]),
        "filename": sheet["filename"],
        "sheet_name": sheet["sheet_name"],
        "row_num": clean_row_num,
        "excel_row_number": (
            int(sheet["header_start_row"] or 0)
            + int(sheet["header_rows_count"] or 0)
            + clean_row_num
            + 1
        ),
        "total_cells": total_cells,
        "returned_cells": len(cells),
        "truncated": total_cells > len(cells),
        "cells": cells,
    }


def _float_vector(blob: Any) -> array:
    raw = bytes(blob)
    vector = array("f")
    if len(raw) % vector.itemsize:
        raise ValueError("Embedding blob length is not a multiple of float size")
    vector.frombytes(raw)
    if not vector:
        raise ValueError("Embedding vector must not be empty")
    if any(not math.isfinite(value) for value in vector):
        raise ValueError("Embedding vector contains NaN or infinity")
    return vector


def _cosine_similarity(left: array, right: array) -> float:
    if len(left) != len(right):
        raise ValueError("Embedding dimensions do not match")
    left_norm = math.sqrt(sum(value * value for value in left))
    right_norm = math.sqrt(sum(value * value for value in right))
    if (
        not math.isfinite(left_norm)
        or not math.isfinite(right_norm)
        or not left_norm
        or not right_norm
    ):
        raise ValueError("Embedding vector must be finite and non-zero")
    score = sum(a * b for a, b in zip(left, right)) / (left_norm * right_norm)
    if not math.isfinite(score):
        raise ValueError("Embedding similarity score must be finite")
    return score


@tool(parse_docstring=True)
def semantic_search_descriptions(
    query: str,
    scope: Literal[
        "all",
        "files",
        "tables",
        "source_tables",
        "target_tables",
        "columns",
        "source_columns",
        "target_columns",
    ] = "all",
    limit: int = 10,
    file_id: Optional[int] = None,
    table_name: Optional[str] = None,
    column_name: Optional[str] = None,
    data_type: Optional[str] = None,
    primary_key: Optional[bool] = None,
    not_null: Optional[bool] = None,
) -> Dict[str, Any]:
    """Найти файлы, логические таблицы и колонки по смысловой близости.

    Используй, когда task требует поиск по смыслу, бизнес-смыслу, назначению,
    описанию или наиболее вероятному соответствию, а точное имя файла, таблицы
    или колонки неизвестно. Передавай цельный запрос на естественном языке: не
    заменяй его поиском буквальной подстроки, набором ключевых слов, синонимов
    или переводов и не придумывай техническое имя. Поисковый домен scope:
    files — только файлы, tables — все логические таблицы, source_tables —
    только исходные таблицы, target_tables — только целевые таблицы, columns —
    все колонки, source_columns — только исходные колонки, target_columns —
    только целевые колонки, all — все перечисленные домены. Для колонковых
    scope можно сначала ограничить подвыборку структурными фильтрами, а затем
    ранжировать только её по смыслу описания.
    Инструмент кодирует запрос query-стороной подтверждённого embedding-профиля
    и до сравнения проверяет сохранённые model, revision, query/document
    encoding, нормализацию и размерность индекса. Затем считает cosine similarity
    по files, source_tables, target_tables, source_columns и target_columns. Это
    не поиск значений ячеек, точных S2T-имён и не lineage.

    Результат является ранжированием смысловой близости, а не подтверждением
    точного равенства имён. Для известного table_name используй
    summarize_table_descriptions; для текста в Excel — search_excel_values; для
    source → target — S2T/lineage tools. В выборку попадают только записи с уже
    сохранённым embedding. Пустой rows или отсутствие конкретного объекта не
    доказывает, что объекта нет в SQLite: у его описания мог не быть embedding.

    Args:
        query: Цельный смысловой запрос на естественном языке.
        scope: Домен: all, files, tables, source_tables, target_tables, columns,
            source_columns или target_columns.
        limit: Максимальное число результатов, от 1 до 50.
        file_id: Опциональный точный идентификатор загрузки для подвыборки.
        table_name: Точное имя таблицы; допустимо только для колонковых scope.
        column_name: Точное имя колонки; допустимо только для колонковых scope.
        data_type: Точный тип данных; допустимо только для колонковых scope.
        primary_key: Фильтр PK; допустим только для колонковых scope.
        not_null: Фильтр not-null; допустим только для колонковых scope.
    """
    text = str(query or "").strip()
    if not text:
        return {"error": "query must be non-empty", "rows": []}
    if len(text) > 1000:
        return {"error": "query too long", "rows": []}
    clean_limit = clamped_int(limit, 10, 1, 50)
    column_scopes = {"columns", "source_columns", "target_columns"}
    column_filters = {
        "table_name": table_name,
        "column_name": column_name,
        "data_type": data_type,
        "primary_key": primary_key,
        "not_null": not_null,
    }
    if scope not in column_scopes and any(
        value is not None for value in column_filters.values()
    ):
        return {
            "error": "column subset filters require a column scope",
            "scope": scope,
            "rows": [],
        }
    try:
        clean_file_id = int(file_id) if file_id is not None else None
    except (TypeError, ValueError):
        return {"error": "file_id must be an integer", "rows": []}
    if clean_file_id is not None and clean_file_id <= 0:
        return {"error": "file_id must be positive", "rows": []}
    clean_column_filters: Dict[str, Any] = {}
    for field in ("table_name", "column_name", "data_type"):
        value = column_filters[field]
        if value is None:
            continue
        clean_value = str(value).strip()
        if not clean_value:
            continue
        if len(clean_value) > 300:
            return {"error": f"{field} too long", "rows": []}
        clean_column_filters[field] = clean_value
    for field in ("primary_key", "not_null"):
        value = column_filters[field]
        if value is not None:
            clean_column_filters[field] = value

    from services.embeddings import (
        embed_query,
        embedding_index_identity_for_blobs,
    )
    from storage.database import get_db_connection
    from storage.embedding_index import (
        EmbeddingIndexCompatibilityError,
        validate_embedding_index,
    )

    try:
        query_blob = embed_query(text)
        query_vector = _float_vector(query_blob)
        runtime_identity = embedding_index_identity_for_blobs([query_blob])
    except (TypeError, ValueError, OverflowError) as exc:
        return {
            "error": "Query embedding is invalid",
            "error_message": str(exc),
            "rows": [],
        }
    conn = get_db_connection()
    try:
        try:
            stored_identity = validate_embedding_index(
                conn.cursor(), runtime_identity
            )
        except EmbeddingIndexCompatibilityError as exc:
            return {
                "error": str(exc),
                "error_code": exc.error_code,
                "rows": [],
            }
        candidates = []
        if scope in ("all", "files"):
            file_where = "WHERE description_embedding IS NOT NULL"
            file_params: List[Any] = []
            if clean_file_id is not None:
                file_where += " AND file_id = ?"
                file_params.append(clean_file_id)
            candidates.extend(
                dict(row)
                for row in conn.execute(
                    f"""
                    SELECT 'files' AS scope, file_id AS record_id, file_id,
                           filename, NULL AS sheet_name,
                           filename AS name, description, description_embedding
                    FROM files
                    {file_where}
                    """,
                    file_params,
                ).fetchall()
            )
        for catalog_name in ("source_tables", "target_tables"):
            if scope not in ("all", "tables", catalog_name):
                continue
            table_where = "WHERE catalog.description_embedding IS NOT NULL"
            table_params: List[Any] = []
            if clean_file_id is not None:
                table_where += " AND catalog.file_id = ?"
                table_params.append(clean_file_id)
            candidates.extend(
                dict(row)
                for row in conn.execute(
                    f"""
                    SELECT '{catalog_name}' AS scope, catalog.id AS record_id,
                           catalog.file_id, files.filename, catalog.sheet_name,
                           catalog.table_name AS name,
                           catalog.description, catalog.description_embedding
                    FROM {catalog_name} AS catalog
                    LEFT JOIN files ON files.file_id = catalog.file_id
                    {table_where}
                    """,
                    table_params,
                ).fetchall()
            )
        for catalog_name in ("source_columns", "target_columns"):
            if scope not in ("all", "columns", catalog_name):
                continue
            conditions = ["catalog.description_embedding IS NOT NULL"]
            column_params: List[Any] = []
            if clean_file_id is not None:
                conditions.append("catalog.file_id = ?")
                column_params.append(clean_file_id)
            for field in ("table_name", "column_name", "data_type"):
                value = clean_column_filters.get(field)
                if value is None:
                    continue
                conditions.append(
                    f"LOWER(TRIM(catalog.{field})) = LOWER(TRIM(?))"
                )
                column_params.append(value)
            for field in ("primary_key", "not_null"):
                if field not in clean_column_filters:
                    continue
                conditions.append(f"catalog.{field} = ?")
                column_params.append(1 if clean_column_filters[field] else 0)
            column_where = "WHERE " + " AND ".join(conditions)
            candidates.extend(
                dict(row)
                for row in conn.execute(
                    f"""
                    SELECT '{catalog_name}' AS scope, catalog.id AS record_id,
                           catalog.file_id, files.filename, catalog.sheet_name,
                           catalog.column_name AS name, catalog.table_name,
                           catalog.column_name,
                           catalog.description, catalog.description_embedding
                    FROM {catalog_name} AS catalog
                    LEFT JOIN files ON files.file_id = catalog.file_id
                    {column_where}
                    """,
                    column_params,
                ).fetchall()
            )
    finally:
        conn.close()

    ranked = []
    for candidate in candidates:
        blob = candidate.pop("description_embedding")
        try:
            vector = _float_vector(blob)
            score = _cosine_similarity(query_vector, vector)
        except (TypeError, ValueError, OverflowError) as exc:
            return {
                "error": "Stored embedding index is corrupted or incompatible",
                "error_message": str(exc),
                "scope": candidate.get("scope"),
                "record_id": candidate.get("record_id"),
                "rows": [],
            }
        candidate["score"] = round(score, 6)
        ranked.append(candidate)
    ranked.sort(key=lambda item: (-item["score"], item["scope"], item["record_id"]))
    rows = ranked[:clean_limit]
    truncated = len(ranked) > len(rows)
    return {
        "query": text,
        "scope": scope,
        "subset": {
            **({"file_id": clean_file_id} if clean_file_id is not None else {}),
            **clean_column_filters,
        },
        "embedding_model": runtime_identity.model_name,
        "embedding_model_revision": runtime_identity.model_revision or None,
        "embedding_profile": runtime_identity.profile_id,
        "embedding_dimension": runtime_identity.dimension,
        "embedding_index_registered": stored_identity is not None,
        "total_candidates": len(ranked),
        "returned_rows": len(rows),
        "coverage": "truncated" if truncated else "complete",
        "truncated": truncated,
        "rows": rows,
    }


__all__ = [
    "get_excel_row",
    "search_excel_values",
    "semantic_search_descriptions",
]
