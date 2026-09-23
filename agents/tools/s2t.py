"""Tools for the common ETL transformation table and table catalogs."""

from typing import Any, Dict, List, Literal, Optional

from langchain_core.tools import tool

from .common import clamped_int, pack_tabular_rows


def _read_s2t_rules_by_ids(
    transformation_ids: List[int],
) -> Dict[str, Any]:
    """Read exact S2T rows for trusted transformation identifiers."""
    clean_ids: List[int] = []
    for value in transformation_ids or []:
        try:
            clean_value = int(value)
        except (TypeError, ValueError):
            continue
        if clean_value > 0 and clean_value not in clean_ids:
            clean_ids.append(clean_value)
        if len(clean_ids) >= 100:
            break
    if not clean_ids:
        return {
            "error": "transformation_ids must contain at least one positive id",
            "requested_ids": [],
            "missing_ids": [],
            "rows": [],
        }

    from storage.database import (
        S2T_TRANSFORMATION_COLUMNS,
        get_db_connection,
    )

    columns = list(S2T_TRANSFORMATION_COLUMNS)
    placeholders = ", ".join("?" for _ in clean_ids)
    with get_db_connection() as conn:
        rows = conn.execute(
            f"SELECT {', '.join(columns)} FROM s2t_transformations "
            f"WHERE id IN ({placeholders})",
            clean_ids,
        ).fetchall()
    rows_by_id = {int(row["id"]): dict(row) for row in rows}
    ordered_rows = [rows_by_id[item] for item in clean_ids if item in rows_by_id]
    return {
        "columns": columns,
        "rows": ordered_rows,
        "requested_ids": clean_ids,
        "missing_ids": [item for item in clean_ids if item not in rows_by_id],
        "returned_rows": len(ordered_rows),
    }


@tool(parse_docstring=True)
def get_s2t_rules_by_ids(
    transformation_ids: List[int],
) -> Dict[str, Any]:
    """Получить точные S2T-правила по transformation_id из lineage.

    Используй после trace_neo4j_lineage, когда его результат уже содержит
    transformation_id и для impact-ответа нужны соответствующие
    transformation_rule. Передай найденные идентификаторы как список чисел без
    SQL и без повторного поиска по именам. Neo4j transformation_id соответствует
    первичному ключу s2t_transformations.id; tool выполняет это сопоставление сам.
    Читает глобальную s2t_transformations без неявного file_id и возвращает
    точные строки, отсутствующие id перечисляет отдельно.

    Args:
        transformation_ids: Непустой список числовых transformation_id,
            дословно полученных из trace_neo4j_lineage; максимум 100 id.
    """
    return _read_s2t_rules_by_ids(transformation_ids)


@tool(parse_docstring=True)
def list_s2t_table_mapping(
    source_table: str,
    target_table: str,
    limit: int = 1000,
) -> Dict[str, Any]:
    """Получить полный S2T-маппинг между двумя точными таблицами.

    Используй, когда task явно задаёт направление source_table → target_table
    и просит перечислить их source/target columns или transformation rules.
    Узкий контракт намеренно не принимает field-фильтры и q, поэтому имена
    таблиц нельзя случайно передать в роли полей. Читает глобальную
    s2t_transformations без неявного file_id и сохраняет исходные дубликаты.

    Args:
        source_table: Точное полное имя исходной S2T-таблицы.
        target_table: Точное полное имя целевой S2T-таблицы.
        limit: Максимальное число возвращаемых строк, от 1 до 1000.
    """
    clean_source_table = str(source_table or "").strip()
    clean_target_table = str(target_table or "").strip()
    if not clean_source_table or not clean_target_table:
        return {
            "error": "source_table and target_table must be non-empty",
            "rows": [],
        }
    from storage.s2t import list_s2t_transformations as db_list_s2t_transformations

    return db_list_s2t_transformations(
        file_id=None,
        limit=clamped_int(limit, 1000, minimum=1, maximum=1000),
        source_table=clean_source_table,
        target_table=clean_target_table,
    )


@tool(parse_docstring=True)
def list_s2t_field_mapping(
    source_table: str,
    source_field: str,
    target_table: str,
    target_field: str,
) -> Dict[str, Any]:
    """Получить одну точную ролевую S2T-пару полей со всеми дублями.

    Все четыре роли обязательны, поэтому planner не может опустить таблицу или
    перепутать имя поля с именем таблицы. Используй для уже известной связи
    ``source_table.source_field → target_table.target_field`` и её полного
    ``transformation_rule``. Глобальную S2T tool не ограничивает ``file_id``.

    Args:
        source_table: Точное полное имя исходной S2T-таблицы.
        source_field: Точное имя исходного поля без имени таблицы.
        target_table: Точное полное имя целевой S2T-таблицы.
        target_field: Точное имя целевого поля без имени таблицы.
    """
    return _list_narrow_s2t_rows(
        source_table=source_table,
        source_field=source_field,
        target_table=target_table,
        target_field=target_field,
    )


@tool(parse_docstring=True)
def read_s2t_mapping(
    source_table: str,
    target_table: str,
) -> Dict[str, Any]:
    """Прочитать полный exact S2T-маппинг заданной source→target-пары.

    Обе таблицы обязательны; field/ID/file-фильтров намеренно нет. Возвращает
    все raw-строки пары без лимита и дедупликации, включая provenance
    ``file_id``/``sheet_name``/``row_num``. Точные поля выбираются upstream из
    полного набора. Формат lossless: позиции задаёт ``columns``, словарные
    значения — 0-based индексы в ``dictionaries``. Это transport references,
    не transformation/group ID; каждый дубль остаётся отдельной row.

    Args:
        source_table: Точное полное имя исходной S2T-таблицы.
        target_table: Точное полное имя целевой S2T-таблицы.
    """
    result = _list_narrow_s2t_rows(
        source_table=source_table,
        target_table=target_table,
    )
    return _compact_s2t_result(result)


@tool(parse_docstring=True)
def read_s2t_source_to_target(
    source_table: str,
    target_table: str,
) -> Dict[str, Any]:
    """Прочитать все S2T-строки точной направленной пары таблиц.

    Используй только когда известны обе роли: ``source_table`` и
    ``target_table``. Tool не принимает поля, ID, file scope или limit,
    возвращает все исходные строки без дедупликации вместе с provenance.
    Формат lossless: позиции задаёт ``columns``, повторяющиеся значения могут
    быть 0-based индексами в ``dictionaries``. Эти индексы не являются domain
    ID; каждая positional row остаётся отдельной исходной строкой.

    Args:
        source_table: Точное полное имя исходной S2T-таблицы.
        target_table: Точное полное имя целевой S2T-таблицы.
    """
    result = _list_narrow_s2t_rows(
        source_table=source_table,
        target_table=target_table,
    )
    return _compact_s2t_result(result)


@tool(parse_docstring=True)
def read_s2t_by_source_table(
    source_table: str,
) -> Dict[str, Any]:
    """Прочитать все S2T-строки одной точной source_table.

    Используй для source-only задачи, когда ``target_table`` не задана. Tool
    фиксирует source-роль сигнатурой, не принимает поля, ID, file scope или
    limit и сохраняет provenance, порядок и исходные дубли. Формат lossless:
    позиции задаёт ``columns``, а словарные значения — 0-based индексы в
    ``dictionaries``; это transport references, не domain ID.

    Args:
        source_table: Точное полное имя исходной S2T-таблицы.
    """
    result = _list_narrow_s2t_rows(source_table=source_table)
    return _compact_s2t_result(result)


@tool(parse_docstring=True)
def read_s2t_by_target_table(
    target_table: str,
) -> Dict[str, Any]:
    """Прочитать все S2T-строки одной точной target_table.

    Используй для target-only задачи, когда ``source_table`` не задана. Tool
    фиксирует target-роль сигнатурой, не принимает поля, ID, file scope или
    limit и сохраняет provenance, порядок и исходные дубли. Формат lossless:
    позиции задаёт ``columns``, а словарные значения — 0-based индексы в
    ``dictionaries``; это transport references, не domain ID.

    Args:
        target_table: Точное полное имя целевой S2T-таблицы.
    """
    result = _list_narrow_s2t_rows(target_table=target_table)
    return _compact_s2t_result(result)


@tool(parse_docstring=True)
def list_s2t_occurrences(
    table_name: str,
) -> Dict[str, Any]:
    """Прочитать точное имя S2T-таблицы сразу в source и target-ролях.

    Для одной таблицы делает два точных ролевых чтения и помечает occurrence
    полем ``matched_role``. Field/ID/file-фильтров нет; возвращаются все строки
    и provenance. Дубли сохраняются, self-match возвращается дважды. Формат
    lossless: позиции задаёт ``columns``, словарные значения — 0-based индексы
    в ``dictionaries``. Индексы — transport references, не domain IDs.

    Args:
        table_name: Точное полное имя S2T-таблицы без имени поля.
    """
    clean_table = str(table_name or "").strip()
    if not clean_table:
        return {"error": "table_name must be non-empty", "rows": []}

    from storage.database import S2T_RECORD_FIELDS
    from storage.s2t import list_s2t_transformations as db_list_s2t_transformations

    result_columns = ["file_id", "sheet_name", "row_num", *S2T_RECORD_FIELDS]

    role_results: Dict[str, Dict[str, Any]] = {}
    rows: List[Dict[str, Any]] = []
    columns: List[str] = []
    for role in ("source", "target"):
        filters: Dict[str, str] = {f"{role}_table": clean_table}
        result = db_list_s2t_transformations(
            file_id=None,
            limit=None,
            columns=result_columns,
            **filters,
        )
        role_results[role] = result
        if not columns:
            columns = list(result.get("columns") or [])
        rows.extend(
            {**dict(row), "matched_role": role}
            for row in (result.get("rows") or [])
        )

    if "matched_role" not in columns:
        columns.append("matched_role")
    role_counts = {
        role: len(result.get("rows") or [])
        for role, result in role_results.items()
    }
    raw_result = {
        "scope": "global_s2t_occurrences",
        "filters": {"table_name": clean_table},
        "columns": columns,
        "role_counts": role_counts,
        "total_matches": sum(role_counts.values()),
        "returned_rows": len(rows),
        "truncated": False,
        "rows": rows,
    }
    return _compact_s2t_result(raw_result, include_matched_role=True)


def _list_narrow_s2t_rows(**filters: str) -> Dict[str, Any]:
    """Read every S2T row matching required exact role filters."""
    clean_filters = {
        name: str(value or "").strip()
        for name, value in filters.items()
    }
    missing = [name for name, value in clean_filters.items() if not value]
    if missing:
        return {
            "error": "Required S2T filters must be non-empty: "
            + ", ".join(missing),
            "rows": [],
        }
    from storage.database import S2T_RECORD_FIELDS
    from storage.s2t import list_s2t_transformations as db_list_s2t_transformations

    result = db_list_s2t_transformations(
        file_id=None,
        limit=None,
        columns=["file_id", "sheet_name", "row_num", *S2T_RECORD_FIELDS],
        **clean_filters,
    )
    rows = list(result.get("rows") or [])
    result["returned_rows"] = len(rows)
    result["truncated"] = False
    return result


def _compact_s2t_result(
    result: Dict[str, Any],
    *,
    include_matched_role: bool = False,
) -> Dict[str, Any]:
    """Pack a complete S2T result without collapsing raw occurrences."""
    if result.get("error"):
        return result
    columns = [
        "file_id",
        "sheet_name",
        "row_num",
        "source_table",
        "source_field",
        "target_table",
        "target_field",
        "transformation_rule",
        "source_layer",
        "target_layer",
    ]
    if include_matched_role:
        columns.insert(3, "matched_role")
    dictionary_columns = [
        column
        for column in (
            "sheet_name",
            "matched_role",
            "source_table",
            "target_table",
            "transformation_rule",
            "source_layer",
            "target_layer",
        )
        if column in columns
    ]
    raw_rows = [dict(row) for row in (result.get("rows") or [])]
    packed = pack_tabular_rows(
        raw_rows,
        columns=columns,
        dictionary_columns=dictionary_columns,
    )
    packed_rows = packed.pop("rows")
    compact: Dict[str, Any] = {
        "scope": result.get("scope") or "global",
        "filters": dict(result.get("filters") or {}),
        **packed,
    }
    if include_matched_role:
        compact["role_counts"] = dict(result.get("role_counts") or {})
    total_matches = result.get(
        "total_matches",
        result.get("total", len(raw_rows)),
    )
    compact.update(
        total_matches=int(total_matches or 0),
        returned_rows=len(raw_rows),
        truncated=False,
        rows=packed_rows,
    )
    return compact


@tool(parse_docstring=True)
def list_s2t_source_table(source_table: str) -> Dict[str, Any]:
    """Получить все S2T-строки одной точной исходной таблицы.

    Args:
        source_table: Точное полное имя исходной S2T-таблицы.
    """
    return _list_narrow_s2t_rows(source_table=source_table)


@tool(parse_docstring=True)
def list_s2t_target_table(target_table: str) -> Dict[str, Any]:
    """Получить все S2T-строки одной точной целевой таблицы.

    Args:
        target_table: Точное полное имя целевой S2T-таблицы.
    """
    return _list_narrow_s2t_rows(target_table=target_table)


@tool(parse_docstring=True)
def list_s2t_source_field(
    source_table: str,
    source_field: str,
) -> Dict[str, Any]:
    """Прочитать все цели одного точного исходного S2T-поля.

    Обе source-роли обязательны. Tool читает глобальную
    ``s2t_transformations`` без ``file_id`` и лимита, возвращает все
    совпавшие исходные строки с provenance и не дедуплицирует одинаковые
    target-пары. Используй его, когда известна точная
    ``source_table.source_field``, а требуется полный набор downstream-целей.

    Args:
        source_table: Точное полное имя исходной S2T-таблицы.
        source_field: Точное имя исходного поля без имени таблицы.
    """
    return _list_narrow_s2t_rows(
        source_table=source_table,
        source_field=source_field,
    )


@tool(parse_docstring=True)
def list_s2t_target_field(
    target_table: str,
    target_field: str,
) -> Dict[str, Any]:
    """Прочитать все источники одного точного целевого S2T-поля.

    Обе target-роли обязательны. Tool читает глобальную
    ``s2t_transformations`` без ``file_id`` и лимита, возвращает все
    совпавшие исходные строки с provenance и не дедуплицирует одинаковые
    source-пары. Используй его, когда известна точная
    ``target_table.target_field``, а требуется полный набор upstream-источников.

    Args:
        target_table: Точное полное имя целевой S2T-таблицы.
        target_field: Точное имя целевого поля без имени таблицы.
    """
    return _list_narrow_s2t_rows(
        target_table=target_table,
        target_field=target_field,
    )


@tool(parse_docstring=True)
def list_s2t_transformations(
    limit: int = 200,
    columns: Optional[List[str]] = None,
    target_table: Optional[str] = None,
    source_table: Optional[str] = None,
    target_field: Optional[str] = None,
    source_field: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Получить строки S2T с точными ролевыми фильтрами и выбранными колонками.

    Выбирай, если известна точная полная пара
    source_table.source_field → target_table.target_field, в том числе в
    запросе «найди», «покажи», «объясни» или «проанализируй сохранённую
    трансформацию». Этот tool сначала получает фактическую строку и
    transformation_rule; требуемую интерпретацию затем выполняет внутренний
    analyze. Также выбирай, если известны точные source/target table или field
    либо нужно вернуть конкретные columns, например только transformation_rule. Для
    точного маппинга обязательно передавай source_table, source_field,
    target_table и target_field отдельными аргументами;
    неполное или неквалифицированное имя сначала разрешай через
    search_s2t_transformations. Набор полей из семантического поиска ещё не
    является подтверждёнными S2T-ролями: не перебирай кандидатов этим tool по
    одному, сначала сузь их через search_s2t_transformations. Ролевые условия передавай только отдельными
    аргументами, не объединённой строкой.
    Если task явно называет target_table/target_field или
    source_table/source_field, передавай все названные ролевые фильтры; не
    заменяй их search_s2t_transformations и не опускай table-фильтр.
    Подстрочный поиск этот tool не выполняет: для него используй
    search_s2t_transformations. Tool не принимает file_id: конкретный file_id
    из запроса не переносится на глобальную s2t_transformations.
    Без columns возвращает row_num и все поля S2T.
    Читает глобальную s2t_transformations без file_id и сохраняет дубликаты.
    Не строит графовый путь и не выполняет агрегации.

    Args:
        limit: Максимальное число возвращаемых строк, от 1 до 1000; по умолчанию 200.
        columns: Точные имена возвращаемых колонок; null означает все колонки.
        target_table: Опциональное точное имя целевой таблицы без имени поля и операторов.
        source_table: Опциональное точное имя исходной таблицы без имени поля и операторов.
        target_field: Опциональное точное имя целевого поля без имени таблицы и операторов.
        source_field: Опциональное точное имя исходного поля без имени таблицы и операторов.
    """
    from storage.s2t import list_s2t_transformations as db_list_s2t_transformations

    clean_limit = max(1, min(int(limit or 200), 1000))
    return db_list_s2t_transformations(
        file_id=None,
        limit=clean_limit,
        q=None,
        columns=columns,
        target_table=target_table,
        source_table=source_table,
        target_field=target_field,
        source_field=source_field,
    )


@tool(parse_docstring=True)
def search_s2t_transformations(
    needle: Optional[str] = None,
    needles: Optional[List[str]] = None,
    limit: int = 20,
) -> Dict[str, Any]:
    """
    Найти строки S2T по одной или нескольким подстрокам во всех полях.

    Выбирай, когда роль искомого значения неизвестна либо известная таблица
    названа неполным или неквалифицированным именем. Во втором случае сначала
    разреши по результату точное полное source_table/target_table, затем передай
    его специализированному tool следующего шага. Для набора технических полей
    из semantic_search_descriptions или прошлого табличного результата передай
    все различающиеся имена одним списком needles. Не заменяй их общей
    подстрокой или исходным бизнес-термином. Каждая строка S2T возвращается один
    раз, даже если совпала с несколькими needles; исходные дубликаты S2T
    сохраняются. Если уже известна направленная пара таблиц, используй exact
    directed mapping-reader активной палитры; если известна только одна таблица
    или поле, используй exact occurrence/role reader. Использование точного
    target_table/target_field или source_table/source_field как needle
    запрещено: это ролевые фильтры, а не подстрочный поиск. Полная точная
    source_table.source_field → target_table.target_field всегда сначала
    читается directed exact reader по обеим таблицам; точные поля выбираются из
    полного результата, даже если пользователь просит объяснение правила.
    Это не семантический поиск: каждый needle проверяется как подстрока в
    target/source table, field, layer и
    transformation_rule. Читает глобальную s2t_transformations без file_id,
    сохраняет дубликаты и возвращает только первые limit совпадений всего.

    Args:
        needle: Одна подстрока для совместимого одиночного вызова.
        needles: Несколько подстрок из одного прошлого результата; максимум 50.
        limit: Общий максимум возвращаемых совпадений, от 1 до 100.
    """
    queries = list(
        dict.fromkeys(
            text
            for item in ([needle] if needle is not None else [])
            + list(needles or [])
            if (text := str(item or "").strip())
        )
    )
    if not queries:
        return {
            "error": "needle or needles must contain a non-empty value",
            "queries": [],
            "total": 0,
            "rows": [],
        }
    if len(queries) > 50:
        return {
            "error": "needles supports at most 50 distinct values",
            "queries": queries[:50],
            "total": 0,
            "rows": [],
        }
    if any(len(item) > 200 for item in queries):
        return {
            "error": "each needle must contain at most 200 characters",
            "queries": queries,
            "total": 0,
            "rows": [],
        }

    from storage.s2t import list_s2t_transformations as db_list_s2t_transformations

    clean_limit = max(1, min(int(limit or 20), 100))
    data = db_list_s2t_transformations(
        file_id=None,
        limit=clean_limit,
        q=queries[0] if len(queries) == 1 else None,
        q_any=queries if len(queries) > 1 else None,
    )
    if len(queries) == 1:
        data["query"] = queries[0]
    else:
        data["queries"] = queries
    data["searched_table"] = "s2t_transformations"
    from storage.database import S2T_RECORD_FIELDS

    data["searched_columns"] = list(S2T_RECORD_FIELDS)
    if len(queries) > 1:
        for row in data.get("rows", []):
            searchable = " ".join(
                str(row.get(field) or "")
                for field in S2T_RECORD_FIELDS
            ).casefold()
            row["matched_needles"] = [
                item for item in queries if item.casefold() in searchable
            ]
    return data


@tool(parse_docstring=True)
def list_s2t_table_names(
    set_operation: Literal[
        "sources",
        "targets",
        "intersection",
        "source_only",
        "target_only",
        "union",
    ],
    limit: int,
) -> Dict[str, Any]:
    """
    Получить уникальные имена S2T-таблиц по принадлежности ролям source/target.

    Это детерминированный инструмент для компактных списков источников,
    приёмников и операций над двумя глобальными множествами; он не фильтрует
    source_table по конкретному target_table и наоборот: для такого условия
    выбирай run_sql с DISTINCT. Для операций над полными глобальными множествами
    не проси planner писать эквивалентный SQL через run_sql. intersection означает, что имя встречается
    хотя бы в одной строке как source_table и хотя бы в одной строке как
    target_table. Это не требует одной и той же строки, общего соседа или
    двунаправленного графового ребра. source_only и target_only возвращают
    разности множеств, union — их объединение, sources и targets — одно
    соответствующее множество. source_only уже гарантирует отсутствие имени в
    target_table, поэтому после него не вызывай target_only или run_sql для
    повторной проверки; для target_only действует симметричное правило.

    Внешние пробелы удаляются, NULL и пустые имена исключаются, дубликаты
    сворачиваются, результат сортируется по имени. Инструмент всегда читает
    глобальную s2t_transformations и не принимает file_id. Используй
    summarize_s2t_tables, если нужны количества маппингов, полей или связанных
    таблиц; run_sql оставляй для нестандартных фильтров и аналитики, не покрытых
    этими операциями. Пустой rows означает, что выбранное множество пусто.

    Args:
        set_operation: sources или targets для одной роли; intersection для имён
            в обеих ролях; source_only или target_only для разности; union для
            объединения ролей.
        limit: Максимальное число уникальных имён, от 1 до 200. Если пользователь
            явно указал число N, передай ровно N, не заменяя его значением по
            умолчанию или верхней границей диапазона.
    """
    from storage.s2t import list_s2t_table_names as db_list_s2t_table_names

    return db_list_s2t_table_names(
        set_operation=set_operation,
        limit=limit,
    )


@tool(parse_docstring=True)
def summarize_s2t_tables(
    group_by: Literal["source", "target"] = "target",
    min_related_tables: int = 1,
    limit: int = 100,
) -> Dict[str, Any]:
    """
    Агрегировать показатели колоночных маппингов по источникам или приёмникам.

    Для компактных списков и операций над множествами ролей не используй этот
    инструмент: вызывай list_s2t_table_names.
    Используй для табличных подсчётов и сводок source → target в SQLite, а не для
    lineage, путей, цепочек зависимостей или других графовых обходов Neo4j.
    Для каждой логической таблицы считает строки маппинга, представленные поля,
    связанные таблицы противоположной роли и заполненность правил трансформации.
    Это агрегатор структуры s2t_transformations, а не поиск текстовых описаний из
    каталогов source_tables и target_tables. Всегда анализирует глобальную таблицу
    без ограничения по file_id.

    Используй для вопросов «какие target/source таблицы самые связанные»,
    «сколько маппингов и правил у каждой таблицы» и для компактного обзора вместо
    выгрузки сотен строк. group_by определяет основную роль результата, а
    min_related_tables фильтрует группы по числу уникальных таблиц другой роли.
    Не используй как доказательство многошагового пути: агрегированные соседи не
    задают порядок цепочки. Пустой rows означает, что текущим условиям агрегации
    не соответствует ни одна группа глобальной s2t_transformations.

    Args:
        group_by: source для источников или target для приёмников.
        min_related_tables: Минимальное число связанных таблиц противоположной роли.
        limit: Максимальное число агрегированных групп, от 1 до 200.
    """
    from storage.s2t import summarize_s2t_transformations

    data = summarize_s2t_transformations(
        group_by=group_by,
        file_id=None,
        min_related_tables=min_related_tables,
        limit=limit,
    )
    data.pop("file_id", None)
    data["scope"] = "global"
    return data


@tool(parse_docstring=True)
def summarize_table_descriptions(
    table_name: str,
    file_id: Optional[int] = None,
    limit: int = 50,
) -> Dict[str, Any]:
    """
    Собрать описания одной логической ETL-таблицы из обоих каталогов.

    Ищет точное имя без учёта регистра и внешних пробелов одновременно в
    source_tables и target_tables через UNION ALL. Возвращает роли, исходные строки
    и объединённый список непустых описаний. Одинаковые записи сохраняются отдельно;
    итоговое краткое русское описание формирует planner по полученным фактам.

    Используй, когда пользователь уже назвал точное логическое table_name и
    спрашивает его назначение или описание. Для неизвестного имени и смыслового
    запроса используй semantic_search_descriptions; для S2T-маппингов этой
    таблицы — search_s2t_transformations. file_id допустим только если
    пользователь явно спрашивает каталог конкретной загрузки, и не переносится
    на глобальные S2T-tools. Пустой matches означает отсутствие точного имени в
    каталогах source_tables/target_tables, но не доказывает отсутствие имени в
    s2t_transformations.

    Args:
        table_name: Точное логическое имя таблицы, например t_agr_cred.
        file_id: Опциональный числовой идентификатор загрузки для ограничения поиска.
        limit: Максимальное число исходных строк, возвращаемых planner-у, от 1 до 100.
    """
    clean_name = str(table_name or "").strip()
    if not clean_name:
        return {
            "error": "table_name must be non-empty",
            "table_name": table_name,
            "matches": [],
            "total_matches": 0,
        }
    if len(clean_name) > 300:
        return {
            "error": "table_name too long",
            "table_name": clean_name,
            "matches": [],
            "total_matches": 0,
        }

    clean_file_id = int(file_id) if file_id is not None else None
    clean_limit = clamped_int(limit, 50, 1, 100)
    scope_sql = " AND catalog.file_id = ?" if clean_file_id is not None else ""
    params: List[Any] = [clean_name]
    if clean_file_id is not None:
        params.append(clean_file_id)
    params.append(clean_name)
    if clean_file_id is not None:
        params.append(clean_file_id)
    params.append(clean_limit)

    query = f"""
        WITH matched AS (
            SELECT
                catalog.id AS catalog_id,
                catalog.file_id,
                files.filename,
                catalog.sheet_name,
                catalog.row_num,
                'source' AS table_role,
                catalog.table_name,
                catalog.description
            FROM source_tables AS catalog
            LEFT JOIN files ON files.file_id = catalog.file_id
            WHERE LOWER(TRIM(catalog.table_name)) = LOWER(TRIM(?))
              {scope_sql}

            UNION ALL

            SELECT
                catalog.id AS catalog_id,
                catalog.file_id,
                files.filename,
                catalog.sheet_name,
                catalog.row_num,
                'target' AS table_role,
                catalog.table_name,
                catalog.description
            FROM target_tables AS catalog
            LEFT JOIN files ON files.file_id = catalog.file_id
            WHERE LOWER(TRIM(catalog.table_name)) = LOWER(TRIM(?))
              {scope_sql}
        )
        SELECT
            catalog_id,
            file_id,
            filename,
            sheet_name,
            row_num,
            table_role,
            table_name,
            description,
            COUNT(*) OVER () AS total_matches,
            SUM(CASE WHEN table_role = 'source' THEN 1 ELSE 0 END)
                OVER () AS source_matches,
            SUM(CASE WHEN table_role = 'target' THEN 1 ELSE 0 END)
                OVER () AS target_matches,
            SUM(CASE WHEN NULLIF(TRIM(description), '') IS NOT NULL THEN 1 ELSE 0 END)
                OVER () AS descriptions_present
        FROM matched
        ORDER BY table_role, file_id, sheet_name, row_num, catalog_id
        LIMIT ?
    """

    from storage.database import get_db_connection

    conn = get_db_connection()
    try:
        fetched = conn.execute(query, params).fetchall()
    finally:
        conn.close()

    total_matches = int(fetched[0]["total_matches"]) if fetched else 0
    role_counts = {
        "source": int(fetched[0]["source_matches"] or 0) if fetched else 0,
        "target": int(fetched[0]["target_matches"] or 0) if fetched else 0,
    }
    descriptions_present = (
        int(fetched[0]["descriptions_present"] or 0) if fetched else 0
    )
    matches = []
    combined_descriptions = []
    for row in fetched:
        item = dict(row)
        for aggregate_field in (
            "total_matches",
            "source_matches",
            "target_matches",
            "descriptions_present",
        ):
            item.pop(aggregate_field, None)
        matches.append(item)
        description = str(item.get("description") or "").strip()
        if description:
            combined_descriptions.append(
                {
                    "table_role": item["table_role"],
                    "description": description,
                }
            )

    return {
        "table_name": clean_name,
        "file_id": clean_file_id,
        "searched_tables": ["source_tables", "target_tables"],
        "total_matches": total_matches,
        "returned_matches": len(matches),
        "role_counts": role_counts,
        "descriptions_present": descriptions_present,
        "matches": matches,
        "combined_descriptions": combined_descriptions,
    }
