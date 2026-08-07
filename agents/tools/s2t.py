"""Tools for the common ETL transformation table and table catalogs."""

from typing import Any, Dict, List, Literal, Optional

from langchain_core.tools import tool

from .common import clamped_int


@tool(parse_docstring=True)
def list_s2t_transformations(
    limit: int = 20,
    q: Optional[str] = None,
    columns: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """
    Получить компактный фрагмент строк колоночного S2T-маппинга.

    Это основной инструмент для запросов «покажи таблицу трансформаций»,
    «покажи строки/маппинги/правила» и обычных связей source → target. Такие
    запросы относятся к табличному SQLite-сценарию, а не к Neo4j lineage.
    В контексте s2t_transformations слова «трансформация» и «правило
    трансформации» являются синонимами поля transformation_rule; просьба показать
    трансформации означает получить фактические строки этим инструментом.
    По умолчанию возвращает row_num и все настроенные поля S2T, включая
    target_table, target_field, source_table, source_field, source_layer,
    target_layer и transformation_rule.
    Если пользователь просит конкретные колонки, передай их точные имена в
    columns: например, columns=["transformation_rule"] вернёт только правила
    трансформаций. Никогда не передавай имя колонки в q: q ищет подстроку внутри
    значений строк и не управляет составом ответа. Данные читаются из всей
    глобальной s2t_transformations. Никогда не
    ограничивает результат file_id, активным UI-файлом или последней загрузкой.
    Результат возвращается planner-у как наблюдение и сам по себе не завершает
    ответ.

    Используй без q, когда нужен обычный preview таблицы; q задавай только если
    пользователь назвал конкретное значение, которое нужно найти внутри строк.
    Для более точного текстового поиска предпочитай search_s2t_transformations,
    для списков и операций над множествами ролей — list_s2t_table_names, для готовой
    агрегации — summarize_s2t_tables.
    Tool не строит многошаговый lineage. Если rows пусты без q, глобальная
    s2t_transformations сейчас пуста; если задан q, пустой результат относится
    только к этому фильтру.

    Args:
        limit: Максимальное число возвращаемых строк; фактически ограничивается 20.
        q: Опциональная подстрока значения для фильтрации строк; не имя колонки.
        columns: Точные имена возвращаемых колонок; null означает все колонки.
    """
    from storage.s2t import list_s2t_transformations as db_list_s2t_transformations

    clean_limit = max(1, min(int(limit or 20), 20))
    return db_list_s2t_transformations(
        file_id=None,
        limit=clean_limit,
        q=q,
        columns=columns,
    )


@tool(parse_docstring=True)
def search_s2t_transformations(
    needle: str,
    limit: int = 20,
) -> Dict[str, Any]:
    """
    Найти строки S2T-трансформаций по известному имени или фрагменту значения.

    Используй для табличного поиска маппингов, колонок и правил в SQLite. Это не
    инструмент поиска графового пути, upstream/downstream или impact analysis:
    для таких задач предназначены Neo4j-tools.
    Ищет подстроку одновременно во всех настроенных S2T-полях, включая
    target_table, target_field, source_table, source_field, source_layer,
    target_layer и transformation_rule.
    Всегда ищет по всей глобальной таблице без file_id, активного UI-файла или
    выбора последней загрузки. Возвращает только фактические строки
    s2t_transformations.

    Используй, когда известен один текстовый фрагмент и нужно найти все строки,
    где он встречается в любой роли. Это поиск подстроки, а не точное разрешение
    имени и не семантическая близость; одинаковые исходные строки не
    дедуплицируются. Для сложного сочетания нескольких условий или выбора
    конкретных полей используй run_sql. Пустой rows означает отсутствие
    совпадений этого фрагмента в глобальной S2T-таблице, но не отсутствие файла,
    Excel-листа или описания в других таблицах SQLite.

    Args:
        needle: Непустая подстрока имени таблицы, колонки или правила преобразования.
        limit: Максимальное число возвращаемых совпадений, от 1 до 100.
    """
    text = (needle or "").strip()
    if not text:
        return {"error": "needle must be non-empty", "query": needle, "total": 0, "rows": []}
    if len(text) > 200:
        return {"error": "needle too long", "query": text, "total": 0, "rows": []}

    from storage.s2t import list_s2t_transformations as db_list_s2t_transformations

    clean_limit = max(1, min(int(limit or 20), 100))
    data = db_list_s2t_transformations(
        file_id=None,
        limit=clean_limit,
        q=text,
    )
    data["query"] = text
    data["searched_table"] = "s2t_transformations"
    from storage.database import S2T_RECORD_FIELDS

    data["searched_columns"] = list(S2T_RECORD_FIELDS)
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
    приёмников и операций над этими двумя множествами; не проси planner писать
    эквивалентный SQL через run_sql. intersection означает, что имя встречается
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
            WHERE TRIM(catalog.table_name) = ? COLLATE NOCASE
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
            WHERE TRIM(catalog.table_name) = ? COLLATE NOCASE
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
