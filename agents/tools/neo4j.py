"""Read-only tools for the synchronized Neo4j projection."""

import logging
import re
from typing import Any, Dict, Literal, Optional

from langchain_core.tools import tool

from graph_storage import execute_neo4j_read

from .common import clamped_int, normalize_column_reference

logger = logging.getLogger(__name__)

MAX_CYPHER_ROWS = 100
MAX_LINEAGE_DEPTH = 50
_READONLY_CYPHER_START = re.compile(
    r"^(?:MATCH|OPTIONAL\s+MATCH|WITH|UNWIND|RETURN|SHOW|EXPLAIN|PROFILE)\b",
    re.IGNORECASE,
)
_MUTATING_CYPHER_CLAUSE = re.compile(
    r"(?<![.:])\b(?:"
    r"CREATE|MERGE|INSERT|DELETE|DETACH|SET|REMOVE|DROP|ALTER|RENAME|"
    r"GRANT|DENY|REVOKE|TERMINATE|START|STOP|FOREACH|CALL"
    r")\b|\bLOAD\s+CSV\b",
    re.IGNORECASE,
)


def _read_rows(
    query: str,
    parameters: Dict[str, Any],
) -> Dict[str, Any]:
    try:
        return {"rows": execute_neo4j_read(query, parameters)}
    except KeyError as exc:
        return {
            "error": f"Neo4j setting is missing: {exc.args[0]}",
            "rows": [],
        }
    except Exception:
        logger.exception("Neo4j read failed")
        return {
            "error": "Neo4j read failed",
            "rows": [],
        }


def _table_lineage_connections(rows: list[Dict[str, Any]]) -> list[Dict[str, Any]]:
    """Group transformation-level rows into exact direct table connections."""
    grouped: Dict[tuple[Any, ...], Dict[str, Any]] = {}
    for row in rows:
        key = (
            row.get("match_direction"),
            row.get("source_table"),
            row.get("source_layer"),
            row.get("target_table"),
            row.get("target_layer"),
        )
        connection = grouped.setdefault(
            key,
            {
                "direction": row.get("match_direction"),
                "source_table": row.get("source_table"),
                "source_layer": row.get("source_layer"),
                "target_table": row.get("target_table"),
                "target_layer": row.get("target_layer"),
                "transformation_count": 0,
                "transformation_ids": [],
            },
        )
        connection["transformation_count"] += 1
        transformation_id = row.get("transformation_id")
        if (
            transformation_id is not None
            and transformation_id not in connection["transformation_ids"]
        ):
            connection["transformation_ids"].append(transformation_id)
    return list(grouped.values())


def _strip_cypher_literals_and_comments(query: str) -> str:
    """Hide literals, quoted identifiers and comments before clause validation."""
    output = []
    index = 0
    state = "text"
    while index < len(query):
        char = query[index]
        following = query[index + 1] if index + 1 < len(query) else ""

        if state == "text":
            if char == "/" and following == "/":
                output.extend((" ", " "))
                state = "line_comment"
                index += 2
                continue
            if char == "/" and following == "*":
                output.extend((" ", " "))
                state = "block_comment"
                index += 2
                continue
            if char in {"'", '"', "`"}:
                output.append(" ")
                state = {
                    "'": "single_quote",
                    '"': "double_quote",
                    "`": "backtick",
                }[char]
                index += 1
                continue
            output.append(char)
            index += 1
            continue

        if state == "line_comment":
            output.append("\n" if char in "\r\n" else " ")
            if char in "\r\n":
                state = "text"
            index += 1
            continue

        if state == "block_comment":
            output.append(" ")
            if char == "*" and following == "/":
                output.append(" ")
                state = "text"
                index += 2
            else:
                index += 1
            continue

        output.append(" ")
        closing = {
            "single_quote": "'",
            "double_quote": '"',
            "backtick": "`",
        }[state]
        if char == "\\" and state != "backtick" and following:
            output.append(" ")
            index += 2
            continue
        if char == closing:
            if following == closing:
                output.append(" ")
                index += 2
                continue
            state = "text"
        index += 1

    return "".join(output)


def _validate_readonly_cypher(query: str) -> Optional[str]:
    text = query.strip()
    if not text:
        return "query must be non-empty"

    stripped = _strip_cypher_literals_and_comments(text).strip()
    statements = [
        statement.strip()
        for statement in stripped.rstrip(";").split(";")
        if statement.strip()
    ]
    if len(statements) != 1:
        return "Exactly one Cypher statement is allowed"
    if not _READONLY_CYPHER_START.match(statements[0]):
        return (
            "Only MATCH, OPTIONAL MATCH, WITH, UNWIND, RETURN, SHOW, "
            "EXPLAIN and PROFILE queries are allowed"
        )
    forbidden = _MUTATING_CYPHER_CLAUSE.search(statements[0])
    if forbidden:
        return f"Mutating or procedural Cypher is not allowed: {forbidden.group(0)}"
    return None


@tool(parse_docstring=True)
def run_cypher(
    query: str,
    parameters: Optional[Dict[str, Any]] = None,
    limit: int = 20,
) -> Dict[str, Any]:
    """Выполнить один свободный read-only Cypher-запрос к lineage-графу Neo4j.

    Идентификатор ETLTable хранится в свойстве name; у ETLColumn имя таблицы —
    table_name, имя колонки — name; используй только для сложных графовых путей,
    цепочек зависимостей, обходов
    соседних таблиц/колонок и impact analysis. В графе есть узлы ETLTable с
    рёбрами TABLE_TRANSFORMS_TO и узлы ETLColumn с рёбрами TRANSFORMS_TO.
    Wildcard моделируется отдельной колонкой name="*" внутри каждой таблицы;
    её уникальный key включает file_id и table_name. Конкретные колонки таблицы
    связаны с ней рёбрами COVERED_BY и EXPANDS_TO.
    Табличное ребро содержит sql_query из transformation_rule. Сведения о
    файлах, листах и точных строках S2T получай из SQLite. Для обычной таблицы S2T-трансформаций,
    фильтрации строк и маппингов используй SQLite-tools, а не этот инструмент.
    Для lineage известной колонки любой глубины предпочитай
    trace_neo4j_lineage: он учитывает wildcard-переходы. Для непосредственных
    соседей таблицы используй trace_neo4j_table_lineage; свободный Cypher нужен,
    когда требуется произвольный табличный обход, несколько условий,
    группировка или нестандартная форма графового ответа.

    Поддерживаются MATCH, OPTIONAL MATCH, WITH, UNWIND, RETURN,
    SHOW, EXPLAIN и PROFILE. Изменяющие конструкции, несколько выражений и
    процедурный CALL отклоняются до обращения к Neo4j. Всегда передавай
    пользовательские значения через parameters, а не собирай их конкатенацией в
    query. Пустой rows говорит только об отсутствии совпадения в текущей
    Neo4j-проекции; это не доказательство отсутствия факта в SQLite.

    Args:
        query: Полный текст одного read-only Cypher-запроса.
        parameters: Именованные параметры Cypher без символа $ в ключах.
        limit: Максимальное число строк в ответе, от 1 до 100.
    """
    text = (query or "").strip()
    validation_error = _validate_readonly_cypher(text)
    if validation_error:
        return {
            "error": validation_error,
            "query": text,
        }

    clean_parameters = dict(parameters or {})
    clean_limit = clamped_int(limit, 20, 1, MAX_CYPHER_ROWS)
    try:
        rows = execute_neo4j_read(
            text,
            clean_parameters,
            row_limit=clean_limit + 1,
        )
    except KeyError as exc:
        return {
            "error": f"Neo4j setting is missing: {exc.args[0]}",
            "query": text,
        }
    except Exception:
        logger.exception("Cypher execution failed")
        return {
            "error": "Cypher query failed",
            "query": text,
        }

    truncated = len(rows) > clean_limit
    visible_rows = rows[:clean_limit]
    return {
        "query": text,
        "parameters": clean_parameters,
        "columns": list(visible_rows[0]) if visible_rows else [],
        "rows": visible_rows,
        "returned_rows": len(visible_rows),
        "truncated": truncated,
        "limit": clean_limit,
    }


_COLUMN_LINEAGE_STEP_QUERY = """
UNWIND $states AS state
MATCH (source:ETLProjection:ETLColumn)
      -[mapping:TRANSFORMS_TO]->
      (target:ETLProjection:ETLColumn)
WHERE
    (state.file_id IS NULL OR mapping.file_id = state.file_id)
    AND source.name <> '*'
    AND target.name <> '*'
    AND (
        (
            $direction = 'downstream'
            AND source.table_name = state.table_name
            AND source.name = state.column_name
        )
        OR (
            $direction = 'upstream'
            AND target.table_name = state.table_name
            AND target.name = state.column_name
        )
    )
RETURN
    state.state_index AS state_index,
    mapping.file_id AS file_id,
    mapping.transformation_id AS transformation_id,
    source.table_name AS source_table,
    mapping.source_layer AS source_layer,
    source.name AS source_field,
    target.table_name AS target_table,
    mapping.target_layer AS target_layer,
    target.name AS target_field,
    source.name AS matched_source_field,
    target.name AS matched_target_field,
    $direction AS match_direction
UNION ALL
UNWIND $states AS state
MATCH (source:ETLProjection:ETLColumn)
      -[:COVERED_BY]->
      (source_wildcard:ETLProjection:ETLColumn {name: '*'})
      -[mapping:TRANSFORMS_TO]->
      (target_wildcard:ETLProjection:ETLColumn {name: '*'})
      -[:EXPANDS_TO]->
      (target:ETLProjection:ETLColumn)
WHERE
    source.name = target.name
    AND (state.file_id IS NULL OR mapping.file_id = state.file_id)
    AND (
        (
            $direction = 'downstream'
            AND source.table_name = state.table_name
            AND source.name = state.column_name
        )
        OR (
            $direction = 'upstream'
            AND target.table_name = state.table_name
            AND target.name = state.column_name
        )
    )
RETURN
    state.state_index AS state_index,
    mapping.file_id AS file_id,
    mapping.transformation_id AS transformation_id,
    source.table_name AS source_table,
    mapping.source_layer AS source_layer,
    source.name AS source_field,
    target.table_name AS target_table,
    mapping.target_layer AS target_layer,
    target.name AS target_field,
    source_wildcard.name AS matched_source_field,
    target_wildcard.name AS matched_target_field,
    $direction AS match_direction
"""


def _path_result(
    *,
    direction: str,
    start_table: str,
    start_column: str,
    end_table: str,
    end_column: str,
    steps: list[Dict[str, Any]],
) -> Dict[str, Any]:
    return {
        "direction": direction,
        "depth": len(steps),
        "start": {
            "table_name": start_table,
            "column_name": start_column,
        },
        "end": {
            "table_name": end_table,
            "column_name": end_column,
        },
        "steps": steps,
    }


def _resolve_column_lineage(
    *,
    table_name: str,
    column_name: str,
    file_id: Optional[int],
    direction: Literal["upstream", "downstream", "both"],
    max_depth: int,
    limit: int,
) -> Dict[str, Any]:
    directions = (
        ["downstream", "upstream"]
        if direction == "both"
        else [direction]
    )
    paths: list[Dict[str, Any]] = []
    truncated = False

    for current_direction in directions:
        if len(paths) >= limit:
            truncated = True
            break
        frontier = [
            {
                "table_name": table_name,
                "column_name": column_name,
                "file_id": file_id,
                "steps": [],
                "visited": {(table_name, column_name)},
            }
        ]

        for depth_index in range(max_depth):
            if not frontier or len(paths) >= limit:
                break
            states = [
                {
                    "state_index": index,
                    "table_name": state["table_name"],
                    "column_name": state["column_name"],
                    "file_id": state["file_id"],
                }
                for index, state in enumerate(frontier)
            ]
            remaining = limit - len(paths)
            rows = execute_neo4j_read(
                _COLUMN_LINEAGE_STEP_QUERY,
                {
                    "states": states,
                    "direction": current_direction,
                },
                row_limit=remaining + 1,
            )
            if len(rows) > remaining:
                truncated = True
                rows = rows[:remaining]
            rows.sort(
                key=lambda row: (
                    int(row.get("state_index") or 0),
                    int(row.get("file_id") or 0),
                    int(row.get("transformation_id") or 0),
                    row.get("matched_source_field") == "*"
                    and row.get("matched_target_field") == "*",
                )
            )

            next_frontier = []
            for row in rows:
                try:
                    state = frontier[int(row["state_index"])]
                except (KeyError, TypeError, ValueError, IndexError):
                    logger.warning("Neo4j lineage returned an invalid state index")
                    continue

                edge_file_id = int(row["file_id"])
                if (
                    state["file_id"] is not None
                    and edge_file_id != int(state["file_id"])
                ):
                    continue
                if current_direction == "downstream":
                    next_table = str(row.get("target_table") or "").strip()
                    next_column = str(row.get("target_field") or "").strip()
                else:
                    next_table = str(row.get("source_table") or "").strip()
                    next_column = str(row.get("source_field") or "").strip()
                if not next_table or not next_column:
                    continue
                next_key = (next_table, next_column)
                if next_key in state["visited"]:
                    continue

                is_wildcard = (
                    row.get("matched_source_field") == "*"
                    and row.get("matched_target_field") == "*"
                )
                step = {
                    key: value
                    for key, value in row.items()
                    if key
                    not in {
                        "state_index",
                        "matched_source_field",
                        "matched_target_field",
                    }
                }
                step["file_id"] = edge_file_id
                if is_wildcard:
                    step["source_field"] = "*"
                    step["target_field"] = "*"
                steps = [*state["steps"], step]
                paths.append(
                    _path_result(
                        direction=current_direction,
                        start_table=table_name,
                        start_column=column_name,
                        end_table=next_table,
                        end_column=next_column,
                        steps=steps,
                    )
                )
                next_frontier.append(
                    {
                        "table_name": next_table,
                        "column_name": next_column,
                        "file_id": edge_file_id,
                        "steps": steps,
                        "visited": {*state["visited"], next_key},
                    }
                )
                if len(paths) >= limit:
                    if depth_index + 1 < max_depth or len(next_frontier) < len(rows):
                        truncated = True
                    break
            frontier = next_frontier

    flat_rows: list[Dict[str, Any]] = []
    seen_steps = set()
    for path in paths:
        for step in path["steps"]:
            signature = (
                step.get("match_direction"),
                step.get("file_id"),
                step.get("transformation_id"),
                step.get("source_table"),
                step.get("source_field"),
                step.get("target_table"),
                step.get("target_field"),
            )
            if signature in seen_steps:
                continue
            seen_steps.add(signature)
            flat_rows.append(step)

    return {
        "rows": flat_rows,
        "paths": paths,
        "returned_rows": len(flat_rows),
        "returned_paths": len(paths),
        "truncated": truncated,
    }


@tool(parse_docstring=True)
def trace_neo4j_lineage(
    table_name: str,
    column_name: Optional[str] = None,
    file_id: Optional[int] = None,
    direction: Literal["upstream", "downstream", "both"] = "both",
    max_depth: int = 1,
    limit: int = 50,
) -> Dict[str, Any]:
    """Найти upstream/downstream lineage конкретной именованной колонки.

    Не используй для lineage всей таблицы без колонки: там нужен
    trace_neo4j_table_lineage. Используй для структуры графа и именованных
    зависимостей колонок; если
    пользователь просит правила преобразования, SQL, additional objects,
    объяснимый сохранённый S2T-путь или его готовую схему, выбирай
    trace_transformation_path: он также возвращает text_diagram.

    Перед вызовом обязательно нормализуй ссылку на именованную колонку. Раздели
    ссылку ``table_name.column_name`` по последней точке: всю левую часть
    передай в table_name, последнюю часть — в column_name. column_name должен
    быть без префикса таблицы. Для колонкового lineage не оставляй column_name
    равным null и не передавай полную ссылку целиком в table_name. Это правило
    одинаково для простых и квалифицированных схемой имён таблиц.

    Используй только когда пользователь просит lineage/upstream/downstream
    именованной таблицы или колонки в графе. Не используй для SQL-текста и
    продолжения «эта трансформация»: там нужен parse_sql_column_lineage. Никогда
    не выбирай произвольный объект из SQL. Граф содержит узлы ETLColumn и рёбра
    TRANSFORMS_TO; имена сравниваются точно.

    Для именованной колонки tool проходит до max_depth связей сам: обычные
    TRANSFORMS_TO и правила ``* -> *`` объединяются в один путь. Wildcard
    представлен отдельным ETLColumn с name="*" для каждой таблицы; его key
    включает file_id и table_name, поэтому wildcard разных таблиц не смешивается.
    Каждая известная конкретная колонка соединена с wildcard своей таблицы
    рёбрами COVERED_BY и EXPANDS_TO; между таблицами сопоставляются только
    одноимённые колонки.
    В публичных шагах такое ребро возвращается как source_field="*" и
    target_field="*" без отдельного boolean-признака.
    Для произвольного графового запроса используй run_cypher, для объяснения
    S2T-правил — trace_transformation_path. file_id допустим только при явном
    файловом scope.
    Пустой rows не отменяет факты SQLite.

    Args:
        table_name: Точное имя логической ETL-таблицы; для ссылки
            table_name.column_name это вся часть слева от последней точки,
            никогда не полная ссылка вместе с колонкой.
        column_name: Точное имя колонки без префикса; для ссылки
            table_name.column_name это часть справа от последней точки.
            Обязательно для любого колонкового lineage; null допустим только
            когда пользователь запросил lineage всей таблицы.
        file_id: Опциональный идентификатор файла для ограничения графа.
        direction: upstream, downstream или оба направления both.
        max_depth: Максимальная глубина пути именованной колонки, от 1 до 50.
        limit: Максимальное число найденных связей колонок, от 1 до 100.
    """
    clean_table_name = str(table_name or "").strip()
    if not clean_table_name:
        return {
            "error": "table_name must be non-empty",
            "rows": [],
        }
    clean_column_name = normalize_column_reference(
        clean_table_name,
        column_name,
    )
    if (
        column_name is not None
        and str(column_name).strip()
        and not clean_column_name
    ):
        return {
            "error": "column_name must contain a column after table_name",
            "rows": [],
        }
    clean_file_id = int(file_id) if file_id is not None else None
    clean_max_depth = clamped_int(max_depth, 1, 1, MAX_LINEAGE_DEPTH)
    clean_limit = clamped_int(limit, 50, 1, 100)
    if clean_column_name is not None:
        try:
            result = _resolve_column_lineage(
                table_name=clean_table_name,
                column_name=clean_column_name,
                file_id=clean_file_id,
                direction=direction,
                max_depth=clean_max_depth,
                limit=clean_limit,
            )
        except KeyError as exc:
            result = {
                "error": f"Neo4j setting is missing: {exc.args[0]}",
                "rows": [],
                "paths": [],
                "returned_rows": 0,
                "returned_paths": 0,
                "truncated": False,
            }
        except Exception:
            logger.exception("Neo4j column lineage read failed")
            result = {
                "error": "Neo4j read failed",
                "rows": [],
                "paths": [],
                "returned_rows": 0,
                "returned_paths": 0,
                "truncated": False,
            }
        result.update(
            {
                "table_name": clean_table_name,
                "column_name": clean_column_name,
                "file_id": clean_file_id,
                "direction": direction,
                "max_depth": clean_max_depth,
                "limit": clean_limit,
            }
        )
        return result

    if clean_max_depth != 1:
        return {
            "error": "column_name is required when max_depth is greater than 1",
            "rows": [],
            "table_name": clean_table_name,
            "column_name": None,
            "file_id": clean_file_id,
            "direction": direction,
            "max_depth": clean_max_depth,
            "limit": clean_limit,
        }
    result = _read_rows(
        """
        MATCH (source:ETLProjection:ETLColumn)
              -[mapping:TRANSFORMS_TO]->
              (target:ETLProjection:ETLColumn)
        WHERE
            ($file_id IS NULL OR mapping.file_id = $file_id)
            AND (
                (
                    $column_name IS NULL
                    AND (
                        (
                            $direction IN ['downstream', 'both']
                            AND source.table_name = $table_name
                        )
                        OR (
                            $direction IN ['upstream', 'both']
                            AND target.table_name = $table_name
                        )
                    )
                )
                OR (
                    $column_name IS NOT NULL
                    AND (
                        (
                            $direction IN ['downstream', 'both']
                            AND source.table_name = $table_name
                            AND source.name = $column_name
                        )
                        OR (
                            $direction IN ['upstream', 'both']
                            AND target.table_name = $table_name
                            AND target.name = $column_name
                        )
                    )
                )
            )
        RETURN
            mapping.file_id AS file_id,
            mapping.transformation_id AS transformation_id,
            source.table_name AS source_table,
            mapping.source_layer AS source_layer,
            source.name AS source_field,
            target.table_name AS target_table,
            mapping.target_layer AS target_layer,
            target.name AS target_field,
            CASE
                WHEN source.table_name = $table_name
                     AND (
                         $column_name IS NULL
                         OR source.name = $column_name
                     )
                THEN 'downstream'
                ELSE 'upstream'
            END AS match_direction
        ORDER BY mapping.file_id, mapping.transformation_id
        LIMIT $limit
        """,
        {
            "table_name": clean_table_name,
            "column_name": clean_column_name,
            "file_id": clean_file_id,
            "direction": direction,
            "limit": clean_limit,
        },
    )
    result.update(
        {
            "table_name": clean_table_name,
            "column_name": clean_column_name,
            "file_id": clean_file_id,
            "direction": direction,
            "max_depth": 1,
            "limit": clean_limit,
            "returned_rows": len(result["rows"]),
        }
    )
    return result


@tool(parse_docstring=True)
def trace_neo4j_table_lineage(
    table_name: str,
    file_id: Optional[int] = None,
    direction: Literal["upstream", "downstream", "both"] = "both",
    limit: int = 50,
) -> Dict[str, Any]:
    """Найти непосредственный upstream/downstream lineage логической таблицы.

    Используй для upstream/downstream уже известной точной логической таблицы в Neo4j;
    tool не ищет неизвестные таблицы и не вычисляет пересечение ролей source/target.
    Не используй для разбора SQL или продолжения «эта трансформация»: там нужен
    parse_sql_table_lineage. Граф содержит узлы ETLTable и прямые рёбра
    TABLE_TRANSFORMS_TO с sql_query; имена сравниваются точно.

    Возвращает связи глубины 1. Поле connections группирует точные пары таблиц и
    содержит transformation_count/transformation_ids; rows сохраняет исходные
    рёбра с sql_query. Для длинного пути используй run_cypher, для объяснения
    правил — trace_transformation_path. file_id не бери из UI без явного
    ограничения. Пустой rows не доказывает отсутствие факта в SQLite.

    Args:
        table_name: Точное имя исходной или целевой логической ETL-таблицы.
        file_id: Опциональный идентификатор файла для ограничения графа.
        direction: upstream, downstream или оба направления both.
        limit: Максимальное число найденных табличных связей, от 1 до 100.
    """
    clean_table_name = str(table_name or "").strip()
    if not clean_table_name:
        return {
            "error": "table_name must be non-empty",
            "rows": [],
        }
    clean_file_id = int(file_id) if file_id is not None else None
    clean_limit = clamped_int(limit, 50, 1, 100)
    result = _read_rows(
        """
        MATCH (source:ETLProjection:ETLTable)
              -[mapping:TABLE_TRANSFORMS_TO]->
              (target:ETLProjection:ETLTable)
        WHERE
            ($file_id IS NULL OR mapping.file_id = $file_id)
            AND (
                (
                    $direction IN ['downstream', 'both']
                    AND source.name = $table_name
                )
                OR (
                    $direction IN ['upstream', 'both']
                    AND target.name = $table_name
                )
            )
        RETURN
            mapping.file_id AS file_id,
            mapping.transformation_id AS transformation_id,
            source.name AS source_table,
            mapping.source_layer AS source_layer,
            target.name AS target_table,
            mapping.target_layer AS target_layer,
            mapping.sql_query AS sql_query,
            CASE
                WHEN source.name = $table_name
                THEN 'downstream'
                ELSE 'upstream'
            END AS match_direction
        ORDER BY mapping.file_id, mapping.transformation_id
        LIMIT $limit
        """,
        {
            "table_name": clean_table_name,
            "file_id": clean_file_id,
            "direction": direction,
            "limit": clean_limit,
        },
    )
    connections = _table_lineage_connections(result["rows"])
    result.update(
        {
            "table_name": clean_table_name,
            "file_id": clean_file_id,
            "direction": direction,
            "limit": clean_limit,
            "returned_rows": len(result["rows"]),
            "connections": connections,
            "connection_count": len(connections),
        }
    )
    return result
