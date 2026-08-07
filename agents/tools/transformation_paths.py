"""Deterministic transformation-path analysis across SQLite, SQL and Neo4j."""

from __future__ import annotations

import re
from collections import defaultdict, deque
from typing import Any, Dict, List, Literal, Optional, Sequence, Tuple

from langchain_core.tools import tool

from graph_storage import execute_neo4j_read, is_neo4j_configured

from .common import clamped_int, normalize_column_reference
from .sql_lineage import analyze_sql_lineage


NodeKey = Tuple[int, str, Optional[str]]
_SQL_PREFIX = re.compile(r"^(?:select|with|insert|create)\b", re.IGNORECASE)


def _text(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _normalize_path_reference(
    table_name: Any,
    column_name: Optional[str],
) -> Tuple[str, Optional[str]]:
    """Normalize both supported forms of an exact table/column reference."""
    clean_table = str(table_name or "").strip()
    if column_name is None:
        return clean_table, None

    raw_column = str(column_name).strip()
    if not raw_column:
        return clean_table, None

    if raw_column.casefold() == clean_table.casefold() and "." in clean_table:
        clean_table, raw_column = clean_table.rsplit(".", 1)
        return clean_table.strip(), raw_column.strip() or None

    clean_column = normalize_column_reference(clean_table, raw_column)
    if clean_column:
        suffix = f".{clean_column}"
        if clean_table.casefold().endswith(suffix.casefold()):
            normalized_table = clean_table[: -len(suffix)].strip()
            if normalized_table:
                clean_table = normalized_table
    return clean_table, clean_column


def _node_key(
    file_id: int,
    table_name: Any,
    column_name: Any,
    include_column: bool,
) -> Optional[NodeKey]:
    table = _text(table_name)
    column = _text(column_name)
    if table is None or (include_column and column is None):
        return None
    return (
        int(file_id),
        table.casefold(),
        column.casefold() if include_column and column else None,
    )


def _node_payload(
    table_name: Any,
    column_name: Any,
    layer: Any = None,
) -> Dict[str, Optional[str]]:
    return {
        "table": _text(table_name),
        "column": _text(column_name),
        "layer": _text(layer),
    }


def _rule_analysis(rule: Any) -> Dict[str, Any]:
    text = _text(rule)
    if text is None or text == "-":
        return {"kind": "direct", "rule": text}
    if not _SQL_PREFIX.match(text):
        return {"kind": "expression", "rule": text}
    parsed = analyze_sql_lineage(text, None, include_columns=True)
    return {
        "kind": "sql" if "error" not in parsed else "invalid_sql",
        "rule": text,
        "lineage": parsed,
    }


def _additional_object_index(rows: Sequence[Dict[str, Any]]) -> Dict[int, List[Dict[str, Any]]]:
    result: Dict[int, List[Dict[str, Any]]] = defaultdict(list)
    for row in rows:
        item = dict(row)
        sql = _text(item.get("sql"))
        item["sql_analysis"] = (
            analyze_sql_lineage(sql, None, include_columns=False) if sql else None
        )
        result[int(item["file_id"])].append(item)
    return result


def _table_tokens(analysis: Optional[Dict[str, Any]]) -> set[str]:
    if not analysis or analysis.get("error"):
        return set()
    values = list(analysis.get("source_tables") or [])
    if analysis.get("target_table"):
        values.append(analysis["target_table"])
    tokens = set()
    for value in values:
        text = str(value).strip().casefold()
        if text:
            tokens.add(text)
            tokens.add(text.rsplit(".", 1)[-1])
    return tokens


def _related_additional_objects(
    edge: Dict[str, Any],
    rule_analysis: Dict[str, Any],
    by_file: Dict[int, List[Dict[str, Any]]],
) -> List[Dict[str, Any]]:
    related = []
    rule_tables = _table_tokens(rule_analysis.get("lineage"))
    edge_tables = {
        text.casefold()
        for text in (
            _text(edge.get("source_table")),
            _text(edge.get("target_table")),
        )
        if text
    }
    for item in by_file.get(int(edge["file_id"]), []):
        name = _text(item.get("name"))
        name_token = name.casefold() if name else None
        if not (
            name_token
            and (
                name_token in rule_tables
                or name_token in edge_tables
            )
        ):
            continue
        related.append(
            {
                "id": int(item["id"]),
                "file_id": int(item["file_id"]),
                "sheet_name": item["sheet_name"],
                "row_num": int(item["row_num"]),
                "name": name,
                "sql": item.get("sql"),
                "sql_lineage": item.get("sql_analysis"),
            }
        )
    return related


def _step_payload(
    edge: Dict[str, Any],
    match_direction: str,
    additional_objects: Dict[int, List[Dict[str, Any]]],
) -> Dict[str, Any]:
    analysis = _rule_analysis(edge.get("transformation_rule"))
    return {
        "transformation_id": int(edge["id"]),
        "file_id": int(edge["file_id"]),
        "sheet_name": edge["sheet_name"],
        "row_num": int(edge["row_num"]),
        "match_direction": match_direction,
        "source": _node_payload(
            edge.get("source_table"),
            edge.get("source_field"),
            edge.get("source_layer"),
        ),
        "target": _node_payload(
            edge.get("target_table"),
            edge.get("target_field"),
            edge.get("target_layer"),
        ),
        "transformation": analysis,
        "additional_objects": _related_additional_objects(
            edge, analysis, additional_objects
        ),
    }


def _walk_paths(
    edges: Sequence[Dict[str, Any]],
    table_name: str,
    column_name: Optional[str],
    direction: str,
    max_depth: int,
    limit: int,
    additional_objects: Dict[int, List[Dict[str, Any]]],
) -> List[Dict[str, Any]]:
    include_column = column_name is not None
    requested_table = table_name.casefold()
    requested_column = column_name.casefold() if column_name else None
    adjacency: Dict[NodeKey, List[Tuple[NodeKey, Dict[str, Any], str]]] = defaultdict(list)
    starts: set[NodeKey] = set()

    for edge in edges:
        source = _node_key(
            edge["file_id"], edge.get("source_table"), edge.get("source_field"), include_column
        )
        target = _node_key(
            edge["file_id"], edge.get("target_table"), edge.get("target_field"), include_column
        )
        if source is None or target is None:
            continue
        exact_source = source[1:] == (requested_table, requested_column)
        exact_target = target[1:] == (requested_table, requested_column)
        if direction in ("downstream", "both"):
            adjacency[source].append((target, edge, "downstream"))
            if exact_source:
                starts.add(source)
        if direction in ("upstream", "both"):
            adjacency[target].append((source, edge, "upstream"))
            if exact_target:
                starts.add(target)
        if (
            direction == "both"
            and include_column
            and not exact_source
            and not exact_target
        ):
            if target[1] == requested_table and source[2] == requested_column:
                starts.add(source)
            if source[1] == requested_table and target[2] == requested_column:
                starts.add(target)

    def start_payload(origin: NodeKey, steps: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
        first_step = steps[0]
        endpoint = (
            first_step["source"]
            if first_step["match_direction"] == "downstream"
            else first_step["target"]
        )
        return {"file_id": origin[0], **endpoint}

    paths = []
    queue = deque((start, start, [], {start}) for start in sorted(starts))
    while queue and len(paths) < limit:
        origin, node, steps, visited = queue.popleft()
        options = adjacency.get(node, [])
        advanced = False
        for next_node, edge, match_direction in options:
            if next_node in visited:
                continue
            next_steps = [
                *steps,
                _step_payload(edge, match_direction, additional_objects),
            ]
            if len(next_steps) >= max_depth or not adjacency.get(next_node):
                paths.append(
                    {
                        "start": start_payload(origin, next_steps),
                        "end": {
                            "file_id": next_node[0],
                            **(
                                next_steps[-1]["target"]
                                if match_direction == "downstream"
                                else next_steps[-1]["source"]
                            ),
                        },
                        "depth": len(next_steps),
                        "steps": next_steps,
                    }
                )
                if len(paths) >= limit:
                    break
            else:
                queue.append((origin, next_node, next_steps, visited | {next_node}))
            advanced = True
        if steps and not advanced and len(paths) < limit:
            last_direction = steps[-1]["match_direction"]
            paths.append(
                {
                    "start": start_payload(origin, steps),
                    "end": {
                        "file_id": node[0],
                        **(
                            steps[-1]["target"]
                            if last_direction == "downstream"
                            else steps[-1]["source"]
                        ),
                    },
                    "depth": len(steps),
                    "steps": steps,
                }
            )
    return paths


def _neo4j_evidence(transformation_ids: Sequence[int]) -> Dict[str, Any]:
    if not transformation_ids:
        return {"configured": is_neo4j_configured(), "rows": []}
    if not is_neo4j_configured():
        return {
            "configured": False,
            "rows": [],
            "note": (
                "Neo4j не настроен. Это не означает отсутствие фактов: "
                "SQLite-путь выше остаётся источником истины."
            ),
        }
    try:
        rows = execute_neo4j_read(
            """
            MATCH (source:ETLProjection)-[mapping]->(target:ETLProjection)
            WHERE type(mapping) IN ['TRANSFORMS_TO', 'TABLE_TRANSFORMS_TO']
              AND mapping.transformation_id IN $transformation_ids
            RETURN
                type(mapping) AS relationship_type,
                mapping.file_id AS file_id,
                mapping.transformation_id AS transformation_id,
                CASE WHEN type(mapping) = 'TRANSFORMS_TO'
                     THEN source.table_name ELSE source.name END AS source_table,
                CASE WHEN type(mapping) = 'TRANSFORMS_TO'
                     THEN source.name ELSE null END AS source_field,
                CASE WHEN type(mapping) = 'TRANSFORMS_TO'
                     THEN target.table_name ELSE target.name END AS target_table,
                CASE WHEN type(mapping) = 'TRANSFORMS_TO'
                     THEN target.name ELSE null END AS target_field,
                mapping.sql_query AS sql_query
            ORDER BY transformation_id, relationship_type
            """,
            {"transformation_ids": list(transformation_ids)},
            row_limit=200,
        )
        return {"configured": True, "rows": rows}
    except Exception as exc:
        return {
            "configured": True,
            "error": str(exc),
            "rows": [],
            "note": (
                "Ошибка чтения Neo4j не отменяет факты пути, найденные в SQLite."
            ),
        }


@tool(parse_docstring=True)
def trace_transformation_path(
    table_name: str,
    column_name: Optional[str] = None,
    direction: Literal["upstream", "downstream", "both"] = "both",
    max_depth: int = 5,
    limit: int = 20,
    include_neo4j: bool = True,
) -> Dict[str, Any]:
    """Построить многоуровневый объяснимый путь из сохранённых S2T-фактов.

    Используй для вопросов о правилах цепочки, исходном SQL, additional objects,
    подтверждении Neo4j, «как значение приходит», end-to-end source → target и
    для явной просьбы показать сохранённый путь схемой: tool сразу возвращает
    готовую text_diagram; обычный trace_neo4j_lineage этих фактов не возвращает.
    Инструмент строит многошаговые пути по глобальной
    s2t_transformations без фильтра file_id, но никогда не склеивает одинаковые
    имена из разных файлов. Для каждого шага отличает прямую трансформацию
    (NULL, пусто или ровно "-"), выражение и полный SQL; полный SQL разбирает
    SQLGlot и связывает с additional_objects.sql того же файла. При включённом
    include_neo4j добавляет только подтверждающие графовые рёбра. Отсутствие
    графового ребра не отменяет факты SQLite.

    Используй, когда важны не только соседние source/target, но и порядок
    нескольких шагов, текст каждого правила и участие additional_objects. Для
    простой выдачи строк предпочитай list/search_s2t_transformations, для одного
    прямого графового соседа — Neo4j trace tools. Этот же tool используется при
    просьбе показать путь схемой: он всегда возвращает готовые text_diagram,
    Mermaid-код и edges без второго анализа. table_name и column_name сравниваются
    как точные имена без смысловой подстановки. include_neo4j не превращает
    Neo4j в fallback и не удаляет SQLite-пути, которых нет в проекции. Пустой
    paths означает, что из указанной стартовой точки в выбранном направлении не
    собран путь по сохранённым S2T-рёбрам.

    Если пользователь указал колонку в форме table_name.column_name, желательно
    разделить ссылку: имя таблицы передать в table_name, а в column_name — только
    имя колонки без префикса. Инструмент также детерминированно исправляет обе
    совместимые формы: полную ссылку в column_name и полную ссылку в table_name,
    когда column_name уже содержит совпадающий последний сегмент.

    Для downstream table_name и column_name означают пару source_table + source_field,
    для upstream — target_table + target_field. Режим both сначала
    ищет обе точные пары, а при их отсутствии также разрешает комбинации ролей
    внутри одной S2T-строки: target_table + source_field и source_table +
    target_field. Для такой комбинации стартом становится фактическая сторона
    названной колонки, после чего путь обходится в обе стороны. Если пользователь
    требует только downstream или только upstream, но передал смешанную пару,
    сначала разреши фактические роли через search_s2t_transformations.

    Args:
        table_name: Точное имя исходной или целевой логической ETL-таблицы;
            совместимая полная ссылка table_name.column_name нормализуется,
            если column_name передан отдельно и совпадает с последним сегментом.
        column_name: Опциональное точное имя колонки без префикса таблицы;
            null строит путь таблиц.
        direction: upstream, downstream или оба направления both.
        max_depth: Максимальная длина пути, от 1 до 10.
        limit: Максимальное число возвращаемых путей, от 1 до 50.
        include_neo4j: Добавить подтверждающие рёбра текущей Neo4j-проекции.
    """
    clean_table, clean_column = _normalize_path_reference(
        table_name,
        column_name,
    )
    if not clean_table:
        empty_edges: List[Dict[str, Any]] = []
        return {
            "error": "table_name must be non-empty",
            "paths": [],
            "returned_paths": 0,
            "text_diagram": _text_path_diagram([]),
            "mermaid": _mermaid_path_diagram(empty_edges),
            "edges": empty_edges,
        }
    if (
        column_name is not None
        and str(column_name).strip()
        and not clean_column
    ):
        empty_edges = []
        return {
            "error": "column_name must contain a column after table_name",
            "paths": [],
            "returned_paths": 0,
            "text_diagram": _text_path_diagram([]),
            "mermaid": _mermaid_path_diagram(empty_edges),
            "edges": empty_edges,
        }
    clean_depth = clamped_int(max_depth, 5, 1, 10)
    clean_limit = clamped_int(limit, 20, 1, 50)

    from storage.database import get_db_connection

    conn = get_db_connection()
    try:
        edges = [
            dict(row)
            for row in conn.execute(
                """
                SELECT id, file_id, sheet_name, row_num,
                       source_table, source_field, target_table, target_field,
                       source_layer, target_layer, transformation_rule
                FROM s2t_transformations
                ORDER BY file_id, id
                """
            ).fetchall()
        ]
        additional_rows = [
            dict(row)
            for row in conn.execute(
                """
                SELECT id, file_id, sheet_name, row_num, name, sql
                FROM additional_objects
                ORDER BY file_id, id
                """
            ).fetchall()
        ]
    finally:
        conn.close()

    paths = _walk_paths(
        edges,
        clean_table,
        clean_column,
        direction,
        clean_depth,
        clean_limit,
        _additional_object_index(additional_rows),
    )
    transformation_ids = sorted(
        {
            step["transformation_id"]
            for path in paths
            for step in path.get("steps", [])
        }
    )
    display_edges = _visual_edges(paths)
    return {
        "table_name": clean_table,
        "column_name": clean_column,
        "direction": direction,
        "max_depth": clean_depth,
        "returned_paths": len(paths),
        "paths": paths,
        "neo4j_evidence": (
            _neo4j_evidence(transformation_ids)
            if include_neo4j
            else {"included": False, "rows": []}
        ),
        "text_diagram": _text_path_diagram(paths),
        "mermaid": _mermaid_path_diagram(display_edges),
        "edges": display_edges,
    }


def _visual_node(node: Dict[str, Any]) -> str:
    table = _text(node.get("table")) or "?"
    column = _text(node.get("column"))
    return f"{table}.{column}" if column else table


def _visual_edges(paths: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    edges: List[Dict[str, Any]] = []
    seen: set[int] = set()
    for path in paths:
        for step in path.get("steps") or []:
            transformation_id = int(step["transformation_id"])
            if transformation_id in seen:
                continue
            seen.add(transformation_id)
            transformation = step.get("transformation") or {}
            edges.append(
                {
                    "transformation_id": transformation_id,
                    "file_id": int(step["file_id"]),
                    "source": _visual_node(step.get("source") or {}),
                    "target": _visual_node(step.get("target") or {}),
                    "kind": transformation.get("kind") or "unknown",
                    "rule": transformation.get("rule"),
                    "additional_objects": [
                        item.get("name")
                        for item in step.get("additional_objects") or []
                        if item.get("name")
                    ],
                }
            )
    return edges


def _text_path_diagram(paths: Sequence[Dict[str, Any]]) -> str:
    if not paths:
        return "Пути не найдены."

    lines: List[str] = []
    for index, path in enumerate(paths, start=1):
        lines.append(f"Путь {index}:")
        steps = list(path.get("steps") or [])
        if steps and all(step.get("match_direction") == "upstream" for step in steps):
            steps.reverse()
        for step_index, step in enumerate(steps):
            source = _visual_node(step.get("source") or {})
            target = _visual_node(step.get("target") or {})
            kind = (step.get("transformation") or {}).get("kind") or "unknown"
            prefix = "  " if step_index == 0 else "    "
            lines.append(f"{prefix}[{source}] --{kind}--> [{target}]")
        if index < len(paths):
            lines.append("")
    return "\n".join(lines)


def _mermaid_label(value: Any) -> str:
    return str(value or "?").replace("\\", "/").replace('"', "'").replace("\n", " ")


def _mermaid_path_diagram(edges: Sequence[Dict[str, Any]]) -> str:
    if not edges:
        return 'flowchart LR\n    empty["Пути не найдены"]'

    node_ids: Dict[Tuple[int, str], str] = {}

    def node_id(file_id: int, label: str) -> str:
        key = (file_id, label)
        if key not in node_ids:
            node_ids[key] = f"n{len(node_ids) + 1}"
        return node_ids[key]

    for edge in edges:
        node_id(edge["file_id"], edge["source"])
        node_id(edge["file_id"], edge["target"])

    lines = ["flowchart LR"]
    for (_, label), identifier in node_ids.items():
        lines.append(f'    {identifier}["{_mermaid_label(label)}"]')
    for edge in edges:
        source_id = node_ids[(edge["file_id"], edge["source"])]
        target_id = node_ids[(edge["file_id"], edge["target"])]
        edge_label = f'{edge["kind"]} · #{edge["transformation_id"]}'
        lines.append(
            f'    {source_id} -->|"{_mermaid_label(edge_label)}"| {target_id}'
        )
    return "\n".join(lines)


__all__ = [
    "trace_transformation_path",
]
