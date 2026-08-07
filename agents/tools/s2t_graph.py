"""Interactive table-level graph built from global S2T transformations."""

from __future__ import annotations

import hashlib
import html
import json
import re
from collections import defaultdict
from typing import Any, Dict, List, Optional, Set, Tuple

from langchain_core.tools import tool
from sqlglot.lineage import GraphHTML

from storage.s2t import load_s2t_table_graph_rows

from .common import PROJECT_ROOT
from .sql_lineage import analyze_sql_lineage


S2T_TABLE_GRAPH_EXPORT_DIR = PROJECT_ROOT / "exports" / "s2t_graphs"
S2T_TABLE_GRAPH_EXPORT_URL_PREFIX = "/exports/s2t-graphs"
_SQL_START = re.compile(
    r"(?im)^\s*(?:select|with|insert|create)\b"
)


def _text(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _sql_candidate(rule: Any) -> Optional[str]:
    text = _text(rule)
    if text is None or text == "-":
        return None
    match = _SQL_START.search(text)
    return text[match.start():].strip() if match else None


def _mapping_payload(row: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "transformation_id": int(row["id"]),
        "file_id": int(row["file_id"]),
        "source_field": _text(row.get("source_field")),
        "source_layer": _text(row.get("source_layer")),
        "target_field": _text(row.get("target_field")),
        "target_layer": _text(row.get("target_layer")),
    }


def _build_table_graph(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    parsed_rules: Dict[str, Dict[str, Any]] = {}
    parse_errors: List[Dict[str, Any]] = []
    edge_data: Dict[Tuple[str, str], Dict[str, Any]] = {}
    ignored_rows = 0

    for row in rows:
        target = _text(row.get("target_table"))
        if target is None:
            ignored_rows += 1
            continue

        sql = _sql_candidate(row.get("transformation_rule"))
        sources: List[str] = []
        evidence = "s2t_fields"
        if sql is not None:
            analysis = parsed_rules.get(sql)
            if analysis is None:
                analysis = analyze_sql_lineage(sql, None, include_columns=False)
                parsed_rules[sql] = analysis
                if analysis.get("error"):
                    parse_errors.append(
                        {
                            "rule_hash": hashlib.sha256(sql.encode("utf-8")).hexdigest()[:16],
                            "details": analysis.get("details") or analysis["error"],
                            "query_preview": sql[:300],
                        }
                    )
            if not analysis.get("error"):
                sources = [
                    name
                    for value in analysis.get("source_tables") or []
                    if (name := _text(value)) is not None
                ]
                evidence = "transformation_rule_sql"

        if not sources:
            source = _text(row.get("source_table"))
            if source is not None:
                sources = [source]

        if not sources:
            ignored_rows += 1
            continue

        mapping = _mapping_payload(row)
        for source in dict.fromkeys(sources):
            key = (source.casefold(), target.casefold())
            edge = edge_data.setdefault(
                key,
                {
                    "source_table": source,
                    "target_table": target,
                    "evidence": set(),
                    "transformation_ids": set(),
                    "mappings": [],
                },
            )
            edge["evidence"].add(evidence)
            if mapping["transformation_id"] not in edge["transformation_ids"]:
                edge["transformation_ids"].add(mapping["transformation_id"])
                edge["mappings"].append(mapping)

    edges: List[Dict[str, Any]] = []
    for edge in edge_data.values():
        transformation_ids = sorted(edge.pop("transformation_ids"))
        evidence = sorted(edge["evidence"])
        edges.append(
            {
                **edge,
                "evidence": evidence,
                "mapping_count": len(transformation_ids),
                "transformation_ids": transformation_ids,
            }
        )
    edges.sort(
        key=lambda edge: (
            edge["source_table"].casefold(),
            edge["target_table"].casefold(),
        )
    )

    return {
        "scope": "global",
        "rows_analyzed": len(rows),
        "ignored_rows": ignored_rows,
        "sql_rules_analyzed": len(parsed_rules),
        "sql_parse_error_count": len(parse_errors),
        "sql_parse_errors": parse_errors,
        "edges": edges,
    }


def _edge_title(edge: Dict[str, Any]) -> str:
    mappings = edge["mappings"]
    lines = [
        f"{edge['source_table']} → {edge['target_table']}",
        f"Маппингов: {edge['mapping_count']}",
    ]
    layer_pairs = sorted(
        {
            f"{mapping['source_layer'] or '?'} → {mapping['target_layer'] or '?'}"
            for mapping in mappings
        }
    )
    if layer_pairs:
        lines.append("Слои: " + ", ".join(layer_pairs))
    for mapping in mappings[:30]:
        source_field = mapping.get("source_field") or "?"
        target_field = mapping.get("target_field") or "?"
        lines.append(f"{source_field} → {target_field}")
    if len(mappings) > 30:
        lines.append(f"… ещё {len(mappings) - 30}")
    return "<br>".join(html.escape(line) for line in lines)


def _table_graph_html(edges: List[Dict[str, Any]]) -> str:
    sources: Set[str] = {edge["source_table"] for edge in edges}
    targets: Set[str] = {edge["target_table"] for edge in edges}
    table_names = sorted(sources | targets, key=str.casefold)
    node_ids = {name: index for index, name in enumerate(table_names, start=1)}
    nodes: Dict[int, Dict[str, Any]] = {}
    for table_name, node_id in node_ids.items():
        if table_name in sources and table_name in targets:
            group = "both"
        elif table_name in targets:
            group = "target"
        else:
            group = "source"
        nodes[node_id] = {
            "id": node_id,
            "label": table_name,
            "title": html.escape(table_name),
            "group": group,
        }

    graph_edges = [
        {
            "from": node_ids[edge["source_table"]],
            "to": node_ids[edge["target_table"]],
            "arrows": "to",
            "label": f"{edge['mapping_count']} мапп.",
            "title": _edge_title(edge),
        }
        for edge in edges
    ]
    graph = GraphHTML(
        nodes,
        graph_edges,
        options={
            "height": "100vh",
            "width": "100%",
            "groups": {
                "source": {"color": {"background": "#dbeafe", "border": "#2563eb"}},
                "target": {"color": {"background": "#dcfce7", "border": "#16a34a"}},
                "both": {"color": {"background": "#fef3c7", "border": "#d97706"}},
            },
            "layout": {
                "hierarchical": {
                    "enabled": True,
                    "direction": "LR",
                    "levelSeparation": 280,
                    "nodeSpacing": 150,
                    "sortMethod": "directed",
                }
            },
            "interaction": {
                "hover": True,
                "navigationButtons": True,
                "keyboard": True,
            },
            "physics": {"enabled": False},
        },
    )
    return (
        '<!doctype html><html lang="ru"><head><meta charset="utf-8">'
        '<meta name="viewport" content="width=device-width,initial-scale=1">'
        '<title>Граф связей S2T-таблиц</title>'
        '<style>html,body{margin:0;width:100%;height:100%;overflow:hidden;'
        'background:#fff}</style></head><body>'
        f"{graph}</body></html>"
    )


def _write_graph_artifacts(result: Dict[str, Any]) -> Dict[str, str]:
    serialized = json.dumps(result, ensure_ascii=False, sort_keys=True)
    digest = hashlib.sha256(serialized.encode("utf-8")).hexdigest()[:24]
    stem = f"s2t_table_graph_{digest}"
    S2T_TABLE_GRAPH_EXPORT_DIR.mkdir(parents=True, exist_ok=True)
    (S2T_TABLE_GRAPH_EXPORT_DIR / f"{stem}.json").write_text(
        json.dumps(result, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    (S2T_TABLE_GRAPH_EXPORT_DIR / f"{stem}.html").write_text(
        _table_graph_html(result["edges"]),
        encoding="utf-8",
    )
    return {
        "visualization_type": "s2t_table_graph_html",
        "visualization_url": f"{S2T_TABLE_GRAPH_EXPORT_URL_PREFIX}/{stem}.html",
        "data_url": f"{S2T_TABLE_GRAPH_EXPORT_URL_PREFIX}/{stem}.json",
    }


@tool(parse_docstring=True)
def visualize_s2t_table_graph() -> Dict[str, Any]:
    """Построить глобальный интерактивный граф всех сохранённых S2T-таблиц.

    Не используй для конкретного переданного SQL-текста: его интерактивный
    lineage-граф строит visualize_sql_lineage. Этот tool нужен только для явной
    просьбы показать глобальную схему связей всех таблиц по сохранённой
    s2t_transformations. Инструмент никогда не принимает и не применяет file_id.
    Для каждой S2T-строки он разбирает SQL
    из transformation_rule через SQLGlot и связывает найденные исходные таблицы
    с сохранённой target_table. Для прямого правила, текста без SQL или ошибки
    разбора используется сохранённая пара source_table → target_table.

    Повторяющиеся строки не исчезают из фактов: они учитываются в количестве
    маппингов, но на диаграмме одна пара таблиц показана одним ребром. Полный
    список подтверждающих строк сохраняется в JSON, а visualization_url ведёт
    на готовый интерактивный HTML. Не печатай DOT, Mermaid или содержимое HTML
    в ответ — приложение само встроит возвращённую визуализацию.
    """
    rows = load_s2t_table_graph_rows()
    if not rows:
        return {
            "error": "s2t_transformations is empty",
            "scope": "global",
            "rows_analyzed": 0,
            "edges": [],
        }

    result = _build_table_graph(rows)
    if not result["edges"]:
        return {
            **result,
            "error": "No table relationships could be built",
        }

    node_count = len(
        {
            table_name
            for edge in result["edges"]
            for table_name in (edge["source_table"], edge["target_table"])
        }
    )
    artifacts = _write_graph_artifacts(result)
    return {
        "scope": result["scope"],
        "rows_analyzed": result["rows_analyzed"],
        "ignored_rows": result["ignored_rows"],
        "node_count": node_count,
        "edge_count": len(result["edges"]),
        "sql_rules_analyzed": result["sql_rules_analyzed"],
        "sql_parse_error_count": result["sql_parse_error_count"],
        **artifacts,
    }


__all__ = [
    "S2T_TABLE_GRAPH_EXPORT_DIR",
    "S2T_TABLE_GRAPH_EXPORT_URL_PREFIX",
    "visualize_s2t_table_graph",
]
