"""SQL lineage analysis powered by SQLGlot."""

import hashlib
import re
from typing import Any, Dict, List, Optional, Tuple

import sqlglot
from langchain_core.tools import tool
from sqlglot import exp
from sqlglot.errors import SqlglotError
from sqlglot.lineage import GraphHTML, Node, lineage

from services.sql_dialects import GREENPLUM_DIALECT  # noqa: F401

from .common import PROJECT_ROOT


_ESCAPED_LAYOUT = re.compile(r"\\r\\n|\\n|\\r|\\t")
SQL_LINEAGE_EXPORT_DIR = PROJECT_ROOT / "exports" / "sql_lineage"
SQL_LINEAGE_EXPORT_URL_PREFIX = "/exports/sql-lineage"


def _normalize_escaped_layout(text: str) -> str:
    """Restore formatting escaped a second time in an LLM tool call."""
    if "\n" in text or "\r" in text or "\t" in text:
        return text
    if not _ESCAPED_LAYOUT.search(text):
        return text
    return (
        text.replace(r"\r\n", "\n")
        .replace(r"\n", "\n")
        .replace(r"\r", "\n")
        .replace(r"\t", "\t")
    )


def _qualified_table_name(table: exp.Table) -> str:
    return ".".join(
        part
        for part in (table.catalog, table.db, table.name)
        if part
    )


def _query_target(
    statement: exp.Expression,
) -> Tuple[Optional[exp.Table], List[str]]:
    target = statement.this if isinstance(statement, (exp.Insert, exp.Create)) else None
    target_columns: List[str] = []
    if isinstance(target, exp.Schema):
        target_columns = [column.name for column in target.expressions]
        target = target.this
    return (
        target if isinstance(target, exp.Table) else None,
        target_columns,
    )


def _is_supported_statement(statement: exp.Expression) -> bool:
    if isinstance(statement, exp.Query):
        return True
    if isinstance(statement, exp.Insert):
        return isinstance(statement.expression, exp.Query)
    if isinstance(statement, exp.Create):
        return (
            str(statement.args.get("kind") or "").upper() in {"TABLE", "VIEW"}
            and isinstance(statement.expression, exp.Query)
        )
    return False


def _source_table_names(
    statement: exp.Expression,
    target: Optional[exp.Table],
) -> List[str]:
    cte_names = {
        cte.alias_or_name.casefold()
        for cte in statement.find_all(exp.CTE)
        if cte.alias_or_name
    }
    names: List[str] = []
    for table in statement.find_all(exp.Table):
        if table is target:
            continue
        if not table.catalog and not table.db and table.name.casefold() in cte_names:
            continue
        name = _qualified_table_name(table)
        if name and name not in names:
            names.append(name)
    return names


def _source_column(node: Node) -> Dict[str, str]:
    table = node.expression
    name = node.name.rsplit(".", 1)[-1]
    return {
        "table": _qualified_table_name(table),
        "column": name,
    }


def _lineage_item(
    root: Node,
    target_table: Optional[str],
    target_column: str,
    dialect: Optional[str],
) -> Dict[str, Any]:
    sources: List[Dict[str, str]] = []
    unresolved_sources: List[str] = []
    for node in root.walk():
        if node.downstream:
            continue
        if isinstance(node.expression, exp.Table):
            source = _source_column(node)
            if source not in sources:
                sources.append(source)
        elif node is not root and node.name not in unresolved_sources:
            unresolved_sources.append(node.name)

    expression = root.expression
    if isinstance(expression, exp.Alias):
        expression = expression.this
    sources.sort(key=lambda source: (source["table"], source["column"]))
    return {
        "target_table": target_table,
        "target_column": target_column,
        "expression": expression.sql(dialect=dialect),
        "source_columns": sources,
        "unresolved_source_columns": unresolved_sources,
    }


def analyze_sql_lineage(
    query: str,
    dialect: Optional[str],
    *,
    include_columns: bool,
) -> Dict[str, Any]:
    text = _normalize_escaped_layout(str(query or "")).strip()
    clean_dialect = str(dialect).strip() if dialect and str(dialect).strip() else None
    if not text:
        return {"error": "query must be non-empty", "query": text}

    try:
        statements = [
            statement
            for statement in sqlglot.parse(text, read=clean_dialect)
            if statement is not None
        ]
        if len(statements) != 1:
            return {
                "error": "Exactly one SQL statement is allowed",
                "query": text,
            }

        statement = statements[0]
        if not _is_supported_statement(statement):
            return {
                "error": (
                    "Only SELECT, INSERT ... SELECT and "
                    "CREATE TABLE/VIEW ... AS SELECT are supported"
                ),
                "query": text,
            }

        target, target_columns = _query_target(statement)
        target_table = _qualified_table_name(target) if target is not None else None
        source_tables = _source_table_names(statement, target)
        result = {
            "query": text,
            "dialect": clean_dialect,
            "statement_type": statement.key.upper(),
            "normalized_sql": statement.sql(dialect=clean_dialect),
            "target_table": target_table,
            "source_tables": source_tables,
        }
        if not include_columns:
            result["table_lineage"] = [
                {
                    "source_table": source_table,
                    "target_table": target_table,
                }
                for source_table in source_tables
            ]
            return result

        roots = lineage(None, statement, dialect=clean_dialect)
        result["column_lineage"] = [
            _lineage_item(
                root,
                target_table,
                target_columns[index] if index < len(target_columns) else output_name,
                clean_dialect,
            )
            for index, (output_name, root) in enumerate(roots.items())
        ]
        return result
    except (SqlglotError, ValueError) as exc:
        return {
            "error": "SQL lineage parsing failed",
            "details": str(exc),
            "query": text,
        }


def _sqlglot_graph_html(
    query: str,
    dialect: Optional[str],
) -> str:
    roots = lineage(None, query, dialect=dialect)
    nodes: Dict[int, Dict[str, Any]] = {}
    edges: List[Dict[str, Any]] = []
    seen_edges: set[Tuple[int, int]] = set()

    for root in roots.values():
        graph = root.to_html(dialect=dialect, imports=False)
        nodes.update(graph.nodes)
        for edge in graph.edges:
            key = (int(edge["from"]), int(edge["to"]))
            if key not in seen_edges:
                seen_edges.add(key)
                edges.append(edge)

    graph = GraphHTML(
        nodes,
        edges,
        options={
            "height": "100vh",
            "width": "100%",
            "layout": {
                "hierarchical": {
                    "enabled": True,
                    "nodeSpacing": 180,
                    "direction": "LR",
                }
            },
            "physics": {"enabled": False},
        },
    )
    return (
        "<!doctype html><html lang=\"ru\"><head><meta charset=\"utf-8\">"
        "<meta name=\"viewport\" content=\"width=device-width,initial-scale=1\">"
        "<title>SQL lineage</title><style>html,body{margin:0;width:100%;height:100%;"
        "overflow:hidden;background:#fff}</style></head><body>"
        f"{graph}</body></html>"
    )


def _write_sqlglot_graph(query: str, dialect: Optional[str]) -> Dict[str, str]:
    digest = hashlib.sha256(
        f"{dialect or ''}\0{query}".encode("utf-8")
    ).hexdigest()[:24]
    filename = f"sql_lineage_{digest}.html"
    SQL_LINEAGE_EXPORT_DIR.mkdir(parents=True, exist_ok=True)
    (SQL_LINEAGE_EXPORT_DIR / filename).write_text(
        _sqlglot_graph_html(query, dialect),
        encoding="utf-8",
    )
    return {
        "visualization_type": "sqlglot_graph_html",
        "visualization_url": f"{SQL_LINEAGE_EXPORT_URL_PREFIX}/{filename}",
    }


@tool(parse_docstring=True)
def parse_sql_column_lineage(
    query: str,
    dialect: Optional[str] = None,
) -> Dict[str, Any]:
    """Разобрать SQL-текст и построить lineage его выходных колонок.

    Используй для зависимостей колонок в переданном SQL или правиле
    трансформации. Также выбирай этот tool для продолжения «этот запрос», «эта
    трансформация» или «для неё lineage», если SQL есть в истории. Это имеет
    приоритет над Neo4j-tools: не извлекай из SQL произвольную таблицу для
    trace_neo4j_lineage. Инструмент ничего не выполняет и не читает БД.

    SQLGlot учитывает алиасы, CTE, SELECT, INSERT ... SELECT и CREATE ... AS
    SELECT. Неразрешимые звёздочки и колонки возвращаются отдельно. Если текста
    SQL ещё нет, сначала прочитай transformation_rule из SQLite.

    Args:
        query: Текст одного SQL-запроса для колонкового разбора.
        dialect: Опциональное имя SQL-диалекта SQLGlot, например greenplum, postgres или spark.
    """
    return analyze_sql_lineage(query, dialect, include_columns=True)


@tool(parse_docstring=True)
def parse_sql_table_lineage(
    query: str,
    dialect: Optional[str] = None,
) -> Dict[str, Any]:
    """Разобрать SQL-текст и построить lineage на уровне таблиц.

    Используй для SQL, когда пользователь просит только исходные и целевую
    таблицы без колонок. Продолжение про «эту трансформацию» относится сюда лишь
    при явном табличном уровне; иначе используй parse_sql_column_lineage.
    Инструмент ничего не выполняет и не читает SQLite или Neo4j. SQLGlot
    раскрывает алиасы и CTE; у обычного SELECT target_table равен null.

    Args:
        query: Текст одного SQL-запроса для табличного разбора.
        dialect: Опциональное имя SQL-диалекта SQLGlot, например greenplum, postgres или spark.
    """
    return analyze_sql_lineage(query, dialect, include_columns=False)


@tool(parse_docstring=True)
def visualize_sql_lineage(
    query: str,
    dialect: Optional[str] = None,
) -> Dict[str, Any]:
    """Построить интерактивный HTML-граф колонкового lineage переданного SQL.

    Это единственный tool для интерактивной визуализации конкретного SQL-текста:
    выбирай его сразу вместо parse_sql_column_lineage и никогда не подменяй
    глобальным visualize_s2t_table_graph. Используй, когда пользователь просит
    нарисовать, визуализировать, вывести граф или схему для переданного либо
    ранее показанного SQL. Инструмент возвращает фактический column_lineage и
    URL интерактивного HTML-графа, созданного нативным SQLGlot GraphHTML/vis.js.
    Не печатай HTML, Mermaid или visualization_url в ответ: приложение само
    добавит и встроит ссылку на готовый граф.
    Инструмент не читает SQLite/Neo4j и не предназначен для сохранённых
    S2T-путей: для них используй trace_transformation_path, который также
    возвращает готовую text_diagram.

    Args:
        query: Текст одного SQL-запроса для построения графа зависимостей.
        dialect: Опциональное имя SQL-диалекта SQLGlot, например greenplum, postgres или spark.
    """
    result = analyze_sql_lineage(query, dialect, include_columns=True)
    if result.get("error"):
        return result

    return {
        **result,
        **_write_sqlglot_graph(result["normalized_sql"], result.get("dialect")),
    }
