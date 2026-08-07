"""Explicit read-only and mutating tool registries."""

from typing import Dict, Iterable, Tuple

from langchain_core.tools import BaseTool

from .files import (
    get_file_description,
    list_files,
    resolve_file,
    update_file_description,
    update_table_info_from_user_query,
)
from .planning import show_plan
from .data import get_excel_row, search_excel_values, semantic_search_descriptions
from .neo4j import run_cypher, trace_neo4j_lineage, trace_neo4j_table_lineage
from .s2t import (
    list_s2t_table_names,
    list_s2t_transformations,
    search_s2t_transformations,
    summarize_s2t_tables,
    summarize_table_descriptions,
)
from .s2t_graph import visualize_s2t_table_graph
from .sheets import (
    list_columns,
    list_file_sheet_headers,
    # list_sheet_group_classifications,  # Только внутренняя диагностика extraction.
    list_sheets,
)
from .sql import run_sql
from .sql_lineage import (
    parse_sql_column_lineage,
    parse_sql_table_lineage,
    visualize_sql_lineage,
)
from .transformation_paths import trace_transformation_path

READ_ONLY_TOOLS: Tuple[BaseTool, ...] = (
    show_plan,
    search_excel_values,
    get_excel_row,
    semantic_search_descriptions,
    visualize_s2t_table_graph,
    trace_transformation_path,
    parse_sql_column_lineage,
    parse_sql_table_lineage,
    visualize_sql_lineage,
    run_sql,
    run_cypher,
    trace_neo4j_lineage,
    trace_neo4j_table_lineage,
    list_files,
    resolve_file,
    get_file_description,
    list_s2t_table_names,
    list_s2t_transformations,
    search_s2t_transformations,
    summarize_s2t_tables,
    summarize_table_descriptions,
    list_sheets,
    list_file_sheet_headers,
    # list_sheet_group_classifications,
    list_columns,
)

WRITE_TOOLS: Tuple[BaseTool, ...] = (
    update_file_description,
    update_table_info_from_user_query,
)

ALL_TOOLS: Tuple[BaseTool, ...] = READ_ONLY_TOOLS + WRITE_TOOLS
TOOLS: Tuple[BaseTool, ...] = READ_ONLY_TOOLS
TOOLS_BY_NAME: Dict[str, BaseTool] = {tool.name: tool for tool in TOOLS}
WRITE_TOOLS_BY_NAME: Dict[str, BaseTool] = {
    tool.name: tool for tool in WRITE_TOOLS
}
ALL_TOOLS_BY_NAME: Dict[str, BaseTool] = {
    tool.name: tool for tool in ALL_TOOLS
}

def get_tools() -> Tuple[BaseTool, ...]:
    """Вернуть неизменяемую коллекцию read-only инструментов."""
    return TOOLS


def get_tools_for_names(
    tool_names: Iterable[str],
) -> Tuple[BaseTool, ...]:
    """Вернуть ровно выбранные read-only tools в порядке общего registry."""
    selected = tuple(dict.fromkeys(tool_names))
    if not selected:
        raise ValueError("Нужно выбрать хотя бы один tool")

    unknown = [name for name in selected if name not in TOOLS_BY_NAME]
    if unknown:
        raise ValueError(f"Неизвестные read-only tools: {', '.join(unknown)}")

    selected_set = set(selected)
    return tuple(tool for tool in TOOLS if tool.name in selected_set)


def get_tools_by_name() -> Dict[str, BaseTool]:
    """Вернуть копию read-only реестра инструментов по именам."""
    return dict(TOOLS_BY_NAME)


def get_write_tools() -> Tuple[BaseTool, ...]:
    """Вернуть мутирующие инструменты для подтверждаемого runtime."""
    return WRITE_TOOLS


def get_all_tools() -> Tuple[BaseTool, ...]:
    """Вернуть полный набор инструментов, включая мутирующие."""
    return ALL_TOOLS
