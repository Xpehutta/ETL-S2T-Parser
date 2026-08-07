"""Build table- and column-lineage Neo4j projections from committed SQLite facts."""

from __future__ import annotations

import json
from typing import Any, Dict, List, Optional, Tuple

from graph_storage import (
    close_neo4j_driver,
    create_neo4j_driver,
    is_neo4j_configured,
    load_neo4j_settings,
)
from storage.database import get_db_connection


def _text(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _column_key(file_id: int, table_name: str, column_name: str) -> str:
    return json.dumps(
        [int(file_id), table_name, column_name],
        ensure_ascii=False,
        separators=(",", ":"),
    )


def _table_key(file_id: int, table_name: str) -> str:
    return json.dumps(
        [int(file_id), table_name],
        ensure_ascii=False,
        separators=(",", ":"),
    )


def _build_file_graph_projection(file_id: int) -> Dict[str, Any]:
    clean_file_id = int(file_id)
    conn = get_db_connection()
    try:
        file_exists = conn.execute(
            "SELECT 1 FROM files WHERE file_id = ?",
            (clean_file_id,),
        ).fetchone()
        if file_exists is None:
            raise ValueError(f"File not found: {clean_file_id}")

        transformations = [
            dict(row)
            for row in conn.execute(
                """
                SELECT id, target_table, target_field,
                       target_layer, source_table, source_field,
                       source_layer, transformation_rule
                FROM s2t_transformations
                WHERE file_id = ?
                ORDER BY id
                """,
                (clean_file_id,),
            ).fetchall()
        ]
    finally:
        conn.close()

    columns_by_key: Dict[str, Dict[str, Any]] = {}
    tables_by_key: Dict[str, Dict[str, Any]] = {}

    def register_table(
        table_name: Any,
        role: str,
        layer: Any = None,
    ) -> Optional[str]:
        clean_table = _text(table_name)
        if clean_table is None:
            return None

        key = _table_key(clean_file_id, clean_table)
        table = tables_by_key.setdefault(
            key,
            {
                "key": key,
                "file_id": clean_file_id,
                "name": clean_table,
                "roles": set(),
                "layers": set(),
            },
        )
        table["roles"].add(role)
        clean_layer = _text(layer)
        if clean_layer is not None:
            table["layers"].add(clean_layer)
        return key

    def register_column(
        table_name: Any,
        column_name: Any,
        role: str,
    ) -> Optional[str]:
        clean_table = _text(table_name)
        clean_column = _text(column_name)
        if clean_table is None or clean_column is None:
            return None

        key = _column_key(clean_file_id, clean_table, clean_column)
        column = columns_by_key.setdefault(
            key,
            {
                "key": key,
                "file_id": clean_file_id,
                "table_name": clean_table,
                "name": clean_column,
                "roles": set(),
            },
        )
        column["roles"].add(role)
        return key

    lineage: List[Dict[str, Any]] = []
    table_lineage: List[Dict[str, Any]] = []
    wildcard_table_pairs: List[Tuple[str, str]] = []
    for row in transformations:
        source_field = _text(row.get("source_field"))
        target_field = _text(row.get("target_field"))
        source_table = _text(row.get("source_table"))
        target_table = _text(row.get("target_table"))
        source_table_key = register_table(
            source_table, "source", row.get("source_layer")
        )
        target_table_key = register_table(
            target_table, "target", row.get("target_layer")
        )
        # Wildcard is a logical column rule. Materialize it as ETLColumn("*")
        # so the complete column graph is represented by TRANSFORMS_TO edges.
        source_key = register_column(
            source_table,
            source_field,
            "source",
        )
        target_key = register_column(
            target_table,
            target_field,
            "target",
        )
        if (
            source_field == "*"
            and target_field == "*"
            and source_table is not None
            and target_table is not None
        ):
            wildcard_table_pairs.append((source_table, target_table))
        if source_key is not None and target_key is not None:
            lineage.append(
                {
                    "file_id": clean_file_id,
                    "transformation_id": int(row["id"]),
                    "source_column_key": source_key,
                    "target_column_key": target_key,
                    "source_layer": _text(row.get("source_layer")),
                    "target_layer": _text(row.get("target_layer")),
                }
            )
        sql_query = _text(row.get("transformation_rule"))
        if (
            source_table_key is not None
            and target_table_key is not None
            and sql_query is not None
        ):
            table_lineage.append(
                {
                    "file_id": clean_file_id,
                    "transformation_id": int(row["id"]),
                    "source_table_key": source_table_key,
                    "target_table_key": target_table_key,
                    "source_layer": _text(row.get("source_layer")),
                    "target_layer": _text(row.get("target_layer")),
                    "sql_query": sql_query,
                }
            )

    # Propagate all currently known concrete column names through wildcard
    # table components. This materializes the same-name columns on both sides
    # without producing false cross-column paths such as id -> name.
    concrete_names_by_table: Dict[str, set[str]] = {}
    for column in columns_by_key.values():
        if column["name"] == "*":
            continue
        concrete_names_by_table.setdefault(column["table_name"], set()).add(
            column["name"]
        )
    changed = True
    while changed:
        changed = False
        for source_table, target_table in wildcard_table_pairs:
            shared_names = (
                concrete_names_by_table.get(source_table, set())
                | concrete_names_by_table.get(target_table, set())
            )
            for table_name in (source_table, target_table):
                table_names = concrete_names_by_table.setdefault(
                    table_name,
                    set(),
                )
                before = len(table_names)
                table_names.update(shared_names)
                changed = changed or len(table_names) != before

    for source_table, target_table in wildcard_table_pairs:
        shared_names = (
            concrete_names_by_table.get(source_table, set())
            | concrete_names_by_table.get(target_table, set())
        )
        for column_name in shared_names:
            register_column(source_table, column_name, "source")
            register_column(target_table, column_name, "target")

    wildcard_memberships_by_key: Dict[Tuple[str, str], Dict[str, Any]] = {}
    wildcard_tables = {
        table_name
        for pair in wildcard_table_pairs
        for table_name in pair
    }
    for table_name in wildcard_tables:
        wildcard_key = _column_key(clean_file_id, table_name, "*")
        if wildcard_key not in columns_by_key:
            continue
        for column_name in sorted(concrete_names_by_table.get(table_name, set())):
            column_key = _column_key(clean_file_id, table_name, column_name)
            if column_key not in columns_by_key:
                continue
            wildcard_memberships_by_key[(column_key, wildcard_key)] = {
                "file_id": clean_file_id,
                "column_key": column_key,
                "wildcard_key": wildcard_key,
            }

    columns = [
        {**column, "roles": sorted(column["roles"])}
        for column in columns_by_key.values()
    ]
    tables = [
        {
            **table,
            "roles": sorted(table["roles"]),
            "layers": sorted(table["layers"]),
        }
        for table in tables_by_key.values()
    ]
    return {
        "file_id": clean_file_id,
        "columns": columns,
        "lineage": lineage,
        "wildcard_memberships": list(wildcard_memberships_by_key.values()),
        "tables": tables,
        "table_lineage": table_lineage,
    }


def _replace_file_graph(tx, projection: Dict[str, Any]) -> None:
    file_id = int(projection["file_id"])

    tx.run(
        "MATCH (node:ETLProjection {file_id: $file_id}) DETACH DELETE node",
        file_id=file_id,
    ).consume()
    tx.run(
        """
        UNWIND $rows AS row
        CREATE (:ETLProjection:ETLColumn {
            file_id: $file_id,
            key: row.key,
            table_name: row.table_name,
            name: row.name,
            roles: row.roles
        })
        """,
        file_id=file_id,
        rows=projection["columns"],
    ).consume()
    tx.run(
        """
        UNWIND $rows AS row
        CREATE (:ETLProjection:ETLTable {
            file_id: $file_id,
            key: row.key,
            name: row.name,
            roles: row.roles,
            layers: row.layers
        })
        """,
        file_id=file_id,
        rows=projection["tables"],
    ).consume()
    tx.run(
        """
        UNWIND $rows AS row
        MATCH (source:ETLProjection:ETLColumn {
            file_id: $file_id,
            key: row.source_column_key
        })
        MATCH (target:ETLProjection:ETLColumn {
            file_id: $file_id,
            key: row.target_column_key
        })
        CREATE (source)-[:TRANSFORMS_TO {
            file_id: $file_id,
            transformation_id: row.transformation_id,
            source_layer: row.source_layer,
            target_layer: row.target_layer
        }]->(target)
        """,
        file_id=file_id,
        rows=projection["lineage"],
    ).consume()
    tx.run(
        """
        UNWIND $rows AS row
        MATCH (column:ETLProjection:ETLColumn {
            file_id: $file_id,
            key: row.column_key
        })
        MATCH (wildcard:ETLProjection:ETLColumn {
            file_id: $file_id,
            key: row.wildcard_key
        })
        CREATE (column)-[:COVERED_BY {file_id: $file_id}]->(wildcard)
        CREATE (wildcard)-[:EXPANDS_TO {file_id: $file_id}]->(column)
        """,
        file_id=file_id,
        rows=projection["wildcard_memberships"],
    ).consume()
    tx.run(
        """
        UNWIND $rows AS row
        MATCH (source:ETLProjection:ETLTable {
            file_id: $file_id,
            key: row.source_table_key
        })
        MATCH (target:ETLProjection:ETLTable {
            file_id: $file_id,
            key: row.target_table_key
        })
        CREATE (source)-[:TABLE_TRANSFORMS_TO {
            file_id: $file_id,
            transformation_id: row.transformation_id,
            source_layer: row.source_layer,
            target_layer: row.target_layer,
            sql_query: row.sql_query
        }]->(target)
        """,
        file_id=file_id,
        rows=projection["table_lineage"],
    ).consume()


def sync_file_graph(file_id: int) -> Dict[str, Any]:
    """Replace one file's table- and column-lineage graph from SQLite facts."""
    projection = _build_file_graph_projection(file_id)
    settings = load_neo4j_settings()
    driver = create_neo4j_driver(settings)
    try:
        with driver.session(database=settings.database) as session:
            session.execute_write(_replace_file_graph, projection)
    finally:
        close_neo4j_driver(driver)

    return {
        "file_id": int(file_id),
        "columns": len(projection["columns"]),
        "lineage_relationships": len(projection["lineage"]),
        "wildcard_membership_relationships": (
            2 * len(projection["wildcard_memberships"])
        ),
        "tables": len(projection["tables"]),
        "table_lineage_relationships": len(projection["table_lineage"]),
    }


def _clear_graph_projection(tx) -> int:
    summary = tx.run(
        "MATCH (node:ETLProjection) DETACH DELETE node"
    ).consume()
    return int(summary.counters.nodes_deleted)


def clear_graph_projection() -> Dict[str, Any]:
    """Delete the complete application-owned Neo4j projection."""
    if not is_neo4j_configured():
        return {
            "nodes": 0,
            "skipped": True,
            "reason": "Neo4j не настроен",
        }
    settings = load_neo4j_settings()
    driver = create_neo4j_driver(settings)
    try:
        with driver.session(database=settings.database) as session:
            deleted = session.execute_write(_clear_graph_projection)
    finally:
        close_neo4j_driver(driver)
    return {"nodes": int(deleted)}
