"""Build the column-lineage Neo4j projection from committed SQLite facts."""

from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

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
                       source_table, source_field
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
    for row in transformations:
        source_key = register_column(
            row.get("source_table"),
            row.get("source_field"),
            "source",
        )
        target_key = register_column(
            row.get("target_table"),
            row.get("target_field"),
            "target",
        )
        if source_key is not None and target_key is not None:
            lineage.append(
                {
                    "file_id": clean_file_id,
                    "transformation_id": int(row["id"]),
                    "source_column_key": source_key,
                    "target_column_key": target_key,
                }
            )

    columns = [
        {**column, "roles": sorted(column["roles"])}
        for column in columns_by_key.values()
    ]
    return {
        "file_id": clean_file_id,
        "columns": columns,
        "lineage": lineage,
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
            transformation_id: row.transformation_id
        }]->(target)
        """,
        file_id=file_id,
        rows=projection["lineage"],
    ).consume()


def sync_file_graph(file_id: int) -> Dict[str, Any]:
    """Replace one file's column-lineage graph from committed SQLite facts."""
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
