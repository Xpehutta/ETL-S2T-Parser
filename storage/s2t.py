"""Persistence API for validated S2T transformation records."""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from config.table_layers import resolve_sheet_layers

from .database import S2T_RECORD_FIELDS, _sql_identifier, get_db_connection

logger = logging.getLogger(__name__)

S2T_TABLE_SET_OPERATIONS = {
    "sources",
    "targets",
    "intersection",
    "source_only",
    "target_only",
    "union",
}


def insert_s2t_transformations(file_id: int, records: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Transactionally append validated S2T records without deleting stored rows."""
    insert_columns = (
        "file_id",
        "sheet_name",
        "row_num",
        *S2T_RECORD_FIELDS,
    )
    columns_sql = ", ".join(_sql_identifier(column) for column in insert_columns)
    placeholders = ", ".join("?" for _ in insert_columns)
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("BEGIN")
        cursor.executemany(
            f"""
            INSERT INTO s2t_transformations
            ({columns_sql})
            VALUES ({placeholders})
            """,
            [
                (
                    row["file_id"],
                    row["sheet_name"],
                    row["row_num"],
                    *(row.get(field) for field in S2T_RECORD_FIELDS),
                )
                for row in records
            ],
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    return {"file_id": file_id, "count": len(records)}


def replace_s2t_transformations_for_source_rows(
    file_id: int,
    source_table: str,
    records: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """Atomically replace generated rows matching metadata source rows."""
    clean_file_id = int(file_id)
    clean_source_table = _sql_identifier(source_table)
    if any(int(row["file_id"]) != clean_file_id for row in records):
        raise ValueError("All replacement records must belong to file_id")

    insert_columns = (
        "file_id",
        "sheet_name",
        "row_num",
        *S2T_RECORD_FIELDS,
    )
    columns_sql = ", ".join(_sql_identifier(column) for column in insert_columns)
    placeholders = ", ".join("?" for _ in insert_columns)
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("BEGIN")
        cursor.execute(
            f"""
            SELECT COUNT(*) AS n
            FROM s2t_transformations
            WHERE file_id = ?
              AND EXISTS (
                  SELECT 1
                  FROM {clean_source_table} AS source
                  WHERE source.file_id = s2t_transformations.file_id
                    AND source.sheet_name = s2t_transformations.sheet_name
                    AND source.row_num = s2t_transformations.row_num
              )
            """,
            (clean_file_id,),
        )
        deleted = int(cursor.fetchone()["n"])
        cursor.execute(
            f"""
            DELETE FROM s2t_transformations
            WHERE file_id = ?
              AND EXISTS (
                  SELECT 1
                  FROM {clean_source_table} AS source
                  WHERE source.file_id = s2t_transformations.file_id
                    AND source.sheet_name = s2t_transformations.sheet_name
                    AND source.row_num = s2t_transformations.row_num
              )
            """,
            (clean_file_id,),
        )
        cursor.executemany(
            f"""
            INSERT INTO s2t_transformations
            ({columns_sql})
            VALUES ({placeholders})
            """,
            [
                (
                    row["file_id"],
                    row["sheet_name"],
                    row["row_num"],
                    *(row.get(field) for field in S2T_RECORD_FIELDS),
                )
                for row in records
            ],
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    return {
        "file_id": clean_file_id,
        "deleted": deleted,
        "count": len(records),
    }


def clear_s2t_transformations(file_id: int) -> int:
    """Delete generated S2T transformation rows for one workbook."""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) AS n FROM s2t_transformations WHERE file_id = ?", (file_id,))
    deleted = int(cursor.fetchone()["n"])
    cursor.execute("DELETE FROM s2t_transformations WHERE file_id = ?", (file_id,))
    conn.commit()
    conn.close()
    logger.info("Cleared %s S2T transformation rows for file %s", deleted, file_id)
    return deleted


def list_s2t_transformations(
    file_id: Optional[int] = None,
    limit: int = 200,
    q: Optional[str] = None,
    columns: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """Return minimal stored S2T transformations for UI/API browsing."""
    clean_limit = max(1, min(int(limit or 200), 1000))
    available_columns = ("row_num", *S2T_RECORD_FIELDS)
    selected_columns = list(available_columns if columns is None else columns)
    invalid_columns = [column for column in selected_columns if column not in available_columns]
    if invalid_columns:
        return {
            "error": f"Unknown s2t_transformations columns: {', '.join(invalid_columns)}",
            "available_columns": list(available_columns),
            "rows": [],
        }
    if not selected_columns:
        return {
            "error": "columns must contain at least one s2t_transformations column",
            "available_columns": list(available_columns),
            "rows": [],
        }
    params: List[Any] = []
    where = ["IFNULL(row_num, 0) >= 0"]
    if file_id is not None:
        where.insert(0, "file_id = ?")
        params.append(int(file_id))
    if q:
        pattern = f"%{q.strip()}%"
        where.append(
            "(" + " OR ".join(f"{_sql_identifier(field)} LIKE ?" for field in S2T_RECORD_FIELDS) + ")"
        )
        params.extend([pattern] * len(S2T_RECORD_FIELDS))
    where_sql = " AND ".join(where)
    selected_columns_sql = ", ".join(
        _sql_identifier(column) for column in selected_columns
    )

    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(f"SELECT COUNT(*) AS n FROM s2t_transformations WHERE {where_sql}", params)
    total = int(cursor.fetchone()["n"])
    cursor.execute(
        f"""
        SELECT {selected_columns_sql}
        FROM s2t_transformations
        WHERE {where_sql}
        ORDER BY id
        LIMIT ?
        """,
        params + [clean_limit],
    )
    rows = [dict(row) for row in cursor.fetchall()]
    conn.close()
    result = {
        "scope": "global" if file_id is None else "file",
        "total": total,
        "limit": clean_limit,
        "columns": selected_columns,
        "rows": rows,
    }
    if file_id is not None:
        result["file_id"] = int(file_id)
    return result


def verify_s2t_transformations(file_id: int, limit: int = 5) -> Dict[str, Any]:
    """Return the stored row count and a small verification sample."""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT COUNT(*) AS n
        FROM s2t_transformations
        WHERE file_id = ? AND IFNULL(row_num, 0) >= 0
        """,
        (file_id,),
    )
    count = int(cursor.fetchone()["n"])
    cursor.execute(
        f"""
        SELECT row_num, {", ".join(_sql_identifier(field) for field in S2T_RECORD_FIELDS)}
        FROM s2t_transformations
        WHERE file_id = ? AND IFNULL(row_num, 0) >= 0
        ORDER BY row_num
        LIMIT ?
        """,
        (file_id, limit),
    )
    rows = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return {"file_id": file_id, "count": count, "rows": rows}


def list_s2t_table_names(
    set_operation: str = "intersection",
    limit: int = 100,
) -> Dict[str, Any]:
    """Return a deterministic set operation over source/target table names."""
    clean_operation = str(set_operation or "").strip().lower()
    if clean_operation not in S2T_TABLE_SET_OPERATIONS:
        return {
            "error": (
                "set_operation must be one of: "
                + ", ".join(sorted(S2T_TABLE_SET_OPERATIONS))
            ),
            "set_operation": clean_operation,
            "columns": ["table_name"],
            "rows": [],
            "returned_rows": 0,
        }

    clean_limit = max(1, min(int(limit or 100), 200))
    selected_sql = {
        "sources": "SELECT table_name FROM source_names",
        "targets": "SELECT table_name FROM target_names",
        "intersection": (
            "SELECT table_name FROM source_names "
            "INTERSECT SELECT table_name FROM target_names"
        ),
        "source_only": (
            "SELECT table_name FROM source_names "
            "EXCEPT SELECT table_name FROM target_names"
        ),
        "target_only": (
            "SELECT table_name FROM target_names "
            "EXCEPT SELECT table_name FROM source_names"
        ),
        "union": (
            "SELECT table_name FROM source_names "
            "UNION SELECT table_name FROM target_names"
        ),
    }[clean_operation]
    query = f"""
        WITH source_names AS (
            SELECT DISTINCT TRIM(source_table) AS table_name
            FROM s2t_transformations
            WHERE NULLIF(TRIM(source_table), '') IS NOT NULL
        ),
        target_names AS (
            SELECT DISTINCT TRIM(target_table) AS table_name
            FROM s2t_transformations
            WHERE NULLIF(TRIM(target_table), '') IS NOT NULL
        ),
        selected_names AS (
            {selected_sql}
        )
        SELECT table_name
        FROM selected_names
        ORDER BY table_name COLLATE NOCASE, table_name
        LIMIT ?
    """

    conn = get_db_connection()
    try:
        rows = conn.execute(query, (clean_limit + 1,)).fetchall()
    finally:
        conn.close()

    truncated = len(rows) > clean_limit
    visible_rows = [dict(row) for row in rows[:clean_limit]]
    return {
        "set_operation": clean_operation,
        "scope": "global",
        "columns": ["table_name"],
        "rows": visible_rows,
        "returned_rows": len(visible_rows),
        "limit": clean_limit,
        "truncated": truncated,
    }


def summarize_s2t_transformations(
    group_by: str = "target",
    file_id: Optional[int] = None,
    min_related_tables: int = 1,
    limit: int = 100,
) -> Dict[str, Any]:
    """Aggregate mapping counts and related tables by source or target table."""
    if group_by not in {"source", "target"}:
        return {"error": "group_by must be 'source' or 'target'"}

    expected_fields = {
        "source_table",
        "source_field",
        "target_table",
        "target_field",
        "transformation_rule",
    }
    missing_fields = sorted(expected_fields - set(S2T_RECORD_FIELDS))
    if missing_fields:
        return {
            "error": "Configured s2t_transformations fields do not support table summary",
            "missing_fields": missing_fields,
        }

    clean_file_id = int(file_id) if file_id is not None else None
    clean_min_related = max(1, min(int(min_related_tables or 1), 1000))
    clean_limit = max(1, min(int(limit or 100), 200))

    table_column = "source_table" if group_by == "source" else "target_table"
    mapped_field = "source_field" if group_by == "source" else "target_field"
    related_table_column = "target_table" if group_by == "source" else "source_table"
    layer_column = "source_layer" if group_by == "source" else "target_layer"

    scope_sql = ""
    params: List[Any] = []
    if clean_file_id is not None:
        scope_sql = "AND file_id = ?"
        params.append(clean_file_id)
    params.extend([clean_min_related, clean_limit])

    query = f"""
        SELECT
            {table_column} AS table_name,
            MAX(NULLIF(TRIM({layer_column}), '')) AS layer,
            COUNT(*) AS mapping_count,
            COUNT(DISTINCT NULLIF(TRIM({mapped_field}), '')) AS field_count,
            COUNT(DISTINCT NULLIF(TRIM({related_table_column}), '')) AS related_table_count,
            GROUP_CONCAT(DISTINCT NULLIF(TRIM({related_table_column}), '')) AS related_tables,
            SUM(
                CASE
                    WHEN NULLIF(TRIM(transformation_rule), '') IS NOT NULL THEN 1
                    ELSE 0
                END
            ) AS mappings_with_rule
        FROM s2t_transformations
        WHERE NULLIF(TRIM({table_column}), '') IS NOT NULL
          {scope_sql}
        GROUP BY {table_column}
        HAVING COUNT(DISTINCT NULLIF(TRIM({related_table_column}), '')) >= ?
        ORDER BY related_table_count DESC, mapping_count DESC, table_name
        LIMIT ?
    """

    conn = get_db_connection()
    try:
        rows = conn.execute(query, params).fetchall()
    finally:
        conn.close()

    groups = []
    for row in rows:
        item = dict(row)
        related_tables = str(item.pop("related_tables") or "")
        item["related_tables"] = sorted(name for name in related_tables.split(",") if name)
        item["rule_coverage"] = (
            item["mappings_with_rule"] / item["mapping_count"]
            if item["mapping_count"]
            else 0.0
        )
        groups.append(item)

    return {
        "group_by": group_by,
        "file_id": clean_file_id,
        "min_related_tables": clean_min_related,
        "groups": groups,
        "group_count": len(groups),
    }


def load_s2t_table_graph_rows() -> List[Dict[str, Any]]:
    """Return every stored S2T row needed to build the global table graph."""
    conn = get_db_connection()
    try:
        rows = conn.execute(
            """
            SELECT
                id,
                file_id,
                source_table,
                source_layer,
                source_field,
                target_table,
                target_layer,
                target_field,
                transformation_rule
            FROM s2t_transformations
            WHERE IFNULL(row_num, 0) >= 0
            ORDER BY id
            """
        ).fetchall()
    finally:
        conn.close()
    return [dict(row) for row in rows]


def backfill_s2t_layers(file_id: Optional[int] = None) -> Dict[str, Any]:
    """Recompute source/target layers from each stored row's source sheet."""
    params: List[Any] = []
    scope_sql = ""
    if file_id is not None:
        scope_sql = "WHERE file_id = ?"
        params.append(int(file_id))
    conn = get_db_connection()
    try:
        rows = conn.execute(
            f"""
            SELECT id, sheet_name, source_table, target_table,
                   source_layer, target_layer
            FROM s2t_transformations
            {scope_sql}
            ORDER BY id
            """,
            params,
        ).fetchall()
        updates = []
        resolved_source = resolved_target = 0
        for row in rows:
            layers = resolve_sheet_layers(row["sheet_name"])
            source_layer = (
                layers["source_layer"]
                if str(row["source_table"] or "").strip()
                else None
            )
            target_layer = (
                layers["target_layer"]
                if str(row["target_table"] or "").strip()
                else None
            )
            resolved_source += int(source_layer is not None)
            resolved_target += int(target_layer is not None)
            if (
                source_layer != row["source_layer"]
                or target_layer != row["target_layer"]
            ):
                updates.append((source_layer, target_layer, int(row["id"])))
        if updates:
            conn.executemany(
                """
                UPDATE s2t_transformations
                SET source_layer = ?, target_layer = ?
                WHERE id = ?
                """,
                updates,
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    return {
        "file_id": int(file_id) if file_id is not None else None,
        "rows": len(rows),
        "updated": len(updates),
        "resolved_source": resolved_source,
        "resolved_target": resolved_target,
    }


__all__ = [
    "clear_s2t_transformations",
    "backfill_s2t_layers",
    "insert_s2t_transformations",
    "list_s2t_transformations",
    "load_s2t_table_graph_rows",
    "summarize_s2t_transformations",
    "verify_s2t_transformations",
]
