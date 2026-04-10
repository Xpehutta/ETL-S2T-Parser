import logging
from typing import Dict, Any, List, Optional
from db_storage import get_db_connection, generate_id

logger = logging.getLogger(__name__)


def get_data_rows(file_hash: str, sheet_name: str) -> List[Dict[str, Any]]:
    """Fetch all data rows for a sheet as list of dicts {column_name: value}."""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT sheet_hash FROM sheets WHERE file_hash = ? AND sheet_name = ?", (file_hash, sheet_name))
    sheet_row = cursor.fetchone()
    if not sheet_row:
        conn.close()
        return []
    sheet_hash = sheet_row["sheet_hash"]
    cursor.execute("SELECT column_hash, column_name_flat FROM columns WHERE sheet_hash = ? ORDER BY column_index",
                   (sheet_hash,))
    columns = cursor.fetchall()
    col_map = {col["column_hash"]: col["column_name_flat"] for col in columns}
    cursor.execute("SELECT column_hash, row_num, value FROM data WHERE sheet_hash = ? ORDER BY row_num, column_hash",
                   (sheet_hash,))
    rows_data = cursor.fetchall()
    conn.close()
    rows_dict = {}
    for row in rows_data:
        row_num = row["row_num"]
        if row_num not in rows_dict:
            rows_dict[row_num] = {}
        col_name = col_map.get(row["column_hash"], "unknown")
        rows_dict[row_num][col_name] = row["value"]
    return list(rows_dict.values())


# ----------------------------------------------------------------------
# Upsert helpers for each target table
# ----------------------------------------------------------------------
def upsert_source_table(name: str, description: Optional[str] = None, system_code: Optional[str] = None) -> str:
    """Insert or update source table, return id."""
    name_lower = name.lower()
    table_id = generate_id(name_lower)
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM source_tables WHERE id = ?", (table_id,))
    exists = cursor.fetchone()
    if exists:
        cursor.execute("""
            UPDATE source_tables
            SET name = ?, description = ?, system_code = ?
            WHERE id = ?
        """, (name, description, system_code, table_id))
    else:
        cursor.execute("""
            INSERT INTO source_tables (id, name, description, system_code)
            VALUES (?, ?, ?, ?)
        """, (table_id, name, description, system_code))
    conn.commit()
    conn.close()
    return table_id


def upsert_target_table(name: str, description: Optional[str] = None) -> str:
    name_lower = name.lower()
    table_id = generate_id(name_lower)
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM target_tables WHERE id = ?", (table_id,))
    exists = cursor.fetchone()
    if exists:
        cursor.execute("""
            UPDATE target_tables SET name = ?, description = ? WHERE id = ?
        """, (name, description, table_id))
    else:
        cursor.execute("""
            INSERT INTO target_tables (id, name, description)
            VALUES (?, ?, ?)
        """, (table_id, name, description))
    conn.commit()
    conn.close()
    return table_id


def upsert_column_mapping(target_table_name: str, target_column: str,
                          source_table_name: str, source_column: Optional[str],
                          transformation_rule: Optional[str], data_type: Optional[str],
                          is_primary_key: bool) -> str:
    # Get or create referenced tables
    target_id = upsert_target_table(target_table_name)  # but we only need id, name may already exist
    source_id = upsert_source_table(source_table_name)
    # Generate mapping id from the combination (lowercase)
    mapping_id = generate_id(target_id, target_column.lower(), source_id,
                             source_column.lower() if source_column else "")
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM column_mappings WHERE id = ?", (mapping_id,))
    exists = cursor.fetchone()
    if exists:
        cursor.execute("""
            UPDATE column_mappings
            SET target_table_id = ?, target_column = ?, source_table_id = ?, source_column = ?,
                transformation_rule = ?, data_type = ?, is_primary_key = ?
            WHERE id = ?
        """, (target_id, target_column, source_id, source_column,
              transformation_rule, data_type, 1 if is_primary_key else 0, mapping_id))
    else:
        cursor.execute("""
            INSERT INTO column_mappings
            (id, target_table_id, target_column, source_table_id, source_column,
             transformation_rule, data_type, is_primary_key)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (mapping_id, target_id, target_column, source_id, source_column,
              transformation_rule, data_type, 1 if is_primary_key else 0))
    conn.commit()
    conn.close()
    return mapping_id


def upsert_addition(table_name: Optional[str], table_description: Optional[str],
                    source_tables_name: Optional[str], sql: Optional[str],
                    description: Optional[str]) -> str:
    add_id = generate_id(table_name, source_tables_name, sql)
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM additions WHERE id = ?", (add_id,))
    exists = cursor.fetchone()
    if exists:
        cursor.execute("""
            UPDATE additions
            SET table_name = ?, table_description = ?, source_tables_name = ?, sql = ?, description = ?
            WHERE id = ?
        """, (table_name, table_description, source_tables_name, sql, description, add_id))
    else:
        cursor.execute("""
            INSERT INTO additions (id, table_name, table_description, source_tables_name, sql, description)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (add_id, table_name, table_description, source_tables_name, sql, description))
    conn.commit()
    conn.close()
    return add_id


# ----------------------------------------------------------------------
# Main loader with configurable similarity levels
# ----------------------------------------------------------------------
def load_similarity_data(file_hash: str, similarity_report: Dict[str, Any],
                         include_medium: bool = True) -> Dict[str, int]:
    """
    Insert or update data for sheets with similarity 'high' (and optionally 'medium').
    Returns counts of upserted records per target table.
    """
    inserted_counts = {}
    for suggestion in similarity_report.get("mapping_suggestions", []):
        sim = suggestion.get("similarity", "low")
        if sim == "high" or (include_medium and sim == "medium"):
            target_table = suggestion["target_table"]
            sheet_name = suggestion["excel_sheet"]
            column_mapping = suggestion.get("column_mapping", {})
            if not column_mapping:
                logger.warning(f"No column mapping for {sheet_name} -> {target_table}, skipping")
                continue

            rows = get_data_rows(file_hash, sheet_name)
            if not rows:
                logger.info(f"No data rows for sheet {sheet_name}")
                continue

            count = 0
            for row in rows:
                # Build record using column mapping
                record = {}
                for target_col, excel_col in column_mapping.items():
                    record[target_col] = row.get(excel_col)

                if target_table == "source_tables":
                    if record.get("name"):
                        upsert_source_table(
                            name=record["name"],
                            description=record.get("description"),
                            system_code=record.get("system_code")
                        )
                        count += 1
                elif target_table == "target_tables":
                    if record.get("name"):
                        upsert_target_table(
                            name=record["name"],
                            description=record.get("description")
                        )
                        count += 1
                elif target_table == "column_mappings":
                    if (record.get("target_table_name") and record.get("target_column")
                            and record.get("source_table_name")):
                        upsert_column_mapping(
                            target_table_name=record["target_table_name"],
                            target_column=record["target_column"],
                            source_table_name=record["source_table_name"],
                            source_column=record.get("source_column"),
                            transformation_rule=record.get("transformation_rule"),
                            data_type=record.get("data_type"),
                            is_primary_key=record.get("is_primary_key") in (True, 1, "Yes", "true")
                        )
                        count += 1
                elif target_table == "additions":
                    if any(record.values()):
                        upsert_addition(
                            table_name=record.get("table_name"),
                            table_description=record.get("table_description"),
                            source_tables_name=record.get("source_tables_name"),
                            sql=record.get("sql"),
                            description=record.get("description")
                        )
                        count += 1
            inserted_counts[target_table] = inserted_counts.get(target_table, 0) + count
    return inserted_counts