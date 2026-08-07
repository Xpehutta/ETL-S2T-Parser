"""Extraction of row-oriented sheet skills described by JSON configuration."""

from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional, Sequence

from config.useful_columns import get_usefull_col_extraction_target
from sheet_skills.column_matching import (
    candidate_sheets,
    clean_value,
    load_columns,
    load_row_values,
    match_fields,
    persist_mapping_aliases,
)
from storage.database import _sql_identifier, get_db_connection


RecordPreparer = Callable[[str, List[Dict[str, Any]]], None]


def _records_from_mapping(
    file_id: int,
    fields: Sequence[str],
    mapping: Dict[str, Any],
) -> List[Dict[str, Any]]:
    column_ids = mapping["field_column_ids"]
    records = []
    for row_num, row_values in load_row_values(
        file_id, mapping["sheet_name"]
    ).items():
        selected_values = {
            field: clean_value(row_values.get(column_ids.get(field)))
            for field in fields
        }
        if any(value is not None for value in selected_values.values()):
            records.append(
                {
                    "file_id": file_id,
                    "sheet_name": mapping["sheet_name"],
                    "row_num": row_num,
                    **selected_values,
                }
            )
    return records


def extract_configured_rows(
    file_id: int,
    sheet_group_analysis: Dict[str, Any],
    target_names: Sequence[str],
    *,
    extra_columns: Optional[Dict[str, Sequence[str]]] = None,
    prepare_records: Optional[RecordPreparer] = None,
) -> Dict[str, Any]:
    """Match configured fields and append source rows to target tables."""
    configs: Dict[str, Dict[str, Any]] = {}
    records_by_target: Dict[str, List[Dict[str, Any]]] = {}
    reports: Dict[str, Dict[str, Any]] = {}
    extras = extra_columns or {}

    for target_name in target_names:
        config = get_usefull_col_extraction_target(target_name)
        configs[target_name] = config
        sheets = candidate_sheets(
            file_id,
            sheet_group_analysis,
            config["sheet_group"],
        )
        mappings = [
            match_fields(
                {
                    **sheet,
                    "columns": load_columns(file_id, sheet["sheet_name"]),
                },
                config["sheet_group"],
                config["fields"],
            )
            for sheet in sheets
        ]
        persist_mapping_aliases(config["sheet_group"], mappings)
        records = [
            record
            for mapping in mappings
            for record in _records_from_mapping(file_id, config["fields"], mapping)
        ]
        if prepare_records:
            prepare_records(target_name, records)
        records_by_target[target_name] = records
        reports[target_name] = {
            "sheet_group": config["sheet_group"],
            "sheet_count": len(sheets),
            "count": len(records),
            "sheet_mappings": mappings,
        }

    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("BEGIN")
        for target_name in target_names:
            fields = tuple(configs[target_name]["fields"])
            insert_columns = (
                "file_id",
                "sheet_name",
                "row_num",
                *fields,
                *tuple(extras.get(target_name, ())),
            )
            columns_sql = ", ".join(
                _sql_identifier(column) for column in insert_columns
            )
            placeholders = ", ".join("?" for _ in insert_columns)
            cursor.executemany(
                f"INSERT INTO {_sql_identifier(target_name)} "
                f"({columns_sql}) VALUES ({placeholders})",
                [
                    tuple(row.get(column) for column in insert_columns)
                    for row in records_by_target[target_name]
                ],
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    return {
        "status": "ok",
        "file_id": file_id,
        "count": sum(report["count"] for report in reports.values()),
        "targets": reports,
    }


__all__ = ["extract_configured_rows"]
