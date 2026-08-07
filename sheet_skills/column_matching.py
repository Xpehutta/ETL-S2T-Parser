"""Shared column loading and deterministic matching for sheet skills."""

from __future__ import annotations

import json
from difflib import SequenceMatcher
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from config.column_mapping import (
    add_field_aliases,
    get_field_aliases,
    normalize_column_alias,
)
from storage.database import get_columns_by_sheet, get_db_connection


FUZZY_MATCH_THRESHOLD = 0.70


def _parse_header(raw: Optional[str], flat_name: str) -> List[Any]:
    if raw:
        try:
            parsed = json.loads(raw)
            return parsed if isinstance(parsed, list) else [parsed]
        except (TypeError, json.JSONDecodeError):
            pass
    return [part.strip() for part in str(flat_name or "").split(">") if part.strip()]


def _header_candidates(column: Dict[str, Any]) -> List[str]:
    header = [
        str(part)
        for part in column.get("column_header") or []
        if part is not None and str(part).strip()
    ]
    values = [str(column.get("column_name_flat") or ""), " ".join(header), *header]
    return list(dict.fromkeys(value.strip() for value in values if value.strip()))


def _best_alias_match(
    column: Dict[str, Any], aliases: Iterable[str]
) -> Tuple[float, Optional[str], str, Optional[str]]:
    best: Tuple[float, Optional[str], str, Optional[str]] = (0.0, None, "none", None)
    candidates = [
        (candidate, normalize_column_alias(candidate))
        for candidate in _header_candidates(column)
    ]
    for alias in aliases:
        normalized_alias = normalize_column_alias(alias)
        if not normalized_alias:
            continue
        for candidate, normalized_candidate in candidates:
            if normalized_candidate == normalized_alias:
                return 1.0, alias, "exact", candidate
            score = SequenceMatcher(None, normalized_candidate, normalized_alias).ratio()
            if score > best[0]:
                best = (score, alias, "fuzzy", candidate)
    return best


def load_columns(
    file_id: int,
    sheet_name: str,
    *,
    sample_limit: int = 0,
) -> List[Dict[str, Any]]:
    samples: Dict[int, List[Any]] = {}
    if sample_limit:
        conn = get_db_connection()
        try:
            rows = conn.execute(
                """
                WITH ranked AS (
                    SELECT column_id, value,
                           ROW_NUMBER() OVER (PARTITION BY column_id ORDER BY row_num) AS n
                    FROM data
                    WHERE file_id = ? AND table_name = ? COLLATE NOCASE
                      AND value IS NOT NULL
                      AND TRIM(CAST(value AS TEXT)) != ''
                )
                SELECT column_id, value
                FROM ranked
                WHERE n <= ?
                ORDER BY column_id, n
                """,
                (file_id, sheet_name, sample_limit),
            ).fetchall()
        finally:
            conn.close()
        for row in rows:
            samples.setdefault(int(row["column_id"]), []).append(row["value"])

    result = []
    for stored in get_columns_by_sheet(file_id, sheet_name):
        column = dict(stored)
        header = _parse_header(
            column.get("column_header"),
            column.get("column_name_flat") or "",
        )
        column.update(
            column_header=header,
            leaf_header=str(header[-1]) if header else column.get("column_name_flat"),
            sample_values=samples.get(int(column["column_id"]), []),
        )
        result.append(column)
    return result


def candidate_sheets(
    file_id: int,
    sheet_group_analysis: Dict[str, Any],
    sheet_group: str,
) -> List[Dict[str, Any]]:
    allowed = {
        str(row["sheet_name"]).casefold()
        for row in sheet_group_analysis.get("classifications") or []
        if row.get("group") == sheet_group and row.get("sheet_name")
    }
    conn = get_db_connection()
    try:
        rows = conn.execute(
            """
            SELECT sheet_name
            FROM file_sheet_headers
            WHERE file_id = ? AND IFNULL(skipped, 0) = 0
            ORDER BY sheet_name
            """,
            (file_id,),
        ).fetchall()
    finally:
        conn.close()
    return [
        dict(row)
        for row in rows
        if str(row["sheet_name"]).casefold() in allowed
    ]


def column_evidence(
    column: Dict[str, Any],
    method: str,
    alias: Optional[str] = None,
    confidence: float = 0.9,
    candidate: Optional[str] = None,
) -> Dict[str, Any]:
    return {
        "column_id": column.get("column_id"),
        "column_name": column.get("column_name_flat"),
        "method": method,
        "matched_alias": alias,
        "matched_header_candidate": candidate or column.get("leaf_header"),
        "confidence": round(float(confidence), 3),
    }


def match_fields(
    sheet: Dict[str, Any],
    sheet_group: str,
    fields: Sequence[str],
    threshold: float = FUZZY_MATCH_THRESHOLD,
) -> Dict[str, Any]:
    selected = {field: None for field in fields}
    evidence: Dict[str, Dict[str, Any]] = {}
    used: set[int] = set()
    for field in fields:
        scored = []
        aliases = get_field_aliases(sheet_group, field)
        for column in sheet.get("columns", []):
            column_id = int(column["column_id"])
            if column_id in used:
                continue
            score, alias, method, candidate = _best_alias_match(column, aliases)
            scored.append(
                (score, -int(column.get("column_index") or 0), column, alias, method, candidate)
            )
        if not scored:
            continue
        score, _, column, alias, method, candidate = max(
            scored, key=lambda item: item[:2]
        )
        if score < threshold:
            continue
        column_id = int(column["column_id"])
        selected[field] = column_id
        used.add(column_id)
        evidence[field] = column_evidence(column, method, alias, score, candidate)
    return {
        "sheet_name": sheet["sheet_name"],
        "field_column_ids": selected,
        "evidence": evidence,
    }


def persist_mapping_aliases(sheet_group: str, mappings: Sequence[Dict[str, Any]]) -> int:
    added = 0
    for mapping in mappings:
        for field, column_id in mapping["field_column_ids"].items():
            evidence = mapping.get("evidence", {}).get(field, {})
            alias = evidence.get("matched_header_candidate")
            if column_id and alias and evidence.get("method") != "exact":
                added += len(add_field_aliases(sheet_group, field, [alias]))
    return added


def load_row_values(file_id: int, sheet_name: str) -> Dict[int, Dict[int, Any]]:
    conn = get_db_connection()
    try:
        rows = conn.execute(
            """
            SELECT row_num, column_id, value
            FROM data
            WHERE file_id = ? AND table_name = ? COLLATE NOCASE
            ORDER BY row_num, id
            """,
            (file_id, sheet_name),
        ).fetchall()
    finally:
        conn.close()
    values: Dict[int, Dict[int, Any]] = {}
    for row in rows:
        values.setdefault(int(row["row_num"]), {})[int(row["column_id"])] = row["value"]
    return values


def clean_value(value: Any) -> Optional[str]:
    return (str(value).strip() or None) if value is not None else None


__all__ = [
    "candidate_sheets",
    "clean_value",
    "column_evidence",
    "load_columns",
    "load_row_values",
    "match_fields",
    "persist_mapping_aliases",
]
