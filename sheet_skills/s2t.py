"""Configured S2T inspection, column matching, writing and verification."""

from __future__ import annotations

import json
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

from dotenv import load_dotenv
from langchain_core.output_parsers import StrOutputParser

from agents.llm_factory import create_chat_model
from agents.sheet_group_classifier import classify_file_sheet_groups
from config.column_mapping import (
    add_field_aliases,
    get_field_aliases,
    get_sheet_column_mapping,
    normalize_column_alias,
)
from config.useful_columns import get_usefull_col_extraction_target
from sheet_skills.table_catalog import extract_table_catalogs
from storage.database import get_columns_by_sheet, get_db_connection
from storage.s2t import replace_s2t_transformations, verify_s2t_transformations

USEFULL_COL_EXTRACTION_TARGET = "s2t_transformations"
USEFULL_COL_EXTRACTION_SUBAGENT = "usefull_col_extraction"
MAX_LLM_REQUESTS_PER_SHEET = 2


def _load_runtime_target_config() -> Dict[str, Any]:
    config = get_usefull_col_extraction_target(USEFULL_COL_EXTRACTION_TARGET)
    if not config:
        raise RuntimeError(f"Missing target: {USEFULL_COL_EXTRACTION_TARGET}")
    return config


_TARGET_CONFIG = _load_runtime_target_config()
USEFULL_SHEET_GROUP = str(_TARGET_CONFIG.get("sheet_group") or "").strip()
S2T_FIELDS = tuple(
    str(field).strip() for field in _TARGET_CONFIG.get("fields") or [] if str(field).strip()
)
if not USEFULL_SHEET_GROUP or not S2T_FIELDS:
    raise RuntimeError(f"Invalid target: {USEFULL_COL_EXTRACTION_TARGET}")


class S2TExtractionError(RuntimeError):
    def __init__(self, message: str, report: Optional[Dict[str, Any]] = None):
        super().__init__(message)
        self.report = report or {"status": "error", "error": message}


class S2TRowValidationError(S2TExtractionError):
    """Raised before writing when a source row has no target_table."""


def _fetchall(sql: str, params: tuple = ()) -> List[Any]:
    conn = get_db_connection()
    try:
        return conn.execute(sql, params).fetchall()
    finally:
        conn.close()


def _clean_value(value: Any) -> Optional[str]:
    return (str(value).strip() or None) if value is not None else None


def _parse_header(raw: Optional[str], flat_name: str) -> List[Any]:
    if raw:
        try:
            parsed = json.loads(raw)
            return parsed if isinstance(parsed, list) else [parsed]
        except (TypeError, json.JSONDecodeError):
            pass
    return [part.strip() for part in str(flat_name or "").split(">") if part.strip()]


def _header_candidates(column: Dict[str, Any]) -> List[str]:
    header = [str(item) for item in column.get("column_header") or [] if item is not None and str(item).strip()]
    values = [str(column.get("column_name_flat") or ""), " ".join(header), *header]
    return list(dict.fromkeys(value.strip() for value in values if value.strip()))


def _best_alias_score(
    column: Dict[str, Any], aliases: Iterable[str]
) -> Tuple[float, Optional[str], str, Optional[str]]:
    best = (0.0, None, "none", None)
    candidates = [
        (value, normalize_column_alias(value)) for value in _header_candidates(column)
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


def _evidence(
    column: Dict[str, Any],
    method: str,
    alias: Optional[str] = None,
    confidence: float = 0.9,
    candidate: Optional[str] = None,
) -> Dict[str, Any]:
    return {
        "column_id": column.get("column_id"),
        "method": method,
        "matched_alias": alias,
        "matched_header_candidate": candidate or column.get("leaf_header"),
        "confidence": round(float(confidence), 3),
    }


def _field_aliases(field: str) -> List[str]:
    result, seen = [], set()
    for alias in get_field_aliases(USEFULL_SHEET_GROUP, field):
        normalized = normalize_column_alias(alias)
        if normalized and normalized not in seen:
            seen.add(normalized)
            result.append(alias)
    return result


def _target_sheet_ids(analysis: Dict[str, Any]) -> set[int]:
    return {
        int(row["sheet_id"])
        for row in analysis.get("classifications") or []
        if row.get("group") == USEFULL_SHEET_GROUP and row.get("sheet_id")
    }


def _load_candidate_sheet_rows(file_id: int, analysis: Dict[str, Any]) -> List[Dict[str, Any]]:
    rows = _fetchall(
        """
        SELECT sheet_id, sheet_name FROM file_sheet_headers
        WHERE file_id = ? AND IFNULL(skipped, 0) = 0 ORDER BY sheet_name
        """,
        (file_id,),
    )
    allowed = _target_sheet_ids(analysis)
    return [dict(row) for row in rows if row["sheet_id"] in allowed]


def _load_samples_for_sheet(sheet_id: int, limit: int = 5) -> Dict[int, List[Any]]:
    rows = _fetchall(
        """
        WITH ranked AS (
            SELECT column_id, value,
                   ROW_NUMBER() OVER (PARTITION BY column_id ORDER BY row_num) AS n
            FROM data WHERE sheet_id = ? AND value IS NOT NULL
              AND TRIM(CAST(value AS TEXT)) != ''
        )
        SELECT column_id, value FROM ranked WHERE n <= ? ORDER BY column_id, n
        """,
        (sheet_id, limit),
    )
    samples: Dict[int, List[Any]] = {}
    for row in rows:
        samples.setdefault(int(row["column_id"]), []).append(row["value"])
    return samples


def _load_columns_for_sheet(sheet_id: int) -> List[Dict[str, Any]]:
    samples, result = _load_samples_for_sheet(sheet_id), []
    for stored in get_columns_by_sheet(sheet_id):
        column = dict(stored)
        header = _parse_header(column.get("column_header"), column.get("column_name_flat") or "")
        column.update(
            column_header=header,
            leaf_header=str(header[-1]) if header else column.get("column_name_flat"),
            sample_values=samples.get(int(column["column_id"]), []),
        )
        result.append(column)
    return result


def _inspect_candidate_sheets(
    file_id: int, sheet_group_analysis: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    analysis = sheet_group_analysis or classify_file_sheet_groups(
        file_id, use_llm=True, persist_aliases=False
    )
    sheets = [
        {**sheet, "columns": _load_columns_for_sheet(sheet["sheet_id"])}
        for sheet in _load_candidate_sheet_rows(file_id, analysis)
    ]
    return {"file_id": file_id, "sheet_group_analysis": analysis, "sheets": sheets}


def _deterministic_sheet_mapping(
    sheet: Dict[str, Any], threshold: float = 0.70
) -> Dict[str, Any]:
    selected = {field: None for field in S2T_FIELDS}
    evidence, used = {}, set()
    for field in S2T_FIELDS:
        aliases, scored = _field_aliases(field), []
        for column in sheet.get("columns", []):
            if column.get("column_id") in used:
                continue
            score, alias, method, candidate = _best_alias_score(column, aliases)
            scored.append(
                (score, -(column.get("column_index") or 0), column, alias, method, candidate)
            )
        if not scored:
            continue
        score, _, column, alias, method, candidate = max(scored, key=lambda item: item[:2])
        if score >= threshold:
            selected[field] = column["column_id"]
            used.add(column["column_id"])
            evidence[field] = _evidence(column, method, alias, score, candidate)
    return {
        "sheet_id": sheet["sheet_id"],
        "sheet_name": sheet["sheet_name"],
        "field_column_ids": selected,
        "evidence": evidence,
    }


def _validate_sheet_mappings(
    file_id: int, mappings: List[Dict[str, Any]], inspection: Dict[str, Any]
) -> List[Dict[str, Any]]:
    del file_id
    sheets = {sheet["sheet_id"]: sheet for sheet in inspection.get("sheets", [])}
    result = []
    for mapping in mappings:
        sheet_id = mapping.get("sheet_id")
        sheet, selected_raw = sheets.get(sheet_id), mapping.get("field_column_ids")
        if not sheet:
            raise ValueError(f"Unknown configured sheet_id: {sheet_id}")
        if not isinstance(selected_raw, dict):
            raise ValueError(f"{sheet_id}: missing field_column_ids object")
        unknown = sorted(set(selected_raw) - set(S2T_FIELDS))
        if unknown:
            raise ValueError(f"{sheet_id}: unknown configured fields {unknown}")

        selected = {field: selected_raw.get(field) or None for field in S2T_FIELDS}
        valid_ids = {column["column_id"] for column in sheet.get("columns", [])}
        invalid = {
            field: column_id
            for field, column_id in selected.items()
            if column_id and column_id not in valid_ids
        }
        if invalid:
            raise ValueError(f"{sheet_id}: fields point to unknown column_id values {invalid}")
        assigned = [value for value in selected.values() if value]
        if len(assigned) != len(set(assigned)):
            raise ValueError(f"{sheet_id}: one physical column is assigned to multiple fields")
        if not selected.get("target_table"):
            raise ValueError(f"{sheet_id}: required target_table is not mapped")

        evidence = dict(mapping.get("evidence") or {})
        missing_evidence = [
            field for field, column_id in selected.items() if column_id and field not in evidence
        ]
        if missing_evidence:
            raise ValueError(f"{sheet_id}: missing evidence for {missing_evidence}")
        result.append(
            {
                "sheet_id": sheet_id,
                "sheet_name": sheet["sheet_name"],
                "field_column_ids": selected,
                "evidence": evidence,
            }
        )
    return result


def _fallback_alias(column: Dict[str, Any]) -> Optional[str]:
    header = column.get("column_header") or []
    for value in (column.get("leaf_header"), header[-1] if header else None, column.get("column_name_flat")):
        if value is not None and str(value).strip():
            return str(value).strip()
    return None


def _persist_mapping_aliases(mappings: List[Dict[str, Any]], inspection: Dict[str, Any]) -> int:
    columns = {
        column["column_id"]: column
        for sheet in inspection.get("sheets", [])
        for column in sheet.get("columns", [])
        if column.get("column_id")
    }
    added = 0
    for mapping in mappings:
        for field, column_id in mapping["field_column_ids"].items():
            evidence, column = mapping.get("evidence", {}).get(field, {}), columns.get(column_id)
            if not column_id or not column or evidence.get("method") == "exact":
                continue
            alias = evidence.get("matched_header_candidate") or _fallback_alias(column)
            added += len(add_field_aliases(USEFULL_SHEET_GROUP, field, [alias]) if alias else [])
    return added


def _load_row_values(sheet_id: int) -> Dict[int, Dict[int, Any]]:
    rows: Dict[int, Dict[int, Any]] = {}
    for stored in _fetchall(
        "SELECT row_num, column_id, value FROM data WHERE sheet_id = ? ORDER BY row_num",
        (sheet_id,),
    ):
        rows.setdefault(int(stored["row_num"]), {})[stored["column_id"]] = stored["value"]
    return rows


def _raw_selected_json(mapping: Dict[str, Any], values: Dict[int, Any]) -> str:
    selected = mapping["field_column_ids"]
    return json.dumps(
        {
            field: {
                "column_id": selected.get(field),
                "value": values.get(selected.get(field)) if selected.get(field) else None,
            }
            for field in S2T_FIELDS
        },
        ensure_ascii=False,
        default=str,
    )


def write_s2t_transformations_from_plan(
    file_id: int,
    sheet_mappings: List[Dict[str, Any]],
    inspection: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Validate the plan and transactionally replace S2T results."""
    source = inspection or _inspect_candidate_sheets(file_id)
    mappings = _validate_sheet_mappings(file_id, sheet_mappings, source)
    records, row_errors = [], []
    for mapping in mappings:
        selected = mapping["field_column_ids"]
        for row_num, row in _load_row_values(mapping["sheet_id"]).items():
            values = {
                field: _clean_value(row.get(selected.get(field))) for field in S2T_FIELDS
            }
            if not values.get("target_table"):
                row_errors.append(
                    {
                        "file_id": file_id,
                        "sheet_id": mapping["sheet_id"],
                        "sheet_name": mapping["sheet_name"],
                        "row_num": row_num,
                        "field": "target_table",
                        "error": "В строке S2T не заполнена целевая таблица",
                    }
                )
            elif values.get("target_field"):
                records.append(
                    {
                        "file_id": file_id,
                        "sheet_id": mapping["sheet_id"],
                        "sheet_name": mapping["sheet_name"],
                        "row_num": row_num,
                        **values,
                        "raw_json": _raw_selected_json(mapping, row),
                    }
                )
    if row_errors:
        report = {
            "status": "error",
            "stage": "validate_rows",
            "file_id": file_id,
            "error": "Найдены строки S2T без целевой таблицы",
            "validation_errors": row_errors,
        }
        raise S2TRowValidationError(report["error"], report)
    return {**replace_s2t_transformations(file_id, records), "sheet_mappings": mappings}


def _invoke_llm_plain_text(prompt: str) -> str:
    load_dotenv(dotenv_path=Path(__file__).resolve().parents[1] / ".env")
    return (create_chat_model() | StrOutputParser()).invoke(prompt)


def _extract_json_object(text: str) -> Dict[str, Any]:
    raw = (text or "").strip()
    if raw.startswith("```"):
        raw = raw.strip("`").replace("json\n", "", 1).strip()
    start, end = raw.find("{"), raw.rfind("}")
    parsed = json.loads(raw[start : end + 1] if start >= 0 and end >= start else raw)
    if not isinstance(parsed, dict):
        raise ValueError("LLM response must be a JSON object")
    return parsed


def _short_samples(values: Iterable[Any]) -> List[str]:
    return [
        text if len(text) <= 120 else text[:119] + "..."
        for text in (str(value) for value in list(values or [])[:3])
    ]


def _build_sheet_llm_prompt(
    sheet: Dict[str, Any], rejection_reason: Optional[str] = None
) -> str:
    mapping = {
        field: aliases
        for field, aliases in get_sheet_column_mapping(USEFULL_SHEET_GROUP).items()
        if field in set(S2T_FIELDS)
    }
    payload = {
        "sheet_name": sheet.get("sheet_name"),
        "column_mapping_json": {USEFULL_SHEET_GROUP: mapping},
        "columns": [
            {
                "column_name": column.get("column_name_flat"),
                "sample_values": _short_samples(column.get("sample_values") or []),
            }
            for column in sheet.get("columns", [])
        ],
    }
    correction = ""
    if rejection_reason:
        payload["previous_attempt"] = {"rejection_reason": rejection_reason}
        correction = (
            "Предыдущий ответ отклонён валидатором. Исправь назначения с учётом "
            "rejection_reason и снова верни полный column_roles. "
        )
    return (
        "Сопоставь полезные колонки одного настроенного листа с доступными полями "
        f"группы column_mapping_json {USEFULL_SHEET_GROUP!r}. "
        + correction
        + "Для каждого входного column_name верни один mapping_field из "
        "column_mapping_json или null. Не придумывай значения и не назначай один "
        "mapping_field нескольким колонкам. Верни только JSON без markdown: "
        '{"sheet_name":"...","column_roles":[{"column_name":"...",'
        '"mapping_field":"target_table"},{"column_name":"...","mapping_field":null}]}.\n\n'
        + json.dumps(payload, ensure_ascii=False, default=str)
    )


def _public_rejection_reason(error: Exception, file_id: int, sheet_id: Optional[int]) -> str:
    reason = str(error)
    for internal_id, replacement in ((sheet_id, "текущий лист"), (file_id, "текущий файл")):
        if internal_id:
            reason = reason.replace(str(internal_id), replacement)
    return reason


def _normalise_llm_field(value: Any) -> Optional[str]:
    text = "" if value is None else str(value).strip()
    if not text or text.casefold() in {"none", "null", "no_match", "not_applicable", "n/a"}:
        return None
    if text not in S2T_FIELDS:
        raise ValueError(f"Unknown configured mapping_field: {text}")
    return text


def _sheet_mapping_from_column_roles(
    parsed: Dict[str, Any], sheet: Dict[str, Any]
) -> Dict[str, Any]:
    sheet_id = sheet.get("sheet_id")
    if parsed.get("sheet_name") and parsed["sheet_name"] != sheet.get("sheet_name"):
        raise ValueError(f"{sheet_id}: LLM returned wrong sheet_name")
    roles = parsed.get("column_roles")
    if not isinstance(roles, list):
        raise ValueError(f"{sheet_id}: missing JSON list field column_roles")

    columns, duplicates = {}, set()
    for column in sheet.get("columns", []):
        name = str(column.get("column_name_flat") or "").strip()
        if name in columns:
            duplicates.add(name)
        if name:
            columns[name] = column
    if duplicates:
        raise ValueError(f"{sheet_id}: duplicate column_name values: {sorted(duplicates)}")

    selected, evidence, seen = {field: None for field in S2T_FIELDS}, {}, set()
    for item in roles:
        if not isinstance(item, dict):
            raise ValueError(f"{sheet_id}: column_roles items must be objects")
        name = str(item.get("column_name") or "").strip()
        if name not in columns or name in seen:
            raise ValueError(f"{sheet_id}: unknown or duplicate column_name: {name!r}")
        seen.add(name)
        field = _normalise_llm_field(item.get("mapping_field"))
        if field is None:
            continue
        if selected[field]:
            raise ValueError(f"{sheet_id}: mapping_field {field} is assigned to multiple columns")
        column = columns[name]
        selected[field] = column["column_id"]
        evidence[field] = _evidence(column, "llm", field)
    missing = sorted(set(columns) - seen)
    if missing:
        raise ValueError(f"{sheet_id}: column_roles must include every column_name, missing {missing}")
    return {
        "sheet_id": sheet_id,
        "sheet_name": sheet.get("sheet_name"),
        "field_column_ids": selected,
        "evidence": evidence,
    }


def _resolve_sheet_mapping(
    file_id: int, sheet: Dict[str, Any], inspection: Dict[str, Any]
) -> Dict[str, Any]:
    draft = _deterministic_sheet_mapping(sheet)
    if all(draft["field_column_ids"].get(field) for field in S2T_FIELDS):
        mapping = _validate_sheet_mappings(file_id, [draft], inspection)[0]
        return {"mapping": mapping, "attempts": 0, "method": "deterministic"}
    reason = None
    for attempt in range(1, MAX_LLM_REQUESTS_PER_SHEET + 1):
        try:
            prompt = _build_sheet_llm_prompt(sheet, reason)
            candidate = _sheet_mapping_from_column_roles(
                _extract_json_object(_invoke_llm_plain_text(prompt)), sheet
            )
            mapping = _validate_sheet_mappings(file_id, [candidate], inspection)[0]
            return {"mapping": mapping, "attempts": attempt, "method": "llm"}
        except Exception as exc:
            reason = _public_rejection_reason(exc, file_id, sheet.get("sheet_id"))
    return {
        "mapping": None,
        "attempts": MAX_LLM_REQUESTS_PER_SHEET,
        "method": "llm",
        "error": reason or "useful-column matching failed",
    }


def _make_report(
    file_id: int,
    status: str,
    attempts: int = 0,
    target: str = USEFULL_COL_EXTRACTION_TARGET,
    **details: Any,
) -> Dict[str, Any]:
    return {
        "status": status,
        "file_id": file_id,
        "subagent": USEFULL_COL_EXTRACTION_SUBAGENT,
        "target": target,
        "attempts": attempts,
        **details,
    }


def run_s2t_extraction_subagent(
    file_id: int, sheet_group_analysis: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """Classify sheets, resolve mappings, write rows and verify the database."""
    analysis = sheet_group_analysis or classify_file_sheet_groups(
        file_id, use_llm=True, persist_aliases=True
    )
    try:
        catalogs = extract_table_catalogs(file_id, analysis)
    except Exception as exc:
        report = _make_report(
            file_id, "error", target="table_catalogs",
            error=str(exc) or "Table catalog extraction failed"
        )
        raise S2TExtractionError(report["error"], report) from exc

    inspection = _inspect_candidate_sheets(file_id, analysis)
    attempts, mappings, sheets = 0, [], []
    try:
        for sheet in inspection["sheets"]:
            resolved = _resolve_sheet_mapping(file_id, sheet, inspection)
            attempts += resolved["attempts"]
            if resolved.get("mapping") is None:
                report = _make_report(
                    file_id, "error", attempts, sheet_name=sheet["sheet_name"],
                    attempt=resolved["attempts"], error=resolved["error"],
                    table_catalogs=catalogs,
                )
                raise S2TExtractionError(report["error"], report)
            mappings.append(resolved["mapping"])
            sheets.append(
                {
                    "sheet_name": sheet["sheet_name"],
                    "method": resolved["method"],
                    "attempts": resolved["attempts"],
                }
            )

        aliases_added = _persist_mapping_aliases(mappings, inspection)
        written = write_s2t_transformations_from_plan(
            file_id, mappings, inspection=inspection
        )["count"]
        verification = verify_s2t_transformations(file_id)
        return _make_report(
            file_id, "ok", attempts, sheets=sheets, aliases_added=aliases_added,
            written=written, verification=verification, table_catalogs=catalogs,
        )
    except S2TExtractionError:
        raise
    except Exception as exc:
        report = _make_report(
            file_id, "error", attempts, error=str(exc) or "Useful-column extraction failed",
            table_catalogs=catalogs,
        )
        raise S2TExtractionError(report["error"], report) from exc
