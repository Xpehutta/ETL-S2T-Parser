import json
import logging
from collections import Counter
from difflib import SequenceMatcher
from typing import Any, Dict, List, Optional

from langchain_core.output_parsers import JsonOutputParser, StrOutputParser

from config.sheet_groups import (
    add_sheet_group_alias,
    find_sheet_group_alias,
    iter_group_aliases,
    load_sheet_groups,
    normalize_sheet_name,
)
from storage.database import get_columns_by_sheet, get_db_connection

logger = logging.getLogger(__name__)


def invoke_llm_plain_text(prompt: str) -> str:
    """Prompt string -> assistant text; patchable in tests."""
    from . import agent

    return (agent.chat_model | StrOutputParser()).invoke(prompt)


def _sheet_group_prompt(payload: Dict[str, str]) -> str:
    return f"""Определи, является ли имя одного листа Excel вариантом существующего алиаса в схеме.
Это задача разрешения алиасов по имени, а не семантическая классификация назначения или содержимого листа.
Используй только переданные алиасы и верни только JSON.

Кандидатные алиасы:
{payload["aliases_json"]}

Лист:
{payload["sheet_json"]}

Правила:
- Имя листа — главный критерий. Непустой результат допустим только тогда, когда имя листа и существующий алиас обозначают один и тот же тип объекта с теми же направлением, уровнем, областью и назначением.
- Допустимые различия: регистр, разделители, пробелы, опечатка, перестановка слов без изменения смысла, общепринятое сокращение, транслитерация, перевод или грамматическая форма.
- Колонки можно использовать только как дополнительную проверку уже установленной эквивалентности имён. Сходство колонок, структуры, данных или бизнес-смысла само по себе никогда не является основанием для выбора группы.
- Если имя содержит отличительные слова, меняющие тип объекта, направление, уровень, область или назначение относительно алиаса, верни null.
- Не сопоставляй разные виды таблиц, маппингов или метаданных только потому, что они относятся к одному процессу или содержат похожие поля.
- Не придумывай алиасы и группы.
- Если эквивалентность имён неоднозначна, требует догадки по содержимому или уверенность ниже high, верни group=null и matched_alias=null.
- Перед непустым ответом проверь: можно ли заменить имя листа на matched_alias без изменения смысла, типа объекта, направления и уровня. Если нельзя — верни null.

Формат:
{{
  "group": "one_group_key_or_null",
  "matched_alias": "one_existing_alias_or_null",
  "confidence": "high|medium|low",
  "reason": "краткая причина"
}}"""


def _flat_columns(columns: List[Any], limit: int = 30) -> List[str]:
    return [
        " > ".join(map(str, filter(None, column)))
        if isinstance(column, list)
        else str(column)
        for column in columns[:limit]
    ]


def _candidate_alias_rows(groups: Dict[str, List[str]]) -> List[Dict[str, str]]:
    rows, seen = [], set()
    for group, alias, normalized in iter_group_aliases(groups):
        if (group, normalized) not in seen:
            seen.add((group, normalized))
            rows.append({"group": group, "alias": alias})
    return rows


def _valid_aliases(groups: Dict[str, List[str]], group: str) -> set:
    return {
        alias
        for candidate, alias, _ in iter_group_aliases(groups)
        if candidate == group
    }


def _fuzzy_match(
    sheet_name: str,
    groups: Dict[str, List[str]],
    threshold: float = 0.84,
) -> Optional[Dict[str, Any]]:
    normalized = normalize_sheet_name(sheet_name)
    candidates = [
        (SequenceMatcher(None, normalized, alias_normalized).ratio(), group, alias)
        for group, alias, alias_normalized in iter_group_aliases(groups)
        if normalized and alias_normalized
    ]
    if not candidates:
        return None
    score, group, alias = max(candidates)
    return (
        {"group": group, "matched_alias": alias, "score": score}
        if score >= threshold
        else None
    )


def _result(
    sheet: Dict[str, Any],
    method: str,
    reason: str,
    group: Optional[str] = None,
    matched_alias: Optional[str] = None,
    confidence: str = "low",
) -> Dict[str, Any]:
    result = {
        "sheet_name": str(sheet.get("sheet_name") or ""),
        "group": group,
        "matched_alias": matched_alias,
        "confidence": confidence,
        "method": method,
        "reason": reason,
    }
    return result


def _llm_match(raw: Any, groups: Dict[str, List[str]]) -> Dict[str, Any]:
    if not isinstance(raw, dict):
        return {"reason": "LLM returned non-object JSON"}

    group, alias = raw.get("group"), raw.get("matched_alias")
    group = None if group in (None, "", "null", "none", "None") else group
    alias = None if alias in (None, "", "null", "none", "None") else alias
    if group is not None and group not in groups:
        return {"reason": f"LLM returned unknown group: {group}"}
    if group is not None and alias not in _valid_aliases(groups, group):
        return {"reason": f"LLM returned alias outside schema/group: {alias}"}

    confidence = raw.get("confidence", "low")
    return {
        "group": group,
        "matched_alias": alias,
        "confidence": confidence if confidence in {"high", "medium", "low"} else "low",
        "reason": str(raw.get("reason") or "").strip(),
    }


def classify_sheet_group(
    sheet: Dict[str, Any],
    use_llm: bool = True,
    groups: Optional[Dict[str, List[str]]] = None,
) -> Dict[str, Any]:
    groups = groups if groups is not None else load_sheet_groups()
    name = str(sheet.get("sheet_name") or "")

    match = find_sheet_group_alias(name, groups)
    if match:
        return _result(
            sheet,
            "alias",
            "Matched sheet_groups.json alias",
            match["group"],
            match["alias"],
            "high",
        )

    match = _fuzzy_match(name, groups)
    if match:
        score = match.pop("score")
        return _result(
            sheet,
            "fuzzy_alias",
            f"Fuzzy matched sheet_groups.json alias with score {score:.3f}",
            confidence="high" if score >= 0.92 else "medium",
            **match,
        )

    if not use_llm:
        return _result(
            sheet,
            "none",
            "No exact or fuzzy alias match and LLM alias resolution disabled",
        )

    payload = {
        "aliases_json": json.dumps(
            _candidate_alias_rows(groups), ensure_ascii=False, indent=2
        ),
        "sheet_json": json.dumps(
            {"sheet_name": name, "columns": _flat_columns(sheet.get("columns") or [])},
            ensure_ascii=False,
            indent=2,
        ),
    }
    try:
        raw = JsonOutputParser().parse(
            invoke_llm_plain_text(_sheet_group_prompt(payload))
        )
        return _result(sheet, "llm_alias", **_llm_match(raw, groups))
    except Exception as exc:
        logger.warning("Sheet group LLM classification failed for %s: %s", name, exc)
        return _result(sheet, "error", str(exc))


def classify_sheet_groups(
    sheets: List[Dict[str, Any]], use_llm: bool = True
) -> List[Dict[str, Any]]:
    groups = load_sheet_groups()
    return [
        classify_sheet_group(sheet, use_llm=use_llm, groups=groups)
        for sheet in sheets
    ]


def _missing(expected: List[str], actual: List[str]) -> List[str]:
    return [
        name
        for name, count in (Counter(expected) - Counter(actual)).items()
        for _ in range(count)
    ]


def _issue(
    severity: str, issue_type: str, message: str, **details: Any
) -> Dict[str, Any]:
    return {
        "severity": severity,
        "type": issue_type,
        **details,
        "message": message,
    }


def verify_sheet_group_mapping(
    sheets: List[Dict[str, Any]],
    classifications: List[Dict[str, Any]],
    groups: Optional[Dict[str, List[str]]] = None,
) -> Dict[str, Any]:
    """Verify that every classification uses the configured groups and aliases."""
    groups = groups if groups is not None else load_sheet_groups()
    expected = [str(sheet.get("sheet_name") or "") for sheet in sheets]
    actual = [str(row.get("sheet_name") or "") for row in classifications]
    issues = []

    for issue_type, names, message in (
        (
            "missing_classification",
            _missing(expected, actual),
            "Some input sheets do not have classification rows",
        ),
        (
            "unexpected_classification",
            _missing(actual, expected),
            "Some classification rows do not match input sheets",
        ),
    ):
        if names:
            issues.append(_issue("error", issue_type, message, sheet_names=names))

    unmatched = []
    for row in classifications:
        name, group, alias = (
            str(row.get("sheet_name") or ""),
            row.get("group"),
            row.get("matched_alias"),
        )
        if group is None:
            unmatched.append(name)
            continue
        if group not in groups:
            issues.append(
                _issue(
                    "error",
                    "unknown_group",
                    "Classification returned a group outside sheet_groups.json",
                    sheet_name=name,
                    group=group,
                )
            )
            continue
        if alias not in _valid_aliases(groups, group):
            issues.append(
                _issue(
                    "error",
                    "alias_outside_schema",
                    "Matched alias is not listed for this group in sheet_groups.json",
                    sheet_name=name,
                    group=group,
                    matched_alias=alias,
                )
            )
        if row.get("method") == "alias":
            exact = find_sheet_group_alias(name, groups)
            if not exact or (exact["group"], exact["alias"]) != (group, alias):
                issues.append(
                    _issue(
                        "error",
                        "alias_recheck_failed",
                        "Alias classification does not reproduce against sheet_groups.json",
                        sheet_name=name,
                        group=group,
                        matched_alias=alias,
                    )
                )

    if unmatched:
        issues.append(
            _issue(
                "warning",
                "unmatched_sheets",
                "Some sheets were not mapped to any group in sheet_groups.json",
                sheet_names=unmatched,
            )
        )
    errors = sum(row["severity"] == "error" for row in issues)
    warnings = sum(row["severity"] == "warning" for row in issues)
    return {
        "status": "failed" if errors else "warning" if warnings else "passed",
        "errors": errors,
        "warnings": warnings,
        "issues": issues,
        "unmatched_sheets": unmatched,
        "counts": {
            "input_sheets": len(sheets),
            "classification_rows": len(classifications),
            "mapped_sheets": sum(row.get("group") is not None for row in classifications),
            "unmatched_sheets": len(unmatched),
            "schema_groups": len(groups),
        },
    }


def _step(name: str, status: str = "ok", **details: Any) -> Dict[str, Any]:
    return {"step": name, "status": status, **details}


class SheetGroupResolverSubagent:
    """Resolve workbook sheets to configured groups."""

    name = "sheet_group_resolver_subagent"
    version = "1"

    def __init__(self, use_llm: bool = True, persist_aliases: bool = True):
        self.use_llm = bool(use_llm)
        self.persist_aliases = bool(persist_aliases)

    def run(
        self, sheets: List[Dict[str, Any]], file_id: Optional[int] = None
    ) -> Dict[str, Any]:
        groups = load_sheet_groups()
        classifications = [
            classify_sheet_group(sheet, use_llm=False, groups=groups)
            for sheet in sheets
        ]
        unresolved = [
            index
            for index, result in enumerate(classifications)
            if result["group"] is None
        ]
        steps = [
            _step(
                "load_schema",
                groups=len(groups),
                candidate_aliases=len(_candidate_alias_rows(groups)),
            ),
            _step("load_sheets", sheets=len(sheets)),
            _step(
                "resolve_exact_fuzzy_aliases",
                matched=len(sheets) - len(unresolved),
                unresolved=len(unresolved),
            ),
        ]

        if self.use_llm:
            for index in unresolved:
                classifications[index] = classify_sheet_group(
                    sheets[index], groups=groups
                )
            resolved = sum(classifications[index]["group"] is not None for index in unresolved)
            steps.append(
                _step(
                    "resolve_llm_aliases",
                    attempted=len(unresolved),
                    resolved=resolved,
                    unresolved=len(unresolved) - resolved,
                )
            )
        else:
            steps.append(
                _step(
                    "resolve_llm_aliases",
                    "skipped",
                    reason="disabled",
                    attempted=0,
                )
            )

        added = []
        if self.persist_aliases:
            for result in classifications:
                if (
                    result["method"] in {"fuzzy_alias", "llm_alias"}
                    and result["group"]
                    and result["sheet_name"]
                ):
                    added.extend(
                        {
                            "sheet_name": result["sheet_name"],
                            "group": result["group"],
                            "alias": alias,
                            "method": result["method"],
                        }
                        for alias in add_sheet_group_alias(
                            result["group"], result["sheet_name"]
                        )
                    )
        steps.append(
            _step(
                "update_sheet_group_aliases",
                added_count=len(added),
                added_aliases=added,
            )
        )

        verification = verify_sheet_group_mapping(sheets, classifications, groups)
        steps.append(
            _step(
                "final_mapping_verification",
                {"passed": "ok", "warning": "warning", "failed": "failed"}[
                    verification["status"]
                ],
                result=verification["status"],
                errors=verification["errors"],
                warnings=verification["warnings"],
                unmatched_sheets=verification["counts"]["unmatched_sheets"],
            )
        )
        return {
            "file_id": file_id,
            "sheet_count": len(sheets),
            "subagent": {
                "name": self.name,
                "version": self.version,
                "use_llm": self.use_llm,
                "persist_aliases": self.persist_aliases,
            },
            "steps": steps,
            "verification": verification,
            "classifications": classifications,
        }

    def run_file(self, file_id: int) -> Dict[str, Any]:
        return self.run(load_file_sheets_for_grouping(file_id), file_id)


def load_file_sheets_for_grouping(file_id: int) -> List[Dict[str, Any]]:
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
        {
            "sheet_name": row["sheet_name"],
            "columns": [
                column["column_name_flat"]
                for column in get_columns_by_sheet(file_id, row["sheet_name"])
                if column.get("column_name_flat")
            ],
        }
        for row in rows
    ]


def classify_file_sheet_groups(
    file_id: int,
    use_llm: bool = True,
    persist_aliases: bool = True,
) -> Dict[str, Any]:
    return SheetGroupResolverSubagent(use_llm, persist_aliases).run_file(file_id)
