from unittest.mock import patch

from agents.sheet_group_classifier import (
    SheetGroupResolverSubagent,
    _sheet_group_prompt,
    classify_sheet_group,
    classify_sheet_groups,
    verify_sheet_group_mapping,
)


def test_sheet_group_prompt_is_russian():
    prompt = _sheet_group_prompt({"aliases_json": "[]", "sheet_json": "{}"})

    assert prompt.startswith("Определи, является ли имя одного листа Excel")
    assert "не семантическая классификация" in prompt
    assert "само по себе никогда не является основанием" in prompt
    assert "уверенность ниже high" in prompt
    assert "без изменения смысла, типа объекта, направления и уровня" in prompt
    assert "You resolve one Excel sheet" not in prompt


def test_classify_sheet_group_uses_alias_before_llm():
    result = classify_sheet_group(
        {"sheet_name": "S2T", "columns": ["id", "value"]},
        use_llm=False,
    )

    assert result["group"] == "s2t"
    assert result["matched_alias"] == "S2T"
    assert result["confidence"] == "high"
    assert result["method"] == "alias"
    assert "layer" not in result


@patch("agents.sheet_group_classifier.invoke_llm_plain_text")
def test_classify_sheet_group_uses_llm_for_unknown_sheet(mock_llm):
    mock_llm.return_value = (
        '{"group": "source_columns", "matched_alias": "Source columns", "confidence": "medium", '
        '"reason": "headers look like source column metadata"}'
    )

    result = classify_sheet_group(
        {"sheet_name": "Unknown source metadata", "columns": ["source column", "data type"]},
        use_llm=True,
    )

    assert result["group"] == "source_columns"
    assert result["matched_alias"] == "Source columns"
    assert result["confidence"] == "medium"
    assert result["method"] == "llm_alias"
    assert "layer" not in result
    assert "source column" in mock_llm.call_args.args[0]


@patch("agents.sheet_group_classifier.invoke_llm_plain_text")
def test_classify_sheet_group_rejects_unknown_llm_group(mock_llm):
    mock_llm.return_value = (
        '{"group": "made_up_group", "matched_alias": "whatever", '
        '"confidence": "high", "reason": "bad"}'
    )

    result = classify_sheet_group(
        {"sheet_name": "Unknown", "columns": ["x"]},
        use_llm=True,
    )

    assert result["group"] is None
    assert result["matched_alias"] is None
    assert result["confidence"] == "low"
    assert "unknown group" in result["reason"].lower()


@patch("agents.sheet_group_classifier.invoke_llm_plain_text")
def test_classify_sheet_group_rejects_alias_outside_schema(mock_llm):
    mock_llm.return_value = (
        '{"group": "source_columns", "matched_alias": "made up alias", '
        '"confidence": "high", "reason": "bad alias"}'
    )

    result = classify_sheet_group(
        {"sheet_name": "Unknown", "columns": ["x"]},
        use_llm=True,
    )

    assert result["group"] is None
    assert result["matched_alias"] is None
    assert result["confidence"] == "low"
    assert "outside schema" in result["reason"].lower()


def test_classify_sheet_groups_loops_over_document_sheets():
    results = classify_sheet_groups(
        [
            {"sheet_name": "S2T", "columns": []},
            {"sheet_name": "Source tables", "columns": []},
            {"sheet_name": "Глоссарий", "columns": []},
        ],
        use_llm=False,
    )

    assert [row["group"] for row in results] == ["s2t", "source_tables", "glossary"]


def test_sheet_group_resolver_subagent_runs_steps_and_verifies_mapping():
    result = SheetGroupResolverSubagent(use_llm=False).run(
        [
            {"sheet_name": "S2T", "columns": []},
            {"sheet_name": "Source tables", "columns": []},
        ],
        file_id="fh_subagent",
    )

    assert result["subagent"]["name"] == "sheet_group_resolver_subagent"
    assert result["verification"]["status"] == "passed"
    assert [step["step"] for step in result["steps"]] == [
        "load_schema",
        "load_sheets",
        "resolve_exact_fuzzy_aliases",
        "resolve_llm_aliases",
        "update_sheet_group_aliases",
        "final_mapping_verification",
    ]
    assert result["steps"][-1]["status"] == "ok"
    assert [row["group"] for row in result["classifications"]] == ["s2t", "source_tables"]


@patch("agents.sheet_group_classifier.invoke_llm_plain_text")
def test_sheet_group_resolver_subagent_uses_llm_only_for_unresolved_sheets(mock_llm):
    mock_llm.return_value = (
        '{"group": "source_columns", "matched_alias": "Source columns", "confidence": "medium", '
        '"reason": "close source columns title"}'
    )

    result = SheetGroupResolverSubagent(use_llm=True, persist_aliases=False).run(
        [
            {"sheet_name": "S2T", "columns": []},
            {"sheet_name": "Unknown source metadata", "columns": ["source column"]},
        ],
        file_id="fh_subagent_llm",
    )

    assert mock_llm.call_count == 1
    assert result["verification"]["status"] == "passed"
    assert result["steps"][3]["attempted"] == 1
    assert result["steps"][3]["resolved"] == 1
    assert result["steps"][4]["step"] == "update_sheet_group_aliases"
    assert result["steps"][4]["added_count"] == 0
    assert [row["method"] for row in result["classifications"]] == ["alias", "llm_alias"]


@patch("agents.sheet_group_classifier.invoke_llm_plain_text")
@patch("agents.sheet_group_classifier.add_sheet_group_alias", return_value=["SourceToTargt"])
def test_sheet_group_resolver_uses_fuzzy_alias_before_llm_and_persists_alias(mock_add_alias, mock_llm):
    result = SheetGroupResolverSubagent(use_llm=True).run(
        [{"sheet_name": "SourceToTargt", "columns": []}],
        file_id="fh_fuzzy_sheet",
    )

    assert mock_llm.call_count == 0
    assert result["verification"]["status"] == "passed"
    assert result["classifications"][0]["group"] == "s2t"
    assert result["classifications"][0]["method"] == "fuzzy_alias"
    assert result["steps"][2]["matched"] == 1
    assert result["steps"][4]["added_count"] == 1
    mock_add_alias.assert_called_once_with("s2t", "SourceToTargt")


def test_verify_sheet_group_mapping_rejects_alias_outside_schema():
    verification = verify_sheet_group_mapping(
        [{"sheet_name": "Source tables", "columns": []}],
        [
            {
                "sheet_name": "Source tables",
                "group": "source_tables",
                "matched_alias": "made up alias",
                "confidence": "high",
                "method": "llm_alias",
                "reason": "bad mapping",
            }
        ],
    )

    assert verification["status"] == "failed"
    assert verification["errors"] >= 1
    assert any(issue["type"] == "alias_outside_schema" for issue in verification["issues"])
