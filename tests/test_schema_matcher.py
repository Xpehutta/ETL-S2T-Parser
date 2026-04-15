import pytest
import json
from unittest.mock import patch, MagicMock
from schema_matcher import (
    TARGET_SCHEMA,
    match_sheets_to_tables,
    map_columns_for_table,
    compare_with_target
)


@pytest.fixture
def sample_excel_json():
    """Sample Excel JSON as returned by /upload."""
    return {
        "filename": "test.xlsx",
        "model_used": "GigaChat-Pro",
        "file_hash": "abc123",
        "summary": "Test summary",
        "sheets": [
            {
                "sheet_name": "Sheet1",
                "skipped": False,
                "ai_decision": {
                    "header_start_row": 0,
                    "header_rows_count": 1,
                    "nested_structure": False
                },
                "columns": ["Name", "Age", "City"],
                "first_data_rows_preview": [["Alice", 30, "NYC"], ["Bob", 25, "LA"]],
                "preview_rows": [["Name", "Age", "City"], ["Alice", 30, "NYC"]]
            },
            {
                "sheet_name": "Source tables",
                "skipped": False,
                "ai_decision": {},
                "columns": ["name", "system_code", "description"],
                "first_data_rows_preview": [["LOANSCONTRACT", "700", "Credit contracts"]]
            },
            {
                "sheet_name": "S2T",
                "skipped": False,
                "ai_decision": {},
                "columns": [
                    "target_table_name", "target_column", "source_table_name",
                    "source_column", "transformation_rule", "data_type", "is_primary_key"
                ]
            }
        ]
    }


def test_target_schema_structure():
    """Verify TARGET_SCHEMA has expected tables and columns."""
    assert "tables" in TARGET_SCHEMA
    table_names = [t["name"] for t in TARGET_SCHEMA["tables"]]
    expected = ["source_tables", "target_tables", "column_mappings", "additions"]
    for name in expected:
        assert name in table_names

    # Check column_mappings has required columns
    col_mappings = next(t for t in TARGET_SCHEMA["tables"] if t["name"] == "column_mappings")
    expected_cols = [
        "target_table_name", "target_column", "source_table_name",
        "source_column", "transformation_rule", "data_type", "is_primary_key"
    ]
    for col in expected_cols:
        assert col in col_mappings["columns"]


@patch('schema_matcher.giga')
def test_match_sheets_to_tables(mock_giga, sample_excel_json):
    """Test sheet matching with mocked GigaChat response."""
    # Mock the API response
    mock_response = MagicMock()
    mock_response.choices = [MagicMock(message=MagicMock(content='''[
        {"sheet_name": "Sheet1", "target_table": null, "similarity": "low", "reason": "No match"},
        {"sheet_name": "Source tables", "target_table": "source_tables", "similarity": "high", "reason": "Direct match"},
        {"sheet_name": "S2T", "target_table": "column_mappings", "similarity": "high", "reason": "Mapping sheet"}
    ]'''))]
    mock_giga.chat.return_value = mock_response

    result = match_sheets_to_tables(sample_excel_json)
    assert len(result) == 3
    # Check structure
    for item in result:
        assert "sheet_name" in item
        assert "target_table" in item
        assert "similarity" in item
        assert "reason" in item

    # Verify specific matches
    source_match = next(r for r in result if r["sheet_name"] == "Source tables")
    assert source_match["target_table"] == "source_tables"
    assert source_match["similarity"] == "high"


@patch('schema_matcher.giga')
def test_map_columns_for_table(mock_giga, sample_excel_json):
    """Test column mapping for a specific table."""
    mock_response = MagicMock()
    mock_response.choices = [MagicMock(message=MagicMock(content='''{
        "mapping": {"name": "name", "system_code": "system_code"},
        "similarity": "high"
    }'''))]
    mock_giga.chat.return_value = mock_response

    result = map_columns_for_table(sample_excel_json, "Source tables", "source_tables")
    assert "mapping" in result
    assert "similarity" in result
    assert result["similarity"] == "high"
    assert result["mapping"]["name"] == "name"
    assert result["mapping"]["system_code"] == "system_code"


@patch('schema_matcher.match_sheets_to_tables')
@patch('schema_matcher.map_columns_for_table')
def test_compare_with_target(mock_map_columns, mock_match_sheets, sample_excel_json):
    """Test the full comparison workflow."""
    # Mock sheet matching results
    mock_match_sheets.return_value = [
        {"sheet_name": "Source tables", "target_table": "source_tables", "similarity": "high", "reason": "good"},
        {"sheet_name": "S2T", "target_table": "column_mappings", "similarity": "high", "reason": "good"},
        {"sheet_name": "Sheet1", "target_table": None, "similarity": "low", "reason": "no"}
    ]
    # Mock column mapping results
    mock_map_columns.side_effect = [
        {"mapping": {"name": "name", "system_code": "system_code"}, "similarity": "high"},
        {"mapping": {"target_table_name": "target_table_name", "target_column": "target_column"}, "similarity": "high"}
    ]

    result = compare_with_target(sample_excel_json)

    assert "similarity_score" in result
    assert "mapping_suggestions" in result
    assert "unmatched_excel_sheets" in result
    assert "unmatched_target_tables" in result

    # There are 2 matched sheets, so similarity score should be >0
    assert result["similarity_score"] > 0
    assert len(result["mapping_suggestions"]) == 3
    # Unmatched excel sheets: ["Sheet1"]
    assert result["unmatched_excel_sheets"] == ["Sheet1"]


def test_compare_with_target_error_handling(sample_excel_json):
    """Test error handling when match_sheets_to_tables fails."""
    with patch('schema_matcher.match_sheets_to_tables', return_value=[]):
        result = compare_with_target(sample_excel_json)
        assert result["similarity_score"] == 0
        assert result["mapping_suggestions"] == []
        assert result["unmatched_excel_sheets"] == []