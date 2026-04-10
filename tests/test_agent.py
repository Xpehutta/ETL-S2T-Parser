import pytest
from unittest.mock import patch, MagicMock
from agent import analyze_sheet, get_header_decision, get_model_name

@pytest.fixture
def mock_giga_response():
    return MagicMock(choices=[MagicMock(message=MagicMock(content='{"header_start_row":0,"header_rows":1,"nested":false,"explanation":"single header"}'))])

@patch('agent.giga')
def test_analyze_sheet_single_header(mock_giga, mock_giga_response):
    mock_giga.chat.return_value = mock_giga_response
    preview_rows = [
        ["Name", "Age", "City"],
        ["Alice", 30, "NYC"],
        ["Bob", 25, "LA"]
    ]
    start_row, rows, nested = analyze_sheet("Sheet1", preview_rows)
    assert start_row == 0
    assert rows == 1
    assert nested == False

@patch('agent.giga')
def test_analyze_sheet_multi_level(mock_giga):
    mock_response = MagicMock(choices=[MagicMock(message=MagicMock(content='{"header_start_row":0,"header_rows":2,"nested":true,"explanation":"multi-level"}'))])
    mock_giga.chat.return_value = mock_response
    preview_rows = [
        ["Product", None, "Date"],
        ["Name", "ID", "Day"],
        ["Widget", 123, "2025-01-01"]
    ]
    start_row, rows, nested = analyze_sheet("Sheet1", preview_rows)
    assert start_row == 0
    assert rows == 2
    assert nested == True

def test_get_model_name():
    # Should return a string (mocked or real from env)
    name = get_model_name()
    assert isinstance(name, str)
    assert name in ["GigaChat-Pro", "GigaChat-Max"] or name == "GigaChat-Pro"  # default