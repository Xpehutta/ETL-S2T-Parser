import pytest
from unittest.mock import patch, MagicMock
from summarizer_agent import generate_summary, summarize_file

@patch('summarizer_agent.fetch_file_data')
@patch('summarizer_agent.giga')
def test_generate_summary(mock_giga, mock_fetch):
    # Mock fetch_file_data
    mock_fetch.return_value = (
        "test.xlsx",
        [
            {"sheet_name": "Sheet1", "columns": ["Name", "Age"], "sample_rows": [{"row_num": 0, "values": "Alice | 30"}]}
        ],
        ["КЮЛ", "S2T"]
    )
    # Mock GigaChat responses for each step
    mock_response = MagicMock()
    mock_response.choices = [MagicMock(message=MagicMock(content='{"key_entities": ["кредиты"]}'))]
    mock_giga.chat.return_value = mock_response
    summary = generate_summary("fake_hash")
    # Since we mocked, it will return the final summary (which might be default or whatever)
    assert isinstance(summary, str)

@patch('summarizer_agent.generate_summary')
@patch('summarizer_agent.update_file_summary')
def test_summarize_file(mock_update, mock_generate):
    mock_generate.return_value = "Generated summary"
    result = summarize_file("hash", save=True)
    assert result == "Generated summary"
    mock_update.assert_called_once_with("hash", "Generated summary")