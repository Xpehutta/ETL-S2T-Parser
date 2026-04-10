# AI‑Powered Excel Metadata Extractor

Extracts file name, sheet names, and column headers from Excel files using GigaChat AI to intelligently detect:
- Whether the first row(s) are long descriptions (skips them)
- Number of header rows (1 or more)
- Merged columns (forward‑filled)
- Nested JSON for multi‑level headers

## Setup

1. Install `uv` (https://github.com/astral-sh/uv)
2. Clone this repository
3. Run `uv sync` (or `uv pip install -r requirements.txt`)
4. Create `.env` with your GigaChat credentials
5. Run `uv run python app.py`
6. Open http://127.0.0.1:5000

## Example output

```json
{
  "filename": "report.xlsx",
  "sheets": [
    {
      "sheet_name": "Data",
      "ai_decision": {
        "header_start_row": 0,
        "header_rows_count": 2,
        "nested_structure": true
      },
      "columns": [
        ["Product", "Name"],
        ["Product", "ID"],
        ["Date", "Day"]
      ]
    }
  ]
}