
# ETL S2T Parser – AI‑Powered Excel Metadata Extractor

[![Python 3.12+](https://img.shields.io/badge/python-3.12+-blue.svg)](https://www.python.org/downloads/)
[![Flask](https://img.shields.io/badge/Flask-2.3.3-green.svg)](https://flask.palletsprojects.com/)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

**ETL S2T Parser** is an intelligent tool that extracts structured metadata from Excel files containing Source‑to‑Target (S2T) mappings, ETL rules, data dictionaries, or any tabular specifications. It uses GigaChat (LLM) to automatically detect column headers (even with merged cells, multi‑level headers, or long descriptions) and provides a web interface for human correction. The extracted data is stored in SQLite, and a business‑focused summary (in Russian) is generated.

---

## ✨ Key Features

- **AI‑driven header detection** – Identifies which rows contain column headers, handles merged cells, skips long descriptive rows.
- **Multi‑sheet support** – Processes all sheets, skips empty/irrelevant ones.
- **Human correction UI** – Override AI decisions per sheet (first row only, second row only, both concatenated). Real‑time header preview.
- **Business summary generation** – Multi‑step LLM pipeline produces a concise Russian description of the file’s domain, entities, and transformation logic.
- **SQLite storage** – Stores file metadata, sheet structures, column headers (flat/nested), and sampled data rows.
- **Schema matching** – Compares Excel structure to a target database schema (source_tables, target_tables, column_mappings, additions) and suggests mappings.
- **Hash‑based deduplication** – Uses SHA256 (shortened) for IDs, lower‑cased for consistency.
- **REST API** – Upload files, apply corrections, preview headers, retrieve summaries.

---

## 🏗 Architecture

```
Excel File → Flask API → Header Detection (GigaChat + LangGraph) → 
Column & Data Extraction → SQLite Storage → Summary Generation → 
Human Correction UI → Final JSON → Database Update
```

Main modules:

| Module | Responsibility |
|--------|----------------|
| `app.py` | Flask web server, endpoints `/upload`, `/apply_corrections`, `/preview_headers`, `/summary/<hash>` |
| `agent.py` | AI header decision using GigaChat and LangGraph |
| `summarizer_agent.py` | Multi‑step business summary generation |
| `db_storage.py` | SQLite schema, data persistence |
| `schema_matcher_agent.py` | Compares Excel JSON with target relational schema |
| `data_loader.py` | Inserts high/medium similarity data into target tables |
| `helper.py` | Loads `skills.md` / `tools.md` for system prompts |
| `templates/index.html` | Human correction interface |

---

## 🚀 Installation

### Prerequisites

- Python 3.12 or higher
- `uv` (recommended) or `pip`
- Access to GigaChat API (credentials)

### Steps

1. **Clone the repository**
   ```bash
   git clone https://github.com/Xpehutta/ETL-S2T-Parser.git
   cd ETL-S2T-Parser
   ```

2. **Install dependencies**
   ```bash
   uv sync
   # or
   pip install -r requirements.txt
   ```

3. **Create a `.env` file** (see [Configuration](#-configuration))

4. **Run the application**
   ```bash
   uv run python app.py
   ```

5. **Open your browser** at `http://127.0.0.1:5000`

---

## ⚙️ Configuration

Create a `.env` file in the project root:

```ini
GIGACHAT_API_KEY=your_api_key_here
GIGACHAT_API_URL=https://gigachat.devices.sberbank.ru/api/v1
GIGACHAT_VERIFY_SSL=false
GIGACHAT_SCOPE=GIGACHAT_API_PERS
MODEL=GigaChat-Pro
GIGACHAT_TIMEOUT=120
```

| Variable | Description |
|----------|-------------|
| `GIGACHAT_API_KEY` | Your GigaChat API key |
| `GIGACHAT_API_URL` | GigaChat endpoint URL |
| `VERIFY_SSL` | Disable SSL verification if behind corporate proxy |
| `SCOPE` | API scope (default `GIGACHAT_API_PERS`) |
| `MODEL` | Model name (`GigaChat-Pro` or `GigaChat-Max`) |
| `TIMEOUT` | Request timeout in seconds |

---

## 📖 Usage

### Web Interface

1. **Upload an Excel file** – click “Upload & Analyze”.
2. **Review AI decisions** – for each sheet, see the detected header rows and a data preview.
3. **Correct if needed** – choose a different header option or skip the sheet. Header preview updates instantly.
4. **Apply corrections** – click “OK – Apply Corrections”. The final JSON is stored in the database and displayed.

### API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/upload` | Upload Excel file, returns JSON with `file_hash`, `summary`, `sheets` |
| `POST` | `/apply_corrections` | Apply user corrections (JSON body with `file_hash` and `corrections` list) |
| `POST` | `/preview_headers` | Preview column headers for a given sheet and option (used by UI) |
| `GET`  | `/summary/<file_hash>` | Retrieve the generated business summary |

### Example `/upload` response (abbreviated)

```json
{
  "filename": "S2T-700-КЮЛ_v5.xlsx",
  "model_used": "GigaChat-Pro",
  "file_hash": "a1b2c3...",
  "summary": "Данный файл представляет собой спецификацию маппинга...",
  "sheets": [
    {
      "sheet_name": "S2T",
      "skipped": false,
      "ai_decision": {
        "header_start_row": 0,
        "header_rows_count": 2,
        "nested_structure": true
      },
      "columns": [["Приемник данных", "Предметная область модели"], ...],
      "first_data_rows_preview": [...]
    }
  ]
}
```

---

## 🗄 Database Schema

SQLite database (`excel_data.db`) is created automatically.

### Core tables

- `files` – `file_hash`, `filename`, `upload_time`, `model_used`, `summary`, `result_json`
- `sheets` – `sheet_hash`, `file_hash`, `sheet_name`, `header_start_row`, `header_rows_count`, `nested_structure`
- `columns` – `column_hash`, `sheet_hash`, `column_index`, `column_header` (JSON), `column_name_flat`
- `data` – `id`, `sheet_hash`, `column_hash`, `row_num`, `value`

### Target tables (for loaded mappings)

- `source_tables`, `target_tables`, `column_mappings`, `additions` – populated by `data_loader.py` based on similarity reports.

---

## 🧠 AI Prompts & Skills

The system uses `skills.md` and `tools.md` to provide structured context to GigaChat. These files are loaded by `helper.py` and injected into the system prompt of both `agent.py` and `summarizer_agent.py`. This improves consistency and reduces hallucinations.

- **skills.md** – lists agent capabilities (header detection, schema matching, summarization, etc.)
- **tools.md** – describes each available function with input/output specs and examples.

---


## 🙏 Acknowledgements

- [GigaChat](https://developers.sber.ru/portal/products/gigachat) for LLM capabilities
- [LangGraph](https://langchain-ai.github.io/langgraph/) for agent orchestration
- [Flask](https://flask.palletsprojects.com/) for the web framework
- [Bootstrap](https://getbootstrap.com/) for the UI components
```
