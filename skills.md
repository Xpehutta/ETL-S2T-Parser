# Agent Skills

## Excel Header Detection
- Identify which rows contain column headers, even with:
  - merged cells (represented as None)
  - long descriptive rows (instructions, SQL, comments)
  - multi‑level headers (first row = category, second row = sub‑column)
  - empty or irrelevant sheets
- Return: `header_start_row` (0‑indexed), `header_rows_count` (0,1,2+), `nested` (bool)

## Schema Matching
- Compare Excel sheet structure (sheet names, column headers) to a target database schema.
- Produce similarity score and column‑level mapping suggestions.

## Data Summarization
- Generate a business‑oriented summary (in Russian) of an Excel file's content.
- Focus on: domain, purpose, key entities, lifecycle coverage, transformation patterns.

## Data Storage
- Persist parsed metadata (sheets, columns, data rows) into SQLite.
- Use hash‑based IDs (SHA256, lower‑cased) for deduplication.
- Support updating existing records.

## Human Correction
- Allow users to override header decisions via web UI dropdowns.
- Provide real‑time preview of resulting column headers.
- Mark sheets to be skipped.