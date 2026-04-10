import logging
import re
from typing import List, Dict, Any, Set
from agent import giga, get_model_name
from gigachat.models import Chat, Messages, MessagesRole
from db_storage import get_db_connection, update_file_summary

logger = logging.getLogger(__name__)

# Patterns to identify important columns for summarization
IMPORTANT_COLUMN_PATTERNS = {
    'source_table': r'(?i)(source|src|исходн|источник).*table',
    'target_table': r'(?i)(target|tgt|целев|приемник).*table',
    'sql': r'(?i)(sql|query|transform|rule|правило)',
    'description': r'(?i)(description|описание|comment|комментарий)',
    'column_mapping': r'(?i)(field|column|поле|атрибут)',
    'business_term': r'(?i)(entity|сущность|объект)',
}


def get_important_column_hashes(file_hash: str, conn) -> List[tuple]:
    """Return list of (column_hash, column_name_flat) for columns matching important patterns."""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT c.column_hash, c.column_name_flat
        FROM columns c
        JOIN sheets s ON c.sheet_hash = s.sheet_hash
        WHERE s.file_hash = ?
    """, (file_hash,))
    columns = cursor.fetchall()

    important = []
    for col in columns:
        col_name = col["column_name_flat"].lower() if col["column_name_flat"] else ""
        for pattern_name, pattern in IMPORTANT_COLUMN_PATTERNS.items():
            if re.search(pattern, col_name):
                important.append((col["column_hash"], col["column_name_flat"]))
                break
    return important[:20]  # limit to 20 columns to avoid token overload


def get_sample_values(column_hash: str, conn, limit: int = 5) -> List[str]:
    """Get distinct non‑null sample values from a column (first few rows)."""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT DISTINCT value
        FROM data
        WHERE column_hash = ? AND value IS NOT NULL AND value != ''
        LIMIT ?
    """, (column_hash, limit))
    rows = cursor.fetchall()
    return [row["value"] for row in rows]


def generate_summary(file_hash: str) -> str:
    """Generate a business summary using file name, structure, and actual data values."""
    conn = get_db_connection()
    cursor = conn.cursor()

    # Get file name
    cursor.execute("SELECT filename FROM files WHERE file_hash = ?", (file_hash,))
    row = cursor.fetchone()
    if not row:
        conn.close()
        raise ValueError(f"File hash {file_hash} not found")
    filename = row["filename"]

    # Get sheets and flattened column names (first 30 columns per sheet)
    cursor.execute("""
        SELECT s.sheet_name, GROUP_CONCAT(c.column_name_flat, ' | ') AS columns_flat
        FROM sheets s
        JOIN columns c ON s.sheet_hash = c.sheet_hash
        WHERE s.file_hash = ?
        GROUP BY s.sheet_hash
        ORDER BY s.sheet_name
    """, (file_hash,))
    sheets = cursor.fetchall()

    sheets_info = []
    for sheet in sheets:
        sheet_name = sheet["sheet_name"]
        columns = sheet["columns_flat"].split(" | ") if sheet["columns_flat"] else []
        columns = columns[:30]
        sheets_info.append(f"Лист '{sheet_name}': колонки: {', '.join(columns)}")

    # Get important columns and sample values
    important_cols = get_important_column_hashes(file_hash, conn)
    important_data = []
    for col_hash, col_name in important_cols:
        sample_vals = get_sample_values(col_hash, conn, limit=3)
        if sample_vals:
            # Format: column_name: value1, value2, value3
            vals_str = ", ".join([v[:100] for v in sample_vals])  # truncate long values
            important_data.append(f"  {col_name}: {vals_str}")

    conn.close()

    sheets_block = "\n".join(sheets_info)
    important_block = "\n".join(important_data) if important_data else "Нет значимых данных."

    # New flexible prompt that uses both structure and data values
    prompt = f"""
Ты – бизнес-аналитик и эксперт по данным. На основе метаданных Excel-файла и примеров данных составь краткое, информативное описание (3–5 абзацев) на РУССКОМ языке.

**Важно:** Не предполагай заранее тематику. Анализируй фактические данные:

- Названия листов (sheets)
- Названия колонок
- Примеры значений из важных колонок (таблицы-источники, SQL-правила, описания)

Имя файла: {filename}

Листы и колонки (уплощённые заголовки, первые 30 колонок на лист):
{sheets_block}

Примеры значимых данных (колонка → образцы значений):
{important_block}

Определи и опиши:
1. **Предметную область / бизнес-направление** (на основе встречающихся терминов)
2. **Назначение файла** (маппинг, словарь, отчёт, ETL-спецификация, etc.)
3. **Ключевые сущности** (из колонок и данных)
4. **Особенности структуры** (многоуровневые заголовки, объединённые ячейки, SQL-трансформации)
5. **Если есть явные указания на источники и приёмники данных** – опиши их
6. **Общую структуру документа** – перечисли ключевые листы и их роль (связным текстом)

Напиши summary на РУССКОМ языке. Стиль – профессиональный, деловой. Не используй маркированные списки. Если не уверен – укажи "предположительно".
"""

    messages = [
        Messages(role=MessagesRole.SYSTEM, content="Ты – эксперт по анализу данных и бизнес-аналитик."),
        Messages(role=MessagesRole.USER, content=prompt)
    ]

    try:
        response = giga.chat(Chat(messages=messages))
        summary = response.choices[0].message.content.strip()
        logger.info(f"Generated summary for {filename} (model: {get_model_name()})")
        return summary
    except Exception as e:
        logger.error(f"Summary generation failed: {e}")
        return f"Не удалось сгенерировать описание из-за ошибки: {str(e)}"


def summarize_file(file_hash: str, save: bool = True) -> str:
    """Generate (and optionally save) a summary for a file."""
    summary = generate_summary(file_hash)
    if save:
        update_file_summary(file_hash, summary)
    return summary