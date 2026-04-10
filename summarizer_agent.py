import logging
import json
import re
from typing import List, Dict, Any, TypedDict
from langgraph.graph import StateGraph, END
from agent import giga, get_model_name
from gigachat.models import Chat, Messages, MessagesRole
from db_storage import get_db_connection, update_file_summary
from load_skills_tools import load_skills, load_tools

logger = logging.getLogger(__name__)

# System prompt with skills and tools
SYSTEM_PROMPT = f"""
{load_skills()}

{load_tools()}

You are a business analyst and technical writer. Use the skills and tools above.
"""


# ------------------------------------------------------------
# State definition
# ------------------------------------------------------------
class SummarizerState(TypedDict):
    file_hash: str
    filename: str
    raw_sheets: List[Dict[str, Any]]
    important_values: List[str]
    schema: Dict[str, Any]
    section_summaries: List[str]
    final_summary: str
    validation_errors: List[str]


# ------------------------------------------------------------
# Data fetching from database
# ------------------------------------------------------------
def fetch_file_data(file_hash: str):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT filename FROM files WHERE file_hash = ?", (file_hash,))
    row = cursor.fetchone()
    if not row:
        conn.close()
        raise ValueError(f"File hash {file_hash} not found")
    filename = row["filename"]

    # Get sheets and columns
    cursor.execute("""
        SELECT s.sheet_name, s.header_start_row, s.header_rows_count, s.nested_structure,
               c.column_index, c.column_name_flat, c.column_header
        FROM sheets s
        JOIN columns c ON s.sheet_hash = c.sheet_hash
        WHERE s.file_hash = ?
        ORDER BY s.sheet_name, c.column_index
    """, (file_hash,))
    rows = cursor.fetchall()

    sheets_dict = {}
    for r in rows:
        sheet_name = r["sheet_name"]
        if sheet_name not in sheets_dict:
            sheets_dict[sheet_name] = {
                "sheet_name": sheet_name,
                "columns": [],
                "sample_rows": []
            }
        sheets_dict[sheet_name]["columns"].append(r["column_name_flat"])

    # Get sample rows (first 5 rows per sheet)
    for sheet_name in sheets_dict:
        cursor.execute("""
            SELECT s.sheet_hash
            FROM sheets s
            WHERE s.file_hash = ? AND s.sheet_name = ?
        """, (file_hash, sheet_name))
        sheet_row = cursor.fetchone()
        if not sheet_row:
            continue
        sheet_hash = sheet_row["sheet_hash"]
        cursor.execute("""
            SELECT d.row_num, GROUP_CONCAT(d.value, ' | ') AS row_values
            FROM data d
            WHERE d.sheet_hash = ?
            GROUP BY d.row_num
            ORDER BY d.row_num
            LIMIT 5
        """, (sheet_hash,))
        sample_rows = cursor.fetchall()
        sheets_dict[sheet_name]["sample_rows"] = [
            {"row_num": r["row_num"], "values": r["row_values"][:500] if r["row_values"] else ""}
            for r in sample_rows
        ]

    # Extract important business values from data
    important_values = set()
    cursor.execute("""
        SELECT DISTINCT d.value
        FROM data d
        JOIN sheets s ON d.sheet_hash = s.sheet_hash
        WHERE s.file_hash = ? AND d.value IS NOT NULL AND d.value != ''
        LIMIT 500
    """, (file_hash,))
    all_values = cursor.fetchall()
    for val_row in all_values:
        val = str(val_row["value"])
        if re.search(r'(КЮЛ|ТБО|Сбер|ВТБ|IFRS|МСФО|субсид|продуктовый регистр|процентная ставка|гарантия|обеспечение)',
                     val, re.IGNORECASE):
            important_values.add(val[:100])
        if re.search(r'\b[A-Z]{2,5}\b', val):
            important_values.add(val[:100])

    conn.close()
    sheets_list = list(sheets_dict.values())
    return filename, sheets_list, list(important_values)[:30]


# ------------------------------------------------------------
# Step 1: Extract schema (entities, history, source/target)
# ------------------------------------------------------------
SCHEMA_EXTRACTION_PROMPT = """
Ты – эксперт по анализу данных. Извлеки структурированную информацию из Excel-файла.

Файл: {filename}

Листы и колонки (первые 30 колонок на лист):
{sheets_columns}

Примеры данных (первые строки):
{sample_data}

Важные бизнес-термины, найденные в данных:
{important_values}

Извлеки следующие элементы в формате JSON. Будь максимально конкретным. Используй найденные термины.

Пример вывода:
{{
    "business_domain": "корпоративное кредитование юридических лиц (КЮЛ)",
    "bank_hint": "Сбербанк (предположительно)",
    "project_codes": ["КЮЛ", "S2T"],
    "key_entities": ["кредитные договоры", "договоры банковской гарантии", "обеспечение", "контрагенты", "валюты", "процентные ставки", "субсидии", "продуктовые регистры", "МСФО (IFRS 9)"],
    "history_types": ["Снимок", "Историзм", "Снимок-История 3"],
    "source_tables": ["список таблиц-источников"],
    "target_tables": ["список целевых таблиц"],
    "transformation_patterns": ["SQL-склейка", "справочники", "суррогатные ключи"]
}}

Теперь извлеки для данного файла. Используй только то, что реально присутствует.
"""


def extract_schema(state: SummarizerState) -> SummarizerState:
    filename = state["filename"]
    sheets = state["raw_sheets"]
    important_vals = state["important_values"]

    sheets_columns = []
    sample_data = []
    for sheet in sheets:
        cols = ", ".join(sheet["columns"][:20])
        sheets_columns.append(f"Лист '{sheet['sheet_name']}': {cols}")
        if sheet["sample_rows"]:
            sample_data.append(f"Лист '{sheet['sheet_name']}':")
            for row in sheet["sample_rows"]:
                sample_data.append(f"  Строка {row['row_num']}: {row['values']}")

    prompt = SCHEMA_EXTRACTION_PROMPT.format(
        filename=filename,
        sheets_columns="\n".join(sheets_columns),
        sample_data="\n".join(sample_data[:10]),
        important_values="\n".join(important_vals[:15])
    )
    messages = [
        Messages(role=MessagesRole.SYSTEM, content=SYSTEM_PROMPT),
        Messages(role=MessagesRole.USER, content=prompt)
    ]
    try:
        response = giga.chat(Chat(messages=messages))
        text = response.choices[0].message.content.strip()
        if "```json" in text:
            text = text.split("```json")[1].split("```")[0]
        elif "```" in text:
            text = text.split("```")[1].split("```")[0]
        schema = json.loads(text)
        state["schema"] = schema
        logger.info(f"Schema extracted: {schema}")
    except Exception as e:
        logger.error(f"Schema extraction failed: {e}")
        state["schema"] = {"business_domain": "не определено", "key_entities": []}
        state["validation_errors"].append(f"Schema error: {e}")
    return state


# ------------------------------------------------------------
# Step 2: Structural summary (pass 1)
# ------------------------------------------------------------
STRUCTURAL_PROMPT = """
Опиши структуру документа (листы и их назначение). 1-2 абзаца на русском.

Листы и колонки:
{sheets_columns}
"""


def structural_summary(state: SummarizerState) -> SummarizerState:
    sheets = state["raw_sheets"]
    sheets_columns = []
    for sheet in sheets:
        cols = ", ".join(sheet["columns"][:20])
        sheets_columns.append(f"Лист '{sheet['sheet_name']}': {cols}")
    prompt = STRUCTURAL_PROMPT.format(sheets_columns="\n".join(sheets_columns))
    messages = [
        Messages(role=MessagesRole.SYSTEM, content=SYSTEM_PROMPT),
        Messages(role=MessagesRole.USER, content=prompt)
    ]
    try:
        response = giga.chat(Chat(messages=messages))
        summary = response.choices[0].message.content.strip()
        state["section_summaries"].append(summary)
    except Exception as e:
        logger.error(f"Structural summary failed: {e}")
        state["section_summaries"].append("Не удалось сгенерировать структурное описание.")
    return state


# ------------------------------------------------------------
# Step 3: Domain summary (pass 2)
# ------------------------------------------------------------
DOMAIN_PROMPT = """
На основе извлечённой схемы:
{schema}

Опиши бизнес-домен, ключевые сущности, жизненный цикл данных. Обязательно упомяни:
- Банк или организацию (если есть намёк)
- Коды проектов (например, КЮЛ)
- Финансовые стандарты (МСФО, IFRS)
- Конкретные продукты (кредиты, гарантии, субсидии, продуктовые регистры)
Напиши 2 абзаца на русском.
"""


def domain_summary(state: SummarizerState) -> SummarizerState:
    schema = state.get("schema", {})
    schema_str = json.dumps(schema, ensure_ascii=False, indent=2)
    prompt = DOMAIN_PROMPT.format(schema=schema_str)
    messages = [
        Messages(role=MessagesRole.SYSTEM, content=SYSTEM_PROMPT),
        Messages(role=MessagesRole.USER, content=prompt)
    ]
    try:
        response = giga.chat(Chat(messages=messages))
        summary = response.choices[0].message.content.strip()
        state["section_summaries"].append(summary)
    except Exception as e:
        logger.error(f"Domain summary failed: {e}")
        state["section_summaries"].append("Не удалось сгенерировать доменное описание.")
    return state


# ------------------------------------------------------------
# Step 4: Final synthesis (pass 3)
# ------------------------------------------------------------
SYNTHESIS_PROMPT = """
На основе следующих резюме создай **ОДИН связный абзац** (5-7 предложений) на русском языке.
Абзац должен быть максимально похож на пример ниже по стилю и детализации.

Промежуточные резюме:
{section_summaries}

Пример желаемого стиля:
"Данный файл представляет собой спецификацию маппинга «Источник-Приёмник» (Source-to-Target, S2T) для хранилища данных по корпоративному кредитованию (код проекта «КЮЛ» — Кредиты Юридическим Лицам) в рамках крупной российской банковской среды (предположительно Сбербанк). Документ определяет логику ETL-процессов для трансформации данных из исходных систем в единую аналитическую модель, охватывающую полный жизненный цикл кредитных продуктов: кредитные договоры, договоры банковской гарантии и договоры обеспечения, а также связанные с ними атрибуты, такие как контрагенты, валюты, правила расчёта процентных ставок, субсидии, метрики классификации по МСФО (IFRS 9), продуктовые регистры, а также плановые и фактические финансовые операции."

Напиши абзац, следуя этому примеру: укажи тип документа (S2T, маппинг, ETL), проект, банк, перечисли конкретные сущности и финансовые показатели.
"""


def synthesize(state: SummarizerState) -> SummarizerState:
    combined = "\n\n".join(state["section_summaries"])
    prompt = SYNTHESIS_PROMPT.format(section_summaries=combined)
    messages = [
        Messages(role=MessagesRole.SYSTEM, content=SYSTEM_PROMPT),
        Messages(role=MessagesRole.USER, content=prompt)
    ]
    try:
        response = giga.chat(Chat(messages=messages))
        final = response.choices[0].message.content.strip()
        state["final_summary"] = final
    except Exception as e:
        logger.error(f"Synthesis failed: {e}")
        state["final_summary"] = "Не удалось сформировать итоговое описание."
    return state


# ------------------------------------------------------------
# Step 5: Validation (grounding)
# ------------------------------------------------------------
def normalize_text(text: str) -> str:
    text = text.lower()
    text = re.sub(r'[^\w\s]', ' ', text)
    return ' '.join(text.split())


def validate(state: SummarizerState) -> SummarizerState:
    final = state["final_summary"]
    schema = state.get("schema", {})
    entities = schema.get("key_entities", [])
    if entities:
        normalized_summary = normalize_text(final)
        found = False
        for entity in entities:
            norm_entity = normalize_text(entity)
            if norm_entity in normalized_summary:
                found = True
                break
            # Check partial word matches
            for word in norm_entity.split():
                if len(word) > 3 and word in normalized_summary:
                    found = True
                    break
            if found:
                break
        if not found:
            state["validation_errors"].append(f"Summary does not mention any key entity from schema: {entities}")
            logger.warning(state["validation_errors"][-1])
        else:
            logger.info("Validation passed: summary mentions at least one key entity")
    return state


# ------------------------------------------------------------
# Build LangGraph workflow
# ------------------------------------------------------------
def build_summarizer_graph():
    builder = StateGraph(SummarizerState)
    builder.add_node("extract_schema", extract_schema)
    builder.add_node("structural", structural_summary)
    builder.add_node("domain", domain_summary)
    builder.add_node("synthesize", synthesize)
    builder.add_node("validate", validate)
    builder.set_entry_point("extract_schema")
    builder.add_edge("extract_schema", "structural")
    builder.add_edge("structural", "domain")
    builder.add_edge("domain", "synthesize")
    builder.add_edge("synthesize", "validate")
    builder.add_edge("validate", END)
    return builder.compile()


summarizer_graph = build_summarizer_graph()


# ------------------------------------------------------------
# Public API
# ------------------------------------------------------------
def generate_summary(file_hash: str) -> str:
    filename, sheets, important_vals = fetch_file_data(file_hash)
    initial_state: SummarizerState = {
        "file_hash": file_hash,
        "filename": filename,
        "raw_sheets": sheets,
        "important_values": important_vals,
        "schema": {},
        "section_summaries": [],
        "final_summary": "",
        "validation_errors": []
    }
    result = summarizer_graph.invoke(initial_state)
    if result["validation_errors"]:
        logger.warning(f"Validation issues for {filename}: {result['validation_errors']}")
    return result["final_summary"]


def summarize_file(file_hash: str, save: bool = True) -> str:
    """Generate and optionally save summary to database."""
    summary = generate_summary(file_hash)
    if save:
        update_file_summary(file_hash, summary)
    return summary