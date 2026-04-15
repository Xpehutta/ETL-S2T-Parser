import logging
import json
import re
from typing import List, Dict, Any, TypedDict
from langgraph.graph import StateGraph, END

# Optional Langfuse imports
try:
    from langfuse import observe
    from langfuse_setup import get_callback_handler
    LANGFUSE_AVAILABLE = True
except ImportError:
    LANGFUSE_AVAILABLE = False
    # dummy decorator
    def observe(*args, **kwargs):
        def decorator(func):
            return func
        return decorator
    # dummy handler
    def get_callback_handler():
        return None

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
# Data fetching (no tracing)
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
        if re.search(r'(КЮЛ|ТБО|Сбер|ВТБ|IFRS|МСФО|субсид|продуктовый регистр|процентная ставка|гарантия|обеспечение)', val, re.IGNORECASE):
            important_values.add(val[:100])
        if re.search(r'\b[A-Z]{2,5}\b', val):
            important_values.add(val[:100])
    conn.close()
    sheets_list = list(sheets_dict.values())
    return filename, sheets_list, list(important_values)[:30]

# ------------------------------------------------------------
# Traced GigaChat call (generation)
# ------------------------------------------------------------
@observe(as_type="generation", capture_input=True, capture_output=True)
def call_gigachat_for_summary(user_content: str) -> str:
    """Call GigaChat for summarization with tracing."""
    if LANGFUSE_AVAILABLE:
        try:
            from langfuse import get_current_observation
            current_obs = get_current_observation()
            if current_obs:
                current_obs.update(model=get_model_name())
        except Exception:
            pass
    messages = [
        Messages(role=MessagesRole.SYSTEM, content=SYSTEM_PROMPT),
        Messages(role=MessagesRole.USER, content=user_content)
    ]
    response = giga.chat(Chat(messages=messages))
    answer = response.choices[0].message.content.strip()
    return answer

# ------------------------------------------------------------
# LangGraph steps (each decorated with @observe)
# ------------------------------------------------------------
@observe()
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
    prompt = """
Ты – эксперт по анализу данных. Извлеки структурированную информацию из Excel-файла.

Файл: {filename}

Листы и колонки:
{sheets_columns}

Примеры данных (первые строки):
{sample_data}

Важные бизнес-термины, найденные в данных:
{important_values}

Извлеки JSON:
{{
    "business_domain": "...",
    "bank_hint": "...",
    "project_codes": [],
    "key_entities": [],
    "history_types": [],
    "source_tables": [],
    "target_tables": [],
    "transformation_patterns": []
}}
"""
    user_content = prompt.format(
        filename=filename,
        sheets_columns="\n".join(sheets_columns),
        sample_data="\n".join(sample_data[:10]),
        important_values="\n".join(important_vals[:15])
    )
    answer = call_gigachat_for_summary(user_content)
    try:
        if "```json" in answer:
            answer = answer.split("```json")[1].split("```")[0]
        elif "```" in answer:
            answer = answer.split("```")[1].split("```")[0]
        schema = json.loads(answer)
        # Ensure key_entities is a list of strings
        if "key_entities" in schema:
            schema["key_entities"] = [
                (e if isinstance(e, str) else e.get("name") or e.get("entity") or str(e))
                for e in schema["key_entities"]
            ]
        state["schema"] = schema
        logger.info(f"Schema extracted: {schema}")
    except Exception as e:
        logger.error(f"Schema extraction failed: {e}")
        state["schema"] = {"business_domain": "не определено", "key_entities": []}
        state["validation_errors"].append(f"Schema error: {e}")
    return state

@observe()
def structural_summary(state: SummarizerState) -> SummarizerState:
    sheets = state["raw_sheets"]
    sheets_columns = []
    for sheet in sheets:
        cols = ", ".join(sheet["columns"][:20])
        sheets_columns.append(f"Лист '{sheet['sheet_name']}': {cols}")
    prompt = "Опиши структуру документа (листы и их назначение). 1-2 абзаца на русском.\n\nЛисты и колонки:\n{sheets_columns}"
    user_content = prompt.format(sheets_columns="\n".join(sheets_columns))
    answer = call_gigachat_for_summary(user_content)
    state["section_summaries"].append(answer)
    return state

@observe()
def domain_summary(state: SummarizerState) -> SummarizerState:
    schema = state.get("schema", {})
    schema_str = json.dumps(schema, ensure_ascii=False, indent=2)
    prompt = """
На основе извлечённой схемы:
{schema}

Опиши бизнес-домен, ключевые сущности, жизненный цикл данных. Обязательно упомяни:
- Банк или организацию (если есть намёк)
- Коды проектов (например, КЮЛ)
- Финансовые стандарты (МСФО, IFRS)
- Конкретные продукты (кредиты, гарантии, субсидии, продуктовые регистры)
Напиши 2 абзаца на русском.
"""
    user_content = prompt.format(schema=schema_str)
    answer = call_gigachat_for_summary(user_content)
    state["section_summaries"].append(answer)
    return state

@observe()
def synthesize(state: SummarizerState) -> SummarizerState:
    combined = "\n\n".join(state["section_summaries"])
    prompt = """
На основе следующих резюме создай **ОДИН связный абзац** (5-7 предложений) на русском языке.
Абзац должен быть максимально похож на пример ниже по стилю и детализации.

Промежуточные резюме:
{section_summaries}

Пример желаемого стиля:
"Данный файл представляет собой спецификацию маппинга «Источник-Приёмник» (Source-to-Target, S2T) для хранилища данных по корпоративному кредитованию (код проекта «КЮЛ» — Кредиты Юридическим Лицам) в рамках крупной российской банковской среды (предположительно Сбербанк). Документ определяет логику ETL-процессов для трансформации данных из исходных систем в единую аналитическую модель, охватывающую полный жизненный цикл кредитных продуктов: кредитные договоры, договоры банковской гарантии и договоры обеспечения, а также связанные с ними атрибуты, такие как контрагенты, валюты, правила расчёта процентных ставок, субсидии, метрики классификации по МСФО (IFRS 9), продуктовые регистры, а также плановые и фактические финансовые операции."

Напиши абзац, следуя этому примеру: укажи тип документа (S2T, маппинг, ETL), проект, банк, перечисли конкретные сущности и финансовые показатели.
"""
    user_content = prompt.format(section_summaries=combined)
    answer = call_gigachat_for_summary(user_content)
    state["final_summary"] = answer
    return state

def normalize_text(text: str) -> str:
    # Ensure we have a string
    if not isinstance(text, str):
        text = str(text)
    text = text.lower()
    text = re.sub(r'[^\w\s]', ' ', text)
    return ' '.join(text.split())

@observe()
def validate(state: SummarizerState) -> SummarizerState:
    final = state["final_summary"]
    schema = state.get("schema", {})
    entities = schema.get("key_entities", [])
    if entities:
        normalized_summary = normalize_text(final)
        found = False
        for entity in entities:
            # Ensure entity is a string
            if isinstance(entity, dict):
                entity_str = entity.get("name") or entity.get("entity") or str(entity)
            else:
                entity_str = str(entity)
            norm_entity = normalize_text(entity_str)
            if norm_entity in normalized_summary:
                found = True
                break
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
            logger.info("Validation passed")
    return state

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

@observe()
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
    handler = get_callback_handler()
    if handler:
        config = {"callbacks": [handler], "run_name": f"summarize_{file_hash}"}
        result = summarizer_graph.invoke(initial_state, config=config)
    else:
        result = summarizer_graph.invoke(initial_state)
    if result["validation_errors"]:
        logger.warning(f"Validation issues: {result['validation_errors']}")
    return result["final_summary"]

def summarize_file(file_hash: str, save: bool = True) -> str:
    summary = generate_summary(file_hash)
    if save:
        update_file_summary(file_hash, summary)
    return summary