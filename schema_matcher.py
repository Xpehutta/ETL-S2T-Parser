import json
import logging
from typing import Dict, Any, List
from agent import giga
from gigachat.models import Chat, Messages, MessagesRole

logger = logging.getLogger(__name__)

TARGET_SCHEMA = {
    "database": "SQLite",
    "version": "1.0",
    "tables": [

        {
            "name": "source_tables",
            "description": "Справочник исходных таблиц",
            "columns": ["name", "description", "system_code"]
        },
        {
            "name": "target_tables",
            "description": "Целевые таблицы в хранилище",
            "columns": ["name", "description"]
        },
        {
            "name": "column_mappings",
            "description": "Правила маппинга исходных колонок в целевые",
            "columns": ["target_table_name", "target_column", "source_table_name",
                        "source_column", "transformation_rule", "data_type", "is_primary_key"]
        },
        {
            "name": "additions",
            "description": "Дополнительные правила и объекты трасформации",
            "columns": ["table_name", "table_description", "source_tables_name",
                        "sql", "description"]
        }
    ]
}

# Prompt for sheet‑to‑table matching
SHEET_MATCHING_PROMPT = """
Ты – эксперт по интеграции данных. Сопоставь каждый лист Excel (его название и колонки) с наиболее подходящей таблицей из целевой схемы.

Excel sheets data:
{excel_sheets}

Target schema tables:
{target_tables}

Для каждого листа укажи:
- наиболее вероятную целевую таблицу (или None, если нет подходящей)
- степень схожести (high, medium, low)
- краткое объяснение

Формат ответа – JSON список:
[
    {{
        "sheet_name": "название листа",
        "target_table": "имя таблицы или null",
        "similarity": "high/medium/low",
        "reason": "почему"
    }}
]
"""


def match_sheets_to_tables(excel_json: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Использует GigaChat для сопоставления листов Excel с целевыми таблицами."""
    # Подготовка данных
    sheets_data = []
    for sheet in excel_json.get("sheets", []):
        if sheet.get("skipped", False):
            continue
        sheet_name = sheet["sheet_name"]
        columns = sheet.get("columns", [])
        # Упрощаем колонки (если вложенные, берём последний уровень)
        flat_cols = []
        for col in columns:
            if isinstance(col, list):
                flat_cols.append(" > ".join(str(c) for c in col if c))
            else:
                flat_cols.append(str(col))
        sheets_data.append({
            "sheet_name": sheet_name,
            "columns": flat_cols[:20]  # ограничим кол-во колонок
        })

    target_tables = [{"name": t["name"], "description": t["description"], "columns": t["columns"]}
                     for t in TARGET_SCHEMA["tables"]]

    prompt = SHEET_MATCHING_PROMPT.format(
        excel_sheets=json.dumps(sheets_data, ensure_ascii=False, indent=2),
        target_tables=json.dumps(target_tables, ensure_ascii=False, indent=2)
    )

    messages = [
        Messages(role=MessagesRole.SYSTEM, content="Ты – аналитик данных. Отвечай только JSON."),
        Messages(role=MessagesRole.USER, content=prompt)
    ]
    try:
        response = giga.chat(Chat(messages=messages))
        answer = response.choices[0].message.content.strip()
        if "```json" in answer:
            answer = answer.split("```json")[1].split("```")[0]
        elif "```" in answer:
            answer = answer.split("```")[1].split("```")[0]
        result = json.loads(answer)
        logger.info("Sheet matching completed")
        return result
    except Exception as e:
        logger.error(f"Sheet matching failed: {e}")
        return []


# Prompt for column‑level mapping
COLUMN_MAPPING_PROMPT = """
Ты – эксперт по маппингу данных. Для таблицы "{target_table}" (колонки: {target_columns}) 
сопоставь колонки из листа Excel "{sheet_name}" (список колонок: {excel_columns}) 
с целевыми колонками. Определи, какие Excel-колонки соответствуют каким целевым колонкам.

Верни JSON-объект, где ключ – целевая колонка, значение – Excel-колонка (или null, если нет соответствия). Также добавь поле "similarity" (high/medium/low) для общего соответствия.

Пример:
{{
    "mapping": {{
        "name": "Название таблицы",
        "system_code": "Код СИ"
    }},
    "similarity": "high"
}}
"""


def map_columns_for_table(excel_json: Dict[str, Any], sheet_name: str, target_table_name: str) -> Dict[str, Any]:
    """Для конкретного листа и целевой таблицы получаем маппинг колонок."""
    # Найти лист
    sheet = None
    for s in excel_json.get("sheets", []):
        if s.get("sheet_name") == sheet_name and not s.get("skipped"):
            sheet = s
            break
    if not sheet:
        return {"error": f"Sheet '{sheet_name}' not found", "mapping": {}, "similarity": "low"}

    # Получить целевые колонки из схемы
    target_table = next((t for t in TARGET_SCHEMA["tables"] if t["name"] == target_table_name), None)
    if not target_table:
        return {"error": f"Target table '{target_table_name}' not found", "mapping": {}, "similarity": "low"}

    target_columns = target_table["columns"]
    excel_columns = []
    for col in sheet.get("columns", []):
        if isinstance(col, list):
            excel_columns.append(" > ".join(str(c) for c in col if c))
        else:
            excel_columns.append(str(col))

    prompt = COLUMN_MAPPING_PROMPT.format(
        target_table=target_table_name,
        target_columns=target_columns,
        sheet_name=sheet_name,
        excel_columns=excel_columns[:30]
    )
    messages = [
        Messages(role=MessagesRole.SYSTEM, content="Ты – эксперт по маппингу. Отвечай только JSON."),
        Messages(role=MessagesRole.USER, content=prompt)
    ]
    try:
        response = giga.chat(Chat(messages=messages))
        answer = response.choices[0].message.content.strip()
        if "```json" in answer:
            answer = answer.split("```json")[1].split("```")[0]
        elif "```" in answer:
            answer = answer.split("```")[1].split("```")[0]
        result = json.loads(answer)
        logger.info(f"Column mapping completed for {sheet_name} -> {target_table_name}")
        return result
    except Exception as e:
        logger.error(f"Column mapping failed: {e}")
        return {"mapping": {}, "similarity": "low", "error": str(e)}


def compare_with_target(excel_json: Dict[str, Any]) -> Dict[str, Any]:
    """
    Полный анализ: сначала сопоставляем листы с таблицами, затем для лучших совпадений получаем маппинг колонок.
    """
    sheet_matches = match_sheets_to_tables(excel_json)
    mapping_suggestions = []
    for match in sheet_matches:
        target_table = match.get("target_table")
        if target_table and target_table != "null":
            # Получаем маппинг колонок
            column_mapping = map_columns_for_table(excel_json, match["sheet_name"], target_table)
            mapping_suggestions.append({
                "excel_sheet": match["sheet_name"],
                "target_table": target_table,
                "similarity": match.get("similarity", "low"),
                "explanation": match.get("reason", ""),
                "column_mapping": column_mapping.get("mapping", {}),
                "mapping_similarity": column_mapping.get("similarity", "low")
            })
        else:
            mapping_suggestions.append({
                "excel_sheet": match["sheet_name"],
                "target_table": None,
                "similarity": "none",
                "explanation": match.get("reason", "No match"),
                "column_mapping": {}
            })

    # Общая оценка схожести
    score_map = {"high": 3, "medium": 2, "low": 1, "none": 0}
    total = sum(score_map.get(m["similarity"], 0) for m in mapping_suggestions)
    count = len([m for m in mapping_suggestions if m["target_table"]])
    avg_score = (total / (count * 3) * 100) if count > 0 else 0

    unmatched_excel = [m["excel_sheet"] for m in mapping_suggestions if not m["target_table"]]
    matched_tables = set(m["target_table"] for m in mapping_suggestions if m["target_table"])
    all_target_tables = set(t["name"] for t in TARGET_SCHEMA["tables"])
    unmatched_target = list(all_target_tables - matched_tables)

    return {
        "similarity_score": round(avg_score),
        "mapping_suggestions": mapping_suggestions,
        "unmatched_excel_sheets": unmatched_excel,
        "unmatched_target_tables": unmatched_target,
        "recommendations": (
            "Рекомендуется использовать column_mapping для загрузки данных. "
            "Для таблиц без маппинга потребуется ручная настройка."
        )
    }