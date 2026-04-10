import os
import json
import logging
import re
from typing import List, Any, Tuple, TypedDict
from dotenv import load_dotenv
from gigachat import GigaChat
from gigachat.models import Chat, Messages, MessagesRole
from langgraph.graph import StateGraph, END
from load_skills_tools import load_skills, load_tools

load_dotenv()
logger = logging.getLogger(__name__)

# GigaChat setup
GIGACHAT_CREDENTIALS = os.getenv("GIGACHAT_API_KEY") or os.getenv("GIGACHAT_EMBEDDINGS_CREDENTIALS")
if not GIGACHAT_CREDENTIALS:
    raise ValueError("Missing GigaChat credentials")

GIGACHAT_BASE_URL = os.getenv("GIGACHAT_API_URL", "https://gigachat.devices.sberbank.ru/api/v1")
VERIFY_SSL = os.getenv("GIGACHAT_VERIFY_SSL", "false").lower() == "true"
SCOPE = os.getenv("GIGACHAT_SCOPE", "GIGACHAT_API_PERS")
MODEL = os.getenv("MODEL", "GigaChat-Pro")
TIMEOUT = int(os.getenv("GIGACHAT_TIMEOUT", "120"))

giga = GigaChat(
    model=MODEL,
    credentials=GIGACHAT_CREDENTIALS,
    base_url=GIGACHAT_BASE_URL,
    verify_ssl_certs=VERIFY_SSL,
    scope=SCOPE,
    timeout=TIMEOUT,
)

# Build system prompt with skills and tools
SYSTEM_PROMPT = f"""
{load_skills()}

{load_tools()}

You are an expert in analyzing messy Excel sheet structures. Use the skills and tools above.
"""


class SheetAnalysisState(TypedDict):
    sheet_name: str
    preview_rows: List[List[Any]]
    header_start_row: int
    header_rows_count: int
    nested_structure: bool
    error: str


ANALYSIS_PROMPT = """
You are given a preview of the first {preview_rows_count} rows of a sheet named "{sheet_name}".
Each row is a list; empty cells may appear as None.

Goal: detect header_start_row, header_rows, nested.

Rules:
- header_start_row = 0 if first row has short labels (even with None for merged cells), else 1 if first row is long text.
- header_rows = 1 for single row headers, 2+ for multi‑level.
- Stop at data rows (numbers, dates, SQL, long text).

Output JSON:
{{
    "header_start_row": <int>,
    "header_rows": <int>,
    "nested": <bool>,
    "explanation": "<short>"
}}

Preview:
{preview_json}
"""


def is_long_text(value: Any) -> bool:
    if isinstance(value, str):
        return len(value) > 100 or '\n' in value
    return False


def looks_like_data(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, (int, float)):
        return True
    if isinstance(value, str):
        if value.isdigit():
            return True
        if any(keyword in value.upper() for keyword in ['SELECT', 'FROM', 'WHERE', 'JOIN']):
            return True
        if len(value) > 50:
            return True
        if re.match(r'^[A-Za-z_][A-Za-z0-9_]*$', value) and not value.isalpha():
            return True
    return False


def analyze_sheet(sheet_name: str, preview_rows: List[List[Any]], max_preview_rows: int = 10) -> Tuple[int, int, bool]:
    limited_preview = preview_rows[:max_preview_rows]
    preview_json = json.dumps(limited_preview, ensure_ascii=False, default=str)
    prompt = ANALYSIS_PROMPT.format(
        sheet_name=sheet_name,
        preview_rows_count=len(limited_preview),
        preview_json=preview_json
    )
    messages = [
        Messages(role=MessagesRole.SYSTEM, content=SYSTEM_PROMPT),
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
        start_row = result.get("header_start_row", 0)
        header_rows = result.get("header_rows", 1)
        nested = result.get("nested", header_rows >= 2)
        if start_row < 0:
            start_row = 0
        if header_rows < 0:
            header_rows = 0
        if header_rows > 5:
            header_rows = 5
        logger.info(
            f"AI decision for '{sheet_name}': start_row={start_row}, header_rows={header_rows}, nested={nested}")

        # Post‑processing overrides
        if header_rows == 2 and len(preview_rows) >= 2:
            second_row = preview_rows[1]
            non_null = [v for v in second_row if v is not None]
            if non_null:
                data_count = sum(1 for v in non_null if looks_like_data(v))
                if data_count / len(non_null) > 0.3:
                    logger.info(f"Override: second row contains data, forcing header_rows=1 for '{sheet_name}'")
                    header_rows = 1
                    nested = False

        if header_rows == 1 and len(preview_rows) >= 2 and start_row == 0:
            first_short = any(v is not None and not is_long_text(v) for v in preview_rows[0])
            second_short = any(v is not None and not is_long_text(v) for v in preview_rows[1])
            if first_short and second_short:
                second_data = any(looks_like_data(v) for v in preview_rows[1])
                if not second_data:
                    logger.info(f"Override: two label rows, setting header_rows=2 for '{sheet_name}'")
                    header_rows = 2
                    nested = True
        return start_row, header_rows, nested
    except Exception as e:
        logger.error(f"GigaChat analysis failed for '{sheet_name}': {e}")
        if len(preview_rows) >= 2:
            first_short = any(v is not None and not is_long_text(v) for v in preview_rows[0])
            second_short = any(v is not None and not is_long_text(v) for v in preview_rows[1])
            if first_short and second_short:
                return 0, 2, True
        return 0, 1, False


def analyze_node(state: SheetAnalysisState) -> dict:
    start_row, header_rows, nested = analyze_sheet(state["sheet_name"], state["preview_rows"])
    return {
        "header_start_row": start_row,
        "header_rows_count": header_rows,
        "nested_structure": nested,
        "error": ""
    }


def finalize_node(state: dict) -> dict:
    return state


builder = StateGraph(SheetAnalysisState)
builder.add_node("analyze", analyze_node)
builder.add_node("finalize", finalize_node)
builder.set_entry_point("analyze")
builder.add_edge("analyze", "finalize")
builder.add_edge("finalize", END)
graph = builder.compile()


def get_header_decision(sheet_name: str, preview_rows: List[List[Any]]) -> Tuple[int, int, bool]:
    initial_state: SheetAnalysisState = {
        "sheet_name": sheet_name,
        "preview_rows": preview_rows,
        "header_start_row": 0,
        "header_rows_count": 0,
        "nested_structure": False,
        "error": ""
    }
    result = graph.invoke(initial_state)
    return result["header_start_row"], result["header_rows_count"], result["nested_structure"]


def get_model_name() -> str:
    return MODEL