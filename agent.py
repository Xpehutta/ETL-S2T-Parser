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

try:
    from langfuse import observe, get_client
    from langfuse.langchain import CallbackHandler
    LANGFUSE_AVAILABLE = True
except ImportError:
    LANGFUSE_AVAILABLE = False
    def observe(*args, **kwargs):
        def decorator(func): return func
        return decorator

load_dotenv()
logger = logging.getLogger(__name__)

GIGACHAT_CREDENTIALS = os.getenv("GIGACHAT_API_KEY") or os.getenv("GIGACHAT_EMBEDDINGS_CREDENTIALS")
if not GIGACHAT_CREDENTIALS:
    raise ValueError("Missing GigaChat credentials")

GIGACHAT_BASE_URL = os.getenv("GIGACHAT_API_URL", "https://gigachat.devices.sberbank.ru/api/v1")
VERIFY_SSL = os.getenv("GIGACHAT_VERIFY_SSL", "false").lower() == "true"
SCOPE = os.getenv("GIGACHAT_SCOPE", "GIGACHAT_API_PERS")
MODEL = os.getenv("MODEL", "GigaChat-Pro")
TIMEOUT = int(os.getenv("GIGACHAT_TIMEOUT", "120"))

giga = GigaChat(
    model=MODEL, credentials=GIGACHAT_CREDENTIALS, base_url=GIGACHAT_BASE_URL,
    verify_ssl_certs=VERIFY_SSL, scope=SCOPE, timeout=TIMEOUT
)

SYSTEM_PROMPT = f"""{load_skills()}\n{load_tools()}\nYou are an expert in analyzing messy Excel sheet structures. Use the skills and tools above."""

class SheetAnalysisState(TypedDict):
    sheet_name: str
    preview_rows: List[List[Any]]
    header_start_row: int
    header_rows_count: int
    nested_structure: bool
    error: str

ANALYSIS_PROMPT = """You are given a preview of the first {preview_rows_count} rows of a sheet named "{sheet_name}".
Each row is a list; empty cells may appear as None.
Goal: detect header_start_row, header_rows, nested.
Rules:
- header_start_row = 0 if first row has short labels (even with None for merged cells), else 1 if first row is long text.
- header_rows = 1 for single row headers, 2+ for multi‑level.
- Stop at data rows (numbers, dates, SQL, long text).
Output JSON:
{{"header_start_row": <int>, "header_rows": <int>, "nested": <bool>, "explanation": "<string>"}}
Preview:
{preview_json}
"""

def safe_extract_json(text: str) -> str:
    text = text.strip()
    match = re.search(r'```(?:json)?\s*([\s\S]*?)\s*```', text, re.IGNORECASE)
    return match.group(1).strip() if match else text

def call_gigachat_with_retry(system_content: str, user_content: str, retries: int = 3) -> str:
    for attempt in range(retries):
        try:
            messages = [
                Messages(role=MessagesRole.SYSTEM, content=system_content),
                Messages(role=MessagesRole.USER, content=user_content)
            ]
            response = giga.chat(Chat(messages=messages))
            return response.choices[0].message.content.strip()
        except Exception as e:
            logger.warning(f"GigaChat attempt {attempt+1}/{retries} failed: {e}")
            if attempt == retries - 1: raise

@observe(as_type="generation", capture_input=True, capture_output=True)
def call_gigachat(system_content: str, user_content: str) -> str:
    answer = call_gigachat_with_retry(system_content, user_content)
    if LANGFUSE_AVAILABLE:
        try:
            from langfuse import get_current_observation
            current_obs = get_current_observation()
            if current_obs: current_obs.update(model=MODEL)
        except Exception: pass
    return answer

def is_long_text(value: Any) -> bool:
    if isinstance(value, str): return len(value) > 100 or '\n' in value
    return False

def looks_like_data(value: Any) -> bool:
    if value is None: return False
    if isinstance(value, (int, float)): return True
    if isinstance(value, str):
        if value.isdigit(): return True
        if any(k in value.upper() for k in ['SELECT', 'FROM', 'WHERE', 'JOIN']): return True
        if len(value) > 50: return True
        if re.match(r'^[A-Za-z_][A-Za-z0-9_]*$', value) and not value.isalpha(): return True
    return False

@observe()
def analyze_sheet(sheet_name: str, preview_rows: List[List[Any]], max_preview_rows: int = 10) -> Tuple[int, int, bool]:
    limited_preview = preview_rows[:max_preview_rows]
    preview_json = json.dumps(limited_preview, ensure_ascii=False, default=str)
    user_prompt = ANALYSIS_PROMPT.format(sheet_name=sheet_name, preview_rows_count=len(limited_preview), preview_json=preview_json)
    answer = call_gigachat(SYSTEM_PROMPT, user_prompt)

    try:
        result = json.loads(safe_extract_json(answer))
        start_row = max(0, result.get("header_start_row", 0))
        header_rows = max(1, min(result.get("header_rows", 1), 5))
        nested = header_rows >= 2
        logger.info(f"AI decision for '{sheet_name}': start_row={start_row}, header_rows={header_rows}, nested={nested}")

        if header_rows == 2 and len(preview_rows) >= 2:
            second_row = [v for v in preview_rows[1] if v is not None]
            if second_row and sum(1 for v in second_row if looks_like_data(v)) / len(second_row) > 0.3:
                header_rows, nested = 1, False

        if header_rows == 1 and len(preview_rows) >= 2 and start_row == 0:
            first_short = any(v is not None and not is_long_text(v) for v in preview_rows[0])
            second_short = any(v is not None and not is_long_text(v) for v in preview_rows[1])
            if first_short and second_short and not any(looks_like_data(v) for v in preview_rows[1]):
                header_rows, nested = 2, True
        return start_row, header_rows, nested
    except Exception as e:
        logger.error(f"GigaChat analysis failed for '{sheet_name}': {e}")
        if len(preview_rows) >= 2:
            f1 = any(v is not None and not is_long_text(v) for v in preview_rows[0])
            f2 = any(v is not None and not is_long_text(v) for v in preview_rows[1])
            if f1 and f2: return 0, 2, True
        return 0, 1, False

@observe()
def analyze_node(state: SheetAnalysisState) -> dict:
    start_row, header_rows, nested = analyze_sheet(state["sheet_name"], state["preview_rows"])
    return {"header_start_row": start_row, "header_rows_count": header_rows, "nested_structure": nested, "error": ""}

def finalize_node(state: dict) -> dict: return state

builder = StateGraph(SheetAnalysisState)
builder.add_node("analyze", analyze_node)
builder.add_node("finalize", finalize_node)
builder.set_entry_point("analyze")
builder.add_edge("analyze", "finalize")
builder.add_edge("finalize", END)
graph = builder.compile()

def get_langfuse_callback():
    if LANGFUSE_AVAILABLE:
        try: return CallbackHandler()
        except Exception as e: logger.warning(f"Failed to create Langfuse callback: {e}")
    return None

def get_header_decision(sheet_name: str, preview_rows: List[List[Any]]) -> Tuple[int, int, bool]:
    initial_state: SheetAnalysisState = {
        "sheet_name": sheet_name, "preview_rows": preview_rows,
        "header_start_row": 0, "header_rows_count": 0, "nested_structure": False, "error": ""
    }
    callback = get_langfuse_callback()
    config = {"callbacks": [callback], "run_name": f"header_decision_{sheet_name}"} if callback else {}
    result = graph.invoke(initial_state, config=config)
    return result["header_start_row"], result["header_rows_count"], result["nested_structure"]

def get_model_name() -> str: return MODEL