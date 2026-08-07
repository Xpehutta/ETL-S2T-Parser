from __future__ import annotations

import json
import logging
import os
import re
from typing import Any, Dict, List, Optional, Tuple

from dotenv import load_dotenv
from langchain_core.messages import BaseMessage, HumanMessage, SystemMessage
from langchain_core.output_parsers import StrOutputParser
from langchain_core.runnables import RunnableLambda
from langchain_core.utils.json import parse_json_markdown

from .chat_graph import run_agent_graph
from .header_classifier import predict_header_row
from .llm_factory import create_chat_model, get_chat_model_name
from .tools.routing import select_chat_route as _select_chat_route
from .tools import (
    get_sqlite_schema_cheatsheet,
    get_tools,
    get_tools_for_names,
    load_chat_agent_context,
    load_skills,
)


try:
    from langfuse import observe
    from .observability import get_callback_handler

    LANGFUSE_AVAILABLE = True
except ImportError:
    LANGFUSE_AVAILABLE = False

    def observe(*args: Any, **kwargs: Any):
        def decorator(func):
            return func

        return decorator


load_dotenv()

logger = logging.getLogger(__name__)


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)

    if raw is None or raw.strip() == "":
        return default

    try:
        return int(raw)
    except ValueError as exc:
        raise ValueError(
            f"{name} must be an integer, got {raw!r}"
        ) from exc


MODEL = get_chat_model_name()

TIMEOUT = _env_int(
    "LLM_TIMEOUT",
    _env_int("GIGACHAT_TIMEOUT", 120),
)

HEADER_TIMEOUT = _env_int(
    "LLM_HEADER_TIMEOUT",
    _env_int("GIGACHAT_HEADER_TIMEOUT", 20),
)

HEADER_RETRY_ATTEMPTS = _env_int(
    "LLM_HEADER_RETRY_ATTEMPTS",
    _env_int("GIGACHAT_HEADER_RETRY_ATTEMPTS", 1),
)

HEADER_PREVIEW_ROWS = _env_int(
    "LLM_HEADER_PREVIEW_ROWS",
    _env_int("GIGACHAT_HEADER_PREVIEW_ROWS", 4),
)


chat_model = create_chat_model(timeout=TIMEOUT)

header_chat_model = create_chat_model(
    timeout=HEADER_TIMEOUT
)

header_chat_model_with_retry = header_chat_model.with_retry(
    stop_after_attempt=max(1, HEADER_RETRY_ATTEMPTS)
)


CHAT_AGENT_CONTEXT = load_chat_agent_context()
SQLITE_SCHEMA_CONTEXT = get_sqlite_schema_cheatsheet()

# Стандартный ToolNode работает со списком BaseTool.
TOOLS = get_tools()


SYSTEM_PROMPT = """
Ты эксперт по разбору неаккуратных и сложных структур Excel-таблиц.
Определи только положение и структуру заголовка по переданному предпросмотру.
""".strip()


ANALYSIS_PROMPT = """
Ниже приведён предпросмотр первых {preview_rows_count} строк листа
"{sheet_name}".

Каждая строка представлена списком. Пустые ячейки могут приходить как None.

Задача: определить:
- header_start_row;
- header_rows;
- nested.

Правила:
- header_start_row = 0, если первая строка содержит короткие подписи,
  в том числе None из-за объединённых ячеек;
- header_start_row = 1, если первая строка выглядит как длинный текст;
- header_rows = 1 для однострочного заголовка;
- header_rows = 2 и больше для многоуровневого заголовка;
- не включай в заголовок строки данных: числа, даты, SQL и длинный текст.

Верни только JSON:

{{
  "header_start_row": <int>,
  "header_rows": <int>,
  "nested": <bool>,
  "explanation": "<string>"
}}

Предпросмотр:
{preview_json}
""".strip()


def _header_completion_messages(
    prompt_vars: Dict[str, str],
) -> List[BaseMessage]:
    return [
        SystemMessage(
            content=prompt_vars["system_content"]
        ),
        HumanMessage(
            content=prompt_vars["user_content"]
        ),
    ]


_header_completion_chain = (
    RunnableLambda(_header_completion_messages)
    | header_chat_model_with_retry
    | StrOutputParser()
)


def call_header_model_with_retry(
    system_content: str,
    user_content: str,
) -> str:
    return _header_completion_chain.invoke(
        {
            "system_content": system_content,
            "user_content": user_content,
        }
    ).strip()


def safe_extract_json(text: str) -> str:
    clean_text = text.strip()

    match = re.search(
        r"```(?:json)?\s*([\s\S]*?)\s*```",
        clean_text,
        re.IGNORECASE,
    )

    if match:
        return match.group(1).strip()

    object_start = clean_text.find("{")
    object_end = clean_text.rfind("}")

    if object_start != -1 and object_end > object_start:
        return clean_text[object_start : object_end + 1]

    array_start = clean_text.find("[")
    array_end = clean_text.rfind("]")

    if array_start != -1 and array_end > array_start:
        return clean_text[array_start : array_end + 1]

    return clean_text


def _parse_header_response(answer: str) -> Dict[str, Any]:
    try:
        result = parse_json_markdown(answer)
    except (json.JSONDecodeError, ValueError, TypeError):
        result = json.loads(safe_extract_json(answer))

    if not isinstance(result, dict):
        raise ValueError(
            "Header model response must be a JSON object"
        )

    return result


@observe()
def get_header_decision(
    sheet_name: str,
    preview_rows: List[List[Any]],
) -> Tuple[int, int, bool]:
    """
    Определить начало, глубину и вложенность заголовка Excel-листа.
    """
    preview_limit = max(1, HEADER_PREVIEW_ROWS)
    limited_preview = preview_rows[:preview_limit]

    try:
        start_row = predict_header_row(preview_rows)
        logger.info(
            "CatBoost header decision for %r: start_row=%s",
            sheet_name,
            start_row,
        )
        return start_row, 1, False
    except Exception:
        logger.exception(
            "CatBoost header analysis failed for %r; using LLM",
            sheet_name,
        )

    preview_json = json.dumps(
        limited_preview,
        ensure_ascii=False,
        default=str,
    )

    user_prompt = ANALYSIS_PROMPT.format(
        sheet_name=sheet_name,
        preview_rows_count=len(limited_preview),
        preview_json=preview_json,
    )

    try:
        answer = call_header_model_with_retry(
            SYSTEM_PROMPT,
            user_prompt,
        )
        result = _parse_header_response(answer)
        start_row = max(0, int(result.get("header_start_row", 0)))
        header_rows = max(1, min(int(result.get("header_rows", 1)), 5))
        nested = bool(result.get("nested", header_rows >= 2))
    except Exception:
        logger.exception("LLM header analysis failed for %r", sheet_name)
        raise

    logger.info(
        "Header decision for %r: start_row=%s, header_rows=%s, nested=%s",
        sheet_name,
        start_row,
        header_rows,
        nested,
    )
    return start_row, header_rows, nested


def get_model_name() -> str:
    return MODEL


def _get_langfuse_callbacks() -> List[Any]:
    if not LANGFUSE_AVAILABLE:
        return []

    try:
        callback = get_callback_handler()
    except Exception:
        logger.exception(
            "Failed to create Langfuse callback handler"
        )
        return []

    return [callback] if callback is not None else []


def agent_chat(
    user_query: str,
    max_steps: int = 5,
    file_id: Optional[int] = None,
    history: Optional[List[Dict[str, str]]] = None,
    session_id: Optional[str] = None,
    user_id: Optional[str] = None,
) -> str:
    """
    Запустить read-only LangGraph-агента с нативным tool calling.
    """
    clean_query = user_query.strip()

    if not clean_query:
        return "Запрос не должен быть пустым."

    active_file_id = (
        int(file_id)
        if file_id is not None
        else None
    )

    callbacks = _get_langfuse_callbacks()
    available_tools = get_tools()
    route = _select_chat_route(
        clean_query,
        history,
        model=chat_model,
        available_tools=available_tools,
        callbacks=callbacks,
    )
    selected_tools = get_tools_for_names(route.tools)
    selected_skills = load_skills(tuple(route.skills))

    logger.info(
        "Chat routed tools=%s skills=%s",
        [tool.name for tool in selected_tools],
        route.skills,
    )

    system_prompt = f"""
{CHAT_AGENT_CONTEXT}

Навыки:
{selected_skills}

{SQLITE_SCHEMA_CONTEXT}
""".strip()

    trace_metadata: Dict[str, Any] = {}

    if active_file_id is not None:
        trace_metadata["file_id"] = active_file_id

    return run_agent_graph(
        user_query=clean_query,
        system_prompt=system_prompt,
        model=chat_model,
        tools=selected_tools,
        max_steps=max_steps,
        history=history,
        file_id=active_file_id,
        session_id=session_id,
        user_id=user_id,
        callbacks=callbacks,
        trace_tags=["chat"],
        trace_metadata=trace_metadata,
    )
