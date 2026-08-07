"""LLM routing that directly selects tools and lazily loads their skills."""

from __future__ import annotations

import json
import logging
import re
from typing import Any, Dict, List, Literal, Mapping, Optional, Sequence, Tuple

from langchain_core.messages import AIMessage, BaseMessage, HumanMessage, SystemMessage
from langchain_core.tools import BaseTool
from langchain_core.utils.json import parse_json_markdown
from pydantic import BaseModel, ConfigDict, Field, ValidationError


logger = logging.getLogger(__name__)


SkillName = Literal[
    "SQLite SQL",
    "SQL lineage",
    "S2T-строки",
    "Путь S2T-преобразования",
    "Neo4j",
    "Excel и описания",
]

SKILL_CATALOG: Dict[str, str] = {
    "SQLite SQL": "Выполнение read-only SQL по сохранённым данным SQLite.",
    "SQL lineage": "Статический разбор и визуализация зависимостей SQL-текста.",
    "S2T-строки": "Общие ETL-строки, S2T-маппинги, additional objects, правила и агрегации s2t_transformations.",
    "Путь S2T-преобразования": (
        "Объяснение и визуализация сохранённых многошаговых source/target "
        "путей: правила, SQL, additional objects и подтверждение Neo4j."
    ),
    "Neo4j": "Графовый lineage именованных ETL-таблиц и колонок.",
    "Excel и описания": (
        "Файлы, листы, заголовки, ячейки и семантические описания."
    ),
}


class ToolRoutingError(RuntimeError):
    """Raised when the tool-router cannot produce a valid selection."""


class ToolRoute(BaseModel):
    """Strict validation schema for raw independent tool/skill selection."""

    model_config = ConfigDict(extra="forbid")

    tools: List[str] = Field(min_length=1)
    skills: List[SkillName]


_TOOL_ROUTER_PROMPT = """
Ты router read-only агента ETL/S2T Parser. Выбери компактную палитру конкретных tools,
из которой planner сможет построить и при необходимости исправить цепочку для текущего
запроса с учётом истории.
Не отвечай пользователю, не составляй аргументы tools и не вызывай tools сам.
Верни ровно один JSON-объект без Markdown, пояснений и дополнительных полей:
{{"tools":["точное_имя_tool"],"skills":["точное_имя_skill"]}}

Правила выбора:
- Возвращай небольшую палитру подходящих tools, обычно от одного до четырёх, и только
  имена из переданного каталога. Это набор возможностей для planner, а не обязательный
  список вызовов: planner сам выберет порядок и может использовать не все tools.
- current_query задаёт текущую цель. recent_history используй шире: переноси из неё
  явно установленные сущности и точные имена, ограничения пользователя, проверенные
  результаты предыдущих шагов, незавершённые части задачи и ссылки вроде «этот запрос».
  Не наследуй прошлый tool или прошлую тему механически, если они не помогают текущей цели.
- Назначение каждого tool определяй только по его description: это контракт
  инструмента. Не переноси в router правила аргументов и не выбирай по одному
  совпавшему слову без учёта действия, которое просит пользователь.
- Skills выбирай независимо по смыслу запроса, а не выводи механически из tools.
  Один универсальный tool может требовать разные skills в разных запросах.
- Можно вернуть пустой список skills, если доменные инструкции не нужны.
- Если один tool должен сначала найти имя или факт для другого, включи оба. Если узкий
  tool не покрывает важную часть условия, добавь подходящий универсальный read-only tool
  как альтернативу, чтобы planner мог самостоятельно составить запрос и проверить ответ.
  Если узкий tool полностью покрывает текущую операцию, не добавляй универсальный tool
  только «на всякий случай».
- Различай выполнение, чтение, анализ и готовую визуализацию по descriptions;
  выбирай ровно тот тип результата, который запросил пользователь.
""".strip()

_TOOL_ROUTER_REPAIR_PROMPT = """
Предыдущий ответ отклонён строгой схемой router. Исправь только формат ответа с
учётом текста ошибки ниже. Повторно выбери tools и skills по исходному payload,
и верни один нативный вызов функции select_tools_and_skills. В её аргументах
должен быть ровно один JSON-объект с двумя и только двумя ключами:
{"tools":["точное_имя_tool"],"skills":["точное_имя_skill"]}
Не добавляй description, reason, пояснения, описания skills или другие поля. Не
пиши обычный текст и не используй Markdown.

Ошибка валидации: {validation_error}
""".strip()
_ROUTE_REPAIR_TOOL_NAME = "select_tools_and_skills"


def _router_response_text(result: Any) -> str:
    content = result.content if isinstance(result, BaseMessage) else result
    if isinstance(content, str):
        return content.strip()
    if isinstance(content, list):
        parts = []
        for block in content:
            if isinstance(block, str):
                parts.append(block)
            elif isinstance(block, Mapping):
                text = block.get("text") or block.get("content")
                if text is not None:
                    parts.append(str(text))
        return "".join(parts).strip()
    return str(content or "").strip()


def _parse_router_response(result: Any) -> Mapping[str, Any]:
    raw_text = _router_response_text(result)
    if not raw_text:
        raise ToolRoutingError("Tool-router вернул пустой ответ")
    try:
        parsed = parse_json_markdown(raw_text)
    except (json.JSONDecodeError, TypeError, ValueError) as exc:
        raise ToolRoutingError(
            "Tool-router вернул невалидный JSON-текст"
        ) from exc
    if not isinstance(parsed, Mapping):
        raise ToolRoutingError("Tool-router должен вернуть один JSON-объект")
    return parsed


def _history_payload(
    history: Optional[Sequence[Mapping[str, str]]],
) -> List[Dict[str, str]]:
    return [
        {
            "role": str(item.get("role") or ""),
            "content": str(item.get("content") or ""),
        }
        for item in (history or [])[-6:]
    ]


def _compact_tool_description(tool: BaseTool) -> str:
    text = " ".join(str(tool.description or "").split())
    sentence_ends = list(re.finditer(r"[.!?](?:\s|$)", text))
    if sentence_ends:
        selected_end = sentence_ends[min(1, len(sentence_ends) - 1)].end()
        text = text[:selected_end].strip()
    if len(text) > 420:
        text = text[:419].rstrip() + "…"
    return text


def _tool_catalog(tools: Sequence[BaseTool]) -> List[Dict[str, str]]:
    return [
        {
            "name": tool.name,
            "description": _compact_tool_description(tool),
        }
        for tool in tools
    ]


def _validated_route(
    result: Any,
    available_tools: Sequence[BaseTool],
) -> ToolRoute:
    try:
        route = (
            result
            if isinstance(result, ToolRoute)
            else ToolRoute.model_validate(result)
        )
    except (TypeError, ValueError, ValidationError) as exc:
        raise ToolRoutingError(
            "Tool-router вернул невалидный JSON-маршрут"
        ) from exc

    selected_tools = list(dict.fromkeys(route.tools))
    selected_skills = list(dict.fromkeys(route.skills))
    available_names = {tool.name for tool in available_tools}
    unknown = [name for name in selected_tools if name not in available_names]
    if unknown:
        raise ToolRoutingError(
            f"Tool-router выбрал неизвестные tools: {', '.join(unknown)}"
        )
    return ToolRoute(tools=selected_tools, skills=selected_skills)


def _route_repair_tool_schema() -> Dict[str, Any]:
    return {
        "type": "function",
        "function": {
            "name": _ROUTE_REPAIR_TOOL_NAME,
            "description": (
                "Вернуть исправленный строгий маршрут tools и skills без пояснений."
            ),
            "parameters": ToolRoute.model_json_schema(),
        },
    }


def _native_repair_payload(result: Any) -> Optional[Mapping[str, Any]]:
    if not isinstance(result, AIMessage) or not result.tool_calls:
        return None
    matching_calls = [
        call
        for call in result.tool_calls
        if call.get("name") == _ROUTE_REPAIR_TOOL_NAME
    ]
    if len(matching_calls) != 1:
        raise ToolRoutingError(
            "Tool-router repair должен вернуть один select_tools_and_skills"
        )
    args = matching_calls[0].get("args")
    if not isinstance(args, Mapping):
        raise ToolRoutingError(
            "Tool-router repair вернул невалидные аргументы"
        )
    return args


def select_chat_route(
    user_query: str,
    history: Optional[List[Dict[str, str]]] = None,
    *,
    model: Any,
    available_tools: Sequence[BaseTool],
    callbacks: Optional[Sequence[Any]] = None,
) -> ToolRoute:
    """Select exact tools and request-relevant skills with one raw LLM call."""
    clean_query = str(user_query or "").strip()
    if not clean_query:
        raise ToolRoutingError("Tool-router получил пустой запрос")
    if not available_tools:
        raise ToolRoutingError("Tool-router не получил каталог tools")

    payload = {
        "current_query": clean_query,
        "recent_history": _history_payload(history),
        "available_tools": _tool_catalog(available_tools),
        "available_skills": SKILL_CATALOG,
    }
    messages = [
        SystemMessage(content=_TOOL_ROUTER_PROMPT),
        HumanMessage(content=json.dumps(payload, ensure_ascii=False)),
    ]
    config = {"callbacks": list(callbacks)} if callbacks else None
    def invoke_router(
        call_messages: Sequence[BaseMessage],
        *,
        selected_model: Any = None,
    ) -> Any:
        active_model = selected_model if selected_model is not None else model
        try:
            return (
                active_model.invoke(call_messages, config=config)
                if config is not None
                else active_model.invoke(call_messages)
            )
        except Exception as exc:
            raise ToolRoutingError(
                f"Ошибка LLM tool-router: {type(exc).__name__}"
            ) from exc

    result = invoke_router(messages)
    try:
        return _validated_route(
            _parse_router_response(result),
            available_tools,
        )
    except ToolRoutingError as first_error:
        raw_text = _router_response_text(result)
        logger.warning(
            "Tool-router response rejected; requesting one LLM repair: error=%s raw=%s",
            first_error,
            raw_text[:2000],
        )
        repair_messages: List[BaseMessage] = [
            *messages,
            AIMessage(content=raw_text),
            HumanMessage(
                content=_TOOL_ROUTER_REPAIR_PROMPT.replace(
                    "{validation_error}", str(first_error)
                )
            ),
        ]
        repair_model = model
        bind_tools = getattr(model, "bind_tools", None)
        if callable(bind_tools):
            try:
                repair_model = bind_tools(
                    [_route_repair_tool_schema()],
                    tool_choice=_ROUTE_REPAIR_TOOL_NAME,
                )
            except TypeError:
                repair_model = bind_tools([_route_repair_tool_schema()])

        repaired_result = invoke_router(
            repair_messages,
            selected_model=repair_model,
        )
        try:
            native_payload = _native_repair_payload(repaired_result)
            return _validated_route(
                (
                    native_payload
                    if native_payload is not None
                    else _parse_router_response(repaired_result)
                ),
                available_tools,
            )
        except ToolRoutingError:
            logger.warning(
                "Tool-router repair rejected: raw=%s",
                _router_response_text(repaired_result)[:2000],
            )
            raise


__all__ = [
    "SKILL_CATALOG",
    "ToolRoute",
    "ToolRoutingError",
    "select_chat_route",
]
