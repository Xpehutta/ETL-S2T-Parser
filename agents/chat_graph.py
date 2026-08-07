"""LangGraph runtime for the read-only chat agent.

Architecture:

    planner (native tool calling)
        ├─ tool_calls -> ToolNode -> observer (plain text) -> planner
        └─ no tool_calls -> responder -> END

Raw ToolMessage content is visible to the observer, subsequent planner calls
and responder, so a later tool can use exact values returned by an earlier one.
The planner also receives compact plain-text observations and produces a
bounded handoff when no more tools are needed. The responder uses that handoff
together with exact tool outputs, without duplicate observer messages.
"""

from __future__ import annotations

import json
import logging
import re
from typing import Annotated, Any, Dict, List, Literal, Mapping, Optional, Sequence, TypedDict

from langchain_core.messages import (
    AIMessage,
    BaseMessage,
    HumanMessage,
    SystemMessage,
    ToolMessage,
)
from langchain_core.tools import BaseTool
from langgraph.graph import END, START, StateGraph
from langgraph.graph.message import add_messages
from langgraph.prebuilt import ToolNode
from pydantic import BaseModel, ConfigDict, Field

from .observability import get_callback_handler, langfuse_trace_context

logger = logging.getLogger(__name__)

_VISUALIZATION_URL = re.compile(
    r"^/exports/(?:sql-lineage|s2t-graphs)/[A-Za-z0-9_.-]+\.html$"
)
_S2T_GRAPH_DATA_URL = re.compile(
    r"^/exports/s2t-graphs/[A-Za-z0-9_.-]+\.json$"
)
_OBSERVATION_SUMMARY_MAX_CHARS = 1200
_OBSERVATION_FACT_MAX_CHARS = 300
_OBSERVATION_FACTS_MAX_COUNT = 8
_OBSERVATION_LIMITATIONS_MAX_COUNT = 4
_PLANNER_HANDOFF_MAX_CHARS = 12000
_COMPLETION_AUDIT_PROMPT = """
Аудит завершения planner. Ещё раз сопоставь исходный запрос пользователя со всеми
фактически завершёнными ToolMessage. Проверь каждую запрошенную часть отдельно.
Считай часть подтверждённой только завершённым ToolMessage инструмента, description
которого непосредственно покрывает эту операцию. Результат предварительного tool,
включая найденное имя, агрегированные счётчики или данные для аргументов следующего
шага, не доказывает, что следующий шаг уже выполнен. Выжимка observer также не
является отдельным инструментальным результатом.
Отдельно сравни явные числовые ограничения исходного запроса с фактическими
аргументами и результатом tool. Если пользователь указал N, а tool получил другой
limit, количество или глубину, задача не завершена: повтори подходящий tool с N.
Если хотя бы одна часть требует доступного инструмента и ещё не подтверждена его
результатом, вызови этот tool сейчас: ответ без tool_calls завершит весь граф.
Если все части уже подтверждены, верни компактную самодостаточную выжимку для
responder без обещаний будущих действий. Не повторяй успешный одинаковый вызов без
новой причины и не выдумывай отсутствующие значения.
""".strip()
_COMPLETION_AUDIT_RETRY_PROMPT = """
Повтори аудит в последний раз. Предыдущий ответ был обычным текстом и поэтому не
выполнил ни одного нового действия. Если ты установил, что часть исходного запроса
ещё не подтверждена и для неё есть доступный tool, не описывай намерение и не пиши
«вызываю»: верни нативный tool_call прямо сейчас. Обычный текст допустим только если
каждая часть запроса уже подтверждена фактическими ToolMessage.
""".strip()
_BOUNDED_ARGUMENT_NAMES = frozenset({"limit", "preview_limit", "max_depth"})


def _completed_tool_names(messages: Sequence[BaseMessage]) -> List[str]:
    return [
        str(message.name or "unknown_tool")
        for message in messages
        if isinstance(message, ToolMessage)
    ]


def _enforce_single_numeric_constraint(
    reply: AIMessage,
    user_query: str,
) -> AIMessage:
    """Copy one unambiguous numeric request into one bounded tool argument."""
    requested_numbers = re.findall(
        r"(?<![\w.])([1-9]\d{0,3})(?![\w.])",
        str(user_query or ""),
    )
    if len(requested_numbers) != 1:
        return reply

    bounded_arguments: List[tuple[int, str]] = []
    for call_index, call in enumerate(reply.tool_calls):
        args = call.get("args") or {}
        if not isinstance(args, Mapping):
            continue
        for argument_name in _BOUNDED_ARGUMENT_NAMES.intersection(args.keys()):
            bounded_arguments.append((call_index, argument_name))
    if len(bounded_arguments) != 1:
        return reply

    requested_value = int(requested_numbers[0])
    call_index, argument_name = bounded_arguments[0]
    current_value = reply.tool_calls[call_index].get("args", {}).get(argument_name)
    if current_value == requested_value:
        return reply

    corrected_calls = [dict(call) for call in reply.tool_calls]
    corrected_args = dict(corrected_calls[call_index].get("args") or {})
    corrected_args[argument_name] = requested_value
    corrected_calls[call_index]["args"] = corrected_args
    logger.info(
        "Planner numeric contract corrected %s from %r to %s",
        argument_name,
        current_value,
        requested_value,
    )
    return reply.model_copy(update={"tool_calls": corrected_calls})


class ChatHistoryMessage(TypedDict):
    role: Literal["user", "assistant"]
    content: str


class Observation(BaseModel):
    """Structured reflection over the latest tool execution result."""

    model_config = ConfigDict(extra="forbid")

    summary: str = Field(
        description=(
            "Краткий фактический вывод из последнего результата инструмента. "
            "Не добавляй факты, которых нет в результате."
        )
    )
    has_error: bool = Field(
        default=False,
        description="Есть ли в результате ошибка выполнения или некорректные данные.",
    )
    important_facts: List[str] = Field(
        default_factory=list,
        description="Факты из результата, важные для следующего шага planner.",
    )
    limitations: List[str] = Field(
        default_factory=list,
        description=(
            "Ограничения, неоднозначности и непроверенные предположения результата. "
            "Не выбирай следующий инструмент."
        ),
    )


class AgentGraphState(TypedDict):
    messages: Annotated[List[BaseMessage], add_messages]
    system_prompt: str
    planner_message: Optional[AIMessage]
    observations: List[Observation]
    tool_steps: int
    max_steps: int
    active_file_id: Optional[int]


def _message_content_text(content: Any) -> str:
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts: List[str] = []
        for block in content:
            if isinstance(block, str):
                parts.append(block)
            elif isinstance(block, dict):
                if "text" in block:
                    parts.append(str(block["text"]))
                elif block.get("type") == "text" and "content" in block:
                    parts.append(str(block["content"]))
        if parts:
            return "".join(parts)
    return str(content)


def _message_text(message: BaseMessage) -> str:
    return _message_content_text(message.content).strip()


def _normalize_tools(
    tools: Mapping[str, BaseTool] | Sequence[BaseTool],
) -> List[BaseTool]:
    if isinstance(tools, Mapping):
        tool_list = list(tools.values())
    else:
        tool_list = list(tools)

    names = [tool.name for tool in tool_list]
    duplicate_names = sorted({name for name in names if names.count(name) > 1})
    if duplicate_names:
        raise ValueError(
            "Имена инструментов должны быть уникальными: "
            + ", ".join(duplicate_names)
        )

    return tool_list


def _history_messages(
    history: Optional[List[ChatHistoryMessage]],
) -> List[BaseMessage]:
    result: List[BaseMessage] = []
    for item in history or []:
        if item["role"] == "user":
            result.append(HumanMessage(content=item["content"]))
        else:
            result.append(AIMessage(content=item["content"]))
    return result


def _clip_text(value: Any, max_chars: int) -> str:
    text = str(value or "").strip()
    if len(text) <= max_chars:
        return text
    return text[: max_chars - 1].rstrip() + "…"


def _compact_observation(observation: Observation) -> Observation:
    return Observation(
        summary=_clip_text(
            observation.summary,
            _OBSERVATION_SUMMARY_MAX_CHARS,
        ),
        has_error=observation.has_error,
        important_facts=[
            _clip_text(item, _OBSERVATION_FACT_MAX_CHARS)
            for item in observation.important_facts[
                :_OBSERVATION_FACTS_MAX_COUNT
            ]
        ],
        limitations=[
            _clip_text(item, _OBSERVATION_FACT_MAX_CHARS)
            for item in observation.limitations[
                :_OBSERVATION_LIMITATIONS_MAX_COUNT
            ]
        ],
    )


def _runtime_context(
    state: AgentGraphState,
    *,
    include_observations: bool = True,
) -> Optional[str]:
    parts: List[str] = []

    active_file_id = state.get("active_file_id")
    if active_file_id is not None:
        parts.append(
            f"Контекст UI: активный файл имеет file_id={active_file_id}. "
            "Используй это значение в аргументах инструментов, работающих "
            "с текущим файлом. Не применяй его к глобальной таблице "
            "s2t_transformations и её list/search/summarize tools."
        )

    if include_observations:
        for index, observation in enumerate(
            state.get("observations") or [],
            start=1,
        ):
            observation_parts = [
                f"Выжимка observer для шага {index}:\n{observation.summary}"
            ]
            if observation.important_facts:
                observation_parts.append(
                    "Важные факты:\n- "
                    + "\n- ".join(observation.important_facts)
                )
            if observation.limitations:
                observation_parts.append(
                    "Ограничения и неоднозначности:\n- "
                    + "\n- ".join(observation.limitations)
                )
            if observation.has_error:
                observation_parts.append(
                    "Этот инструментальный шаг содержал ошибку. Реши, нужно ли "
                    "исправить аргументы, выбрать другой инструмент или завершить "
                    "работу с честным указанием ограничения."
                )
            parts.append("\n".join(observation_parts))

    if not parts:
        return None

    return "\n\n".join(parts)


def _planner_instruction(available_tool_names: Sequence[str]) -> str:
    available = ", ".join(available_tool_names) or "нет"
    return (
        "Ты planner read-only агента. Выбери tool call либо не вызывай tool, "
        "если проверенных фактов уже достаточно. Не пиши окончательный ответ "
        "и не повторяй уже успешный одинаковый вызов без новой причины. "
        f"Используй только доступные tools: {available}. Перед вызовом читай "
        "description и схему аргументов выбранного tool: они являются его "
        "контрактом. Не переноси правила и аргументы одного tool на другой, не "
        "выдумывай значения. Сохраняй явные числовые ограничения пользователя: "
        "если запрос содержит максимум, глубину или количество N и tool имеет "
        "соответствующий аргумент, передай ровно N, не заменяя его значением по "
        "умолчанию или границей диапазона. Не описывай будущий вызов словами: если для "
        "незавершённой части запроса нужен доступный tool, верни его tool_call "
        "в этом же сообщении. Текст о намерении вызвать tool без tool_call не "
        "считается выполнением шага. Если следующий tool не нужен, верни не "
        "пользовательский ответ, а компактную самодостаточную выжимку для "
        "responder: сохрани точные имена, числа, результаты, ссылки и ограничения "
        "из всех выжимок observer, убрав повторы и служебные детали. В истории "
        "находятся реальные завершённые ToolMessage: используй их точные значения "
        "для аргументов следующего шага. Если предыдущий ToolMessage или observer "
        "сообщает об ошибке, не считай задачу выполненной: выясни причину, исправь "
        "аргументы и снова вызови подходящий доступный tool. Повтор того же tool "
        "после ошибки разрешён; те же args повторяй только при явно временном сбое."
    )


def _planner_messages(
    state: AgentGraphState,
    available_tool_names: Sequence[str] = (),
) -> List[BaseMessage]:
    limit_reached = state["tool_steps"] >= state["max_steps"]
    planner_instruction = _planner_instruction(available_tool_names)

    if limit_reached:
        planner_instruction += (
            "\n\nЛимит инструментальных шагов исчерпан. "
            "Не вызывай инструменты; верни обычное сообщение без tool_calls, "
            "чтобы граф перешёл к responder."
        )

    system_parts = [state["system_prompt"].strip(), planner_instruction]
    runtime_context = _runtime_context(state)
    if runtime_context is not None:
        system_parts.append(runtime_context)

    messages: List[BaseMessage] = [
        SystemMessage(content="\n\n".join(system_parts))
    ]

    messages.extend(state["messages"])
    return messages


def _latest_tool_exchange(
    messages: Sequence[BaseMessage],
) -> tuple[AIMessage, List[ToolMessage]]:
    tool_results: List[ToolMessage] = []

    for message in reversed(messages):
        if isinstance(message, ToolMessage):
            tool_results.append(message)
            continue

        if isinstance(message, AIMessage) and message.tool_calls:
            tool_results.reverse()
            if not tool_results:
                raise RuntimeError(
                    "После tool call отсутствует соответствующий ToolMessage."
                )
            return message, tool_results

    raise RuntimeError("В истории не найден последний инструментальный обмен.")


def _last_user_query(messages: Sequence[BaseMessage]) -> str:
    for message in reversed(messages):
        if isinstance(message, HumanMessage):
            return _message_text(message)
    return ""


def _tool_message_payload(message: ToolMessage) -> Dict[str, Any]:
    return {
        "name": message.name,
        "tool_call_id": message.tool_call_id,
        "content": message.content,
        "status": getattr(message, "status", None),
        "is_error": _tool_message_has_error(message),
    }


def _tool_message_has_error(message: ToolMessage) -> bool:
    if getattr(message, "status", None) == "error":
        return True

    content = message.content
    if isinstance(content, dict):
        payload = content
    else:
        try:
            payload = json.loads(_message_content_text(content))
        except (json.JSONDecodeError, TypeError):
            return False

    return isinstance(payload, dict) and bool(payload.get("error"))


def _visualization_urls(messages: Sequence[BaseMessage]) -> List[str]:
    urls: List[str] = []
    for message in messages:
        if not isinstance(message, ToolMessage):
            continue
        try:
            payload = json.loads(_message_content_text(message.content))
        except (json.JSONDecodeError, TypeError):
            continue
        if not isinstance(payload, dict):
            continue
        url = payload.get("visualization_url")
        if (
            isinstance(url, str)
            and _VISUALIZATION_URL.fullmatch(url)
            and url not in urls
        ):
            urls.append(url)
    return urls


def _s2t_graph_data_urls(messages: Sequence[BaseMessage]) -> List[str]:
    urls: List[str] = []
    for message in messages:
        if not isinstance(message, ToolMessage):
            continue
        try:
            payload = json.loads(_message_content_text(message.content))
        except (json.JSONDecodeError, TypeError):
            continue
        if not isinstance(payload, dict):
            continue
        url = payload.get("data_url")
        if (
            isinstance(url, str)
            and _S2T_GRAPH_DATA_URL.fullmatch(url)
            and url not in urls
        ):
            urls.append(url)
    return urls


def _fallback_observation(
    tool_call_message: AIMessage,
    tool_results: Sequence[ToolMessage],
    error: Exception,
) -> Observation:
    raw_output = getattr(error, "llm_output", None)
    if raw_output:
        return Observation(summary=str(raw_output))

    names = [call.get("name", "unknown_tool") for call in tool_call_message.tool_calls]
    result_preview = "\n".join(
        f"{message.name or 'unknown_tool'}: {_message_content_text(message.content)[:1500]}"
        for message in tool_results
    )

    return Observation(
        summary=(
            "Observer не смог получить текстовую выжимку. "
            f"Выполнены инструменты: {', '.join(names)}. "
            f"Сырой результат:\n{result_preview}"
        ),
        has_error=True,
        important_facts=[],
        limitations=[f"Ошибка observer: {type(error).__name__}"],
    )


def build_agent_graph(
    model: Any,
    tools: Mapping[str, BaseTool] | Sequence[BaseTool],
):
    """Build the planner -> tools -> observer -> planner graph."""
    tool_list = _normalize_tools(tools)
    tool_names = tuple(tool.name for tool in tool_list)
    planner_model = model.bind_tools(tool_list)
    tool_node = ToolNode(tool_list, handle_tool_errors=True)

    def planner(state: AgentGraphState) -> Dict[str, Any]:
        limit_reached = state["tool_steps"] >= state["max_steps"]
        selected_model = model if limit_reached else planner_model
        planner_messages = _planner_messages(state, tool_names)

        try:
            reply = selected_model.invoke(planner_messages)
        except Exception as exc:
            logger.exception("LLM error in planner")
            # A plain message routes to responder, which will produce the user
            # facing error rather than exposing an internal graph exception.
            reply = AIMessage(content=f"Planner error: {type(exc).__name__}")

        if not isinstance(reply, AIMessage):
            reply = AIMessage(content=_message_content_text(reply))

        if not limit_reached and reply.tool_calls:
            reply = _enforce_single_numeric_constraint(
                reply,
                _last_user_query(state["messages"]),
            )

        if (
            not limit_reached
            and state["tool_steps"] > 0
            and not reply.tool_calls
        ):
            try:
                audit_messages: List[BaseMessage] = [
                    *planner_messages,
                    AIMessage(content=_message_text(reply)),
                    HumanMessage(
                        content=(
                            f"{_COMPLETION_AUDIT_PROMPT}\n\n"
                            "Исходный пользовательский запрос: "
                            f"{_last_user_query(state['messages'])!r}.\n"
                            "Фактически завершённые tools: "
                            f"{_completed_tool_names(state['messages']) or ['нет']}.\n"
                            f"Доступные tools: {list(tool_names) or ['нет']}."
                        )
                    ),
                ]
                last_audited_reply: Optional[AIMessage] = None
                for attempt in range(2):
                    audited_reply = planner_model.invoke(audit_messages)
                    if not isinstance(audited_reply, AIMessage):
                        audited_reply = AIMessage(
                            content=_message_content_text(audited_reply)
                        )
                    last_audited_reply = audited_reply
                    logger.info(
                        "Planner completion audit %s after %s tool step(s): "
                        "tool_calls=%s content=%s",
                        attempt + 1,
                        state["tool_steps"],
                        [call.get("name") for call in audited_reply.tool_calls],
                        _message_text(audited_reply)[:1000],
                    )
                    if audited_reply.tool_calls:
                        reply = audited_reply
                        break
                    if attempt == 0:
                        audit_messages.extend(
                            [
                                audited_reply,
                                HumanMessage(
                                    content=_COMPLETION_AUDIT_RETRY_PROMPT
                                ),
                            ]
                        )
                if not reply.tool_calls and last_audited_reply is not None:
                    reply = last_audited_reply
            except Exception:
                logger.exception(
                    "LLM error in planner completion audit; using initial decision"
                )

        logger.info(
            "Agent planner after %s tool step(s): tool_calls=%s content=%s",
            state["tool_steps"],
            [call.get("name") for call in reply.tool_calls],
            _message_text(reply)[:1000],
        )

        return {"planner_message": reply}

    def prepare_tool_call(state: AgentGraphState) -> Dict[str, Any]:
        planner_message = state.get("planner_message")
        if planner_message is None or not planner_message.tool_calls:
            raise RuntimeError("Planner не выбрал инструмент.")

        return {
            "messages": [planner_message],
            "planner_message": None,
        }

    def execute_tools(state: AgentGraphState) -> Dict[str, Any]:
        last_message = state["messages"][-1]
        if not isinstance(last_message, AIMessage) or not last_message.tool_calls:
            raise RuntimeError("ToolNode вызван без AIMessage.tool_calls.")

        logger.info(
            "Executing tool step %s: %s",
            state["tool_steps"] + 1,
            [
                {
                    "name": call.get("name"),
                    "args": call.get("args", {}),
                }
                for call in last_message.tool_calls
            ],
        )

        result = tool_node.invoke(state)
        tool_messages = [
            message.model_copy(update={"status": "error"})
            if isinstance(message, ToolMessage)
            and _tool_message_has_error(message)
            and getattr(message, "status", None) != "error"
            else message
            for message in result.get("messages", [])
        ]
        logger.info(
            "Tool step result: %s",
            json.dumps(
                [
                    _tool_message_payload(message)
                    if isinstance(message, ToolMessage)
                    else str(message)
                    for message in tool_messages
                ],
                ensure_ascii=False,
                default=str,
            )[:3000],
        )

        return {
            "messages": tool_messages,
            "tool_steps": state["tool_steps"] + len(last_message.tool_calls),
        }

    def observer(state: AgentGraphState) -> Dict[str, Any]:
        tool_call_message, tool_results = _latest_tool_exchange(state["messages"])

        payload = {
            "user_request": _last_user_query(state["messages"]),
            "tool_calls": tool_call_message.tool_calls,
            "tool_results": [
                _tool_message_payload(message) for message in tool_results
            ],
        }

        observer_messages: List[BaseMessage] = [
            SystemMessage(
                content=(
                    "Ты observer многошагового агента. Проанализируй только "
                    "фактический результат последнего инструмента. Выдели важные "
                    "факты, ошибки, ограничения и неоднозначности. Не выбирай "
                    "следующий инструмент, не решай завершать ли работу и не "
                    "формулируй ответ пользователю. Не придумывай отсутствующие "
                    "данные. Ответь обычным компактным текстом, не JSON и без "
                    "tool call."
                )
            ),
            HumanMessage(
                content=json.dumps(payload, ensure_ascii=False, default=str)
            ),
        ]

        try:
            result = model.invoke(observer_messages)
            if isinstance(result, Observation):
                observation = result
            else:
                raw_summary = (
                    _message_text(result)
                    if isinstance(result, BaseMessage)
                    else _message_content_text(result).strip()
                )
                has_error = any(
                    _tool_message_has_error(message)
                    for message in tool_results
                )
                observation = Observation(
                    summary=(
                        raw_summary
                        or "Observer не вернул текстовую выжимку результата."
                    ),
                    has_error=has_error,
                    limitations=(
                        ["Один или несколько tools завершились с ошибкой."]
                        if has_error
                        else []
                    ),
                )
        except Exception as exc:
            logger.exception("Plain-text observer failed")
            observation = _fallback_observation(
                tool_call_message,
                tool_results,
                exc,
            )

        observation = _compact_observation(observation)
        logger.info(
            "Observer result: %s",
            observation.model_dump_json()[:2000],
        )
        return {
            "observations": [
                *(state.get("observations") or []),
                observation,
            ]
        }

    def responder(state: AgentGraphState) -> Dict[str, Any]:
        response_instruction = """
Сформируй окончательный ответ пользователю по выжимке planner и фактическим
результатам tools.

Не вызывай инструменты. Используй исходный запрос, пользовательскую историю диалога,
реальные ToolMessage и planner_handoff ниже. Внутренние observations намеренно не
передаются, чтобы не дублировать и не искажать результаты tools. Если пользователь
просит список, таблицу, строки или полный результат, перенеси соответствующие данные
из ToolMessage без сокращения. Не упоминай внутренние узлы или устройство графа. Если
в handoff или ToolMessage указано, что данных недостаточно либо инструмент завершился
ошибкой, явно укажи ограничение и не придумывай отсутствующие факты.
Для ответа о s2t_transformations не связывай глобальный результат с активным
файлом и не упоминай его file_id или имя. Пустой результат означает только то,
что глобальная таблица сейчас пуста.
Табличные данные возвращай только компактным текстовым блоком `table`: внутри должен
быть валидный JSON-список списков, где первая строка содержит названия колонок, а
остальные строки — значения. Пример: ```table
[["target_table","count"],["t_bus_srv",5],["t_agr_dep",2]]
```. Markdown-таблица с `|` и строкой `---` запрещена: браузер сам отрисует блок
`table` как таблицу. Не добавляй вводную фразу «Полученные результаты», если она
не нужна по смыслу. Компактный блок не является сокращением: сохраняй все запрошенные
строки, порядок колонок и точные значения из ToolMessage.
Если фактический ToolMessage содержит text_diagram, обязательно перенеси ровно
готовый text_diagram без изменений внутри блока ```text```. Не заменяй его
списком, не пересказывай и не добавляй собственную классификацию структуры пути.
Если ToolMessage содержит
visualization_url, не печатай HTML, DOT или Mermaid; кратко опиши результат —
приложение само добавит интерактивный граф. Mermaid-код
показывай только по прямой просьбе пользователя получить Mermaid.
""".strip()

        planner_message = state.get("planner_message")
        planner_text = _clip_text(
            _message_text(planner_message),
            _PLANNER_HANDOFF_MAX_CHARS,
        ) if (
            planner_message is not None and not planner_message.tool_calls
        ) else ""
        if planner_text:
            response_instruction += f"""

Planner решил, что дополнительных инструментов больше не требуется, и сформировал
следующую выжимку проверенных фактов:

<planner_handoff>
{planner_text}
</planner_handoff>

Используй выжимку как навигацию по результатам, но точные значения и полный
запрошенный вывод бери из реальных ToolMessage. Верни полноценный окончательный
ответ, а не комментарий к выжимке.
""".rstrip()

        system_parts = [state["system_prompt"].strip(), response_instruction]
        runtime_context = _runtime_context(
            state,
            include_observations=False,
        )
        if runtime_context is not None:
            system_parts.append(runtime_context)

        messages: List[BaseMessage] = [
            SystemMessage(content="\n\n".join(system_parts))
        ]

        messages.extend(state["messages"])

        try:
            reply = model.invoke(messages)
            if not isinstance(reply, AIMessage):
                reply = AIMessage(content=_message_content_text(reply))
        except Exception as exc:
            logger.exception("LLM error in responder")
            reply = AIMessage(
                content=f"Не удалось сформировать ответ из-за ошибки связи с LLM: {exc}"
            )

        return {
            "messages": [reply],
            "planner_message": None,
        }

    def route_after_planner(
        state: AgentGraphState,
    ) -> Literal["prepare_tool", "responder"]:
        if state["tool_steps"] >= state["max_steps"]:
            return "responder"
        planner_message = state.get("planner_message")
        if planner_message is not None and planner_message.tool_calls:
            return "prepare_tool"
        return "responder"

    graph = StateGraph(AgentGraphState)
    graph.add_node("planner", planner)
    graph.add_node("prepare_tool", prepare_tool_call)
    graph.add_node("tools", execute_tools)
    graph.add_node("observer", observer)
    graph.add_node("responder", responder)

    graph.add_edge(START, "planner")
    graph.add_conditional_edges(
        "planner",
        route_after_planner,
        {
            "prepare_tool": "prepare_tool",
            "responder": "responder",
        },
    )
    graph.add_edge("prepare_tool", "tools")
    graph.add_edge("tools", "observer")
    graph.add_edge("observer", "planner")
    graph.add_edge("responder", END)

    return graph.compile()


def run_agent_graph(
    user_query: str,
    system_prompt: str,
    model: Any,
    tools: Mapping[str, BaseTool] | Sequence[BaseTool],
    max_steps: int = 5,
    history: Optional[List[ChatHistoryMessage]] = None,
    session_id: Optional[str] = None,
    user_id: Optional[str] = None,
    trace_tags: Optional[List[str]] = None,
    trace_metadata: Optional[Dict[str, Any]] = None,
    file_id: Optional[int] = None,
    callbacks: Optional[List[Any]] = None,
) -> str:
    """Run the graph and return the final responder message."""
    clean_query = user_query.strip()
    if not clean_query:
        return "Запрос не должен быть пустым."

    bounded_steps = max(1, int(max_steps))
    graph = build_agent_graph(model, tools)

    initial_messages = [
        *_history_messages(history),
        HumanMessage(content=clean_query),
    ]

    initial_state: AgentGraphState = {
        "messages": initial_messages,
        "system_prompt": system_prompt,
        "planner_message": None,
        "observations": [],
        "tool_steps": 0,
        "max_steps": bounded_steps,
        "active_file_id": int(file_id) if file_id is not None else None,
    }

    config: Dict[str, Any] = {
        "recursion_limit": bounded_steps * 4 + 8,
        "run_name": "agent_chat",
    }

    callback_list = list(callbacks or [])
    handler = get_callback_handler()
    if handler is not None and handler not in callback_list:
        callback_list.append(handler)
    if callback_list:
        config["callbacks"] = callback_list

    with langfuse_trace_context(
        trace_name="agent_chat",
        session_id=session_id,
        user_id=user_id,
        metadata=trace_metadata,
        tags=trace_tags or ["chat"],
    ):
        final_state = graph.invoke(initial_state, config=config)

    messages = final_state.get("messages") or []
    if messages and isinstance(messages[-1], AIMessage):
        answer = _message_text(messages[-1])
        if answer:
            for url in _visualization_urls(messages):
                label = (
                    "Открыть интерактивный граф связей S2T-таблиц"
                    if url.startswith("/exports/s2t-graphs/")
                    else "Открыть интерактивный SQL lineage-граф"
                )
                link = f"[{label}]({url})"
                if url not in answer:
                    answer += f"\n\n{link}"
            for url in _s2t_graph_data_urls(messages):
                link = f"[Открыть данные графа в JSON]({url})"
                if link not in answer:
                    answer += f"\n\n{link}"
            logger.info(
                "Agent final response (%d chars):\n%s",
                len(answer),
                answer,
            )
            return answer

    logger.warning("Agent finished without a final AIMessage response")
    return "Модель не вернула финальный ответ."
