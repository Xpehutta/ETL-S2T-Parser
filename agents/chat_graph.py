"""LangGraph runtime for the read-only chat agent.

Architecture:

    planner (native tool calling)
        ├─ tool_calls -> ToolNode -> observer (structured output) -> planner
        └─ no tool_calls -> responder -> END

The planner alone decides whether another tool is needed. The observer only
interprets the latest tool result. The responder produces the final user-facing
answer without access to tools.
"""

from __future__ import annotations

import json
import logging
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
    observation: Optional[Observation]
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


def _runtime_context(state: AgentGraphState) -> Optional[SystemMessage]:
    parts: List[str] = []

    active_file_id = state.get("active_file_id")
    if active_file_id is not None:
        parts.append(
            f"Контекст UI: активный файл имеет file_id={active_file_id}. "
            "Используй это значение в аргументах инструментов, работающих "
            "с текущим файлом. Не применяй его к глобальной таблице "
            "s2t_transformations и её list/search/summarize tools."
        )

    observation = state.get("observation")
    if observation is not None:
        parts.append(f"Вывод observer:\n{observation.summary}")

        if observation.important_facts:
            parts.append(
                "Важные факты:\n- " + "\n- ".join(observation.important_facts)
            )

        if observation.limitations:
            parts.append(
                "Ограничения и неоднозначности:\n- "
                + "\n- ".join(observation.limitations)
            )

        if observation.has_error:
            parts.append(
                "Последний инструментальный шаг содержал ошибку. "
                "Самостоятельно реши: исправить аргументы, выбрать другой "
                "инструмент или перейти к ответу с честным указанием ограничения."
            )

    if not parts:
        return None

    return SystemMessage(content="\n\n".join(parts))


def _planner_messages(state: AgentGraphState) -> List[BaseMessage]:
    limit_reached = state["tool_steps"] >= state["max_steps"]

    planner_instruction = """
Ты planner многошагового read-only агента.

Твоя задача — выбрать ровно один следующий шаг:
- вызови ровно один доступный инструмент, если нужны дополнительные факты;
- вызови show_plan, если в многошаговой задаче перед следующим действием нужно
  явно зафиксировать, что уже сделано и что осталось сделать;
- не вызывай инструмент, если фактов уже достаточно и можно формировать ответ.

Перед каждым tool call сначала выбери ровно один сценарий результата:

1. TABULAR_SQLITE — пользователь просит таблицу трансформаций, строки,
   S2T-маппинги, правила преобразования, обычные связи source → target,
   фильтрацию, подсчёт или агрегацию. Используй только
   list_s2t_transformations, search_s2t_transformations, summarize_s2t_tables
   или run_sql. s2t_transformations является глобальной таблицей: никогда не
   передавай и не добавляй file_id для её просмотра, поиска, суммаризации или
   SQL-фильтрации. Не вызывай Neo4j-tools для этого сценария.

2. GRAPH_NEO4J — пользователь просит lineage, графовый путь, цепочку,
   upstream, downstream, что от чего зависит, соседние узлы или impact analysis.
   Используй trace_neo4j_lineage для непосредственного lineage и run_cypher
   для сложного графового обхода. В Neo4j есть только колонки ETLColumn и связи
   TRANSFORMS_TO. Остальные факты получай из SQLite.
   Не вызывай SQLite-tools для обхода графа и не подменяй ими Cypher-путь.

Не формулируй окончательный ответ пользователю: это сделает отдельный responder.
Не имитируй результаты инструментов и не повторяй одинаковый вызов или
неизменившийся show_plan без причины.
При поиске логической S2T-таблицы учитывай обе роли: source_table и target_table.
""".strip()

    if limit_reached:
        planner_instruction += (
            "\n\nЛимит инструментальных шагов исчерпан. "
            "Не вызывай инструменты; верни обычное сообщение без tool_calls, "
            "чтобы граф перешёл к responder."
        )

    messages: List[BaseMessage] = [
        SystemMessage(content=state["system_prompt"].strip()),
        SystemMessage(content=planner_instruction),
    ]

    runtime_message = _runtime_context(state)
    if runtime_message is not None:
        messages.append(runtime_message)

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
    }


def _fallback_observation(
    tool_call_message: AIMessage,
    tool_results: Sequence[ToolMessage],
    error: Exception,
) -> Observation:
    names = [call.get("name", "unknown_tool") for call in tool_call_message.tool_calls]
    result_preview = "\n".join(
        f"{message.name or 'unknown_tool'}: {_message_content_text(message.content)[:1500]}"
        for message in tool_results
    )

    return Observation(
        summary=(
            "Observer не смог получить структурированный вывод. "
            f"Выполнены инструменты: {', '.join(names)}. "
            f"Сырой результат:\n{result_preview}"
        ),
        has_error=True,
        important_facts=[],
        limitations=[f"Ошибка observer: {type(error).__name__}"],
    )


def _build_observer_model(model: Any) -> Any:
    """Create a structured observer with a compatibility fallback."""
    try:
        return model.with_structured_output(
            Observation,
            method="json_schema",
        )
    except TypeError:
        # Some LangChain integrations expose with_structured_output but do not
        # accept an explicit method argument.
        return model.with_structured_output(Observation)


def build_agent_graph(
    model: Any,
    tools: Mapping[str, BaseTool] | Sequence[BaseTool],
):
    """Build the planner -> tools -> observer -> planner graph."""
    tool_list = _normalize_tools(tools)
    planner_model = model.bind_tools(tool_list)
    observer_model = _build_observer_model(model)
    tool_node = ToolNode(tool_list, handle_tool_errors=True)

    def planner(state: AgentGraphState) -> Dict[str, Any]:
        limit_reached = state["tool_steps"] >= state["max_steps"]
        selected_model = model if limit_reached else planner_model

        try:
            reply = selected_model.invoke(_planner_messages(state))
        except Exception as exc:
            logger.exception("LLM error in planner")
            # A plain message routes to responder, which will produce the user
            # facing error rather than exposing an internal graph exception.
            reply = AIMessage(content=f"Planner error: {type(exc).__name__}")

        if not isinstance(reply, AIMessage):
            reply = AIMessage(content=_message_content_text(reply))

        if len(reply.tool_calls) > 1:
            logger.warning(
                "Planner returned %s tool calls; only one call per step is supported",
                len(reply.tool_calls),
            )
            reply = AIMessage(
                content=(
                    "Planner выбрал несколько инструментов одновременно. "
                    "Перейди к формированию ответа по уже полученным данным."
                )
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
        tool_messages = result.get("messages", [])

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
            "previous_observation": (
                state["observation"].model_dump()
                if state.get("observation") is not None
                else None
            ),
        }

        observer_messages: List[BaseMessage] = [
            SystemMessage(
                content=(
                    "Ты observer многошагового агента. Проанализируй только "
                    "фактический результат последнего инструмента. Выдели важные "
                    "факты, ошибки, ограничения и неоднозначности. Не выбирай "
                    "следующий инструмент, не решай завершать ли работу и не "
                    "формулируй ответ пользователю. Не придумывай отсутствующие данные."
                )
            ),
            HumanMessage(
                content=json.dumps(payload, ensure_ascii=False, default=str)
            ),
        ]

        try:
            result = observer_model.invoke(observer_messages)
            if isinstance(result, Observation):
                observation = result
            elif isinstance(result, Mapping):
                observation = Observation.model_validate(result)
            else:
                observation = Observation.model_validate_json(str(result))
        except Exception as exc:
            logger.exception("Structured observer failed")
            observation = _fallback_observation(
                tool_call_message,
                tool_results,
                exc,
            )

        logger.info(
            "Observer result: %s",
            observation.model_dump_json()[:2000],
        )
        return {"observation": observation}

    def responder(state: AgentGraphState) -> Dict[str, Any]:
        response_instruction = """
Сформируй окончательный ответ пользователю.

Не вызывай инструменты. Используй только исходный запрос, историю диалога,
реальные ToolMessage и вывод observer. Не упоминай внутренние узлы planner,
observer или устройство графа. Если данных недостаточно или инструмент завершился
ошибкой, явно укажи ограничение и не придумывай отсутствующие факты.
Для ответа о s2t_transformations не связывай глобальный результат с активным
файлом и не упоминай его file_id или имя. Пустой результат означает только то,
что глобальная таблица сейчас пуста.
""".strip()

        planner_message = state.get("planner_message")
        planner_text = (
            _message_text(planner_message)
            if planner_message is not None and not planner_message.tool_calls
            else ""
        )
        if planner_text:
            response_instruction += f"""

Planner решил, что дополнительных инструментов больше не требуется, и оставил
следующий черновик ответа:

<planner_draft>
{planner_text}
</planner_draft>

Используй этот черновик как основу, но сверь его с реальными ToolMessage и
выводом observer. Верни полноценный окончательный ответ, а не комментарий к
черновику.
""".rstrip()

        messages: List[BaseMessage] = [
            SystemMessage(content=state["system_prompt"].strip()),
            SystemMessage(content=response_instruction),
        ]

        runtime_message = _runtime_context(state)
        if runtime_message is not None:
            messages.append(runtime_message)

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
        "observation": None,
        "tool_steps": 0,
        "max_steps": bounded_steps,
        "active_file_id": int(file_id) if file_id is not None else None,
    }

    config: Dict[str, Any] = {
        # One tool iteration uses planner -> prepare -> tools -> observer.
        # Add room for the initial planner and final responder.
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
            return answer

    return "Модель не вернула финальный ответ."
