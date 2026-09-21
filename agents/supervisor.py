"""Top-level supervisor LangGraph for the coordinated worker experiment."""

from __future__ import annotations

import asyncio
import json
import logging
import re
from typing import Any, Dict, List, Literal, Mapping, Optional, Sequence, TypedDict

from langchain_core.messages import AIMessage, BaseMessage, HumanMessage, SystemMessage
from langgraph.graph import END, START, StateGraph

from .agent import chat_model
from .async_runtime import ainvoke_compat, ainvoke_graph_compat, run_coroutine_sync
from .chat_graph import WorkerRunResult
from .coordinator import (
    COORDINATOR_CONTEXT_MAX_CHARS,
    coordinator_chat,
    coordinator_chat_async,
)
from .observability import get_callback_handler, langfuse_trace_context
from .run_metrics import (
    capture_agent_run,
    get_run_metrics_callback,
    llm_stage,
    record_display_tools,
    record_supervisor_decision,
)
from .worker import resolve_worker_display_refs

logger = logging.getLogger(__name__)
_DEFAULT_SYNC_COORDINATOR_CHAT = coordinator_chat


async def _call_coordinator_chat(task: str, **kwargs: Any) -> Any:
    """Keep legacy injected coordinators off the event loop."""

    if coordinator_chat is not _DEFAULT_SYNC_COORDINATOR_CHAT:
        return await asyncio.to_thread(coordinator_chat, task, **kwargs)
    return await coordinator_chat_async(task, **kwargs)

_DELEGATE_TOOL_NAME = "delegate_to_coordinator"
_EMPTY_DECISION_MAX_RETRIES = 1
_RESOLVED_REFERENCES_MAX_CHARS = COORDINATOR_CONTEXT_MAX_CHARS
_PSEUDO_DELEGATE_KEYS = {
    "resolved_references",
    "context",
    "current_query",
    "delegate_to_coordinator",
}


class SupervisorGraphState(TypedDict):
    """State of the top-level supervisor LangGraph."""

    current_query: str
    recent_history: List[Dict[str, str]]
    display_refs: List[str]
    supervisor_message: Optional[AIMessage]
    final_answer: Optional[str]


_SUPERVISOR_PROMPT = """
Ты верхний supervisor read-only приложения. По `current_query` и
`recent_history` реши, ответить сразу или вызвать `delegate_to_coordinator`.
После этого system-сообщения сначала идут сообщения
`recent_history` в их исходных ролях user/assistant, а последнее
user-сообщение — это дословный `current_query`, не часть истории.

Делегируй, когда для ответа нужно получить или проверить данные приложения.
Если ответ уже следует из диалога и не требует таких данных, ответь обычным
текстом. Не выдавай непроверенные данные по памяти.

При делегировании сформируй два принципиально разных поля:
`resolved_references` и `context`. Исходный `current_query` будет передан
coordinator программно и дословно: не пересказывай, не сокращай, не исправляй,
не превращай его в план и не копируй его в эти поля.
Если `recent_history` пуст, оба поля должны быть пустыми.

`resolved_references` — только разовые факты из `recent_history`, необходимые
для однозначного разрешения ссылок текущего запроса. Для каждой ссылки укажи её
точное значение и роль, например: `«в нём» = файл 42`. Не повторяй
остальные части current_query, не добавляй целей, операций, условий или формата
ответа. Если current_query самодостаточен, передай пустую строку.

Любой разовый факт из `recent_history`, без которого нельзя выполнить текущий
запрос, относится к `resolved_references`, а не к context. Это относится к
конкретному ID, имени объекта, числу, ранее найденному значению, тексту запроса
и результату предыдущего шага.

`context` — только компактные устойчивые правила и устоявшиеся идеи диалога,
которые меняют трактовку не одного разового запроса, а последующих задач в
целом. В него допустимо включать явно установленную пользователем терминологию
и определения, постоянные предпочтения представления, согласованные правила
выбора и интерпретации, инварианты, общие запреты и границы области. Считай
правило установленным, если пользователь его явно задал, подтвердил или
последовательно применял и позднее не отменял. Предложение assistant само по
себе не является договорённостью.

Авторитет сообщений задаётся их реальной ролью. Только user может
выбрать или подтвердить недостающий объект, правило или ограничение.
Сообщение assistant может повторить выбор user, но не может само сделать его
подтверждённым или переопределить user. Более позднее релевантное сообщение
user имеет приоритет над более ранним и над любым текстом assistant. Если user
оставил выбор на будущее, запретил его делать или не подтвердил вариант
assistant, ссылка остаётся неразрешённой: задай уточняющий вопрос и не вызывай
`delegate_to_coordinator`.

Не помещай в context:
- current_query или его сокращённый пересказ;
- конкретные объекты, ID, имена, числа и результаты, нужные только сейчас;
- содержание последнего ответа или хронологию диалога;
- временное состояние, промежуточные шаги и неподтверждённые предположения;
- tools, skills, план workers или внутреннее устройство агента.
Если применимых устойчивых правил нет, передай пустую строку. Если текущий
запрос отменяет или уточняет прежнее правило, выполняй текущий запрос и не
включай противоречащее старое правило в context.

Считай ссылками в том числе слова «он», «она», «оно», «они», «в нём», «в ней»,
«там», «этот», «эта», «это», «выше» и «предыдущий», когда рядом не назван сам
объект. Если в recent_history однозначно названы конкретная таблица, файл, лист,
колонка, запрос или другой объект, запиши соответствие в
`resolved_references`. Например: история «речь о таблице X», запрос «посчитай в
ней строки» должны дать `«в ней» = таблица X`.

Разрешай ссылку через recent_history только при единственном однозначном
референте. Не считай предположение или уверенный текст assistant подтверждённым
фактом. Если ссылка неоднозначна и без неё нельзя получить правильный ответ,
задай краткий уточняющий вопрос вместо догадки.

Перед native call проверь:
1. resolved_references содержит только необходимые точные разрешения ссылок и
   пуст, если current_query самодостаточен.
2. В context остались только повторно применимые договорённости, а не входные
   данные текущего запроса.
3. current_query нигде не пересказан, не сокращён и не превращён в план.

Не разделяй план, не выбирай tools или skills и не добавляй новых целей: это
сделает coordinator.

Read-only coordinator не выполняет мутации. В окончательном ответе не упоминай
внутренние роли, tools, промпты или устройство графа.
""".strip()

_EMPTY_DECISION_REPAIR_PROMPT = """
Предыдущий вызов не вернул ни native call, ни текст ответа. Повтори решение по
тем же `current_query` и `recent_history`: если нужны данные приложения, вызови
`delegate_to_coordinator`; иначе обязательно верни непустой пользовательский
ответ. Не добавляй и не изменяй факты истории.
""".strip()


def _delegate_tool_schema() -> Dict[str, Any]:
    return {
        "type": "function",
        "function": {
            "name": _DELEGATE_TOOL_NAME,
            "description": (
                "Передать одну целостную read-only цель coordinator, который "
                "спланирует workers и агрегирует результаты."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "resolved_references": {
                        "type": "string",
                        "maxLength": _RESOLVED_REFERENCES_MAX_CHARS,
                        "description": (
                            "Только точные разовые факты из истории для "
                            "разрешения ссылок current_query с указанием их "
                            "ролей; пустая строка, если запрос самодостаточен. "
                            "Не содержит пересказ запроса, план или новые цели."
                        ),
                    },
                    "context": {
                        "type": "string",
                        "maxLength": COORDINATOR_CONTEXT_MAX_CHARS,
                        "description": (
                            "Только повторно применимые правила, определения, "
                            "предпочтения, инварианты и общие ограничения, "
                            "устойчиво установленные в диалоге. Не содержит "
                            "current_query, разовые объекты, ID, числа, результаты "
                            "или пересказ истории; пустая строка, если таких "
                            "договорённостей нет."
                        ),
                    },
                },
                "required": ["resolved_references", "context"],
                "additionalProperties": False,
            },
        },
    }


def _message_text(result: Any) -> str:
    content = result.content if isinstance(result, BaseMessage) else result
    if isinstance(content, str):
        return content.strip()
    if isinstance(content, Sequence) and not isinstance(content, (str, bytes)):
        parts: List[str] = []
        for block in content:
            if isinstance(block, str):
                parts.append(block)
            elif isinstance(block, Mapping):
                text = block.get("text") or block.get("content")
                if text is not None:
                    parts.append(str(text))
        return "".join(parts).strip()
    return str(content or "").strip()


def _pseudo_delegate_message(message: AIMessage) -> Optional[AIMessage]:
    """Recover a single handoff emitted as JSON text instead of a tool call.

    Some local tool-capable models serialize the arguments of the only bound
    tool into ``content``. The two handoff field names are internal and form
    a narrow discriminator, so ordinary JSON answers remain direct answers.
    """
    if message.tool_calls:
        return None
    text = _message_text(message)
    if not text:
        return None

    fenced = re.search(
        r"```(?:json)?\s*(\{.*?\})\s*```",
        text,
        flags=re.IGNORECASE | re.DOTALL,
    )
    candidate = fenced.group(1) if fenced else text[text.find("{") :]
    if not candidate or not candidate.startswith("{"):
        return None
    try:
        payload, _ = json.JSONDecoder().raw_decode(candidate)
    except (TypeError, ValueError, json.JSONDecodeError):
        return None
    if not isinstance(payload, Mapping):
        return None
    payload_keys = {str(key) for key in payload}
    required_keys = {"resolved_references", "context"}
    if not required_keys.issubset(payload_keys):
        return None
    if payload_keys - _PSEUDO_DELEGATE_KEYS:
        return None

    def handoff_text(value: Any) -> str:
        if value is None or value == [] or value == {}:
            return ""
        if isinstance(value, str):
            return value.strip()
        return json.dumps(value, ensure_ascii=False, default=str)

    return AIMessage(
        content="",
        tool_calls=[
            {
                "name": _DELEGATE_TOOL_NAME,
                "args": {
                    "resolved_references": handoff_text(
                        payload.get("resolved_references")
                    ),
                    "context": handoff_text(payload.get("context")),
                },
                "id": "recovered-pseudo-delegate",
                "type": "tool_call",
            }
        ],
    )


def _parse_delegate_handoff(decision: Any) -> tuple[str, str]:
    """Validate the native supervisor handoff without interpreting its text."""

    if not isinstance(decision, AIMessage):
        raise RuntimeError(
            "Supervisor ожидал AIMessage с native call "
            f"{_DELEGATE_TOOL_NAME}."
        )
    tool_calls = decision.tool_calls
    if len(tool_calls) != 1:
        raise RuntimeError(
            "Supervisor должен вернуть ровно один native call "
            f"{_DELEGATE_TOOL_NAME}."
        )
    call = tool_calls[0]
    if not isinstance(call, Mapping) or call.get("name") != _DELEGATE_TOOL_NAME:
        raise RuntimeError(
            "Supervisor должен вернуть ровно один native call "
            f"{_DELEGATE_TOOL_NAME}."
        )
    arguments = call.get("args")
    if not isinstance(arguments, Mapping):
        raise RuntimeError(
            "Supervisor вернул не-object arguments для "
            f"{_DELEGATE_TOOL_NAME}."
        )

    values: Dict[str, str] = {}
    limits = {
        "resolved_references": _RESOLVED_REFERENCES_MAX_CHARS,
        "context": COORDINATOR_CONTEXT_MAX_CHARS,
    }
    for field_name, max_chars in limits.items():
        value = arguments.get(field_name)
        if not isinstance(value, str):
            raise RuntimeError(
                "Supervisor вернул не-string поле "
                f"{_DELEGATE_TOOL_NAME}.{field_name}."
            )
        if len(value) > max_chars:
            raise RuntimeError(
                "Supervisor превысил лимит поля "
                f"{_DELEGATE_TOOL_NAME}.{field_name}: "
                f"{len(value)} > {max_chars}."
            )
        values[field_name] = value.strip()
    return values["resolved_references"], values["context"]


def _supervisor_messages(
    *,
    current_query: str,
    recent_history: Sequence[Mapping[str, str]],
    repair_empty: bool = False,
) -> List[BaseMessage]:
    """Build role-aware model input without interpreting conversation text."""

    system_prompt = _SUPERVISOR_PROMPT
    if repair_empty:
        system_prompt = f"{system_prompt}\n\n{_EMPTY_DECISION_REPAIR_PROMPT}"
    messages: List[BaseMessage] = [SystemMessage(content=system_prompt)]
    for item in recent_history:
        content = str(item.get("content") or "")
        if item.get("role") == "user":
            messages.append(HumanMessage(content=content))
        else:
            messages.append(AIMessage(content=content))
    messages.append(HumanMessage(content=current_query))
    return messages


def build_supervisor_graph(
    model: Any,
    *,
    callbacks: Optional[Sequence[Any]] = None,
    collected_display_refs: Optional[List[str]] = None,
):
    """Build supervisor -> coordinator -> end as a LangGraph."""
    callback_list = list(callbacks or [])
    model_config = {"callbacks": callback_list} if callback_list else None
    supervisor_model = model.bind_tools([_delegate_tool_schema()])

    async def invoke_supervisor(
        call_messages: Sequence[BaseMessage],
    ) -> AIMessage:
        try:
            with llm_stage("supervisor"):
                result = await ainvoke_compat(
                    supervisor_model,
                    call_messages,
                    config=model_config,
                    category="llm",
                )
        except Exception as exc:
            raise RuntimeError(
                f"Ошибка LLM supervisor: {type(exc).__name__}"
            ) from exc
        if not isinstance(result, AIMessage):
            return AIMessage(content=_message_text(result))
        return result

    async def supervisor_node(
        state: SupervisorGraphState,
    ) -> Dict[str, Any]:
        decision = await invoke_supervisor(
            _supervisor_messages(
                current_query=state["current_query"],
                recent_history=state["recent_history"],
            )
        )
        for retry_index in range(_EMPTY_DECISION_MAX_RETRIES):
            if decision.tool_calls or _message_text(decision):
                break
            logger.warning(
                "Supervisor returned an empty decision; retrying (%s/%s)",
                retry_index + 1,
                _EMPTY_DECISION_MAX_RETRIES,
            )
            decision = await invoke_supervisor(
                _supervisor_messages(
                    current_query=state["current_query"],
                    recent_history=state["recent_history"],
                    repair_empty=True,
                )
            )
        recovered_decision = _pseudo_delegate_message(decision)
        if recovered_decision is not None:
            logger.warning(
                "Supervisor emitted delegate arguments as text; recovered "
                "delegate_to_coordinator native call"
            )
            decision = recovered_decision
        final_answer = None if decision.tool_calls else _message_text(decision)
        if not decision.tool_calls and not final_answer:
            raise RuntimeError(
                "LLM supervisor повторно вернул пустой ответ без native call."
            )
        if final_answer:
            logger.info("Supervisor answered directly")
            record_supervisor_decision(route="direct")
        return {
            "supervisor_message": decision,
            "final_answer": final_answer,
        }

    async def coordinator_node(
        state: SupervisorGraphState,
    ) -> Dict[str, Any]:
        decision = state.get("supervisor_message")
        if decision is None or not decision.tool_calls:
            raise RuntimeError(
                "Supervisor вызвал coordinator без delegate_to_coordinator."
            )
        delegated_task = str(state["current_query"]).strip()
        if not state["recent_history"]:
            if (
                len(decision.tool_calls) != 1
                or decision.tool_calls[0].get("name") != _DELEGATE_TOOL_NAME
            ):
                raise RuntimeError(
                    "Supervisor должен вернуть ровно один native call "
                    f"{_DELEGATE_TOOL_NAME}."
                )
            resolved_references = ""
            delegated_context = ""
        else:
            resolved_references, delegated_context = _parse_delegate_handoff(
                decision
            )
        if resolved_references:
            delegated_task = (
                f"{delegated_task}\n\n"
                "Однозначно разрешённые ссылки из истории:\n"
                f"{resolved_references}"
            )
        record_supervisor_decision(
            route="delegate",
            resolved_references=resolved_references,
            context=delegated_context,
        )
        logger.info(
            "Supervisor delegated coordinator task=%s",
            delegated_task[:1000],
        )
        coordinator_result = await _call_coordinator_chat(
            delegated_task,
            context=delegated_context,
        )
        if collected_display_refs is not None:
            collected_display_refs.extend(coordinator_result.display_refs)
        return {
            "display_refs": [
                *state["display_refs"],
                *coordinator_result.display_refs,
            ],
            "supervisor_message": None,
            "final_answer": coordinator_result.answer,
        }

    def route_after_supervisor(
        state: SupervisorGraphState,
    ) -> Literal["coordinator", "finish"]:
        decision = state.get("supervisor_message")
        if decision is None or not decision.tool_calls:
            return "finish"
        return "coordinator"

    graph = StateGraph(SupervisorGraphState)
    graph.add_node("supervisor", supervisor_node)
    graph.add_node("coordinator", coordinator_node)
    graph.add_edge(START, "supervisor")
    graph.add_conditional_edges(
        "supervisor",
        route_after_supervisor,
        {
            "coordinator": "coordinator",
            "finish": END,
        },
    )
    graph.add_edge("coordinator", END)
    return graph.compile()


async def _supervisor_chat_impl_async(
    clean_query: str,
    *,
    history: Optional[List[Dict[str, str]]] = None,
    session_id: Optional[str] = None,
) -> WorkerRunResult:
    initial_state: SupervisorGraphState = {
        "current_query": clean_query,
        "recent_history": [
            {
                "role": str(item.get("role") or ""),
                "content": str(item.get("content") or ""),
            }
            for item in (history or [])[-6:]
        ],
        "display_refs": [],
        "supervisor_message": None,
        "final_answer": None,
    }
    callback = get_callback_handler()
    callbacks = [callback] if callback is not None else []
    metrics_callback = get_run_metrics_callback()
    if metrics_callback is not None and metrics_callback not in callbacks:
        callbacks.append(metrics_callback)
    collected_display_refs: List[str] = []
    graph = build_supervisor_graph(
        chat_model,
        callbacks=callbacks,
        collected_display_refs=collected_display_refs,
    )
    graph_config = {
        "recursion_limit": 4,
        "run_name": "worker_supervisor",
    }

    with langfuse_trace_context(
        trace_name="worker_supervisor",
        session_id=session_id,
        tags=["supervisor", "coordinator", "worker", "experiment"],
    ):
        try:
            final_state = await ainvoke_graph_compat(
                graph,
                initial_state,
                config=graph_config,
            )
            final_answer = str(final_state.get("final_answer") or "").strip()
            if not final_answer:
                raise RuntimeError("Supervisor LangGraph завершился без ответа.")
            display_refs = list(final_state.get("display_refs") or [])
            display_items = resolve_worker_display_refs(display_refs)
            record_display_tools([item.name for item in display_items])
            return WorkerRunResult(
                answer=final_answer,
                display_items=display_items,
            )
        except BaseException:
            resolve_worker_display_refs(collected_display_refs)
            raise


async def supervisor_chat_async(
    user_query: str,
    *,
    history: Optional[List[Dict[str, str]]] = None,
    session_id: Optional[str] = None,
) -> WorkerRunResult:
    """Answer directly or coordinate one or more planned worker groups."""
    clean_query = str(user_query or "").strip()
    if not clean_query:
        return WorkerRunResult(
            answer="Запрос не должен быть пустым.",
            display_items=[],
        )

    with capture_agent_run(session_id):
        return await _supervisor_chat_impl_async(
            clean_query,
            history=history,
            session_id=session_id,
        )


def supervisor_chat(
    user_query: str,
    *,
    history: Optional[List[Dict[str, str]]] = None,
    session_id: Optional[str] = None,
) -> WorkerRunResult:
    """Compatibility facade for non-ASGI callers."""

    return run_coroutine_sync(
        supervisor_chat_async(
            user_query,
            history=history,
            session_id=session_id,
        )
    )


__all__ = [
    "SupervisorGraphState",
    "build_supervisor_graph",
    "supervisor_chat",
    "supervisor_chat_async",
]
