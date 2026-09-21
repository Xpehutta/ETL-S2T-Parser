"""Compatibility wrapper for local models that emit textual tool calls."""

from __future__ import annotations

import json
import logging
import os
import re
from typing import Any, Mapping, Optional, Sequence

from langchain_core.messages import AIMessage, BaseMessage, HumanMessage, SystemMessage
from langchain_core.runnables import RunnableLambda
from langchain_core.utils.function_calling import convert_to_openai_tool
from langchain_ollama import ChatOllama


_ARGUMENT_KEYS = ("args", "arguments", "parameters", "input")
_NAME_KEYS = ("name", "tool", "tool_name", "function_name")
logger = logging.getLogger(__name__)
_TEXT_TOOL_MODEL_PREFIXES = ("deepseek-r1", "llama3.1")


def _message_text(message: Any) -> str:
    if not isinstance(message, BaseMessage):
        return ""
    content = message.content
    if isinstance(content, str):
        return content.strip()
    if not isinstance(content, Sequence) or isinstance(content, (bytes, bytearray)):
        return str(content or "").strip()
    parts: list[str] = []
    for block in content:
        if isinstance(block, str):
            parts.append(block)
        elif isinstance(block, Mapping):
            text = block.get("text") or block.get("content")
            if isinstance(text, str):
                parts.append(text)
            else:
                parts.append(json.dumps(block, ensure_ascii=False, default=str))
    return "\n".join(parts).strip()


def _json_value(value: Any) -> Any:
    if not isinstance(value, str):
        return value
    text = value.strip()
    if not text:
        return value
    try:
        return json.loads(text)
    except (TypeError, ValueError, json.JSONDecodeError):
        return value


def _first_json_value(text: str) -> Any:
    decoder = json.JSONDecoder()
    fenced = re.search(r"```(?:json)?\s*", text, flags=re.IGNORECASE)
    search_from = fenced.end() if fenced else 0
    starts = [
        index
        for index in range(search_from, len(text))
        if text[index] in "[{"
    ]
    for start in starts:
        try:
            payload, _ = decoder.raw_decode(text[start:])
        except (TypeError, ValueError, json.JSONDecodeError):
            continue
        if isinstance(payload, (Mapping, list)):
            return payload
    return None


def json_mapping_from_message(message: Any) -> Optional[Mapping[str, Any]]:
    """Extract the first JSON object from message content, if present."""
    text = _message_text(message)
    if not text:
        return None
    payload = _first_json_value(text)
    return payload if isinstance(payload, Mapping) else None


def _tool_name(tool: Any) -> str:
    try:
        formatted = convert_to_openai_tool(tool)
    except Exception:
        formatted = None
    if isinstance(formatted, Mapping):
        function = formatted.get("function")
        if isinstance(function, Mapping):
            return str(function.get("name") or "").strip()
    return str(getattr(tool, "name", "") or getattr(tool, "__name__", "")).strip()


def _positional_spec(tool: Any) -> tuple[str, str]:
    try:
        formatted = convert_to_openai_tool(tool)
    except Exception:
        return "", ""
    function = formatted.get("function") if isinstance(formatted, Mapping) else None
    parameters = function.get("parameters") if isinstance(function, Mapping) else None
    if not isinstance(parameters, Mapping):
        return "", ""
    argument_name = ""
    required = parameters.get("required")
    if isinstance(required, Sequence) and not isinstance(
        required, (str, bytes, bytearray)
    ):
        names = [str(item).strip() for item in required if str(item).strip()]
        if len(names) == 1:
            argument_name = names[0]
    properties = parameters.get("properties")
    if not argument_name and isinstance(properties, Mapping) and len(properties) == 1:
        argument_name = str(next(iter(properties))).strip()
    if not argument_name or not isinstance(properties, Mapping):
        return "", ""
    argument_schema = properties.get(argument_name)
    item_argument = ""
    if isinstance(argument_schema, Mapping) and argument_schema.get("type") == "array":
        item_schema = argument_schema.get("items")
        if isinstance(item_schema, Mapping):
            item_required = item_schema.get("required")
            if isinstance(item_required, Sequence) and not isinstance(
                item_required, (str, bytes, bytearray)
            ):
                item_names = [
                    str(item).strip()
                    for item in item_required
                    if str(item).strip()
                ]
                if len(item_names) == 1:
                    item_argument = item_names[0]
    return argument_name, item_argument


def _choice_name(tool_choice: Any, allowed: Sequence[str]) -> str:
    allowed_set = set(allowed)
    candidate = ""
    if isinstance(tool_choice, str):
        if tool_choice.casefold() not in {"auto", "any", "required", "none"}:
            candidate = tool_choice.strip()
    elif isinstance(tool_choice, Mapping):
        function = tool_choice.get("function")
        if isinstance(function, Mapping):
            candidate = str(function.get("name") or "").strip()
        else:
            candidate = str(tool_choice.get("name") or "").strip()
    if candidate in allowed_set:
        return candidate
    return allowed[0] if len(allowed) == 1 else ""


def _arguments(mapping: Mapping[str, Any]) -> Any:
    for key in _ARGUMENT_KEYS:
        if key in mapping:
            return _json_value(mapping[key])
    return None


def _explicit_call(payload: Any) -> Optional[tuple[str, Mapping[str, Any]]]:
    if not isinstance(payload, Mapping):
        return None
    function = payload.get("function")
    if isinstance(function, Mapping):
        name = str(function.get("name") or "").strip()
        args = _arguments(function)
        if name and isinstance(args, Mapping):
            return name, args
    name = next(
        (
            str(payload.get(key) or "").strip()
            for key in _NAME_KEYS
            if str(payload.get(key) or "").strip()
        ),
        "",
    )
    args = _arguments(payload)
    if name and isinstance(args, Mapping):
        return name, args
    return None


def _payload_calls(payload: Any) -> list[tuple[str, Mapping[str, Any]]]:
    if isinstance(payload, Sequence) and not isinstance(
        payload, (str, bytes, bytearray)
    ):
        result: list[tuple[str, Mapping[str, Any]]] = []
        for item in payload:
            result.extend(_payload_calls(item))
        return result
    if not isinstance(payload, Mapping):
        return []
    for key in ("tool_calls", "calls"):
        calls = payload.get(key)
        if isinstance(calls, Sequence) and not isinstance(
            calls, (str, bytes, bytearray)
        ):
            return _payload_calls(calls)
    for key in ("tool_call", "function_call"):
        call = payload.get(key)
        if isinstance(call, Mapping):
            return _payload_calls(call)
    explicit = _explicit_call(payload)
    return [explicit] if explicit is not None else []


def _required_args(payload: Any, tool_name: str) -> Optional[Mapping[str, Any]]:
    calls = _payload_calls(payload)
    matching = [args for name, args in calls if name.strip() == tool_name]
    if len(matching) == 1:
        return matching[0]
    if not isinstance(payload, Mapping):
        return None
    nested = payload.get(tool_name)
    if isinstance(nested, Mapping):
        nested_args = _arguments(nested)
        return nested_args if isinstance(nested_args, Mapping) else nested
    if any(key in payload for key in ("tool_calls", "calls", "tool_call")):
        return None
    explicit_names = {
        str(payload.get(key) or "").strip()
        for key in _NAME_KEYS
        if str(payload.get(key) or "").strip()
    }
    function = payload.get("function")
    if isinstance(function, Mapping):
        function_name = str(function.get("name") or "").strip()
        if function_name:
            explicit_names.add(function_name)
    if explicit_names and explicit_names != {tool_name}:
        return None
    args = _arguments(payload)
    if isinstance(args, Mapping):
        return args
    clean = dict(payload)
    if clean.get(tool_name) is True:
        clean.pop(tool_name, None)
    for key in (*_NAME_KEYS, "function"):
        clean.pop(key, None)
    return clean


def _calls_from_text(
    text: str,
    allowed_names: Sequence[str],
    required_name: str,
    positional_specs: Mapping[str, tuple[str, str]],
    coerce_required_name: bool,
) -> list[tuple[str, Mapping[str, Any]]]:
    payload = _first_json_value(text)
    calls = _payload_calls(payload)
    allowed = set(allowed_names)
    if required_name and coerce_required_name and len(calls) == 1:
        return [(required_name, calls[0][1])]
    if calls and any(name not in allowed for name, _ in calls):
        return []
    if required_name:
        args = _required_args(payload, required_name)
        if args is not None:
            return [(required_name, args)]
    matching = [(name, args) for name, args in calls if name in allowed]
    if matching:
        return matching
    mentioned = [name for name in allowed_names if name and name in text]
    if len(mentioned) != 1:
        return []
    name = mentioned[0]
    positional_name, item_argument = positional_specs.get(name, ("", ""))
    positional_values: list[Any] = []
    decoder = json.JSONDecoder()
    for match in re.finditer(re.escape(name) + r"\s*\(\s*", text):
        try:
            value, _ = decoder.raw_decode(text[match.end() :])
        except (TypeError, ValueError, json.JSONDecodeError):
            continue
        positional_values.append(value)
    if positional_name and positional_values:
        values: list[Any] = []
        for value in positional_values:
            if isinstance(value, list):
                values.extend(value)
            elif isinstance(value, Mapping):
                nested = value.get(positional_name)
                if isinstance(nested, list):
                    values.extend(nested)
                else:
                    values.append(dict(value))
            elif item_argument:
                values.append({item_argument: value})
            else:
                values.append(value)
        return [(name, {positional_name: values})]
    name_at = text.find(name)
    payload_after_name = _first_json_value(text[name_at + len(name) :])
    args = _required_args(payload_after_name, name)
    return [(name, args)] if args is not None else []


def normalize_tool_call_message(
    message: Any,
    allowed_names: Sequence[str],
    *,
    required_name: str = "",
    positional_specs: Optional[Mapping[str, tuple[str, str]]] = None,
    coerce_required_name: bool = False,
) -> Any:
    """Normalize native, envelope, and textual calls into AIMessage.tool_calls."""
    if not isinstance(message, AIMessage):
        return message
    allowed = tuple(dict.fromkeys(name.strip() for name in allowed_names if name.strip()))
    allowed_set = set(allowed)
    calls: list[tuple[str, Mapping[str, Any]]] = []
    native_calls = list(message.tool_calls or [])
    if native_calls and any(
        str(call.get("name") or "").strip() not in allowed_set
        for call in native_calls
    ):
        return message
    for call in native_calls:
        name = str(call.get("name") or "").strip()
        args = _json_value(call.get("args") or {})
        if name in allowed_set and isinstance(args, Mapping):
            calls.append((name, args))
    if not calls:
        envelope_calls = _payload_calls(message.additional_kwargs)
        if envelope_calls and any(
            name not in allowed_set for name, _ in envelope_calls
        ):
            return message
        calls = envelope_calls
    if not calls:
        calls = _calls_from_text(
            _message_text(message),
            allowed,
            required_name,
            positional_specs or {},
            coerce_required_name,
        )
    if required_name:
        calls = [item for item in calls if item[0] == required_name]
    if not calls:
        return message
    normalized = [
        {
            "name": name,
            "args": dict(args),
            "id": f"recovered-{name}-{index}",
            "type": "tool_call",
        }
        for index, (name, args) in enumerate(calls, start=1)
    ]
    return AIMessage(
        content=message.content,
        additional_kwargs=dict(message.additional_kwargs),
        response_metadata=dict(message.response_metadata),
        tool_calls=normalized,
        id=message.id,
        name=message.name,
        usage_metadata=message.usage_metadata,
    )


def recover_required_tool_call(message: Any, tool_name: str) -> Any:
    """Convert JSON content into the one tool call already required by flow."""
    return normalize_tool_call_message(
        message,
        [tool_name],
        required_name=tool_name,
    )


def recover_named_tool_call(
    message: Any,
    allowed_names: Sequence[str],
) -> Any:
    """Recover an explicitly named textual call from a multi-tool choice."""
    return normalize_tool_call_message(message, allowed_names)


class CompatibleChatOllama(ChatOllama):
    """ChatOllama that repairs textual calls at the model boundary."""

    def _uses_text_tool_mode(self) -> bool:
        mode = os.getenv("OLLAMA_TOOL_CALL_MODE", "auto").strip().casefold()
        if mode not in {"auto", "native", "text"}:
            raise ValueError(
                "OLLAMA_TOOL_CALL_MODE must be auto, native, or text, "
                f"got {mode!r}"
            )
        if mode != "auto":
            return mode == "text"
        model_name = str(self.model or "").strip().casefold()
        return any(
            model_name.startswith(prefix)
            for prefix in _TEXT_TOOL_MODEL_PREFIXES
        )

    def bind_tools(
        self,
        tools: Sequence[Any],
        *,
        tool_choice: Any = None,
        **kwargs: Any,
    ) -> Any:
        allowed_names = tuple(name for tool in tools if (name := _tool_name(tool)))
        positional_specs = {
            name: spec
            for tool in tools
            if (name := _tool_name(tool))
            if (spec := _positional_spec(tool))[0]
        }
        required_name = _choice_name(tool_choice, allowed_names)
        text_tool_mode = self._uses_text_tool_mode()
        if text_tool_mode:
            formatted_tools = [convert_to_openai_tool(tool) for tool in tools]
            requirement = (
                f"Обязательно вызови ровно `{required_name}`."
                if required_name
                else "Если нужен инструмент, выбери ровно один из списка."
            )
            adapter_prompt = (
                "Режим совместимости tool calls. "
                f"{requirement} Верни вызов без пояснений как JSON "
                '{"name":"имя","arguments":{...}}. '
                "Не печатай Python-вызов и не добавляй markdown. "
                "Схемы инструментов: "
                + json.dumps(formatted_tools, ensure_ascii=False, separators=(",", ":"))
            )

            def add_tool_prompt(value: Any) -> Any:
                if isinstance(value, str):
                    return [
                        SystemMessage(content=adapter_prompt),
                        HumanMessage(content=value),
                    ]
                if isinstance(value, Sequence) and not isinstance(
                    value, (str, bytes, bytearray)
                ):
                    return [SystemMessage(content=adapter_prompt), *list(value)]
                to_messages = getattr(value, "to_messages", None)
                if callable(to_messages):
                    return [
                        SystemMessage(content=adapter_prompt),
                        *list(to_messages()),
                    ]
                return value

            bound = RunnableLambda(add_tool_prompt) | self.bind(**kwargs)
        else:
            bound = super().bind_tools(
                tools,
                tool_choice=tool_choice,
                **kwargs,
            )
        def normalize(message: Any) -> Any:
            normalized = normalize_tool_call_message(
                message,
                allowed_names,
                required_name=required_name,
                positional_specs=positional_specs,
                coerce_required_name=text_tool_mode,
            )
            if (
                required_name
                and isinstance(normalized, AIMessage)
                and not normalized.tool_calls
            ):
                logger.warning(
                    "Ollama wrapper could not recover required call %s "
                    "from text length=%s native_calls=%s",
                    required_name,
                    len(_message_text(normalized)),
                    len(normalized.tool_calls),
                )
                logger.debug(
                    "Unrecovered Ollama output: content=%r additional_kwargs=%r",
                    _message_text(normalized)[:1200],
                    str(normalized.additional_kwargs)[:1200],
                )
            return normalized

        return bound | RunnableLambda(normalize)


__all__ = [
    "CompatibleChatOllama",
    "json_mapping_from_message",
    "normalize_tool_call_message",
    "recover_named_tool_call",
    "recover_required_tool_call",
]
