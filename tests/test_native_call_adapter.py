import json

import pytest
from langchain_core.messages import AIMessage, HumanMessage
from langchain_core.runnables import RunnableLambda
from langchain_ollama import ChatOllama
from pydantic import BaseModel

import agents.native_call_adapter as adapter
from agents.native_call_adapter import (
    CompatibleChatOllama,
    json_mapping_from_message,
    normalize_tool_call_message,
    recover_named_tool_call,
    recover_required_tool_call,
)


def test_required_call_recovers_plain_argument_object():
    recovered = recover_required_tool_call(
        AIMessage(content='```json\n{"skills": ["S2T-строки"]}\n```'),
        "select_operation_skills",
    )

    assert recovered.content.startswith("```json")
    assert len(recovered.tool_calls) == 1
    assert recovered.tool_calls[0]["name"] == "select_operation_skills"
    assert recovered.tool_calls[0]["args"] == {"skills": ["S2T-строки"]}
    assert recovered.tool_calls[0]["id"].startswith(
        "recovered-select_operation_skills-"
    )
    assert recovered.tool_calls[0]["type"] == "tool_call"


def test_recovered_tool_call_ids_are_unique_across_messages():
    message = AIMessage(content='{"skills": ["S2T-строки"]}')

    first = recover_required_tool_call(message, "select_operation_skills")
    second = recover_required_tool_call(message, "select_operation_skills")

    assert first.tool_calls[0]["id"] != second.tool_calls[0]["id"]


def test_named_call_requires_explicit_allowed_tool():
    message = AIMessage(
        content='{"name":"read_s2t_source_to_target",'
        '"arguments":{"source_table":"src"}}'
    )

    recovered = recover_named_tool_call(
        message,
        ["read_s2t_source_to_target", "finish_worker"],
    )
    untouched = recover_named_tool_call(message, ["finish_worker"])

    assert recovered.tool_calls[0]["name"] == "read_s2t_source_to_target"
    assert recovered.tool_calls[0]["args"] == {"source_table": "src"}
    assert untouched is message


def test_json_extractor_ignores_non_json_content():
    assert json_mapping_from_message(AIMessage(content="Обычный ответ")) is None


def test_required_call_recovers_openai_envelope_with_string_arguments():
    message = AIMessage(
        content=json.dumps(
            {
                "tool_calls": [
                    {
                        "type": "function",
                        "function": {
                            "name": "submit_worker_plan",
                            "arguments": json.dumps(
                                {"steps": [{"task": "read exact pair"}]}
                            ),
                        },
                    }
                ]
            }
        )
    )

    recovered = recover_required_tool_call(message, "submit_worker_plan")

    assert recovered.tool_calls[0]["name"] == "submit_worker_plan"
    assert recovered.tool_calls[0]["args"] == {
        "steps": [{"task": "read exact pair"}]
    }


def test_named_call_recovers_deepseek_function_syntax():
    message = AIMessage(
        content=(
            "Нужно вызвать submit_worker_plan("
            '{"steps":[{"task":"read exact pair"}]})'
        )
    )

    recovered = recover_named_tool_call(
        message,
        ["submit_worker_plan", "submit_upstream_answer"],
    )

    assert recovered.tool_calls[0]["name"] == "submit_worker_plan"
    assert recovered.tool_calls[0]["args"]["steps"][0]["task"] == (
        "read exact pair"
    )


def test_normalizer_recovers_additional_kwargs_envelope():
    message = AIMessage(
        content="",
        additional_kwargs={
            "tool_calls": [
                {
                    "function": {
                        "name": "finish_worker",
                        "arguments": '{"summary":"done","facts":[]}',
                    }
                }
            ]
        },
    )

    recovered = normalize_tool_call_message(message, ["finish_worker"])

    assert recovered.tool_calls[0]["args"] == {
        "summary": "done",
        "facts": [],
    }


def test_ollama_wrapper_normalizes_bound_tool_output(monkeypatch):
    class Plan(BaseModel):
        steps: list[dict[str, str]]

    raw = AIMessage(
        content=(
            '{"name":"Plan","arguments":'
            '"{\\"steps\\":[{\\"task\\":\\"read exact pair\\"}]}"}'
        )
    )
    monkeypatch.setattr(
        ChatOllama,
        "bind_tools",
        lambda self, tools, **kwargs: RunnableLambda(lambda _: raw),
    )
    model = CompatibleChatOllama(model="test")

    parsed = model.with_structured_output(
        Plan,
        method="function_calling",
    ).invoke("test")

    assert parsed == Plan(steps=[{"task": "read exact pair"}])


def test_ollama_wrapper_wraps_positional_array_in_single_required_argument(
    monkeypatch,
):
    class Plan(BaseModel):
        steps: list[dict[str, str]]

    raw = AIMessage(
        content='Plan([{"task":"source"},{"task":"target"}])'
    )
    monkeypatch.setattr(
        ChatOllama,
        "bind_tools",
        lambda self, tools, **kwargs: RunnableLambda(lambda _: raw),
    )
    model = CompatibleChatOllama(model="test")

    parsed = model.with_structured_output(
        Plan,
        method="function_calling",
    ).invoke("test")

    assert parsed == Plan(steps=[{"task": "source"}, {"task": "target"}])


def test_ollama_wrapper_merges_repeated_scalar_calls_into_array_items(
    monkeypatch,
):
    class Step(BaseModel):
        task: str

    class Plan(BaseModel):
        steps: list[Step]

    raw = AIMessage(
        content='Plan("read source")\n\nPlan("read target")'
    )
    monkeypatch.setattr(
        ChatOllama,
        "bind_tools",
        lambda self, tools, **kwargs: RunnableLambda(lambda _: raw),
    )
    model = CompatibleChatOllama(model="test")

    parsed = model.with_structured_output(
        Plan,
        method="function_calling",
    ).invoke("test")

    assert parsed == Plan(
        steps=[Step(task="read source"), Step(task="read target")]
    )


def test_deepseek_wrapper_uses_text_mode_before_normalization(monkeypatch):
    class Plan(BaseModel):
        steps: list[dict[str, str]]

    seen = {}

    def fake_bind(self, **kwargs):
        def invoke(messages):
            seen["messages"] = messages
            return AIMessage(
                content=(
                    '{"name":"Plan","arguments":'
                    '{"steps":[{"task":"read pair"}]}}'
                )
            )

        return RunnableLambda(invoke)

    monkeypatch.setattr(ChatOllama, "bind", fake_bind, raising=False)
    monkeypatch.setenv("OLLAMA_TOOL_CALL_MODE", "auto")
    model = CompatibleChatOllama(model="deepseek-r1:8b")

    parsed = model.with_structured_output(
        Plan,
        method="function_calling",
    ).invoke("test")

    assert parsed == Plan(steps=[{"task": "read pair"}])
    assert "Режим совместимости tool calls" in seen["messages"][0].content


def test_qwen_wrapper_keeps_native_tool_mode(monkeypatch):
    called = {}

    def fake_bind_tools(self, tools, **kwargs):
        called["tools"] = tools
        return RunnableLambda(lambda _: AIMessage(content="normal answer"))

    monkeypatch.setattr(ChatOllama, "bind_tools", fake_bind_tools)
    monkeypatch.setenv("OLLAMA_TOOL_CALL_MODE", "auto")
    model = CompatibleChatOllama(model="qwen3.5:9b")

    model.bind_tools(
        [{"name": "finish", "description": "done", "parameters": {}}]
    ).invoke("test")

    assert len(called["tools"]) == 1


def test_text_mode_coerces_explicit_unknown_name_to_only_bound_tool(
    monkeypatch,
):
    class Delegate(BaseModel):
        resolved_references: str = ""
        context: str = ""

    raw = AIMessage(
        content='{"name":"invented_delegate","arguments":{}}'
    )
    monkeypatch.setattr(
        ChatOllama,
        "bind",
        lambda self, **kwargs: RunnableLambda(lambda _: raw),
        raising=False,
    )
    monkeypatch.setenv("OLLAMA_TOOL_CALL_MODE", "text")
    model = CompatibleChatOllama(model="llama3.1:8b")

    parsed = model.with_structured_output(
        Delegate,
        method="function_calling",
    ).invoke("test")

    assert parsed == Delegate()


def test_message_text_handles_multiblock_and_non_message_content():
    message = AIMessage(
        content=[
            "prefix",
            {"type": "text", "text": "middle"},
            {"content": "suffix"},
            {"value": 7},
        ]
    )

    assert adapter._message_text(object()) == ""
    assert adapter._message_text(message) == (
        'prefix\nmiddle\nsuffix\n{"value": 7}'
    )

    object.__setattr__(message, "content", 17)
    assert adapter._message_text(message) == "17"


def test_json_helpers_handle_empty_invalid_and_array_payloads():
    assert adapter._json_value({"ready": True}) == {"ready": True}
    assert adapter._json_value("   ") == "   "
    assert adapter._json_value("not-json") == "not-json"
    assert adapter._first_json_value("{broken} then [1, 2]") == [1, 2]
    assert json_mapping_from_message(AIMessage(content="[1, 2]")) is None
    assert json_mapping_from_message(object()) is None


def test_tool_metadata_falls_back_when_conversion_fails(monkeypatch):
    class NamedTool:
        name = "fallback_tool"

    monkeypatch.setattr(
        adapter,
        "convert_to_openai_tool",
        lambda _tool: (_ for _ in ()).throw(ValueError("invalid schema")),
    )

    assert adapter._tool_name(NamedTool()) == "fallback_tool"
    assert adapter._positional_spec(NamedTool()) == ("", "")


@pytest.mark.parametrize(
    ("formatted", "expected"),
    [
        ({}, ("", "")),
        ({"function": {}}, ("", "")),
        (
            {
                "function": {
                    "parameters": {
                        "properties": {"value": {"type": "string"}}
                    }
                }
            },
            ("value", ""),
        ),
    ],
)
def test_positional_spec_handles_incomplete_schemas(monkeypatch, formatted, expected):
    monkeypatch.setattr(adapter, "convert_to_openai_tool", lambda _tool: formatted)

    assert adapter._positional_spec(object()) == expected


@pytest.mark.parametrize(
    ("choice", "allowed", "expected"),
    [
        ("auto", ["one", "two"], ""),
        ("one", ["one", "two"], "one"),
        ({"function": {"name": "two"}}, ["one", "two"], "two"),
        ({"name": "one"}, ["one", "two"], "one"),
        ("unknown", ["only"], "only"),
    ],
)
def test_choice_name_normalizes_supported_tool_choice(choice, allowed, expected):
    assert adapter._choice_name(choice, allowed) == expected


def test_payload_helpers_cover_nested_and_rejected_shapes():
    assert adapter._explicit_call("not-a-mapping") is None
    assert adapter._payload_calls(7) == []
    assert adapter._payload_calls(
        {"function_call": {"name": "run", "args": {"x": 1}}}
    ) == [("run", {"x": 1})]
    assert adapter._required_args(
        {"run": {"arguments": '{"x": 2}'}},
        "run",
    ) == {"x": 2}
    assert adapter._required_args(
        {"tool_calls": [{"name": "other", "args": {}}]},
        "run",
    ) is None
    assert adapter._required_args(
        {"function": {"name": "other"}, "x": 3},
        "run",
    ) is None
    assert adapter._required_args(
        {"name": "run", "args": '{"x": 4}'},
        "run",
    ) == {"x": 4}
    assert adapter._required_args({"args": {"x": 4}}, "run") == {"x": 4}
    assert adapter._required_args(
        {"run": True, "name": "run", "x": 5},
        "run",
    ) == {"x": 5}


def test_normalizer_preserves_non_ai_and_rejects_unknown_native_calls():
    human = HumanMessage(content="hello")
    assert normalize_tool_call_message(human, ["run"]) is human

    native_unknown = AIMessage(
        content="",
        tool_calls=[{"name": "other", "args": {}, "id": "call-1"}],
    )
    assert normalize_tool_call_message(native_unknown, ["run"]) is native_unknown

    envelope_unknown = AIMessage(
        content="",
        additional_kwargs={
            "tool_calls": [{"name": "other", "args": {}}],
        },
    )
    assert normalize_tool_call_message(envelope_unknown, ["run"]) is envelope_unknown


def test_text_parser_merges_nested_positional_lists_and_skips_invalid_call():
    calls = adapter._calls_from_text(
        'Plan(not-json) Plan({"steps":[{"task":"one"}]}) '
        'Plan({"task":"two"}) Plan(3)',
        ["Plan"],
        "",
        {"Plan": ("steps", "task")},
        False,
    )

    assert calls == [
        (
            "Plan",
            {
                "steps": [
                    {"task": "one"},
                    {"task": "two"},
                    {"task": 3},
                ]
            },
        )
    ]

    assert adapter._calls_from_text(
        "Values(3)",
        ["Values"],
        "",
        {"Values": ("values", "")},
        False,
    ) == [("Values", {"values": [3]})]


def test_text_parser_rejects_ambiguous_or_unknown_calls():
    assert adapter._calls_from_text(
        '{"name":"unknown","arguments":{}}',
        ["run", "finish"],
        "",
        {},
        False,
    ) == []
    assert adapter._calls_from_text(
        "run and finish",
        ["run", "finish"],
        "",
        {},
        False,
    ) == []


def test_ollama_wrapper_rejects_invalid_tool_call_mode(monkeypatch):
    monkeypatch.setenv("OLLAMA_TOOL_CALL_MODE", "broken")
    model = CompatibleChatOllama(model="test")

    with pytest.raises(ValueError, match="must be auto, native, or text"):
        model.bind_tools([])


@pytest.mark.parametrize("input_kind", ["messages", "prompt", "other"])
def test_text_mode_accepts_supported_prompt_input_shapes(monkeypatch, input_kind):
    seen = {}
    raw = AIMessage(content='{"name":"finish","arguments":{}}')

    def fake_bind(self, **kwargs):
        def invoke(value):
            seen["value"] = value
            return raw

        return RunnableLambda(invoke)

    class PromptValue:
        def to_messages(self):
            return [HumanMessage(content="from prompt")]

    monkeypatch.setattr(ChatOllama, "bind", fake_bind, raising=False)
    monkeypatch.setenv("OLLAMA_TOOL_CALL_MODE", "text")
    model = CompatibleChatOllama(model="llama3.1:8b")
    bound = model.bind_tools(
        [{"name": "finish", "description": "done", "parameters": {}}]
    )
    value = {
        "messages": [HumanMessage(content="from list")],
        "prompt": PromptValue(),
        "other": 7,
    }[input_kind]

    result = bound.invoke(value)

    assert result.tool_calls[0]["name"] == "finish"
    if input_kind == "messages":
        assert seen["value"][1].content == "from list"
    elif input_kind == "prompt":
        assert seen["value"][1].content == "from prompt"
    else:
        assert seen["value"] == 7
