import json

from langchain_core.messages import AIMessage
from langchain_core.runnables import RunnableLambda
from langchain_ollama import ChatOllama
from pydantic import BaseModel

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
    assert recovered.tool_calls == [
        {
            "name": "select_operation_skills",
            "args": {"skills": ["S2T-строки"]},
            "id": "recovered-select_operation_skills-1",
            "type": "tool_call",
        }
    ]


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
