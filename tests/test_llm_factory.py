import pytest
from langchain_ollama import ChatOllama
from langchain_openai import ChatOpenAI

from agents.llm_factory import (
    DEFAULT_GIGACHAT_JUDGE_MODEL,
    DEFAULT_GIGACHAT_MODEL,
    DEFAULT_LLM_PROVIDER,
    DEFAULT_OLLAMA_BASE_URL,
    DEFAULT_OLLAMA_MODEL,
    DEFAULT_OPENROUTER_MODEL,
    create_chat_model,
    create_judge_chat_model,
    get_chat_model_name,
    get_judge_provider,
    get_judge_model_name,
    get_llm_provider,
)
from agents.native_call_adapter import CompatibleChatOllama


def test_default_llm_provider_is_gigachat(monkeypatch):
    monkeypatch.delenv("LLM_PROVIDER", raising=False)

    assert get_llm_provider() == DEFAULT_LLM_PROVIDER
    assert DEFAULT_LLM_PROVIDER == "gigachat"


def test_gigachat_factory_uses_model_fallback(monkeypatch):
    from langchain_gigachat import GigaChat

    monkeypatch.delenv("LLM_PROVIDER", raising=False)
    monkeypatch.setenv("GIGACHAT_API_KEY", "test-credentials")
    monkeypatch.delenv("GIGACHAT_MODEL", raising=False)
    monkeypatch.setenv("MODEL", "GigaChat-Pro")

    model = create_chat_model(timeout=5)

    assert isinstance(model, GigaChat)
    assert model.model == "GigaChat-Pro"
    assert get_chat_model_name() == "GigaChat-Pro"


@pytest.mark.parametrize(("value", "expected"), [("0", False), ("1", True)])
def test_gigachat_factory_passes_binary_verify_ssl(
    monkeypatch,
    value,
    expected,
):
    from langchain_gigachat import GigaChat

    monkeypatch.setenv("LLM_PROVIDER", "gigachat")
    monkeypatch.setenv("GIGACHAT_API_KEY", "test-credentials")
    monkeypatch.setenv("GIGACHAT_VERIFY_SSL", value)

    model = create_chat_model(timeout=5)

    assert isinstance(model, GigaChat)
    assert model.verify_ssl_certs is expected


def test_gigachat_judge_uses_pro_independently_from_agent(monkeypatch):
    from langchain_gigachat import GigaChat

    monkeypatch.setenv("LLM_PROVIDER", "gigachat")
    monkeypatch.setenv("GIGACHAT_API_KEY", "test-credentials")
    monkeypatch.setenv("GIGACHAT_MODEL", "GigaChat-3-Ultra")
    monkeypatch.delenv("GIGACHAT_JUDGE_MODEL", raising=False)
    monkeypatch.delenv("LLM_JUDGE_MODEL", raising=False)

    model = create_judge_chat_model(timeout=5)

    assert isinstance(model, GigaChat)
    assert model.model == DEFAULT_GIGACHAT_JUDGE_MODEL
    assert get_judge_model_name() == "GigaChat-2-Pro"


def test_gigachat_judge_model_can_be_overridden(monkeypatch):
    monkeypatch.setenv("LLM_PROVIDER", "gigachat")
    monkeypatch.setenv("GIGACHAT_JUDGE_MODEL", "GigaChat-Max")

    assert get_judge_model_name() == "GigaChat-Max"


def test_judge_provider_defaults_to_agent_provider(monkeypatch):
    monkeypatch.setenv("LLM_PROVIDER", "ollama")
    monkeypatch.delenv("LLM_JUDGE_PROVIDER", raising=False)

    assert get_judge_provider() == "ollama"


def test_ollama_agent_can_use_gigachat_judge(monkeypatch):
    from langchain_gigachat import GigaChat

    monkeypatch.setenv("LLM_PROVIDER", "ollama")
    monkeypatch.setenv("OLLAMA_MODEL", "qwen3.5:9b")
    monkeypatch.setenv("LLM_JUDGE_PROVIDER", "gigachat")
    monkeypatch.setenv("GIGACHAT_API_KEY", "test-credentials")
    monkeypatch.setenv("GIGACHAT_JUDGE_MODEL", "GigaChat-2-Max")

    model = create_judge_chat_model(timeout=5)

    assert get_judge_provider() == "gigachat"
    assert get_judge_model_name() == "GigaChat-2-Max"
    assert isinstance(model, GigaChat)
    assert model.model == "GigaChat-2-Max"


def test_judge_provider_rejects_unknown_value(monkeypatch):
    monkeypatch.setenv("LLM_JUDGE_PROVIDER", "unknown")

    with pytest.raises(ValueError, match="LLM_JUDGE_PROVIDER"):
        get_judge_provider()


def test_openrouter_factory_uses_free_router_by_default(monkeypatch):
    monkeypatch.setenv("LLM_PROVIDER", "openrouter")
    monkeypatch.setenv("OPENROUTER_API_KEY", "sk-or-v1-test")
    monkeypatch.delenv("OPENROUTER_MODEL", raising=False)

    model = create_chat_model(timeout=3)

    assert isinstance(model, ChatOpenAI)
    assert model.model_name == DEFAULT_OPENROUTER_MODEL
    assert model.request_timeout == 3
    assert get_chat_model_name() == DEFAULT_OPENROUTER_MODEL


def test_openrouter_factory_requires_api_key(monkeypatch):
    monkeypatch.setenv("LLM_PROVIDER", "openrouter")
    monkeypatch.delenv("OPENROUTER_API_KEY", raising=False)

    with pytest.raises(ValueError, match="OPENROUTER_API_KEY"):
        create_chat_model()


def test_ollama_factory_uses_local_openai_compatible_endpoint(monkeypatch):
    monkeypatch.setenv("LLM_PROVIDER", "ollama")
    monkeypatch.delenv("OLLAMA_MODEL", raising=False)
    monkeypatch.delenv("OLLAMA_BASE_URL", raising=False)
    monkeypatch.delenv("OLLAMA_NUM_CTX", raising=False)
    monkeypatch.delenv("OLLAMA_REASONING", raising=False)

    model = create_chat_model(timeout=4)

    assert isinstance(model, ChatOllama)
    assert isinstance(model, CompatibleChatOllama)
    assert model.model == DEFAULT_OLLAMA_MODEL
    assert model.base_url == DEFAULT_OLLAMA_BASE_URL
    assert model.client_kwargs["timeout"] == 4
    assert model.reasoning is False
    assert get_chat_model_name() == DEFAULT_OLLAMA_MODEL


def test_ollama_factory_passes_configured_context_window(monkeypatch):
    monkeypatch.setenv("LLM_PROVIDER", "ollama")
    monkeypatch.setenv("OLLAMA_MODEL", "qwen3.5:9b")
    monkeypatch.setenv("OLLAMA_NUM_CTX", "16384")
    monkeypatch.delenv("OLLAMA_REASONING", raising=False)

    model = create_chat_model()

    assert isinstance(model, ChatOllama)
    assert model.model == "qwen3.5:9b"
    assert model.num_ctx == 16384


@pytest.mark.parametrize(("value", "expected"), [("0", False), ("1", True)])
def test_ollama_factory_passes_configured_reasoning(
    monkeypatch,
    value,
    expected,
):
    monkeypatch.setenv("LLM_PROVIDER", "ollama")
    monkeypatch.setenv("OLLAMA_REASONING", value)

    model = create_chat_model()

    assert isinstance(model, ChatOllama)
    assert model.reasoning is expected


@pytest.mark.parametrize(
    ("provider", "flag"),
    [
        ("gigachat", "GIGACHAT_VERIFY_SSL"),
        ("ollama", "OLLAMA_REASONING"),
    ],
)
@pytest.mark.parametrize("invalid", ["", "true", "false", " 1 "])
def test_llm_service_binary_flags_reject_non_binary_values(
    monkeypatch,
    provider,
    flag,
    invalid,
):
    monkeypatch.setenv("LLM_PROVIDER", provider)
    monkeypatch.setenv("GIGACHAT_API_KEY", "test-credentials")
    monkeypatch.setenv(flag, invalid)

    with pytest.raises(ValueError, match=rf"{flag} must be 0 or 1"):
        create_chat_model()
