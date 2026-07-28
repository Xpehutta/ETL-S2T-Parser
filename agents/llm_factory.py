from __future__ import annotations

import os
from typing import Optional

from dotenv import load_dotenv
from langchain_core.language_models.chat_models import BaseChatModel
from langchain_gigachat import GigaChat
from langchain_ollama import ChatOllama
from langchain_openai import ChatOpenAI

load_dotenv()

DEFAULT_LLM_PROVIDER = "gigachat"
SUPPORTED_LLM_PROVIDERS = ("gigachat", "openrouter", "ollama")

DEFAULT_GIGACHAT_BASE_URL = "https://gigachat.devices.sberbank.ru/api/v1"
DEFAULT_GIGACHAT_MODEL = "GigaChat"
DEFAULT_OPENROUTER_BASE_URL = "https://openrouter.ai/api/v1"
DEFAULT_OPENROUTER_MODEL = "openrouter/free"
DEFAULT_OLLAMA_BASE_URL = "http://localhost:11434"
DEFAULT_OLLAMA_MODEL = "qwen2.5:7b"


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer, got {raw!r}") from exc


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    try:
        return float(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be a number, got {raw!r}") from exc


def _env_optional_int(name: str) -> Optional[int]:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return None
    try:
        return int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer, got {raw!r}") from exc


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    normalized = raw.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"{name} must be a boolean, got {raw!r}")


def get_llm_provider() -> str:
    return os.getenv("LLM_PROVIDER", DEFAULT_LLM_PROVIDER).strip().lower()


def get_chat_model_name() -> str:
    provider = get_llm_provider()
    if provider == "openrouter":
        return os.getenv("OPENROUTER_MODEL", DEFAULT_OPENROUTER_MODEL)
    if provider == "ollama":
        return os.getenv("OLLAMA_MODEL", DEFAULT_OLLAMA_MODEL)
    if provider == "gigachat":
        return os.getenv(
            "GIGACHAT_MODEL",
            os.getenv("MODEL", DEFAULT_GIGACHAT_MODEL),
        )
    raise ValueError(
        f"Unsupported LLM_PROVIDER={provider!r}. "
        f"Use {', '.join(repr(item) for item in SUPPORTED_LLM_PROVIDERS)}."
    )


def _normalize_ollama_base_url(value: str) -> str:
    base_url = value.strip().rstrip("/")
    if base_url.endswith("/v1"):
        base_url = base_url[:-3].rstrip("/")
    return base_url


def _create_gigachat_chat_model(timeout: Optional[float] = None) -> GigaChat:
    credentials = (
        os.getenv("GIGACHAT_API_KEY")
        or os.getenv("GIGACHAT_CREDENTIALS")
        or os.getenv("GIGACHAT_EMBEDDINGS_CREDENTIALS")
    )
    if not credentials:
        raise ValueError(
            "Missing GigaChat credentials. Set GIGACHAT_API_KEY or "
            "GIGACHAT_CREDENTIALS."
        )

    return GigaChat(
        model=os.getenv(
            "GIGACHAT_MODEL",
            os.getenv("MODEL", DEFAULT_GIGACHAT_MODEL),
        ),
        credentials=credentials,
        base_url=os.getenv("GIGACHAT_API_URL", DEFAULT_GIGACHAT_BASE_URL),
        verify_ssl_certs=_env_bool("GIGACHAT_VERIFY_SSL", False),
        scope=os.getenv("GIGACHAT_SCOPE", "GIGACHAT_API_PERS"),
        timeout=float(
            timeout if timeout is not None else _env_int("GIGACHAT_TIMEOUT", 120)
        ),
        temperature=_env_float("GIGACHAT_TEMPERATURE", 0.0),
    )


def _create_openrouter_chat_model(timeout: Optional[float] = None) -> ChatOpenAI:
    api_key = os.getenv("OPENROUTER_API_KEY")
    if not api_key:
        raise ValueError("Missing OPENROUTER_API_KEY")

    headers: dict[str, str] = {}
    app_url = os.getenv("OPENROUTER_APP_URL", "").strip()
    app_title = os.getenv("OPENROUTER_APP_TITLE", "ETL S2T Parser").strip()
    if app_url:
        headers["HTTP-Referer"] = app_url
    if app_title:
        headers["X-Title"] = app_title

    return ChatOpenAI(
        model=os.getenv("OPENROUTER_MODEL", DEFAULT_OPENROUTER_MODEL),
        api_key=api_key,
        base_url=os.getenv("OPENROUTER_BASE_URL", DEFAULT_OPENROUTER_BASE_URL),
        timeout=float(
            timeout if timeout is not None else _env_int("OPENROUTER_TIMEOUT", 120)
        ),
        temperature=_env_float("OPENROUTER_TEMPERATURE", 0.0),
        max_tokens=_env_optional_int("OPENROUTER_MAX_TOKENS"),
        default_headers=headers or None,
        max_retries=_env_int("OPENROUTER_MAX_RETRIES", 2),
    )


def _create_ollama_chat_model(timeout: Optional[float] = None) -> ChatOllama:
    base_url = _normalize_ollama_base_url(
        os.getenv("OLLAMA_BASE_URL", DEFAULT_OLLAMA_BASE_URL)
    )
    client_kwargs = {
        "timeout": float(
            timeout if timeout is not None else _env_int("OLLAMA_TIMEOUT", 120)
        )
    }

    return ChatOllama(
        model=os.getenv("OLLAMA_MODEL", DEFAULT_OLLAMA_MODEL),
        base_url=base_url,
        temperature=_env_float("OLLAMA_TEMPERATURE", 0.0),
        reasoning=_env_bool("OLLAMA_REASONING", False),
        num_ctx=_env_optional_int("OLLAMA_NUM_CTX"),
        num_predict=_env_optional_int("OLLAMA_MAX_TOKENS"),
        client_kwargs=client_kwargs,
    )


def create_chat_model(timeout: Optional[float] = None) -> BaseChatModel:
    provider = get_llm_provider()
    if provider == "gigachat":
        return _create_gigachat_chat_model(timeout)
    if provider == "openrouter":
        return _create_openrouter_chat_model(timeout)
    if provider == "ollama":
        return _create_ollama_chat_model(timeout)
    raise ValueError(
        f"Unsupported LLM_PROVIDER={provider!r}. "
        f"Use {', '.join(repr(item) for item in SUPPORTED_LLM_PROVIDERS)}."
    )
