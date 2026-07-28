import os
import logging
from contextlib import nullcontext
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

_langfuse_client = None


def is_langfuse_configured() -> bool:
    enabled = (os.getenv("LANGFUSE_ENABLED") or "true").strip().lower()
    if enabled in {"0", "false", "no", "off"}:
        return False
    return bool(
        (os.getenv("LANGFUSE_PUBLIC_KEY") or "").strip()
        and (os.getenv("LANGFUSE_SECRET_KEY") or "").strip()
    )

def get_langfuse_client():
    global _langfuse_client
    if not is_langfuse_configured():
        return None
    if _langfuse_client is None:
        try:
            from langfuse import get_client
            _langfuse_client = get_client()
            if os.getenv("LANGFUSE_TRACING_ENVIRONMENT") == "development":
                try:
                    _langfuse_client.auth_check()
                    logger.info("Langfuse client authenticated successfully")
                except Exception as e:
                    logger.warning(f"Langfuse auth check failed: {e}")
        except ImportError:
            logger.warning("Langfuse not installed. Tracing disabled.")
        except Exception as e:
            logger.warning(f"Langfuse initialization failed: {e}")
    return _langfuse_client


def get_callback_handler():
    """Return Langfuse callback handler for LangGraph if available."""
    client = get_langfuse_client()
    if client:
        try:
            from langfuse.langchain import CallbackHandler
            return CallbackHandler()
        except Exception as e:
            logger.warning(f"Failed to create CallbackHandler: {e}")
    return None


def langfuse_trace_context(
    *,
    trace_name: Optional[str] = None,
    session_id: Optional[str] = None,
    user_id: Optional[str] = None,
    metadata: Optional[Dict[str, Any]] = None,
    tags: Optional[List[str]] = None,
):
    """Return a propagate_attributes context manager when Langfuse is configured."""
    if not get_langfuse_client():
        return nullcontext()

    clean_metadata = {
        key: value
        for key, value in (metadata or {}).items()
        if value is not None
    }
    clean_tags = [tag for tag in (tags or []) if isinstance(tag, str) and tag.strip()]
    try:
        from langfuse import propagate_attributes
    except ImportError:
        return nullcontext()

    kwargs: Dict[str, Any] = {
        "environment": (os.getenv("LANGFUSE_TRACING_ENVIRONMENT") or "").strip() or None,
        "trace_name": trace_name,
        "session_id": session_id,
        "user_id": user_id,
        "metadata": clean_metadata or None,
        "tags": clean_tags or None,
    }
    return propagate_attributes(**kwargs)
