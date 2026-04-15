import os
import logging
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

_langfuse_client = None

def get_langfuse_client():
    global _langfuse_client
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