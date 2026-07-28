"""Neo4j configuration loaded independently from the application runtime."""

import os
from dataclasses import dataclass

from dotenv import load_dotenv


class Neo4jConfigurationError(RuntimeError):
    """Raised when Neo4j connection settings are missing or incomplete."""


NEO4J_ENV_KEYS = (
    "NEO4J_URI",
    "NEO4J_USERNAME",
    "NEO4J_PASSWORD",
    "NEO4J_DATABASE",
)


@dataclass(frozen=True)
class Neo4jSettings:
    """Connection settings for an external Neo4j database."""

    uri: str
    username: str
    password: str
    database: str


def is_neo4j_configured() -> bool:
    """Return True when all Neo4j connection variables are present."""
    load_dotenv()
    username = (os.getenv("NEO4J_USERNAME") or os.getenv("NEO4J_USER") or "").strip()
    return all(
        [
            (os.getenv("NEO4J_URI") or "").strip(),
            username,
            (os.getenv("NEO4J_PASSWORD") or "").strip(),
            (os.getenv("NEO4J_DATABASE") or "neo4j").strip(),
        ]
    )


def load_neo4j_settings() -> Neo4jSettings:
    """Load Neo4j connection settings from the project environment."""
    load_dotenv()
    username = (os.getenv("NEO4J_USERNAME") or os.getenv("NEO4J_USER") or "").strip()
    password = (os.getenv("NEO4J_PASSWORD") or "").strip()
    uri = (os.getenv("NEO4J_URI") or "").strip()
    database = (os.getenv("NEO4J_DATABASE") or "neo4j").strip()
    missing = []
    if not uri:
        missing.append("NEO4J_URI")
    if not username:
        missing.append("NEO4J_USERNAME")
    if not password:
        missing.append("NEO4J_PASSWORD")
    if not database:
        missing.append("NEO4J_DATABASE")
    if missing:
        raise Neo4jConfigurationError(
            "Neo4j не настроен: задайте в .env "
            + ", ".join(missing)
            + ". Синхронизация lineage опциональна; SQLite-анализ работает без Neo4j."
        )
    return Neo4jSettings(
        uri=uri,
        username=username,
        password=password,
        database=database,
    )
