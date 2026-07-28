from unittest.mock import MagicMock, patch

import pytest


def test_load_neo4j_settings_reads_explicit_environment(monkeypatch):
    from graph_storage.config import load_neo4j_settings

    monkeypatch.setenv("NEO4J_URI", "neo4j://graph.example:7687")
    monkeypatch.setenv("NEO4J_USERNAME", "neo4j-user")
    monkeypatch.setenv("NEO4J_PASSWORD", "secret")
    monkeypatch.setenv("NEO4J_DATABASE", "etl")

    settings = load_neo4j_settings()

    assert settings.uri == "neo4j://graph.example:7687"
    assert settings.username == "neo4j-user"
    assert settings.password == "secret"
    assert settings.database == "etl"


def test_is_neo4j_configured_accepts_legacy_neo4j_user(monkeypatch):
    from graph_storage.config import is_neo4j_configured

    monkeypatch.setenv("NEO4J_URI", "bolt://localhost:7687")
    monkeypatch.delenv("NEO4J_USERNAME", raising=False)
    monkeypatch.setenv("NEO4J_USER", "neo4j")
    monkeypatch.setenv("NEO4J_PASSWORD", "secret")
    monkeypatch.delenv("NEO4J_DATABASE", raising=False)

    assert is_neo4j_configured() is True


def test_load_neo4j_settings_raises_when_env_missing(monkeypatch):
    from graph_storage.config import Neo4jConfigurationError, load_neo4j_settings

    for key in (
        "NEO4J_URI",
        "NEO4J_USERNAME",
        "NEO4J_USER",
        "NEO4J_PASSWORD",
        "NEO4J_DATABASE",
    ):
        monkeypatch.setenv(key, "")

    with pytest.raises(Neo4jConfigurationError, match="Neo4j не настроен"):
        load_neo4j_settings()


def test_create_neo4j_driver_only_builds_driver():
    from graph_storage.config import Neo4jSettings
    from graph_storage.connection import create_neo4j_driver

    settings = Neo4jSettings(
        uri="neo4j://localhost:7687",
        username="neo4j",
        password="secret",
        database="neo4j",
    )
    driver = MagicMock()

    with patch(
        "graph_storage.connection.GraphDatabase.driver",
        return_value=driver,
    ) as create_driver:
        result = create_neo4j_driver(settings)

    assert result is driver
    create_driver.assert_called_once_with(
        "neo4j://localhost:7687",
        auth=("neo4j", "secret"),
    )
    driver.execute_query.assert_not_called()


def test_verify_and_close_neo4j_driver_do_not_execute_queries():
    from graph_storage.connection import (
        close_neo4j_driver,
        verify_neo4j_connectivity,
    )

    driver = MagicMock()

    verify_neo4j_connectivity(driver)
    close_neo4j_driver(driver)

    driver.verify_connectivity.assert_called_once_with()
    driver.close.assert_called_once_with()
    driver.execute_query.assert_not_called()


def test_execute_neo4j_read_uses_read_access_transaction():
    from neo4j import READ_ACCESS

    from graph_storage.config import Neo4jSettings
    from graph_storage.read import execute_neo4j_read

    settings = Neo4jSettings(
        uri="neo4j://localhost:7687",
        username="neo4j",
        password="secret",
        database="etl",
    )
    driver = MagicMock()
    session = driver.session.return_value.__enter__.return_value
    transaction = MagicMock()
    record = MagicMock()
    record.data.return_value = {
        "file_id": 7,
        "filename": "mapping.xlsx",
    }
    transaction.run.return_value = [record]
    session.execute_read.side_effect = lambda operation: operation(transaction)

    with patch(
        "graph_storage.read.load_neo4j_settings",
        return_value=settings,
    ), patch(
        "graph_storage.read.create_neo4j_driver",
        return_value=driver,
    ), patch("graph_storage.read.close_neo4j_driver") as close_driver:
        result = execute_neo4j_read(
            "MATCH (file:ETLFile {file_id: $file_id}) RETURN file",
            {"file_id": 7},
        )

    assert result == [
        {
            "file_id": 7,
            "filename": "mapping.xlsx",
        }
    ]
    driver.session.assert_called_once_with(
        database="etl",
        default_access_mode=READ_ACCESS,
    )
    session.execute_read.assert_called_once()
    transaction.run.assert_called_once_with(
        "MATCH (file:ETLFile {file_id: $file_id}) RETURN file",
        file_id=7,
    )
    close_driver.assert_called_once_with(driver)


def test_execute_neo4j_read_closes_driver_after_query_error():
    from graph_storage.config import Neo4jSettings
    from graph_storage.read import execute_neo4j_read

    settings = Neo4jSettings(
        uri="neo4j://localhost:7687",
        username="neo4j",
        password="secret",
        database="etl",
    )
    driver = MagicMock()
    session = driver.session.return_value.__enter__.return_value
    session.execute_read.side_effect = RuntimeError("query failed")

    with patch(
        "graph_storage.read.load_neo4j_settings",
        return_value=settings,
    ), patch(
        "graph_storage.read.create_neo4j_driver",
        return_value=driver,
    ), patch("graph_storage.read.close_neo4j_driver") as close_driver:
        try:
            execute_neo4j_read("MATCH (n) RETURN n")
        except RuntimeError as exc:
            assert str(exc) == "query failed"
        else:
            raise AssertionError("RuntimeError was not raised")

    close_driver.assert_called_once_with(driver)


def test_execute_neo4j_read_stops_after_row_limit():
    from graph_storage.config import Neo4jSettings
    from graph_storage.read import execute_neo4j_read

    settings = Neo4jSettings(
        uri="neo4j://localhost:7687",
        username="neo4j",
        password="secret",
        database="etl",
    )
    driver = MagicMock()
    session = driver.session.return_value.__enter__.return_value
    transaction = MagicMock()
    records = [MagicMock(), MagicMock()]
    records[0].data.return_value = {"value": 1}
    records[1].data.return_value = {"value": 2}
    transaction.run.return_value = records
    session.execute_read.side_effect = lambda operation: operation(transaction)

    with patch(
        "graph_storage.read.load_neo4j_settings",
        return_value=settings,
    ), patch(
        "graph_storage.read.create_neo4j_driver",
        return_value=driver,
    ), patch("graph_storage.read.close_neo4j_driver"):
        result = execute_neo4j_read(
            "MATCH (node) RETURN node.value AS value",
            row_limit=1,
        )

    assert result == [{"value": 1}]
    records[0].data.assert_called_once_with()
    records[1].data.assert_not_called()
