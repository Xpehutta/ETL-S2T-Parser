from services.embeddings import DEFAULT_EMBEDDING_MODEL, embedding_model_name


def test_embedding_model_name_uses_default_when_env_missing(monkeypatch):
    monkeypatch.delenv("EMBEDDING_MODEL", raising=False)
    assert embedding_model_name() == DEFAULT_EMBEDDING_MODEL


def test_embedding_model_name_reads_env(monkeypatch):
    monkeypatch.setenv("EMBEDDING_MODEL", "custom/model")
    assert embedding_model_name() == "custom/model"
