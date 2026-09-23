import json

from scripts import run_word9_small_model_benchmark as benchmark


def test_registry_has_requested_local_models_and_word_split():
    assert benchmark.DEFAULT_LOCAL_MODELS == (
        "qwen3.5:9b",
        "qwen3:8b",
        "ministral-3:8b",
        "deepseek-r1:8b",
        "llama3.1:8b",
    )
    assert [case.number for case in benchmark.REFERENCE_CASES] == list(range(1, 8))
    assert len({case.scenario for case in benchmark.REFERENCE_CASES}) == 7
    assert len(benchmark.WORD_OFFLINE_GATES) == 2


def test_references_are_fixed_nonempty_human_examples():
    assert all(case.query.strip() for case in benchmark.REFERENCE_CASES)
    assert all(case.reference_answer.strip() for case in benchmark.REFERENCE_CASES)
    assert "написанный человеком" in benchmark.JUDGE_PROMPT.casefold()


def test_dry_run_preregisters_references_without_model_calls(tmp_path, monkeypatch):
    database = tmp_path / "fixture.db"
    database.write_bytes(b"fixture")
    output_dir = tmp_path / "benchmark"
    monkeypatch.setattr(
        benchmark,
        "_sha256",
        lambda path: benchmark.EXPECTED_FIXTURE_SHA256,
    )

    result = benchmark.main(
        [
            "--db",
            str(database),
            "--output-dir",
            str(output_dir),
            "--model",
            "deepseek-r1:8b",
            "--dry-run",
        ]
    )

    assert result == 0
    payload = json.loads(
        (output_dir / "preregistration.json").read_text(encoding="utf-8")
    )
    assert payload["reference_source"] == "human_authored_static"
    assert payload["reference_judge_model"] == "GigaChat-2-Max"
    assert payload["candidate_provider"] == "ollama"
    assert payload["candidate_models"] == ["deepseek-r1:8b"]
    assert len(payload["references"]) == 7
