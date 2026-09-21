import ast
from pathlib import Path

import pytest

from scripts import run_live_agent_benchmark as benchmark
from scripts.run_live_agent_benchmark import (
    LIVE_SCENARIO_GROUP_MARKERS,
    SCENARIO_FILE,
    ModeResult,
    _comparison_report,
    _group_pytest_args,
    _parse_transcript,
    _selected_scenario_count,
    _scenario_mark,
    build_parser,
)


def test_benchmark_parser_sums_multiline_execution_metrics(tmp_path):
    transcript = tmp_path / "run.md"
    transcript.write_text(
        """
### Ответ — HTTP 200
agent_seconds: 1.250
llm_calls: 3
tokens: input=100, output=20, total=120, cache_read=10
stage_tokens[supervisor]: calls=1, errors=0, input=20, output=5, total=25, cache_read=2, seconds=0.250
stage_tokens[router]: calls=2, errors=0, input=80, output=15, total=95, cache_read=8, seconds=1.000
reader_calls: 1
tool_errors: 0
reroutes: 0
pipelines: agentic
dag_cycles: 1
dag_depth: 3
dag_max_parallel_width: 4
dag_observed_concurrency: 3
dag_blocked: 0
dag_cancelled: 0
dag_statuses: {"complete":4}
dag_input_result_ids: ["result_a"]
dag_output_result_ids: ["result_b"]
tools: run_sql
judge_model: GigaChat-2-Max
judge_calls: attempts=3, completed=2, errors=1
judge_tokens: input=300, output=30, total=330, cache_read=0

### Ответ — HTTP 500
agent_seconds: 2.750
llm_calls: 5
tokens: input=200, output=30, total=230, cache_read=15
stage_tokens[supervisor]: calls=1, errors=0, input=30, output=5, total=35, cache_read=3, seconds=0.500
stage_tokens[upstream]: calls=4, errors=1, input=170, output=25, total=195, cache_read=12, seconds=2.000
reader_calls: 2
tool_errors: 1
reroutes: 2
pipelines: validation_protocol, agentic
dag_cycles: 2
dag_depth: 2
dag_max_parallel_width: 2
dag_observed_concurrency: 2
dag_blocked: 1
dag_cancelled: 1
dag_statuses: {"complete":2,"blocked_by_dependency":1,"cancelled_running":1}
dag_input_result_ids: ["result_b","result_c"]
dag_output_result_ids: ["result_d"]
tools: run_sql, run_cypher
judge_model: GigaChat-2-Max
judge_calls: attempts=1, completed=1, errors=0
judge_tokens: input=150, output=15, total=165, cache_read=5
<!-- LIVE_WARNING {"category":"presentation","scenario":"test_live_agent_path","message":"missing display"} -->
<!-- LIVE_WARNING {"category":"efficiency","scenario":"test_live_agent_path","message":"llm_calls=14 exceeds budget=12"} -->
<!-- LIVE_SEMANTIC {"scenario":"test_live_agent_path","status":"not_evaluated"} -->
""".strip(),
        encoding="utf-8",
    )
    result = ModeResult(
        mode="multiagent",
        return_code=1,
        transcript_path=transcript,
        junit_path=Path("missing.xml"),
    )

    _parse_transcript(result)

    assert result.measured_runs == 2
    assert result.agent_seconds == 4.0
    assert result.llm_calls == 8
    assert result.tool_calls == 3
    assert result.reader_calls == 3
    assert result.tool_errors == 1
    assert result.reroutes == 2
    assert result.pipelines == {"agentic": 2, "validation_protocol": 1}
    assert result.dag_cycles == 3
    assert result.dag_depth == 3
    assert result.dag_max_parallel_width == 4
    assert result.dag_observed_concurrency == 3
    assert result.dag_blocked == 1
    assert result.dag_cancelled == 1
    assert result.dag_statuses == {
        "complete": 6,
        "blocked_by_dependency": 1,
        "cancelled_running": 1,
    }
    assert result.dag_input_result_ids == [
        "result_a",
        "result_b",
        "result_c",
    ]
    assert result.dag_output_result_ids == ["result_b", "result_d"]
    assert result.input_tokens == 300
    assert result.output_tokens == 50
    assert result.total_tokens == 350
    assert result.cache_read_tokens == 25
    assert result.judge_models == {"GigaChat-2-Max": 2}
    assert result.judge_attempts == 4
    assert result.judge_completed == 3
    assert result.judge_errors == 1
    assert result.judge_input_tokens == 450
    assert result.judge_output_tokens == 45
    assert result.judge_total_tokens == 495
    assert result.judge_cache_read_tokens == 5
    assert result.stage_usage == {
        "supervisor": {
            "calls": 2,
            "errors": 0,
            "input_tokens": 50,
            "output_tokens": 10,
            "total_tokens": 60,
            "cache_read_tokens": 5,
            "elapsed_seconds": 0.75,
        },
        "router": {
            "calls": 2,
            "errors": 0,
            "input_tokens": 80,
            "output_tokens": 15,
            "total_tokens": 95,
            "cache_read_tokens": 8,
            "elapsed_seconds": 1.0,
        },
        "upstream": {
            "calls": 4,
            "errors": 1,
            "input_tokens": 170,
            "output_tokens": 25,
            "total_tokens": 195,
            "cache_read_tokens": 12,
            "elapsed_seconds": 2.0,
        },
    }
    assert result.http_500 == 1
    assert result.presentation_warnings == 1
    assert result.efficiency_warnings == 1
    assert result.scenario_warnings == {
        "test_live_agent_path": ["presentation", "efficiency"]
    }
    assert result.warning_details == [
        {
            "category": "presentation",
            "scenario": "test_live_agent_path",
            "message": "missing display",
        },
        {
            "category": "efficiency",
            "scenario": "test_live_agent_path",
            "message": "llm_calls=14 exceeds budget=12",
        },
    ]
    assert result.semantic_statuses == {
        "test_live_agent_path": "not_evaluated"
    }


def test_benchmark_report_marks_semantics_as_not_evaluated(tmp_path):
    report = tmp_path / "comparison.md"
    result = ModeResult(
        mode="multiagent",
        return_code=0,
        transcript_path=tmp_path / "run.md",
        junit_path=tmp_path / "run.xml",
        passed=1,
        scenario_statuses={"test_live_agent_path": "passed"},
        semantic_statuses={"test_live_agent_path": "not_evaluated"},
        presentation_warnings=1,
        efficiency_warnings=1,
        reader_calls=3,
        tool_errors=1,
        reroutes=2,
        pipelines={"agentic": 1},
        warning_details=[
            {
                "category": "presentation",
                "scenario": "test_live_agent_path",
                "message": "missing display",
            },
            {
                "category": "efficiency",
                "scenario": "test_live_agent_path",
                "message": "extra call",
            },
        ],
        scenario_warnings={
            "test_live_agent_path": ["presentation", "efficiency"]
        },
        stage_usage={
            "upstream": {
                "calls": 2,
                "errors": 0,
                "input_tokens": 100,
                "output_tokens": 20,
                "total_tokens": 120,
                "cache_read_tokens": 10,
                "elapsed_seconds": 1.25,
            }
        },
        judge_models={"GigaChat-2-Max": 1},
        judge_attempts=3,
        judge_completed=2,
        judge_errors=1,
        judge_input_tokens=200,
        judge_output_tokens=30,
        judge_total_tokens=230,
        judge_cache_read_tokens=5,
        dag_cycles=2,
        dag_depth=3,
        dag_max_parallel_width=4,
        dag_observed_concurrency=3,
        dag_blocked=1,
        dag_cancelled=1,
        dag_statuses={"complete": 5, "blocked_by_dependency": 1},
        dag_input_result_ids=["result_a"],
        dag_output_result_ids=["result_b"],
    )

    _comparison_report(
        provider="gigachat",
        model="GigaChat-3-Ultra",
        results=[result],
        report_path=report,
    )

    text = report.read_text(encoding="utf-8")
    assert "Pytest passed" in text
    assert "Pytest failures" in text
    assert "Semantic failures" in text
    assert "Presentation warnings" in text
    assert "Efficiency warnings" in text
    assert "📝 ⚠P×1 ⚠E×1" in text
    assert "LLM-as-judge" in text
    assert "сценарий в failed" in text
    assert "## Расход LLM по этапам" in text
    assert "Accuracy" in text
    assert "Reroutes" in text
    assert "Tool errors" in text
    assert "Reader calls" in text
    assert "Judge attempts" in text
    assert "Judge errors" in text
    assert "Judge tokens" in text
    assert "## Расход LLM-as-judge" in text
    assert "GigaChat-2-Max×1" in text
    assert "| multiagent | GigaChat-2-Max×1 | 3 | 2 | 1 | 200 | 30 | 230 | 5 |" in text
    assert "agentic×1" in text
    assert "100.0%" in text
    assert "| multiagent | upstream | 2 | 0 | 100 | 20 | 120 | 10 | 1.250 |" in text
    assert "## DAG execution" in text
    assert "| multiagent | 2 | 3 | 4 | 3 | 1 | 1 |" in text
    assert "blocked_by_dependency×1" in text
    assert "`result_a`" in text
    assert "`result_b`" in text


def test_benchmark_mark_uses_llm_judge_verdict():
    result = ModeResult(
        mode="multiagent",
        return_code=0,
        transcript_path=Path("run.md"),
        junit_path=Path("run.xml"),
        scenario_statuses={
            "semantic-pass": "passed",
            "semantic-fail": "passed",
            "judge-error": "passed",
        },
        semantic_statuses={
            "semantic-pass": "passed",
            "semantic-fail": "failed",
            "judge-error": "judge_error",
        },
    )

    assert _scenario_mark(result, "semantic-pass") == "✅"
    assert _scenario_mark(result, "semantic-fail") == "❌"
    assert _scenario_mark(result, "judge-error") == "💥"


def test_mode_result_accuracy_counts_every_selected_scenario():
    result = ModeResult(
        mode="multiagent",
        return_code=1,
        transcript_path=Path("run.md"),
        junit_path=Path("run.xml"),
        passed=3,
        failed=1,
        skipped=1,
    )

    assert result.selected_scenarios == 5
    assert result.accuracy == pytest.approx(0.6)


def test_live_group_filter_builds_stable_or_expression():
    assert _group_pytest_args([]) == []
    assert _group_pytest_args(["history", "catalog", "history"]) == [
        "-m",
        "live_history or live_catalog",
    ]
    with pytest.raises(ValueError, match="unknown live scenario group: missing"):
        _group_pytest_args(["missing"])


def test_benchmark_parser_accepts_only_named_live_groups():
    parser = build_parser()

    args = parser.parse_args(["--group", "history", "--group", "handoff"])

    assert args.group == ["history", "handoff"]
    with pytest.raises(SystemExit) as exc_info:
        parser.parse_args(["--group", "missing"])
    assert exc_info.value.code == 2


def test_benchmark_parser_accepts_resolution_group():
    args = build_parser().parse_args(["--group", "resolution"])

    assert args.group == ["resolution"]
    assert _group_pytest_args(args.group) == ["-m", "live_resolution"]


@pytest.mark.parametrize(
    ("llm_judge", "expected_judge_flag"),
    [(False, "0"), (True, "1")],
)
def test_run_mode_applies_binary_environment_and_extra_values_last(
    monkeypatch,
    tmp_path,
    llm_judge,
    expected_judge_flag,
):
    observed = {}

    def fake_run(command, *, cwd, env, check):
        observed.update({"command": command, "cwd": cwd, "env": env, "check": check})

        class Completed:
            returncode = 0

        return Completed()

    monkeypatch.setattr(benchmark.subprocess, "run", fake_run)

    benchmark._run_mode(
        mode="multiagent",
        provider="ollama",
        model="base-model",
        targets=[str(SCENARIO_FILE)],
        pytest_args=[],
        output_dir=tmp_path,
        run_label="extra-env",
        llm_judge=llm_judge,
        extra_env={"OLLAMA_MODEL": "experiment-model", "E1_VARIANT": "capability"},
    )

    assert observed["env"]["OLLAMA_MODEL"] == "experiment-model"
    assert observed["env"]["E1_VARIANT"] == "capability"
    assert observed["env"]["RUN_LIVE_AGENT_SCENARIOS"] == "1"
    assert observed["env"]["LIVE_AGENT_LLM_JUDGE"] == expected_judge_flag


def test_benchmark_loads_dotenv_before_resolving_model(
    monkeypatch,
    tmp_path,
):
    events = []
    monkeypatch.delenv("GIGACHAT_MODEL", raising=False)
    monkeypatch.delenv("MODEL", raising=False)

    def fake_load_dotenv(path, *, override):
        events.append(("dotenv", path, override))
        # The runtime factory supports legacy MODEL as a GigaChat fallback.
        monkeypatch.setenv("MODEL", "GigaChat-3-Ultra")

    def fake_run_mode(**kwargs):
        events.append(("run", kwargs["model"]))
        return ModeResult(
            mode=kwargs["mode"],
            return_code=0,
            transcript_path=tmp_path / "run.md",
            junit_path=tmp_path / "run.xml",
        )

    monkeypatch.setattr(benchmark, "load_dotenv", fake_load_dotenv)
    monkeypatch.setattr(benchmark, "_run_mode", fake_run_mode)
    monkeypatch.setattr(benchmark, "_comparison_report", lambda **kwargs: None)

    assert benchmark.main(
        [
            "--provider",
            "gigachat",
            "--modes",
            "multiagent",
            "--scenario",
            "test_live_agent_resolves_history_reference_into_task",
            "--output-dir",
            str(tmp_path),
        ]
    ) == 0

    assert events[0] == ("dotenv", benchmark.PROJECT_ROOT / ".env", False)
    assert events[1:] == [("run", "GigaChat-3-Ultra")]


def test_selected_scenario_count_supports_group_and_exact_target():
    all_count = _selected_scenario_count([], [])
    whole_file_count = _selected_scenario_count(
        ["tests/test_live_agent_scenarios.py"],
        [],
    )
    history_count = _selected_scenario_count([], ["history"])
    one_count = _selected_scenario_count(
        ["test_live_agent_resolves_history_reference_into_task"],
        ["history"],
    )
    shared_resolution_count = _selected_scenario_count(
        ["test_live_validation_and_agentic_use_same_resolution_semantics"],
        ["resolution"],
    )

    assert whole_file_count == all_count
    assert all_count >= history_count >= 1
    assert one_count == 1
    assert shared_resolution_count == 2

    with pytest.raises(ValueError, match="supports only tests"):
        _selected_scenario_count(["tests/test_worker.py"], [])


def test_benchmark_main_combines_exact_scenario_with_group(
    monkeypatch,
    tmp_path,
):
    calls = []

    def fake_run_mode(**kwargs):
        calls.append(kwargs)
        return ModeResult(
            mode=kwargs["mode"],
            return_code=0,
            transcript_path=tmp_path / "run.md",
            junit_path=tmp_path / "run.xml",
        )

    monkeypatch.setattr(benchmark, "_run_mode", fake_run_mode)
    monkeypatch.setattr(benchmark, "_comparison_report", lambda **kwargs: None)

    return_code = benchmark.main(
        [
            "--provider",
            "ollama",
            "--modes",
            "multiagent",
            "--scenario",
            "test_live_agent_resolves_history_reference_into_task",
            "--group",
            "history",
            "--output-dir",
            str(tmp_path),
        ]
    )

    assert return_code == 0
    assert len(calls) == 1
    assert calls[0]["targets"] == [
        f"{SCENARIO_FILE}::test_live_agent_resolves_history_reference_into_task"
    ]
    assert calls[0]["pytest_args"] == ["-m", "live_history"]


def test_benchmark_rejects_competing_marker_expressions(tmp_path):
    with pytest.raises(SystemExit) as exc_info:
        benchmark.main(
            [
                "--group",
                "history",
                "--pytest-arg=-m",
                "--pytest-arg=live_graph",
                "--output-dir",
                str(tmp_path),
            ]
        )

    assert exc_info.value.code == 2


def _pytest_marker_name(decorator: ast.expr) -> str | None:
    target = decorator.func if isinstance(decorator, ast.Call) else decorator
    if not isinstance(target, ast.Attribute):
        return None
    mark = target.value
    if not isinstance(mark, ast.Attribute) or mark.attr != "mark":
        return None
    if not isinstance(mark.value, ast.Name) or mark.value.id != "pytest":
        return None
    return target.attr


def test_every_live_scenario_belongs_to_exactly_one_semantic_group():
    tree = ast.parse(SCENARIO_FILE.read_text(encoding="utf-8"))
    group_markers = set(LIVE_SCENARIO_GROUP_MARKERS.values())
    assignments = {}
    for node in tree.body:
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        if not node.name.startswith("test_live_"):
            continue
        markers = [
            marker
            for decorator in node.decorator_list
            if (marker := _pytest_marker_name(decorator)) in group_markers
        ]
        assignments[node.name] = markers

    assert assignments
    assert all(len(markers) == 1 for markers in assignments.values()), assignments
    assert {markers[0] for markers in assignments.values()} == group_markers


def test_improvement_plan_live_scenarios_are_named_and_grouped_exactly():
    validation_names = {
        "test_live_validation_protocol_standard_mode",
        "test_live_validation_protocol_exhaustive_mode",
        "test_live_validation_protocol_key_reconciliation",
        "test_live_validation_protocol_field_level_reconciliation",
        "test_live_validation_protocol_preload_constraint_checks",
        "test_live_validation_protocol_expression_projection",
        "test_live_validation_protocol_explicit_key_without_catalog_pk",
        "test_live_validation_protocol_separate_load_scopes",
        "test_live_validation_protocol_minimal_readers",
        "test_live_validation_protocol_table_typo_resolution",
        "test_live_validation_protocol_ambiguous_typo",
        "test_live_validation_protocol_semantic_file_resolution",
        "test_live_validation_protocol_without_file",
        "test_live_validation_protocol_without_file_no_catalog_dependency",
        "test_live_validation_protocol_source_catalog_dependency",
    }
    resolution_names = {
        "test_live_agent_resolves_table_typo_before_exact_reader",
        "test_live_agent_skips_resolution_for_exact_table",
        "test_live_agent_resolves_partial_table_name",
        "test_live_agent_resolves_semantic_table_mention",
        "test_live_agent_does_not_guess_ambiguous_entity",
        "test_live_entity_resolution_preserves_source_target_role",
        "test_live_validation_and_agentic_use_same_resolution_semantics",
    }
    assert len(validation_names | resolution_names) == 22

    tree = ast.parse(SCENARIO_FILE.read_text(encoding="utf-8"))
    functions = {
        node.name: node
        for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }
    assert validation_names | resolution_names <= set(functions)
    for name in validation_names:
        markers = {_pytest_marker_name(item) for item in functions[name].decorator_list}
        assert "live_validation" in markers, name
        assert "live_resolution" not in markers, name
    for name in resolution_names:
        markers = {_pytest_marker_name(item) for item in functions[name].decorator_list}
        assert "live_resolution" in markers, name
        assert "live_validation" not in markers, name
