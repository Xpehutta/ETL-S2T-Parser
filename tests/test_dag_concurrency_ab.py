from __future__ import annotations

import ast
import json
import sqlite3
from dataclasses import replace
from pathlib import Path

import scripts.run_dag_concurrency_ab as dag_ab
from scripts.run_dag_concurrency_ab import (
    ARMS,
    CASES,
    REPEATS,
    RunRecord,
    evaluate,
    fixture_errors,
    load_journal,
    permanent_external_failure,
    read_trace,
    record_failures,
    resume_configuration_errors,
    topology_errors,
    validate_journal_prefix,
    validate_spec,
)
from scripts.run_live_agent_benchmark import ModeResult


def _trace(case_name: str, *, concurrency: int = 2) -> dict:
    case = next(item for item in CASES if item.name == case_name)
    if case.topology == "independent_roots":
        dependencies = {"a": [], "b": []}
    elif case.topology == "ready_child":
        dependencies = {"a": [], "b": [], "c": ["a"]}
    elif case.topology == "fan_in":
        dependencies = {"a": [], "b": [], "merge": ["a", "b"]}
    elif case.topology == "fan_out":
        dependencies = {"a": [], "b": ["a"], "c": ["a"]}
    elif case.topology == "failed_parent":
        dependencies = {"a": [], "b": [], "c": ["a"]}
    elif case.topology == "partial_parent":
        dependencies = {"a": [], "b": ["a"]}
    elif case.topology == "eight_roots":
        dependencies = {f"r{index}": [] for index in range(8)}
    else:
        dependencies = {"a": [], "b": ["a"], "c": ["b"]}

    statuses = {step_id: "complete" for step_id in dependencies}
    if case.topology == "failed_parent":
        statuses.update(a="failed", c="blocked_by_dependency")
    elif case.topology == "partial_parent":
        statuses["a"] = "partial"

    plan = [
        {
            "cycle": 1,
            "pipeline": "agentic",
            "id": step_id,
            "task": step_id,
            "depends_on": parents,
        }
        for step_id, parents in dependencies.items()
    ]
    outputs = {
        step_id: ([] if statuses[step_id] in {"failed", "blocked_by_dependency"}
                  else [f"result_{step_id}"])
        for step_id in dependencies
    }
    workers = []
    for index, (step_id, parents) in enumerate(dependencies.items()):
        inputs = [
            result_id
            for parent in parents
            for result_id in outputs[parent]
        ]
        workers.append(
            {
                "step_id": step_id,
                "depends_on": parents,
                "status": statuses[step_id],
                "started_at_seconds": 0.1 + index * 0.1,
                "ended_at_seconds": (
                    1.0 if case.topology == "ready_child" and step_id == "b"
                    else 0.2 + index * 0.1
                ),
                "input_result_ids": inputs,
                "output_result_ids": outputs[step_id],
            }
        )
    depths = {
        "ready_child": 2,
        "fan_in": 2,
        "fan_out": 2,
        "failed_parent": 2,
        "partial_parent": 2,
        "sequential_control": 3,
    }
    widths = {
        "independent_roots": 2,
        "ready_child": 2,
        "fan_in": 2,
        "fan_out": 2,
        "failed_parent": 2,
        "partial_parent": 1,
        "eight_roots": 8,
        "sequential_control": 1,
    }
    return {
        "coordinator_plan": plan,
        "coordinator_dag": [
            {
                "cycle": 1,
                "plan_size": len(plan),
                "dag_depth": depths.get(case.topology, 1),
                "max_parallel_width": widths[case.topology],
                "max_observed_concurrency": concurrency,
                "workers": workers,
            }
        ],
        "upstream_output": {"used_evidence_ids": ["evidence_1"]},
    }


def _result(
    tmp_path: Path,
    *,
    arm_name: str,
    seconds: float,
    concurrency: int,
) -> ModeResult:
    return ModeResult(
        mode="multiagent",
        return_code=0,
        transcript_path=tmp_path / f"{arm_name}.md",
        junit_path=tmp_path / f"{arm_name}.xml",
        passed=1,
        agent_seconds=seconds,
        total_tokens=100,
        dag_observed_concurrency=concurrency,
    )


def _fixture_db(path: Path) -> None:
    with sqlite3.connect(path) as conn:
        for table in (
            "files",
            "source_tables",
            "target_tables",
            "source_columns",
            "target_columns",
            "additional_objects",
            "pxf_to_a",
            "s2t_transformations",
        ):
            conn.execute(f"CREATE TABLE {table} (id INTEGER)")


def test_preregistered_spec_is_complete():
    validate_spec()
    assert dag_ab.COMMON_ENVIRONMENT["GIGACHAT_VERIFY_SSL"] == "0"
    assert dag_ab.COMMON_ENVIRONMENT["EMBEDDING_PROFILE"] == (
        "plain-normalized-v1"
    )
    assert dag_ab.COMMON_ENVIRONMENT["LLM_MAX_CONCURRENCY"] == "1"
    assert dag_ab.COMMON_ENVIRONMENT["GIGACHAT_MAX_RETRIES"] == "3"
    assert dag_ab.COMMON_ENVIRONMENT["GIGACHAT_RETRY_BACKOFF_FACTOR"] == "1"


def test_failed_parent_live_case_forces_a_structured_worker_failure():
    tree = ast.parse(dag_ab.SCENARIO_FILE.read_text(encoding="utf-8"))
    function = next(
        node
        for node in tree.body
        if isinstance(node, ast.FunctionDef)
        and node.name == "test_live_dag_ab_failed_parent"
    )
    scenario = " ".join(
        str(node.value)
        for node in ast.walk(function)
        if isinstance(node, ast.Constant) and isinstance(node.value, str)
    )

    assert "run_sql" in scenario
    assert 'SELECT COUNT(*) FROM "__dag_ab_missing_target_7f31__"' in scenario


def test_fixture_preflight_requires_public_tables(tmp_path):
    db_path = tmp_path / "fixture.db"
    with sqlite3.connect(db_path) as conn:
        conn.execute("CREATE TABLE files (file_id INTEGER)")

    assert fixture_errors(db_path) == [
        "SQLite fixture misses table additional_objects"
    ]
    assert fixture_errors(tmp_path / "missing.db") == [
        f"SQLite fixture does not exist: {tmp_path / 'missing.db'}"
    ]


def test_read_trace_returns_last_agent_trace(tmp_path):
    path = tmp_path / "transcript.md"
    path.write_text(
        "### Agent trace\n```json\n{\"cycle\": 1}\n```\n"
        "### Agent trace\n```json\n{\"cycle\": 2}\n```\n",
        encoding="utf-8",
    )

    assert read_trace(path) == {"cycle": 2}
    assert read_trace(tmp_path / "missing.md") == {}

    path.write_text(
        "### Agent trace\n```json\nnot-json\n```\n",
        encoding="utf-8",
    )
    assert read_trace(path) == {}


def test_all_preregistered_topologies_and_direct_inputs_are_accepted():
    for case in CASES:
        assert topology_errors(
            case,
            _trace(case.name),
            enforce_parallel_timing=True,
        ) == []


def test_sibling_or_transitive_result_leakage_is_rejected():
    case = next(item for item in CASES if item.topology == "fan_in")
    trace = _trace(case.name)
    merge = trace["coordinator_dag"][0]["workers"][2]
    merge["input_result_ids"].append("result_sibling")

    assert topology_errors(case, trace) == [
        "merge: input_result_ids=['result_a', 'result_b', "
        "'result_sibling'], direct=['result_a', 'result_b']"
    ]


def test_blocked_worker_does_not_require_parent_inputs():
    case = next(item for item in CASES if item.topology == "fan_out")
    trace = _trace(case.name)
    child = trace["coordinator_dag"][0]["workers"][1]
    child["status"] = "blocked_by_dependency"
    child["input_result_ids"] = []

    assert topology_errors(case, trace) == []


def test_ready_child_timing_gate_applies_only_when_requested():
    case = next(item for item in CASES if item.topology == "ready_child")
    trace = _trace(case.name)
    child = trace["coordinator_dag"][0]["workers"][2]
    child["started_at_seconds"] = 2.0

    assert topology_errors(case, trace) == []
    assert topology_errors(case, trace, enforce_parallel_timing=True) == [
        "ready child did not start before unrelated root ended"
    ]


def test_topology_gate_rejects_wrong_shape_and_terminal_states():
    for topology, expected in (
        ("independent_roots", "expected exactly two independent roots"),
        ("ready_child", "expected two roots and a single-parent child"),
        ("fan_in", "expected a fan-in node with at least two parents"),
        ("fan_out", "expected one parent with at least two children"),
        ("failed_parent", "expected failed, blocked and independent complete steps"),
        ("partial_parent", "expected a structured partial parent"),
        ("eight_roots", "expected exactly eight independent roots"),
        ("sequential_control", "expected a sequential DAG with depth at least three"),
    ):
        case = next(item for item in CASES if item.topology == topology)
        trace = _trace(case.name)
        trace["coordinator_plan"] = trace["coordinator_plan"][:1]
        trace["coordinator_dag"][0]["workers"] = trace["coordinator_dag"][0][
            "workers"
        ][:1]
        trace["coordinator_dag"][0]["max_parallel_width"] = 2
        trace["coordinator_dag"][0]["dag_depth"] = 1
        if topology == "partial_parent":
            trace["coordinator_dag"][0]["workers"][0]["status"] = "complete"
        errors = topology_errors(case, trace)
        assert expected in errors, (topology, errors)

    case = CASES[0]
    assert topology_errors(case, {}) == [
        "missing coordinator plan or DAG trace"
    ]


def test_topology_gate_rejects_execution_drift():
    case = next(item for item in CASES if item.topology == "fan_out")
    trace = _trace(case.name)
    trace["coordinator_dag"][0]["workers"][1]["depends_on"] = []

    errors = topology_errors(case, trace)

    assert "b: executed topology drift" in errors
    assert "b: input_result_ids=['result_a'], direct=[]" in errors


def test_record_failures_collects_every_hard_gate(tmp_path):
    case = CASES[0]
    result = _result(
        tmp_path,
        arm_name="candidate",
        seconds=1.0,
        concurrency=1,
    )
    result.return_code = 1
    result.failed = 1
    result.skipped = 1
    result.http_500 = 1
    record = RunRecord(
        repeat=1,
        case=case,
        arm=ARMS[1],
        order="AB",
        result=result,
        db_sha256_before="changed",
        db_sha256_after="changed",
        trace=_trace(case.name),
    )
    record.trace["upstream_output"] = None

    failures = record_failures(record, "expected")

    assert "SQLite SHA256 changed" in failures
    assert "pytest run failed" in failures
    assert "live scenario was skipped" in failures
    assert "HTTP 500 observed" in failures
    assert "expected exactly one passed scenario" in failures
    assert "semantic judge did not pass" in failures
    assert "missing upstream evidence provenance" in failures


def test_evaluate_uses_complete_paired_runs(tmp_path):
    records = []
    for repeat in range(1, REPEATS + 1):
        for case in CASES:
            for arm in ARMS:
                candidate = arm.name == "candidate"
                result = _result(
                    tmp_path,
                    arm_name=arm.name,
                    seconds=8.0 if candidate else 10.0,
                    concurrency=2 if candidate else 1,
                )
                result.semantic_statuses[case.name] = "passed"
                records.append(
                    RunRecord(
                        repeat=repeat,
                        case=case,
                        arm=arm,
                        order="AB",
                        result=result,
                        db_sha256_before="same",
                        db_sha256_after="same",
                        trace=_trace(
                            case.name,
                            concurrency=2 if candidate else 1,
                        ),
                    )
                )

    verdict = evaluate(records)

    assert verdict.status == "improved"
    assert verdict.latency_ratio == 0.8
    assert verdict.median_pair_latency_ratio == 0.8
    assert verdict.paired_latency_wins == len(CASES) * REPEATS
    assert verdict.failures == ()

    incomplete = evaluate(records[:-1])
    assert incomplete.status == "inconclusive"
    assert incomplete.failures == ("completed 47/48 runs",)


def test_evaluate_rejects_unpaired_duplicate_and_baseline_parallelism(tmp_path):
    records = []
    for repeat in range(1, REPEATS + 1):
        for case in CASES:
            for arm in ARMS:
                result = _result(
                    tmp_path,
                    arm_name=arm.name,
                    seconds=10.0,
                    concurrency=2,
                )
                records.append(
                    RunRecord(
                        repeat=repeat,
                        case=case,
                        arm=arm,
                        order="BA",
                        result=result,
                        db_sha256_before="same",
                        db_sha256_after="same",
                    )
                )
    records[-1] = replace(records[-1], case=CASES[-2])

    verdict = evaluate(records)

    assert verdict.status == "not_improved"
    assert "usable latency pairs 23/24" in verdict.failures
    assert "baseline observed concurrency exceeds 1" in verdict.failures


def test_main_writes_complete_artifacts_with_stubbed_live_runs(
    monkeypatch,
    tmp_path,
):
    db_path = tmp_path / "fixture.db"
    output_dir = tmp_path / "runs"
    _fixture_db(db_path)
    case = CASES[0]
    monkeypatch.setattr(dag_ab, "REPEATS", 1)
    monkeypatch.setattr(dag_ab, "CASES", (case,))
    monkeypatch.setattr(dag_ab, "validate_spec", lambda: None)

    def fake_run_mode(**kwargs):
        candidate = kwargs["extra_env"]["WORKER_MAX_CONCURRENCY"] == "4"
        transcript_path = kwargs["output_dir"] / (
            f"{kwargs['run_label']}_multiagent.md"
        )
        transcript_path.write_text(
            "### Agent trace\n```json\n"
            + json.dumps(_trace(case.name, concurrency=2 if candidate else 1))
            + "\n```\n",
            encoding="utf-8",
        )
        result = ModeResult(
            mode="multiagent",
            return_code=0,
            transcript_path=transcript_path,
            junit_path=transcript_path.with_suffix(".xml"),
            passed=1,
            agent_seconds=8.0 if candidate else 10.0,
            total_tokens=100,
            dag_observed_concurrency=2 if candidate else 1,
        )
        result.semantic_statuses[case.name] = "passed"
        return result

    monkeypatch.setattr(dag_ab, "_run_mode", fake_run_mode)

    assert dag_ab.main(
        ["--db-path", str(db_path), "--output-dir", str(output_dir)]
    ) == 0
    run_dir = next(output_dir.iterdir())
    assert len(list(run_dir.glob("*_preregistration.md"))) == 1
    assert len(list(run_dir.glob("*_journal.json"))) == 1
    verdict_path = next(
        path
        for path in run_dir.glob("*_verdict.json")
        if "semantic" not in path.name
    )
    assert json.loads(verdict_path.read_text(encoding="utf-8"))["status"] == (
        "improved"
    )
    assert "Verdict: `improved`" in next(run_dir.glob("*_report.md")).read_text(
        encoding="utf-8"
    )


def test_main_stops_on_invalid_fixture(tmp_path, capsys):
    db_path = tmp_path / "invalid.db"
    db_path.write_text("not sqlite", encoding="utf-8")

    assert dag_ab.main(
        ["--db-path", str(db_path), "--output-dir", str(tmp_path / "runs")]
    ) == 2
    assert "fixture preflight failed" in capsys.readouterr().out


def test_main_aborts_after_pre_scenario_subprocess_failure(
    monkeypatch,
    tmp_path,
):
    db_path = tmp_path / "fixture.db"
    output_dir = tmp_path / "runs"
    _fixture_db(db_path)
    monkeypatch.setattr(dag_ab, "validate_spec", lambda: None)
    calls = []

    def fake_run_mode(**kwargs):
        calls.append(kwargs)
        transcript_path = kwargs["output_dir"] / "failed.md"
        return ModeResult(
            mode="multiagent",
            return_code=4,
            transcript_path=transcript_path,
            junit_path=transcript_path.with_suffix(".xml"),
        )

    monkeypatch.setattr(dag_ab, "_run_mode", fake_run_mode)

    assert dag_ab.main(
        ["--db-path", str(db_path), "--output-dir", str(output_dir)]
    ) == 2
    assert len(calls) == 1
    report = next(output_dir.glob("*/*_report.md")).read_text(encoding="utf-8")
    assert "Verdict: `inconclusive`" in report
    assert "live subprocess failed before scenario execution" in report


def test_journal_round_trip_and_prefix_validation(tmp_path):
    case = CASES[0]
    result = _result(
        tmp_path,
        arm_name="baseline",
        seconds=1.0,
        concurrency=1,
    )
    record = RunRecord(
        repeat=1,
        case=case,
        arm=ARMS[0],
        order="AB",
        result=result,
        db_sha256_before="same",
        db_sha256_after="same",
        trace=_trace(case.name),
    )
    path = tmp_path / "journal.json"
    dag_ab._write_json(path, [record])

    loaded = load_journal(path)

    validate_journal_prefix(loaded)
    assert loaded[0].result.transcript_path == result.transcript_path
    loaded[0].order = "BA"
    try:
        validate_journal_prefix(loaded)
    except ValueError as exc:
        assert "diverges at record 1" in str(exc)
    else:
        raise AssertionError("invalid journal prefix was accepted")


def test_resume_rejects_changed_preregistered_transport_settings():
    config = dag_ab._json_value(dag_ab.frozen_experiment_config())
    assert resume_configuration_errors(config) == []

    config["common_environment"]["LLM_MAX_CONCURRENCY"] = "2"
    assert resume_configuration_errors(config) == [
        "preregistered field changed: common_environment"
    ]


def test_permanent_external_failure_detects_payment_required(tmp_path):
    result = _result(
        tmp_path,
        arm_name="candidate",
        seconds=0.0,
        concurrency=0,
    )
    assert permanent_external_failure(result) == ""
    result.transcript_path.write_text(
        "ResponseError: 402 Payment Required",
        encoding="utf-8",
    )
    assert permanent_external_failure(result) == (
        "GigaChat returned 402 Payment Required"
    )

    result.transcript_path.write_text(
        "Ошибка LLM supervisor: ResponseError",
        encoding="utf-8",
    )
    result.return_code = 1
    result.llm_calls = 1
    result.judge_errors = 3
    result.total_tokens = 0
    assert permanent_external_failure(result) == (
        "GigaChat failed before producing tokens for agent and judge"
    )


def test_main_finalizes_existing_partial_journal_without_live_calls(
    monkeypatch,
    tmp_path,
):
    db_path = tmp_path / "fixture.db"
    _fixture_db(db_path)
    run_dir = tmp_path / "20260921_000000"
    run_dir.mkdir()
    timestamp = run_dir.name
    initial_hash = dag_ab.sqlite_sha256(db_path)
    dag_ab._write_json(
        run_dir / f"{timestamp}_config.json",
        {"initial_db_sha256": initial_hash},
    )
    result = _result(
        run_dir,
        arm_name="baseline",
        seconds=1.25,
        concurrency=1,
    )
    record = RunRecord(
        repeat=1,
        case=CASES[0],
        arm=ARMS[0],
        order="AB",
        result=result,
        db_sha256_before=initial_hash,
        db_sha256_after=initial_hash,
        trace=_trace(CASES[0].name),
    )
    dag_ab._write_json(
        run_dir / f"{timestamp}_journal.json",
        [record],
    )
    monkeypatch.setattr(dag_ab, "validate_spec", lambda: None)
    monkeypatch.setattr(
        dag_ab,
        "_run_mode",
        lambda **kwargs: (_ for _ in ()).throw(
            AssertionError("live execution must not start")
        ),
    )

    assert dag_ab.main(
        [
            "--db-path",
            str(db_path),
            "--resume-dir",
            str(run_dir),
            "--finalize-incomplete",
        ]
    ) == 2

    verdict = json.loads(
        (run_dir / f"{timestamp}_verdict.json").read_text(encoding="utf-8")
    )
    assert verdict["status"] == "inconclusive"
    assert verdict["baseline_seconds"] == 1.25
    assert verdict["failures"][:2] == [
        "run finalized incomplete after an external service failure",
        "completed 1/48 runs",
    ]


def test_main_refuses_to_resume_a_changed_preregistration(
    monkeypatch,
    tmp_path,
    capsys,
):
    db_path = tmp_path / "fixture.db"
    _fixture_db(db_path)
    run_dir = tmp_path / "20260921_000001"
    run_dir.mkdir()
    timestamp = run_dir.name
    config = dag_ab._json_value(dag_ab.frozen_experiment_config())
    config["common_environment"]["LLM_MAX_CONCURRENCY"] = "2"
    config["initial_db_sha256"] = dag_ab.sqlite_sha256(db_path)
    dag_ab._write_json(run_dir / f"{timestamp}_config.json", config)
    dag_ab._write_json(run_dir / f"{timestamp}_journal.json", [])
    monkeypatch.setattr(dag_ab, "validate_spec", lambda: None)
    monkeypatch.setattr(
        dag_ab,
        "_run_mode",
        lambda **kwargs: (_ for _ in ()).throw(
            AssertionError("changed experiment must not resume")
        ),
    )

    assert dag_ab.main(
        ["--db-path", str(db_path), "--resume-dir", str(run_dir)]
    ) == 2
    assert "preregistered field changed: common_environment" in (
        capsys.readouterr().out
    )
