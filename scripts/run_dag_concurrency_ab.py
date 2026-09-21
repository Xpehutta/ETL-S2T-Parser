#!/usr/bin/env python
"""Run the preregistered multiagent DAG concurrency A/B experiment."""

from __future__ import annotations

import argparse
import ast
import hashlib
import json
import os
import sqlite3
import statistics
from dataclasses import dataclass, field, fields
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

from dotenv import load_dotenv

try:
    from scripts.run_live_agent_benchmark import (
        DEFAULT_OUTPUT_DIR,
        SCENARIO_FILE,
        ModeResult,
        _run_mode,
        _scenario_targets,
        _selected_scenario_count,
        _slug,
    )
except ModuleNotFoundError:  # direct ``python scripts/...`` execution
    from run_live_agent_benchmark import (  # type: ignore[no-redef]
        DEFAULT_OUTPUT_DIR,
        SCENARIO_FILE,
        ModeResult,
        _run_mode,
        _scenario_targets,
        _selected_scenario_count,
        _slug,
    )


PROJECT_ROOT = Path(__file__).resolve().parents[1]
PROVIDER = "gigachat"
MODEL = "GigaChat-2-Max"
REPEATS = 3
DEFAULT_OUTPUT_DIR_DAG = DEFAULT_OUTPUT_DIR / "dag_concurrency_ab"
LATENCY_IMPROVEMENT_RATIO = 0.95
TOKEN_GUARD_RATIO = 1.10
HARD_PYTEST_ARGS = (
    "-W",
    "error:live presentation warning:UserWarning",
)


@dataclass(frozen=True)
class DagAbCase:
    name: str
    topology: str
    hypothesis: str


@dataclass(frozen=True)
class DagAbArm:
    name: str
    worker_max_concurrency: int


@dataclass
class RunRecord:
    repeat: int
    case: DagAbCase
    arm: DagAbArm
    order: str
    result: ModeResult
    db_sha256_before: str
    db_sha256_after: str
    trace: dict[str, Any] = field(default_factory=dict)
    failures: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class DagAbVerdict:
    status: str
    failures: tuple[str, ...]
    baseline_seconds: float
    candidate_seconds: float
    latency_ratio: float
    median_pair_latency_ratio: float
    paired_latency_wins: int
    token_ratio: float
    candidate_observed_concurrency: int


CASES = (
    DagAbCase(
        "test_live_dag_ab_independent_roots",
        "independent_roots",
        "Two independent reads can overlap without changing their answer.",
    ),
    DagAbCase(
        "test_live_dag_ab_ready_child",
        "ready_child",
        "A child unlocked by a fast root does not wait for an unrelated root.",
    ),
    DagAbCase(
        "test_live_dag_ab_fan_in",
        "fan_in",
        "A merge reads only the two direct parent results.",
    ),
    DagAbCase(
        "test_live_dag_ab_fan_out",
        "fan_out",
        "Two children of one parent can become ready together.",
    ),
    DagAbCase(
        "test_live_dag_ab_failed_parent",
        "failed_parent",
        "A failed parent blocks descendants while an independent root finishes.",
    ),
    DagAbCase(
        "test_live_dag_ab_partial_parent",
        "partial_parent",
        "A usable partial parent preserves explicit lineage for its child.",
    ),
    DagAbCase(
        "test_live_dag_ab_eight_roots",
        "eight_roots",
        "The maximum eight-root plan remains bounded and leak-free.",
    ),
    DagAbCase(
        "test_live_dag_ab_sequential_control",
        "sequential_control",
        "A linear control sees no artificial concurrency benefit.",
    ),
)
ARMS = (
    DagAbArm("baseline", 1),
    DagAbArm("candidate", 4),
)
ORDERS = {
    "AB": ("baseline", "candidate"),
    "BA": ("candidate", "baseline"),
}
COMMON_ENVIRONMENT = {
    "CHAT_AGENT_MODE": "multiagent",
    "SERVER_WORKER_MAX_CONCURRENCY": "4",
    "GIGACHAT_JUDGE_MODEL": MODEL,
    # Keep the isolated live subprocess compatible with strict binary flags
    # even when a developer's legacy .env still uses ``false``.
    "GIGACHAT_VERIFY_SSL": "0",
    # The configured ru-en-RoSBERTa index uses unprefixed normalized vectors.
    "EMBEDDING_PROFILE": "plain-normalized-v1",
    "GIGACHAT_TEMPERATURE": "0",
    "GIGACHAT_TIMEOUT": "180",
    "LLM_TIMEOUT": "180",
    "LIVE_AGENT_HTTP_TIMEOUT": "600",
    "AGENT_RUN_METRICS_ENABLED": "1",
    "NEO4J_URI": "",
    "NEO4J_USERNAME": "",
    "NEO4J_USER": "",
    "NEO4J_PASSWORD": "",
    "NEO4J_DATABASE": "",
}


def sqlite_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _defined_live_tests() -> set[str]:
    tree = ast.parse(SCENARIO_FILE.read_text(encoding="utf-8"))
    return {
        node.name
        for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and node.name.startswith("test_live_")
    }


def validate_spec() -> None:
    if REPEATS < 3:
        raise ValueError("DAG A/B requires at least three paired repeats")
    if len(CASES) != 8 or len({case.name for case in CASES}) != 8:
        raise ValueError("DAG A/B requires exactly eight unique scenarios")
    unknown = sorted({case.name for case in CASES} - _defined_live_tests())
    if unknown:
        raise ValueError(f"unknown DAG A/B live scenario: {unknown[0]}")
    if _selected_scenario_count([case.name for case in CASES], []) != 8:
        raise ValueError("each DAG A/B scenario must issue one HTTP exchange")
    if tuple(arm.name for arm in ARMS) != ("baseline", "candidate"):
        raise ValueError("DAG A/B arms must remain baseline and candidate")
    if {arm.worker_max_concurrency for arm in ARMS} != {1, 4}:
        raise ValueError("DAG A/B concurrency values must remain 1 and 4")


def fixture_errors(path: Path) -> list[str]:
    if not path.is_file():
        return [f"SQLite fixture does not exist: {path}"]
    try:
        with sqlite3.connect(f"file:{path.as_posix()}?mode=ro", uri=True) as conn:
            tables = {
                str(row[0])
                for row in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
            }
    except sqlite3.Error as exc:
        return [f"SQLite fixture is unreadable: {exc}"]
    required = {
        "files",
        "source_tables",
        "target_tables",
        "source_columns",
        "target_columns",
        "additional_objects",
        "pxf_to_a",
        "s2t_transformations",
    }
    missing = sorted(required - tables)
    return [f"SQLite fixture misses table {missing[0]}"] if missing else []


_TRACE_MARKER = "### Agent trace"


def read_trace(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {}
    text = path.read_text(encoding="utf-8")
    payloads: list[dict[str, Any]] = []
    cursor = 0
    decoder = json.JSONDecoder()
    while True:
        marker_index = text.find(_TRACE_MARKER, cursor)
        if marker_index < 0:
            break
        fence_index = text.find("```json", marker_index + len(_TRACE_MARKER))
        if fence_index < 0:
            break
        payload_start = fence_index + len("```json")
        try:
            payload, consumed = decoder.raw_decode(text[payload_start:].lstrip())
        except json.JSONDecodeError:
            cursor = payload_start
            continue
        if isinstance(payload, dict):
            payloads.append(payload)
        cursor = payload_start + consumed
    return payloads[-1] if payloads else {}


def _selected_cycle(trace: Mapping[str, Any]) -> tuple[list[dict], dict]:
    plans = [
        dict(item)
        for item in trace.get("coordinator_plan") or []
        if isinstance(item, Mapping)
        and str(item.get("pipeline") or "agentic") == "agentic"
    ]
    dags = [
        dict(item)
        for item in trace.get("coordinator_dag") or []
        if isinstance(item, Mapping)
    ]
    if not dags:
        return [], {}
    dag = max(dags, key=lambda item: int(item.get("plan_size") or 0))
    cycle = int(dag.get("cycle") or 0)
    cycle_plan = [
        item for item in plans if int(item.get("cycle") or 0) == cycle
    ]
    return cycle_plan, dag


def _direct_result_leakage_errors(dag: Mapping[str, Any]) -> list[str]:
    workers = [
        dict(item)
        for item in dag.get("workers") or []
        if isinstance(item, Mapping)
    ]
    by_id = {str(item.get("step_id")): item for item in workers}
    errors: list[str] = []
    for worker in workers:
        step_id = str(worker.get("step_id") or "")
        if str(worker.get("status") or "") in {
            "blocked_by_dependency",
            "cancelled_before_start",
            "cancelled_running",
        }:
            continue
        expected: list[str] = []
        for dependency_id in worker.get("depends_on") or []:
            dependency = by_id.get(str(dependency_id))
            if dependency is None:
                continue
            for result_id in dependency.get("output_result_ids") or []:
                value = str(result_id)
                if value not in expected:
                    expected.append(value)
        actual = [str(value) for value in worker.get("input_result_ids") or []]
        if actual != expected:
            errors.append(
                f"{step_id}: input_result_ids={actual!r}, direct={expected!r}"
            )
    return errors


def topology_errors(
    case: DagAbCase,
    trace: Mapping[str, Any],
    *,
    enforce_parallel_timing: bool = False,
) -> list[str]:
    plan, dag = _selected_cycle(trace)
    if not plan or not dag:
        return ["missing coordinator plan or DAG trace"]
    steps = {str(item.get("id")): item for item in plan}
    workers = [
        dict(item)
        for item in dag.get("workers") or []
        if isinstance(item, Mapping)
    ]
    worker_ids = {str(item.get("step_id")) for item in workers}
    errors: list[str] = []
    if set(steps) != worker_ids:
        errors.append("planned and terminal DAG step IDs differ")
    for worker in workers:
        step = steps.get(str(worker.get("step_id")))
        if step is not None and list(worker.get("depends_on") or []) != list(
            step.get("depends_on") or []
        ):
            errors.append(f"{worker.get('step_id')}: executed topology drift")
    errors.extend(_direct_result_leakage_errors(dag))

    roots = [item for item in plan if not item.get("depends_on")]
    children_by_parent: dict[str, list[dict]] = {step_id: [] for step_id in steps}
    for item in plan:
        for parent_id in item.get("depends_on") or []:
            children_by_parent.setdefault(str(parent_id), []).append(item)
    topology = case.topology
    if topology == "independent_roots":
        if len(plan) != 2 or len(roots) != 2:
            errors.append("expected exactly two independent roots")
    elif topology == "ready_child":
        structurally_matched = False
        timing_matched = False
        worker_by_id = {str(item.get("step_id")): item for item in workers}
        for child in plan:
            dependencies = list(child.get("depends_on") or [])
            if len(dependencies) != 1:
                continue
            unrelated = [
                root for root in roots if root.get("id") not in dependencies
            ]
            if unrelated:
                structurally_matched = True
            child_metric = worker_by_id.get(str(child.get("id")), {})
            child_started = child_metric.get("started_at_seconds")
            for root in unrelated:
                root_metric = worker_by_id.get(str(root.get("id")), {})
                root_ended = root_metric.get("ended_at_seconds")
                if (
                    child_started is not None
                    and root_ended is not None
                    and float(child_started) < float(root_ended)
                ):
                    timing_matched = True
        if not structurally_matched:
            errors.append("expected two roots and a single-parent child")
        elif enforce_parallel_timing and not timing_matched:
            errors.append("ready child did not start before unrelated root ended")
    elif topology == "fan_in":
        if not any(len(item.get("depends_on") or []) >= 2 for item in plan):
            errors.append("expected a fan-in node with at least two parents")
    elif topology == "fan_out":
        if not any(len(children) >= 2 for children in children_by_parent.values()):
            errors.append("expected one parent with at least two children")
    elif topology == "failed_parent":
        statuses = {str(item.get("status") or "") for item in workers}
        if not {"failed", "blocked_by_dependency", "complete"}.issubset(statuses):
            errors.append("expected failed, blocked and independent complete steps")
    elif topology == "partial_parent":
        if not any(str(item.get("status") or "") == "partial" for item in workers):
            errors.append("expected a structured partial parent")
    elif topology == "eight_roots":
        if len(plan) != 8 or len(roots) != 8:
            errors.append("expected exactly eight independent roots")
    elif topology == "sequential_control":
        if len(plan) < 3 or int(dag.get("max_parallel_width") or 0) != 1:
            errors.append("expected a sequential DAG with depth at least three")
        if int(dag.get("dag_depth") or 0) < 3:
            errors.append("sequential DAG depth is below three")
    return errors


def record_failures(record: RunRecord, initial_hash: str) -> list[str]:
    result = record.result
    failures: list[str] = []
    if record.db_sha256_before != initial_hash or record.db_sha256_after != initial_hash:
        failures.append("SQLite SHA256 changed")
    if result.return_code != 0 or result.failed or result.errors:
        failures.append("pytest run failed")
    if result.skipped:
        failures.append("live scenario was skipped")
    if result.http_500:
        failures.append("HTTP 500 observed")
    if result.selected_scenarios != 1 or result.passed != 1:
        failures.append("expected exactly one passed scenario")
    if result.semantic_statuses.get(record.case.name) != "passed":
        failures.append("semantic judge did not pass")
    upstream = record.trace.get("upstream_output")
    if not isinstance(upstream, Mapping) or not upstream.get("used_evidence_ids"):
        failures.append("missing upstream evidence provenance")
    failures.extend(
        topology_errors(
            record.case,
            record.trace,
            enforce_parallel_timing=(record.arm.name == "candidate"),
        )
    )
    return failures


def evaluate(records: Sequence[RunRecord]) -> DagAbVerdict:
    expected_runs = len(CASES) * len(ARMS) * REPEATS
    failures = tuple(
        f"repeat={record.repeat} case={record.case.name} arm={record.arm.name}: "
        + failure
        for record in records
        for failure in record.failures
    )
    baseline = [record for record in records if record.arm.name == "baseline"]
    candidate = [record for record in records if record.arm.name == "candidate"]
    baseline_seconds = sum(item.result.agent_seconds for item in baseline)
    candidate_seconds = sum(item.result.agent_seconds for item in candidate)
    latency_ratio = (
        candidate_seconds / baseline_seconds
        if baseline_seconds > 0
        else float("inf")
    )
    baseline_by_pair = {
        (item.repeat, item.case.name): item for item in baseline
    }
    candidate_by_pair = {
        (item.repeat, item.case.name): item for item in candidate
    }
    expected_pairs = len(CASES) * REPEATS
    pair_keys = sorted(set(baseline_by_pair) & set(candidate_by_pair))
    pair_ratios = [
        candidate_by_pair[key].result.agent_seconds
        / baseline_by_pair[key].result.agent_seconds
        for key in pair_keys
        if baseline_by_pair[key].result.agent_seconds > 0
    ]
    median_pair_latency_ratio = (
        statistics.median(pair_ratios) if pair_ratios else float("inf")
    )
    paired_latency_wins = sum(
        ratio <= LATENCY_IMPROVEMENT_RATIO for ratio in pair_ratios
    )
    baseline_tokens = sum(item.result.total_tokens for item in baseline)
    candidate_tokens = sum(item.result.total_tokens for item in candidate)
    token_ratio = (
        candidate_tokens / baseline_tokens
        if baseline_tokens > 0
        else float("inf")
    )
    candidate_concurrency = max(
        (item.result.dag_observed_concurrency for item in candidate),
        default=0,
    )
    if len(records) != expected_runs:
        return DagAbVerdict(
            status="inconclusive",
            failures=(
                f"completed {len(records)}/{expected_runs} runs",
                *failures,
            ),
            baseline_seconds=baseline_seconds,
            candidate_seconds=candidate_seconds,
            latency_ratio=latency_ratio,
            median_pair_latency_ratio=median_pair_latency_ratio,
            paired_latency_wins=paired_latency_wins,
            token_ratio=token_ratio,
            candidate_observed_concurrency=candidate_concurrency,
        )
    aggregate_failures = list(failures)
    if len(pair_keys) != expected_pairs or len(pair_ratios) != expected_pairs:
        aggregate_failures.append(
            f"usable latency pairs {len(pair_ratios)}/{expected_pairs}"
        )
    if candidate_concurrency < 2:
        aggregate_failures.append("candidate observed concurrency is below 2")
    if any(item.result.dag_observed_concurrency > 1 for item in baseline):
        aggregate_failures.append("baseline observed concurrency exceeds 1")
    if token_ratio > TOKEN_GUARD_RATIO:
        aggregate_failures.append(
            f"candidate token ratio {token_ratio:.3f} exceeds {TOKEN_GUARD_RATIO:.2f}"
        )
    if aggregate_failures:
        status = "not_improved"
    elif median_pair_latency_ratio <= LATENCY_IMPROVEMENT_RATIO:
        status = "improved"
    else:
        status = "not_improved"
        aggregate_failures.append(
            "median paired latency ratio "
            f"{median_pair_latency_ratio:.3f} exceeds improvement gate "
            f"{LATENCY_IMPROVEMENT_RATIO:.2f}"
        )
    return DagAbVerdict(
        status=status,
        failures=tuple(aggregate_failures),
        baseline_seconds=baseline_seconds,
        candidate_seconds=candidate_seconds,
        latency_ratio=latency_ratio,
        median_pair_latency_ratio=median_pair_latency_ratio,
        paired_latency_wins=paired_latency_wins,
        token_ratio=token_ratio,
        candidate_observed_concurrency=candidate_concurrency,
    )


def _json_value(value: Any) -> Any:
    if isinstance(value, float) and (value == float("inf") or value != value):
        return None
    if isinstance(value, Path):
        return str(value)
    if hasattr(value, "__dataclass_fields__"):
        return {
            key: _json_value(getattr(value, key))
            for key in value.__dataclass_fields__
        }
    if isinstance(value, Mapping):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    return value


def _write_json(path: Path, value: Any) -> None:
    path.write_text(
        json.dumps(_json_value(value), ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


def load_journal(path: Path) -> list[RunRecord]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, list):
        raise ValueError("DAG A/B journal must contain a list")
    mode_field_names = {item.name for item in fields(ModeResult)}
    records: list[RunRecord] = []
    for raw in payload:
        if not isinstance(raw, Mapping):
            raise ValueError("DAG A/B journal record must be an object")
        raw_result = raw.get("result")
        if not isinstance(raw_result, Mapping):
            raise ValueError("DAG A/B journal record misses result")
        result_values = {
            key: value
            for key, value in raw_result.items()
            if key in mode_field_names
        }
        for key in ("transcript_path", "junit_path"):
            if key in result_values:
                result_values[key] = Path(str(result_values[key]))
        records.append(
            RunRecord(
                repeat=int(raw["repeat"]),
                case=DagAbCase(**dict(raw["case"])),
                arm=DagAbArm(**dict(raw["arm"])),
                order=str(raw["order"]),
                result=ModeResult(**result_values),
                db_sha256_before=str(raw["db_sha256_before"]),
                db_sha256_after=str(raw["db_sha256_after"]),
                trace=dict(raw.get("trace") or {}),
                failures=[str(item) for item in raw.get("failures") or []],
            )
        )
    return records


def scheduled_runs() -> list[tuple[int, int, DagAbCase, str, DagAbArm]]:
    arms = {arm.name: arm for arm in ARMS}
    schedule: list[tuple[int, int, DagAbCase, str, DagAbArm]] = []
    for repeat in range(1, REPEATS + 1):
        for case_index, case in enumerate(CASES, start=1):
            order = "AB" if (repeat + case_index) % 2 == 0 else "BA"
            schedule.extend(
                (repeat, case_index, case, order, arms[arm_name])
                for arm_name in ORDERS[order]
            )
    return schedule


def validate_journal_prefix(records: Sequence[RunRecord]) -> None:
    schedule = scheduled_runs()
    if len(records) > len(schedule):
        raise ValueError("DAG A/B journal is longer than the fixed schedule")
    for index, (record, expected) in enumerate(zip(records, schedule)):
        repeat, _, case, order, arm = expected
        actual_key = (
            record.repeat,
            record.case.name,
            record.order,
            record.arm.name,
        )
        expected_key = (repeat, case.name, order, arm.name)
        if actual_key != expected_key:
            raise ValueError(
                f"DAG A/B journal diverges at record {index + 1}: "
                f"{actual_key!r} != {expected_key!r}"
            )


def permanent_external_failure(result: ModeResult) -> str:
    if not result.transcript_path.is_file():
        return ""
    text = result.transcript_path.read_text(
        encoding="utf-8",
        errors="replace",
    ).casefold()
    if "402" in text and "payment required" in text:
        return "GigaChat returned 402 Payment Required"
    if (
        result.return_code != 0
        and result.total_tokens == 0
        and result.llm_calls > 0
        and result.judge_errors > 0
        and result.judge_completed == 0
        and "responseerror" in text
    ):
        return "GigaChat failed before producing tokens for agent and judge"
    return ""


def write_preregistration(path: Path, db_path: Path, db_hash: str) -> None:
    lines = [
        "# Preregistered DAG concurrency A/B",
        "",
        f"- provider/model: `{PROVIDER}` / `{MODEL}`",
        f"- semantic judge: `{MODEL}`",
        f"- SQLite: `{db_path}`",
        f"- SQLite SHA256: `{db_hash}`",
        f"- paired repeats: `{REPEATS}`",
        "- baseline: `WORKER_MAX_CONCURRENCY=1`",
        "- candidate: `WORKER_MAX_CONCURRENCY=4`",
        "- order: AB/BA counterbalanced by repeat and scenario index",
        f"- latency improvement gate: ratio <= `{LATENCY_IMPROVEMENT_RATIO}`",
        f"- token guard: ratio <= `{TOKEN_GUARD_RATIO}`",
        "- default configuration is never changed automatically",
        "",
        "## Scenarios",
        "",
    ]
    lines.extend(
        f"{index}. `{case.name}` — `{case.topology}`: {case.hypothesis}"
        for index, case in enumerate(CASES, start=1)
    )
    lines.extend(
        [
            "",
            "## Hard gates",
            "",
            "- no HTTP 500, skip or technical test failure;",
            "- unchanged SQLite SHA256 before and after every exchange;",
            "- planned topology equals terminal execution topology;",
            "- direct-parent result IDs only, with no sibling/transitive leakage;",
            "- candidate observed concurrency is at least 2;",
            "- every semantic judge verdict passes;",
            "- every data-backed answer records used evidence;",
            "- no token/context failure and no token regression above guard;",
            "- latency is compared only across the fixed paired runs.",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_report(path: Path, verdict: DagAbVerdict, records: Sequence[RunRecord]) -> None:
    lines = [
        "# DAG concurrency A/B result",
        "",
        f"Verdict: `{verdict.status}`",
        "",
        f"- baseline seconds: `{verdict.baseline_seconds:.3f}`",
        f"- candidate seconds: `{verdict.candidate_seconds:.3f}`",
        f"- paired aggregate latency ratio: `{verdict.latency_ratio:.3f}`",
        "- median per-pair latency ratio: "
        f"`{verdict.median_pair_latency_ratio:.3f}`",
        f"- latency pairs passing the gate: `{verdict.paired_latency_wins}`",
        f"- token ratio: `{verdict.token_ratio:.3f}`",
        "- candidate max observed concurrency: "
        f"`{verdict.candidate_observed_concurrency}`",
        "- default changed: `false`",
        "",
        "## Runs",
        "",
        "| Repeat | Scenario | Order | Arm | Technical | Semantic | Seconds | "
        "Tokens | Concurrency | Failures |",
        "|---:|---|---|---|---|---|---:|---:|---:|---|",
    ]
    for record in records:
        semantic = record.result.semantic_statuses.get(
            record.case.name,
            "missing",
        )
        failures = "; ".join(record.failures).replace("|", "\\|") or "—"
        lines.append(
            f"| {record.repeat} | `{record.case.name}` | {record.order} | "
            f"{record.arm.name} | {record.result.return_code} | {semantic} | "
            f"{record.result.agent_seconds:.3f} | {record.result.total_tokens} | "
            f"{record.result.dag_observed_concurrency} | {failures} |"
        )
    lines.extend(["", "## Failures", ""])
    lines.extend(f"- {item}" for item in verdict.failures)
    if not verdict.failures:
        lines.append("- none")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db-path", type=Path, default=PROJECT_ROOT / "excel_data.db")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR_DAG)
    parser.add_argument(
        "--resume-dir",
        type=Path,
        help="Resume the exact fixed schedule from an existing journal.",
    )
    parser.add_argument(
        "--finalize-incomplete",
        action="store_true",
        help="Write an inconclusive verdict from a resumed partial journal.",
    )
    parser.add_argument("--dry-run", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    load_dotenv(PROJECT_ROOT / ".env", override=False)
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.finalize_incomplete and args.resume_dir is None:
        parser.error("--finalize-incomplete requires --resume-dir")
    if args.dry_run and args.resume_dir is not None:
        parser.error("--dry-run cannot be combined with --resume-dir")
    try:
        validate_spec()
    except ValueError as exc:
        parser.error(str(exc))
    db_path = args.db_path.expanduser().resolve()
    errors = fixture_errors(db_path)
    if errors:
        for error in errors:
            print(f"fixture preflight failed: {error}", flush=True)
        return 2
    if args.resume_dir is not None:
        output_dir = args.resume_dir.expanduser().resolve()
        timestamp = output_dir.name
        config_path = output_dir / f"{timestamp}_config.json"
        journal_path = output_dir / f"{timestamp}_journal.json"
        if not config_path.is_file() or not journal_path.is_file():
            print("resume preflight failed: config or journal is missing", flush=True)
            return 2
        config = json.loads(config_path.read_text(encoding="utf-8"))
        initial_hash = str(config.get("initial_db_sha256") or "")
        records = load_journal(journal_path)
        try:
            validate_journal_prefix(records)
        except ValueError as exc:
            print(f"resume preflight failed: {exc}", flush=True)
            return 2
        if sqlite_sha256(db_path) != initial_hash:
            print("resume preflight failed: SQLite SHA256 changed", flush=True)
            return 2
        for record in records:
            record.failures = record_failures(record, initial_hash)
        _write_json(journal_path, records)
    else:
        initial_hash = sqlite_sha256(db_path)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_dir = args.output_dir.expanduser().resolve() / timestamp
        output_dir.mkdir(parents=True, exist_ok=True)
        write_preregistration(
            output_dir / f"{timestamp}_preregistration.md",
            db_path,
            initial_hash,
        )
        _write_json(
            output_dir / f"{timestamp}_config.json",
            {
                "provider": PROVIDER,
                "model": MODEL,
                "judge_model": MODEL,
                "db_path": db_path,
                "repeats": REPEATS,
                "arms": ARMS,
                "cases": CASES,
                "common_environment": COMMON_ENVIRONMENT,
                "pytest_args": HARD_PYTEST_ARGS,
                "latency_improvement_ratio": LATENCY_IMPROVEMENT_RATIO,
                "token_guard_ratio": TOKEN_GUARD_RATIO,
                "initial_db_sha256": initial_hash,
            },
        )
        coverage_report = PROJECT_ROOT / "DAG_RUNTIME_COVERAGE.md"
        if coverage_report.is_file():
            (output_dir / f"{timestamp}_coverage_summary.md").write_text(
                coverage_report.read_text(encoding="utf-8"),
                encoding="utf-8",
            )
        records = []
    if args.dry_run:
        print(f"Preregistration: {output_dir}", flush=True)
        print("dry-run: no HTTP or LLM calls started", flush=True)
        return 0

    abort_reason = ""
    if args.finalize_incomplete:
        abort_reason = (
            permanent_external_failure(records[-1].result)
            if records
            else ""
        ) or "run finalized incomplete after an external service failure"
    if not args.finalize_incomplete:
        schedule = scheduled_runs()
        for repeat, case_index, case, order, arm in schedule[len(records) :]:
            before = sqlite_sha256(db_path)
            if before != initial_hash:
                abort_reason = "SQLite SHA256 changed before live exchange"
                break
            label = (
                f"{timestamp}_r{repeat}_{case_index:02d}_{order.lower()}_"
                f"{_slug(case.name)}_{arm.name}"
            )
            result = _run_mode(
                mode="multiagent",
                provider=PROVIDER,
                model=MODEL,
                targets=_scenario_targets((case.name,)),
                pytest_args=HARD_PYTEST_ARGS,
                output_dir=output_dir,
                run_label=label,
                llm_judge=True,
                extra_env={
                    **COMMON_ENVIRONMENT,
                    "WORKER_MAX_CONCURRENCY": str(arm.worker_max_concurrency),
                    "LIVE_AGENT_DB_PATH": str(db_path),
                },
            )
            after = sqlite_sha256(db_path)
            record = RunRecord(
                repeat=repeat,
                case=case,
                arm=arm,
                order=order,
                result=result,
                db_sha256_before=before,
                db_sha256_after=after,
                trace=read_trace(result.transcript_path),
            )
            record.failures = record_failures(record, initial_hash)
            records.append(record)
            _write_json(
                output_dir / f"{timestamp}_journal.json",
                records,
            )
            if after != initial_hash:
                abort_reason = "SQLite SHA256 changed after live exchange"
                break
            external_failure = permanent_external_failure(result)
            if external_failure:
                abort_reason = external_failure
                break
            if result.return_code != 0 and result.selected_scenarios == 0:
                abort_reason = "live subprocess failed before scenario execution"
                break

    verdict = evaluate(records)
    if abort_reason:
        verdict = DagAbVerdict(
            status="inconclusive",
            failures=(abort_reason, *verdict.failures),
            baseline_seconds=verdict.baseline_seconds,
            candidate_seconds=verdict.candidate_seconds,
            latency_ratio=verdict.latency_ratio,
            median_pair_latency_ratio=verdict.median_pair_latency_ratio,
            paired_latency_wins=verdict.paired_latency_wins,
            token_ratio=verdict.token_ratio,
            candidate_observed_concurrency=(
                verdict.candidate_observed_concurrency
            ),
        )
    _write_json(
        output_dir / f"{timestamp}_dag_execution_metrics.json",
        [
            {
                "repeat": item.repeat,
                "scenario": item.case.name,
                "arm": item.arm.name,
                "order": item.order,
                "coordinator_plan": item.trace.get("coordinator_plan", []),
                "coordinator_dag": item.trace.get("coordinator_dag", []),
            }
            for item in records
        ],
    )
    _write_json(
        output_dir / f"{timestamp}_semantic_verdict.json",
        [
            {
                "repeat": item.repeat,
                "scenario": item.case.name,
                "arm": item.arm.name,
                "statuses": item.result.semantic_statuses,
            }
            for item in records
        ],
    )
    _write_json(
        output_dir / f"{timestamp}_latency_tokens.json",
        {
            "baseline_seconds": verdict.baseline_seconds,
            "candidate_seconds": verdict.candidate_seconds,
            "latency_ratio": verdict.latency_ratio,
            "median_pair_latency_ratio": verdict.median_pair_latency_ratio,
            "paired_latency_wins": verdict.paired_latency_wins,
            "token_ratio": verdict.token_ratio,
            "paired_repeats": REPEATS,
        },
    )
    _write_json(
        output_dir / f"{timestamp}_failures.json",
        {"verdict": verdict.status, "failures": verdict.failures},
    )
    _write_json(output_dir / f"{timestamp}_verdict.json", verdict)
    report_path = output_dir / f"{timestamp}_report.md"
    write_report(report_path, verdict, records)
    print(f"DAG A/B report: {report_path}", flush=True)
    if verdict.status == "improved":
        return 0
    if verdict.status == "inconclusive":
        return 2
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
