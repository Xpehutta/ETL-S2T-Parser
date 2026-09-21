#!/usr/bin/env python
"""Run the real agent scenarios sequentially and compare runtime modes.

Example:
    uv run python scripts/run_live_agent_benchmark.py \
        --provider ollama --model qwen3.5:9b --modes multiagent single_agent
"""

from __future__ import annotations

import argparse
import ast
import json
import os
import re
import subprocess
import sys
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Mapping, Sequence

from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCENARIO_FILE = PROJECT_ROOT / "tests" / "test_live_agent_scenarios.py"
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / ".test_runs"
MODEL_ENV_BY_PROVIDER = {
    "gigachat": "GIGACHAT_MODEL",
    "ollama": "OLLAMA_MODEL",
    "openrouter": "OPENROUTER_MODEL",
}
LIVE_SCENARIO_GROUP_MARKERS = {
    "smoke": "live_smoke",
    "history": "live_history",
    "display": "live_display",
    "handoff": "live_handoff",
    "graph": "live_graph",
    "validation": "live_validation",
    "resolution": "live_resolution",
    "catalog": "live_catalog",
}


@dataclass
class ModeResult:
    mode: str
    return_code: int
    transcript_path: Path
    junit_path: Path
    passed: int = 0
    failed: int = 0
    errors: int = 0
    skipped: int = 0
    pytest_seconds: float = 0.0
    scenario_statuses: dict[str, str] = field(default_factory=dict)
    measured_runs: int = 0
    agent_seconds: float = 0.0
    llm_calls: int = 0
    tool_calls: int = 0
    reader_calls: int = 0
    tool_errors: int = 0
    reroutes: int = 0
    pipelines: dict[str, int] = field(default_factory=dict)
    dag_cycles: int = 0
    dag_depth: int = 0
    dag_max_parallel_width: int = 0
    dag_observed_concurrency: int = 0
    dag_blocked: int = 0
    dag_cancelled: int = 0
    dag_statuses: dict[str, int] = field(default_factory=dict)
    dag_input_result_ids: list[str] = field(default_factory=list)
    dag_output_result_ids: list[str] = field(default_factory=list)
    input_tokens: int = 0
    output_tokens: int = 0
    total_tokens: int = 0
    cache_read_tokens: int = 0
    stage_usage: dict[str, dict[str, int | float]] = field(default_factory=dict)
    judge_attempts: int = 0
    judge_completed: int = 0
    judge_errors: int = 0
    judge_input_tokens: int = 0
    judge_output_tokens: int = 0
    judge_total_tokens: int = 0
    judge_cache_read_tokens: int = 0
    judge_models: dict[str, int] = field(default_factory=dict)
    http_500: int = 0
    presentation_warnings: int = 0
    efficiency_warnings: int = 0
    warning_details: list[dict[str, str]] = field(default_factory=list)
    scenario_warnings: dict[str, list[str]] = field(default_factory=dict)
    semantic_statuses: dict[str, str] = field(default_factory=dict)

    @property
    def selected_scenarios(self) -> int:
        return self.passed + self.failed + self.errors + self.skipped

    @property
    def accuracy(self) -> float:
        return (
            self.passed / self.selected_scenarios
            if self.selected_scenarios
            else 0.0
        )


def _slug(value: str) -> str:
    clean = re.sub(r"[^A-Za-z0-9_.-]+", "_", value.strip())
    return clean.strip("._-") or "default"


def _configured_model(provider: str, explicit_model: str = "") -> str:
    """Resolve the model exactly as the runtime factory will resolve it."""
    explicit = str(explicit_model or "").strip()
    if explicit:
        return explicit
    configured = str(os.getenv(MODEL_ENV_BY_PROVIDER[provider], "")).strip()
    if configured:
        return configured
    # ``agents.llm_factory.get_chat_model_name`` keeps this compatibility
    # fallback for GigaChat.  Resolve it only after loading dotenv.
    if provider == "gigachat":
        return str(os.getenv("MODEL", "")).strip()
    return ""


def _scenario_targets(names: Sequence[str]) -> list[str]:
    if not names:
        return [str(SCENARIO_FILE)]
    targets: list[str] = []
    for name in names:
        clean = name.strip()
        if not clean:
            continue
        if "::" in clean or clean.endswith(".py"):
            targets.append(clean)
        else:
            targets.append(f"{SCENARIO_FILE}::{clean}")
    return targets or [str(SCENARIO_FILE)]


def _group_pytest_args(groups: Sequence[str]) -> list[str]:
    """Translate stable live-suite group names into one pytest expression."""
    unique_groups = list(dict.fromkeys(groups))
    unknown = [
        group
        for group in unique_groups
        if group not in LIVE_SCENARIO_GROUP_MARKERS
    ]
    if unknown:
        raise ValueError(f"unknown live scenario group: {unknown[0]}")
    if not unique_groups:
        return []
    expression = " or ".join(
        LIVE_SCENARIO_GROUP_MARKERS[group] for group in unique_groups
    )
    return ["-m", expression]


def _has_pytest_marker_expression(arguments: Sequence[str]) -> bool:
    return any(argument.startswith("-m") for argument in arguments)


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


def _selected_scenario_count(
    scenario_names: Sequence[str],
    groups: Sequence[str],
) -> int:
    """Count selected live HTTP exchanges."""
    tree = ast.parse(SCENARIO_FILE.read_text(encoding="utf-8"))
    requested_names: set[str] = set()
    whole_scenario_file = False
    for value in scenario_names:
        clean = str(value).strip()
        if not clean:
            continue
        path_part, separator, function_name = clean.rpartition("::")
        if separator:
            if path_part.endswith(".py"):
                candidate_path = Path(path_part)
                if not candidate_path.is_absolute():
                    candidate_path = PROJECT_ROOT / candidate_path
                if candidate_path.resolve() != SCENARIO_FILE.resolve():
                    raise ValueError(
                        "--scenario supports only tests from "
                        f"{SCENARIO_FILE}"
                    )
            requested_names.add(function_name.strip())
            continue
        if clean.endswith(".py"):
            candidate_path = Path(clean)
            if not candidate_path.is_absolute():
                candidate_path = PROJECT_ROOT / candidate_path
            if candidate_path.resolve() != SCENARIO_FILE.resolve():
                raise ValueError(
                    "--scenario supports only tests from "
                    f"{SCENARIO_FILE}"
                )
            whole_scenario_file = True
            continue
        requested_names.add(clean)
    if whole_scenario_file:
        requested_names.clear()
    requested_markers = {
        LIVE_SCENARIO_GROUP_MARKERS[group] for group in dict.fromkeys(groups)
    }
    count = 0
    for node in tree.body:
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        if not node.name.startswith("test_live_"):
            continue
        if requested_names and node.name not in requested_names:
            continue
        markers = {
            marker
            for decorator in node.decorator_list
            if (marker := _pytest_marker_name(decorator)) is not None
        }
        if requested_markers and not (markers & requested_markers):
            continue
        exchange_count = sum(
            isinstance(child, ast.Call)
            and isinstance(child.func, ast.Name)
            and child.func.id == "_chat"
            for child in ast.walk(node)
        )
        count += max(1, exchange_count)
    return max(1, count)


def _parse_junit(result: ModeResult) -> None:
    if not result.junit_path.is_file():
        return
    root = ET.parse(result.junit_path).getroot()
    testcases = list(root.iter("testcase"))
    result.pytest_seconds = sum(
        float(case.attrib.get("time") or 0.0) for case in testcases
    )
    for case in testcases:
        name = str(case.attrib.get("name") or "unknown")
        if case.find("skipped") is not None:
            status = "skipped"
            result.skipped += 1
        elif case.find("failure") is not None:
            status = "failed"
            result.failed += 1
        elif case.find("error") is not None:
            status = "error"
            result.errors += 1
        else:
            status = "passed"
            result.passed += 1
        result.scenario_statuses[name] = status


def _metric_values(text: str, pattern: str) -> list[int]:
    return [
        int(value)
        for value in re.findall(pattern, text, flags=re.MULTILINE)
    ]


def _parse_transcript(result: ModeResult) -> None:
    if not result.transcript_path.is_file():
        return
    text = result.transcript_path.read_text(encoding="utf-8")
    seconds = [
        float(value)
        for value in re.findall(r"^agent_seconds: ([0-9.]+)$", text, re.MULTILINE)
    ]
    result.measured_runs = len(seconds)
    result.agent_seconds = sum(seconds)
    result.llm_calls = sum(
        _metric_values(text, r"^llm_calls: (\d+)$")
    )
    result.reader_calls = sum(
        _metric_values(text, r"^reader_calls: (\d+)$")
    )
    result.tool_errors = sum(
        _metric_values(text, r"^tool_errors: (\d+)$")
    )
    result.reroutes = sum(_metric_values(text, r"^reroutes: (\d+)$"))
    result.dag_cycles = sum(
        _metric_values(text, r"^dag_cycles: (\d+)$")
    )
    result.dag_depth = max(
        _metric_values(text, r"^dag_depth: (\d+)$") or [0]
    )
    result.dag_max_parallel_width = max(
        _metric_values(text, r"^dag_max_parallel_width: (\d+)$") or [0]
    )
    result.dag_observed_concurrency = max(
        _metric_values(text, r"^dag_observed_concurrency: (\d+)$") or [0]
    )
    result.dag_blocked = sum(
        _metric_values(text, r"^dag_blocked: (\d+)$")
    )
    result.dag_cancelled = sum(
        _metric_values(text, r"^dag_cancelled: (\d+)$")
    )
    for raw_statuses in re.findall(
        r"^dag_statuses: (.+)$",
        text,
        re.MULTILINE,
    ):
        try:
            statuses = json.loads(raw_statuses)
        except (TypeError, json.JSONDecodeError):
            continue
        if not isinstance(statuses, dict):
            continue
        for status, count in statuses.items():
            clean_status = str(status or "unknown").strip()
            result.dag_statuses[clean_status] = (
                result.dag_statuses.get(clean_status, 0) + int(count or 0)
            )
    for metric_name, destination in (
        ("dag_input_result_ids", result.dag_input_result_ids),
        ("dag_output_result_ids", result.dag_output_result_ids),
    ):
        for raw_ids in re.findall(
            rf"^{metric_name}: (.+)$",
            text,
            re.MULTILINE,
        ):
            try:
                values = json.loads(raw_ids)
            except (TypeError, json.JSONDecodeError):
                continue
            if not isinstance(values, list):
                continue
            for value in values:
                clean_value = str(value or "").strip()
                if clean_value and clean_value not in destination:
                    destination.append(clean_value)
    for pipelines_line in re.findall(r"^pipelines: (.+)$", text, re.MULTILINE):
        for pipeline in pipelines_line.split(","):
            clean_pipeline = pipeline.strip()
            if clean_pipeline and clean_pipeline != "Нет":
                result.pipelines[clean_pipeline] = (
                    result.pipelines.get(clean_pipeline, 0) + 1
                )
    token_rows = re.findall(
        r"^tokens: input=(\d+), output=(\d+), total=(\d+), cache_read=(\d+)$",
        text,
        re.MULTILINE,
    )
    result.input_tokens = sum(int(row[0]) for row in token_rows)
    result.output_tokens = sum(int(row[1]) for row in token_rows)
    result.total_tokens = sum(int(row[2]) for row in token_rows)
    result.cache_read_tokens = sum(int(row[3]) for row in token_rows)
    for model in re.findall(r"^judge_model: (.+)$", text, re.MULTILINE):
        clean_model = model.strip()
        if clean_model and clean_model != "not_configured":
            result.judge_models[clean_model] = (
                result.judge_models.get(clean_model, 0) + 1
            )
    judge_call_rows = re.findall(
        r"^judge_calls: attempts=(\d+), completed=(\d+), errors=(\d+)$",
        text,
        re.MULTILINE,
    )
    result.judge_attempts = sum(int(row[0]) for row in judge_call_rows)
    result.judge_completed = sum(int(row[1]) for row in judge_call_rows)
    result.judge_errors = sum(int(row[2]) for row in judge_call_rows)
    judge_token_rows = re.findall(
        r"^judge_tokens: input=(\d+), output=(\d+), total=(\d+), "
        r"cache_read=(\d+)$",
        text,
        re.MULTILINE,
    )
    result.judge_input_tokens = sum(int(row[0]) for row in judge_token_rows)
    result.judge_output_tokens = sum(int(row[1]) for row in judge_token_rows)
    result.judge_total_tokens = sum(int(row[2]) for row in judge_token_rows)
    result.judge_cache_read_tokens = sum(int(row[3]) for row in judge_token_rows)
    stage_rows = re.findall(
        r"^stage_tokens\[([^\]]+)\]: calls=(\d+), errors=(\d+), "
        r"input=(\d+), output=(\d+), total=(\d+), cache_read=(\d+), "
        r"seconds=([0-9.]+)$",
        text,
        re.MULTILINE,
    )
    for row in stage_rows:
        stage = row[0]
        usage = result.stage_usage.setdefault(
            stage,
            {
                "calls": 0,
                "errors": 0,
                "input_tokens": 0,
                "output_tokens": 0,
                "total_tokens": 0,
                "cache_read_tokens": 0,
                "elapsed_seconds": 0.0,
            },
        )
        usage["calls"] += int(row[1])
        usage["errors"] += int(row[2])
        usage["input_tokens"] += int(row[3])
        usage["output_tokens"] += int(row[4])
        usage["total_tokens"] += int(row[5])
        usage["cache_read_tokens"] += int(row[6])
        usage["elapsed_seconds"] += float(row[7])
    result.http_500 = len(re.findall(r"^### Ответ — HTTP 500$", text, re.MULTILINE))
    warning_rows = re.findall(
        r"^<!-- LIVE_WARNING (\{.+\}) -->$",
        text,
        re.MULTILINE,
    )
    for raw_warning in warning_rows:
        try:
            warning = json.loads(raw_warning)
        except (TypeError, json.JSONDecodeError):
            continue
        if not isinstance(warning, dict):
            continue
        category = str(warning.get("category") or "warning").strip().lower()
        scenario = str(warning.get("scenario") or "unknown").strip()
        message = str(warning.get("message") or "").strip()
        detail = {
            "category": category,
            "scenario": scenario,
            "message": message,
        }
        result.warning_details.append(detail)
        result.scenario_warnings.setdefault(scenario, []).append(category)
        if category == "presentation":
            result.presentation_warnings += 1
        elif category == "efficiency":
            result.efficiency_warnings += 1
    semantic_rows = re.findall(
        r"^<!-- LIVE_SEMANTIC (\{.+\}) -->$",
        text,
        re.MULTILINE,
    )
    for raw_status in semantic_rows:
        try:
            semantic_status = json.loads(raw_status)
        except (TypeError, json.JSONDecodeError):
            continue
        if not isinstance(semantic_status, dict):
            continue
        scenario = str(semantic_status.get("scenario") or "unknown").strip()
        status = str(
            semantic_status.get("status") or "not_evaluated"
        ).strip()
        result.semantic_statuses[scenario] = status
    for tools_line in re.findall(r"^tools: (.+)$", text, re.MULTILINE):
        clean = tools_line.strip()
        if clean and clean != "Нет":
            result.tool_calls += len(
                [item for item in clean.split(",") if item.strip()]
            )


def _status_mark(status: str | None) -> str:
    return {
        "passed": "✅",
        "failed": "❌",
        "error": "💥",
        "skipped": "⏭",
    }.get(status or "", "—")


def _scenario_mark(result: ModeResult, scenario: str) -> str:
    technical_status = result.scenario_statuses.get(scenario)
    semantic_status = result.semantic_statuses.get(scenario)
    if technical_status == "passed":
        mark = {
            "passed": "✅",
            "failed": "❌",
            "judge_error": "💥",
            "not_evaluated": "📝",
        }.get(semantic_status or "not_evaluated", "📝")
    else:
        mark = _status_mark(technical_status)
    categories = result.scenario_warnings.get(scenario, [])
    suffixes = []
    presentation_count = categories.count("presentation")
    efficiency_count = categories.count("efficiency")
    if presentation_count:
        suffixes.append(f"⚠P×{presentation_count}")
    if efficiency_count:
        suffixes.append(f"⚠E×{efficiency_count}")
    return " ".join((mark, *suffixes))


def _comparison_report(
    *,
    provider: str,
    model: str,
    results: Sequence[ModeResult],
    report_path: Path,
) -> None:
    evaluated = any(
        status not in {"", "not_evaluated"}
        for result in results
        for status in result.semantic_statuses.values()
    )
    semantic_note = (
        "Содержательная корректность оценена LLM-as-judge: `✅` означает "
        "semantic pass, `❌` — semantic либо technical failure, `💥` — ошибку "
        "judge."
        if evaluated
        else (
            "Содержательная корректность пока не оценивается автоматически: "
            "`📝` означает, что ответ получен и сохранён для ручного разбора. "
            "Позже этот статус сможет заменить LLM-as-judge."
        )
    )
    lines = [
        f"# Live agent benchmark: {provider} / {model}",
        "",
        "Запросы выполнялись последовательно через реальный HTTP `/chat`, "
        "без mock и параллельных LLM-вызовов.",
        "",
        semantic_note,
        "",
        "| Режим | Pytest passed | Pytest failures | Semantic failures | "
        "Skipped | HTTP 500 | Presentation warnings | Efficiency warnings | "
        "Accuracy | Reroutes | Tool errors | Reader calls | Pipelines | "
        "Agent, с | Agent LLM calls | Tool calls | Agent tokens | "
        "Judge attempts | Judge errors | Judge tokens |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|"
        "---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for result in results:
        semantic_failures = sum(
            status in {"failed", "judge_error"}
            for status in result.semantic_statuses.values()
        )
        pipeline_summary = ", ".join(
            f"{name}×{count}" for name, count in sorted(result.pipelines.items())
        ) or "—"
        lines.append(
            f"| {result.mode} | {result.passed} | "
            f"{result.failed + result.errors} | {semantic_failures} | "
            f"{result.skipped} | "
            f"{result.http_500} | {result.presentation_warnings} | "
            f"{result.efficiency_warnings} | {result.accuracy:.1%} | "
            f"{result.reroutes} | {result.tool_errors} | "
            f"{result.reader_calls} | {pipeline_summary} | "
            f"{result.agent_seconds:.3f} | "
            f"{result.llm_calls} | {result.tool_calls} | "
            f"{result.total_tokens} | {result.judge_attempts} | "
            f"{result.judge_errors} | {result.judge_total_tokens} |"
        )

    if any(result.judge_attempts or result.judge_models for result in results):
        lines.extend(
            [
                "",
                "## Расход LLM-as-judge",
                "",
                "Attempts включают повторные HTTP-попытки structured-вызовов; "
                "errors — попытки, завершившиеся ошибкой до успешного retry либо "
                "окончательного judge_error.",
                "",
                "| Режим | Judge models | Attempts | Completed | Errors | "
                "Input | Output | Total | Cache read |",
                "|---|---|---:|---:|---:|---:|---:|---:|---:|",
            ]
        )
        for result in results:
            models = ", ".join(
                f"{name}×{count}"
                for name, count in sorted(result.judge_models.items())
            ) or "—"
            lines.append(
                f"| {result.mode} | {models} | {result.judge_attempts} | "
                f"{result.judge_completed} | {result.judge_errors} | "
                f"{result.judge_input_tokens} | {result.judge_output_tokens} | "
                f"{result.judge_total_tokens} | "
                f"{result.judge_cache_read_tokens} |"
            )

    if any(result.stage_usage for result in results):
        lines.extend(
            [
                "",
                "## Расход LLM по этапам",
                "",
                "| Режим | Этап | Calls | Errors | Input | Output | "
                "Total | Cache read | LLM, с |",
                "|---|---|---:|---:|---:|---:|---:|---:|---:|",
            ]
        )
        for result in results:
            for stage, usage in result.stage_usage.items():
                lines.append(
                    f"| {result.mode} | {stage} | {usage['calls']} | "
                    f"{usage['errors']} | {usage['input_tokens']} | "
                    f"{usage['output_tokens']} | {usage['total_tokens']} | "
                    f"{usage['cache_read_tokens']} | "
                    f"{usage['elapsed_seconds']:.3f} |"
                )

    if any(result.dag_cycles for result in results):
        lines.extend(
            [
                "",
                "## DAG execution",
                "",
                "| Режим | Cycles | Depth | Max width | Observed concurrency | "
                "Blocked | Cancelled | Terminal statuses | Input result IDs | "
                "Output result IDs |",
                "|---|---:|---:|---:|---:|---:|---:|---|---|---|",
            ]
        )
        for result in results:
            statuses = ", ".join(
                f"{status}×{count}"
                for status, count in sorted(result.dag_statuses.items())
            ) or "—"
            input_ids = ", ".join(
                f"`{value}`" for value in result.dag_input_result_ids
            ) or "—"
            output_ids = ", ".join(
                f"`{value}`" for value in result.dag_output_result_ids
            ) or "—"
            lines.append(
                f"| {result.mode} | {result.dag_cycles} | "
                f"{result.dag_depth} | {result.dag_max_parallel_width} | "
                f"{result.dag_observed_concurrency} | {result.dag_blocked} | "
                f"{result.dag_cancelled} | {statuses} | {input_ids} | "
                f"{output_ids} |"
            )

    scenario_names = sorted(
        {
            name
            for result in results
            for name in result.scenario_statuses
        }
    )
    if scenario_names:
        lines.extend(
            [
                "",
                "## Сценарии",
                "",
                "| Сценарий | "
                + " | ".join(result.mode for result in results)
                + " |",
                "|---|" + "---:|" * len(results),
            ]
        )
        for name in scenario_names:
            label = name.removeprefix("test_live_agent_")
            lines.append(
                f"| {label} | "
                + " | ".join(
                    _scenario_mark(result, name)
                    for result in results
                )
                + " |"
            )

    warning_details = [
        (result.mode, detail)
        for result in results
        for detail in result.warning_details
    ]
    if warning_details:
        lines.extend(
            [
                "",
                "## Некритичные предупреждения",
                "",
                "`P` — presentation, `E` — efficiency. Они не переводят "
                "сценарий в failed.",
                "",
                "| Режим | Сценарий | Категория | Детали |",
                "|---|---|---|---|",
            ]
        )
        for mode, detail in warning_details:
            clean_message = detail["message"].replace("|", "\\|")
            lines.append(
                f"| {mode} | {detail['scenario']} | "
                f"{detail['category']} | {clean_message} |"
            )

    lines.extend(["", "## Артефакты", ""])
    for result in results:
        lines.append(
            f"- `{result.mode}`: `{result.transcript_path}`; "
            f"JUnit: `{result.junit_path}`"
        )
    report_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _run_mode(
    *,
    mode: str,
    provider: str,
    model: str,
    targets: Sequence[str],
    pytest_args: Sequence[str],
    output_dir: Path,
    run_label: str,
    llm_judge: bool,
    extra_env: Mapping[str, str] | None = None,
) -> ModeResult:
    transcript_path = output_dir / f"{run_label}_{mode}.md"
    junit_path = output_dir / f"{run_label}_{mode}.xml"
    env = os.environ.copy()
    env.update(
        {
            "RUN_LIVE_AGENT_SCENARIOS": "1",
            "LIVE_AGENT_MODE": mode,
            "LIVE_AGENT_TRANSCRIPT_PATH": str(transcript_path),
            "LLM_PROVIDER": provider,
            "PYTHONUTF8": "1",
            "LIVE_AGENT_LLM_JUDGE": "1" if llm_judge else "0",
        }
    )
    if model:
        env[MODEL_ENV_BY_PROVIDER[provider]] = model
    if extra_env:
        env.update({str(key): str(value) for key, value in extra_env.items()})

    command = [
        sys.executable,
        "-m",
        "pytest",
        *targets,
        "-q",
        f"--junitxml={junit_path}",
        *pytest_args,
    ]
    print(f"\n=== {mode}: sequential live run ===", flush=True)
    print(" ".join(command), flush=True)
    completed = subprocess.run(
        command,
        cwd=PROJECT_ROOT,
        env=env,
        check=False,
    )
    result = ModeResult(
        mode=mode,
        return_code=completed.returncode,
        transcript_path=transcript_path,
        junit_path=junit_path,
    )
    _parse_junit(result)
    _parse_transcript(result)
    return result


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Последовательно прогнать реальные agent-сценарии и сравнить режимы."
        )
    )
    parser.add_argument(
        "--provider",
        choices=sorted(MODEL_ENV_BY_PROVIDER),
        default=os.getenv("LLM_PROVIDER", "ollama"),
    )
    parser.add_argument("--model", default="")
    parser.add_argument(
        "--modes",
        nargs="+",
        choices=("multiagent", "single_agent"),
        default=("multiagent", "single_agent"),
    )
    parser.add_argument(
        "--scenario",
        action="append",
        default=[],
        help=(
            "Имя test-функции или полный pytest node id. Можно повторять; "
            "без параметра запускаются все сценарии."
        ),
    )
    parser.add_argument(
        "--group",
        action="append",
        choices=tuple(LIVE_SCENARIO_GROUP_MARKERS),
        default=[],
        help=(
            "Смысловая группа live-сценариев. Можно повторять; группы "
            "объединяются через OR. Вместе с --scenario действует как фильтр."
        ),
    )
    parser.add_argument(
        "--pytest-arg",
        action="append",
        default=[],
        help="Дополнительный аргумент pytest; можно повторять.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
    )
    parser.add_argument(
        "--llm-judge",
        action="store_true",
        help=(
            "Оценить answer и display-results каждого завершённого сценария "
            "настроенной LLM."
        ),
    )
    parser.add_argument(
        "--allow-failures",
        action="store_true",
        help="Вернуть код 0 после benchmark, даже если acceptance-тесты упали.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    # Resolve the same project-local configuration that the Flask subprocess
    # will use.
    load_dotenv(PROJECT_ROOT / ".env", override=False)
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.group and _has_pytest_marker_expression(args.pytest_arg):
        parser.error(
            "--group нельзя сочетать с pytest marker expression через "
            "--pytest-arg=-m"
        )
    output_dir = args.output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    model = _configured_model(args.provider, args.model)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_label = "_".join(
        (
            "LIVE_AGENT_BENCHMARK",
            _slug(args.provider),
            _slug(model or "default"),
            timestamp,
        )
    )
    targets = _scenario_targets(args.scenario)
    pytest_args = [
        *_group_pytest_args(args.group),
        *args.pytest_arg,
    ]
    try:
        _selected_scenario_count(args.scenario, args.group)
    except ValueError as exc:
        parser.error(str(exc))
    results = []
    for mode in args.modes:
        result = _run_mode(
            mode=mode,
            provider=args.provider,
            model=model,
            targets=targets,
            pytest_args=pytest_args,
            output_dir=output_dir,
            run_label=run_label,
            llm_judge=args.llm_judge,
        )
        results.append(result)
    report_path = output_dir / f"{run_label}_comparison.md"
    _comparison_report(
        provider=args.provider,
        model=model or "default",
        results=results,
        report_path=report_path,
    )
    print(f"\nComparison report: {report_path}", flush=True)
    for result in results:
        print(
            f"{result.mode}: passed={result.passed}, "
            f"failed={result.failed + result.errors}, "
            f"tokens={result.total_tokens}, seconds={result.agent_seconds:.3f}",
            flush=True,
        )
    if args.allow_failures:
        return 0
    return 1 if any(result.return_code for result in results) else 0


if __name__ == "__main__":
    raise SystemExit(main())
