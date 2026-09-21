#!/usr/bin/env python
"""Enforce the canonical statement and branch coverage baselines."""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping, Sequence


MIN_STATEMENT_COVERAGE = 88.00
MIN_BRANCH_COVERAGE = 76.35
COMPONENT_PATHS = {
    "coordinator": "agents/coordinator.py",
    "contracts": "agents/contracts.py",
    "worker": "agents/worker.py",
    "saved results": "agents/tools/saved_results.py",
    "async runtime": "agents/async_runtime.py",
}


@dataclass(frozen=True)
class FileCoverage:
    statement: float
    branch: float


@dataclass(frozen=True)
class CoverageSummary:
    statement: float
    branch: float
    components: dict[str, FileCoverage] = field(default_factory=dict)


def _percentage(
    summary: Mapping[str, Any],
    *,
    percent_key: str,
    covered_key: str,
    total_key: str,
) -> float:
    if percent_key in summary:
        return float(summary[percent_key])
    total = int(summary.get(total_key) or 0)
    if total == 0:
        return 100.0
    return 100.0 * int(summary.get(covered_key) or 0) / total


def _file_coverage(summary: Mapping[str, Any]) -> FileCoverage:
    return FileCoverage(
        statement=_percentage(
            summary,
            percent_key="percent_statements_covered",
            covered_key="covered_lines",
            total_key="num_statements",
        ),
        branch=_percentage(
            summary,
            percent_key="percent_branches_covered",
            covered_key="covered_branches",
            total_key="num_branches",
        ),
    )


def summarize_coverage(payload: Mapping[str, Any]) -> CoverageSummary:
    totals = payload.get("totals")
    files = payload.get("files")
    if not isinstance(totals, Mapping) or not isinstance(files, Mapping):
        raise ValueError("coverage JSON must contain totals and files objects")

    normalized_files = {
        str(path).replace("\\", "/"): value
        for path, value in files.items()
        if isinstance(value, Mapping)
    }
    components: dict[str, FileCoverage] = {}
    for label, path in COMPONENT_PATHS.items():
        file_payload = normalized_files.get(path)
        if not isinstance(file_payload, Mapping):
            continue
        file_summary = file_payload.get("summary")
        if isinstance(file_summary, Mapping):
            components[label] = _file_coverage(file_summary)

    total = _file_coverage(totals)
    return CoverageSummary(
        statement=total.statement,
        branch=total.branch,
        components=components,
    )


def coverage_failures(summary: CoverageSummary) -> list[str]:
    failures: list[str] = []
    if summary.statement + 1e-9 < MIN_STATEMENT_COVERAGE:
        failures.append(
            "statement coverage "
            f"{summary.statement:.2f}% is below {MIN_STATEMENT_COVERAGE:.2f}%"
        )
    if summary.branch + 1e-9 < MIN_BRANCH_COVERAGE:
        failures.append(
            "branch coverage "
            f"{summary.branch:.2f}% is below {MIN_BRANCH_COVERAGE:.2f}%"
        )
    return failures


def render_markdown(summary: CoverageSummary) -> str:
    lines = [
        "## Coverage gate",
        "",
        "| Scope | Statement | Branch |",
        "|---|---:|---:|",
        f"| total | {summary.statement:.2f}% | {summary.branch:.2f}% |",
    ]
    for label in COMPONENT_PATHS:
        component = summary.components.get(label)
        if component is None:
            continue
        lines.append(
            f"| {label} | {component.statement:.2f}% | "
            f"{component.branch:.2f}% |"
        )
    lines.extend(
        [
            "",
            f"Gates: statement >= {MIN_STATEMENT_COVERAGE:.2f}%; "
            f"branch >= {MIN_BRANCH_COVERAGE:.2f}%.",
        ]
    )
    return "\n".join(lines) + "\n"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("coverage_json", type=Path)
    parser.add_argument("--github-summary", type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    payload = json.loads(args.coverage_json.read_text(encoding="utf-8"))
    summary = summarize_coverage(payload)
    markdown = render_markdown(summary)
    print(markdown, end="")
    if args.github_summary is not None:
        with args.github_summary.open(
            "a",
            encoding="utf-8",
            newline="\n",
        ) as target:
            target.write(markdown)
    failures = coverage_failures(summary)
    for failure in failures:
        print(f"ERROR: {failure}")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
