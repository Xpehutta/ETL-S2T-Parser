from __future__ import annotations

import json
from pathlib import Path

from scripts.check_coverage import (
    MIN_BRANCH_COVERAGE,
    MIN_STATEMENT_COVERAGE,
    coverage_failures,
    main,
    render_markdown,
    summarize_coverage,
)


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _payload(*, statements: float, branches: float):
    return {
        "totals": {
            "percent_statements_covered": statements,
            "percent_branches_covered": branches,
        },
        "files": {
            "agents\\coordinator.py": {
                "summary": {
                    "percent_statements_covered": 92.0,
                    "percent_branches_covered": 84.0,
                }
            }
        },
    }


def test_coverage_gate_enforces_statement_and_branch_baselines():
    summary = summarize_coverage(
        _payload(
            statements=MIN_STATEMENT_COVERAGE,
            branches=MIN_BRANCH_COVERAGE,
        )
    )

    assert coverage_failures(summary) == []
    assert "coordinator" in summary.components

    failures = coverage_failures(
        summarize_coverage(
            _payload(
                statements=MIN_STATEMENT_COVERAGE - 0.01,
                branches=MIN_BRANCH_COVERAGE - 0.01,
            )
        )
    )
    assert len(failures) == 2
    assert "statement" in failures[0]
    assert "branch" in failures[1]


def test_repository_uses_one_canonical_branch_coverage_gate():
    coverage_config = (PROJECT_ROOT / ".coveragerc").read_text(encoding="utf-8")
    pytest_config = (PROJECT_ROOT / "pytest.ini").read_text(encoding="utf-8")
    workflow = (PROJECT_ROOT / ".github/workflows/ci.yml").read_text(
        encoding="utf-8"
    )

    assert "branch = True" in coverage_config
    assert "fail_under" not in coverage_config
    assert "cov-fail-under" not in pytest_config
    assert workflow.count("scripts/check_coverage.py") == 1


def test_coverage_gate_fallback_math_and_markdown():
    summary = summarize_coverage(
        {
            "totals": {
                "covered_lines": 88,
                "num_statements": 100,
                "covered_branches": 0,
                "num_branches": 0,
            },
            "files": {},
        }
    )

    assert summary.statement == 88.0
    assert summary.branch == 100.0
    markdown = render_markdown(summary)
    assert "| total | 88.00% | 100.00% |" in markdown
    assert "statement >= 88.00%" in markdown


def test_coverage_gate_cli_writes_summary_and_reports_failure(tmp_path, capsys):
    passing = tmp_path / "passing.json"
    passing.write_text(
        json.dumps(
            _payload(
                statements=MIN_STATEMENT_COVERAGE,
                branches=MIN_BRANCH_COVERAGE,
            )
        ),
        encoding="utf-8",
    )
    github_summary = tmp_path / "summary.md"

    assert main(
        [str(passing), "--github-summary", str(github_summary)]
    ) == 0
    assert "Coverage gate" in github_summary.read_text(encoding="utf-8")

    failing = tmp_path / "failing.json"
    failing.write_text(
        json.dumps(_payload(statements=1.0, branches=2.0)),
        encoding="utf-8",
    )
    assert main([str(failing)]) == 1
    assert "ERROR: statement coverage" in capsys.readouterr().out


def test_coverage_gate_rejects_invalid_json_shape():
    import pytest

    with pytest.raises(ValueError, match="totals and files"):
        summarize_coverage({"totals": {}})
