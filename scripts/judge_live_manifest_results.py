#!/usr/bin/env python
"""Re-judge saved live-agent transcripts against manifest references.

The regular live suite judges only the user query and the public response.
This utility additionally supplies the immutable ``expected_invariants`` from
the domain manifest, so an answer cannot pass merely because it is internally
consistent with an incorrectly guessed entity or route.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Sequence


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.run_word9_small_model_benchmark import (  # noqa: E402
    CandidateExchange,
    ReferenceCase,
    _judge,
)


BLOCK_PATTERN = re.compile(
    r"(?ms)^## (?P<index>\d+)\. Запрос\r?\n\r?\n"
    r"(?P<body>.*?)(?=^## \d+\. Запрос|\Z)"
)
ANSWER_PATTERN = re.compile(
    r"(?ms)^(?P<query>.*?)^### Ответ — HTTP (?P<http>\d+)\r?\n\r?\n"
    r"(?P<answer>.*?)(?=^### Display-results)"
    r"^### Display-results\r?\n\r?\n"
    r"(?P<display>.*?)(?=^### Execution metrics)"
)
SCENARIO_PATTERN = re.compile(
    r"<!-- LIVE_SEMANTIC (?P<payload>\{.*?\}) -->"
)


@dataclass(frozen=True)
class TranscriptExchange:
    scenario: str
    query: str
    answer: str
    display_results: str
    http_status: int
    transcript: str


def _clean_query(value: str) -> str:
    return re.sub(
        r"\Aagent_mode:\s*[^\r\n]+\r?\n\r?\n",
        "",
        value.strip(),
        count=1,
    ).strip()


def parse_transcript(path: Path) -> dict[str, TranscriptExchange]:
    """Read one benchmark transcript keyed by its recorded scenario name."""

    text = path.read_text(encoding="utf-8")
    exchanges: dict[str, TranscriptExchange] = {}
    for block_match in BLOCK_PATTERN.finditer(text):
        body = block_match.group("body")
        answer_match = ANSWER_PATTERN.search(body)
        scenario_match = SCENARIO_PATTERN.search(body)
        if answer_match is None or scenario_match is None:
            continue
        payload = json.loads(scenario_match.group("payload"))
        scenario = str(payload.get("scenario") or "").strip()
        if not scenario:
            continue
        exchanges[scenario] = TranscriptExchange(
            scenario=scenario,
            query=_clean_query(answer_match.group("query")),
            answer=answer_match.group("answer").strip(),
            display_results=answer_match.group("display").strip(),
            http_status=int(answer_match.group("http")),
            transcript=str(path.resolve()),
        )
    return exchanges


def _assignment(value: str, *, separator: str = "=") -> tuple[str, str]:
    left, found, right = value.partition(separator)
    if not found or not left.strip() or not right.strip():
        raise argparse.ArgumentTypeError(
            f"expected NAME{separator}VALUE, got {value!r}"
        )
    return left.strip(), right.strip()


def _override_assignment(value: str) -> tuple[str, str, str]:
    owner, path = _assignment(value)
    model, scenario = _assignment(owner, separator="|")
    return model, scenario, path


def _reference_text(case: dict[str, Any]) -> str:
    title = str(case.get("title") or case["case_id"]).strip()
    invariants = str(case.get("expected_invariants") or "").strip()
    return (
        f"Reference case {case['case_id']}: {title}.\n"
        f"Обязательные инварианты: {invariants}\n"
        "Эти инварианты являются обязательными. Не засчитывай ответ, который "
        "угадывает неоднозначный объект, меняет source/target, пропускает "
        "запрошенную часть, нарушает требуемый display/handoff или заменяет "
        "результат сообщением о нехватке данных, когда reference требует ответ."
    )


def _markdown_report(
    *,
    manifest_path: Path,
    selected_cases: Sequence[dict[str, Any]],
    results: dict[str, list[dict[str, Any]]],
) -> str:
    lines = [
        "# Reference-aware GigaChat-Max evaluation",
        "",
        f"Manifest: `{manifest_path.resolve()}`",
        "",
        "В Max передавались исходный query, публичный candidate answer и "
        "reference из `title + expected_invariants`. Результаты локальных "
        "моделей повторно не генерировались.",
        "",
        "## Summary",
        "",
        "| Model | Passed | Failed | Judge error | Not executed | Mean score |",
        "|---|---:|---:|---:|---:|---:|",
    ]
    for model, items in results.items():
        statuses = Counter(item["status"] for item in items)
        judged_scores = [
            int(item["score"])
            for item in items
            if item["status"] in {"passed", "failed"}
        ]
        mean_score = (
            sum(judged_scores) / len(judged_scores) if judged_scores else 0.0
        )
        lines.append(
            f"| {model} | {statuses['passed']} | {statuses['failed']} | "
            f"{statuses['judge_error']} | {statuses['not_executed']} | "
            f"{mean_score:.1f} |"
        )

    lines.extend(["", "## By domain", ""])
    for model, items in results.items():
        lines.extend(
            [
                f"### {model}",
                "",
                "| Domain | Passed | Failed | Not executed |",
                "|---|---:|---:|---:|",
            ]
        )
        by_domain: dict[str, Counter[str]] = defaultdict(Counter)
        for item in items:
            by_domain[item["domain"]][item["status"]] += 1
        for domain in dict.fromkeys(case["domain"] for case in selected_cases):
            counts = by_domain[domain]
            lines.append(
                f"| {domain} | {counts['passed']} | {counts['failed']} | "
                f"{counts['not_executed']} |"
            )
        lines.append("")

    lines.extend(["## Cases", ""])
    for model, items in results.items():
        lines.extend(
            [
                f"### {model}",
                "",
                "| Case | Domain | Status | Score | Reason |",
                "|---|---|---|---:|---|",
            ]
        )
        for item in items:
            reason = str(item["reason"]).replace("|", "\\|").replace("\n", " ")
            lines.append(
                f"| {item['case_id']} | {item['domain']} | "
                f"{item['status']} | {item['score']} | {reason} |"
            )
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Judge saved live transcripts against manifest invariants."
    )
    parser.add_argument("--manifest", required=True, type=Path)
    parser.add_argument(
        "--run",
        action="append",
        default=[],
        metavar="MODEL=TRANSCRIPT",
        help="Model label and its primary transcript; repeat per model.",
    )
    parser.add_argument(
        "--override",
        action="append",
        default=[],
        metavar="MODEL|SCENARIO=TRANSCRIPT",
        help="Replace one scenario with a corrected/repeated transcript.",
    )
    parser.add_argument("--exclude-case-id", action="append", default=[])
    parser.add_argument("--output-dir", required=True, type=Path)
    args = parser.parse_args(argv)

    manifest_path = args.manifest.resolve()
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    excluded = set(args.exclude_case_id)
    selected_cases = [
        case
        for case in manifest.get("cases", [])
        if case.get("tier") == "core" and case.get("case_id") not in excluded
    ]
    if not selected_cases:
        parser.error("manifest selection is empty")

    run_paths = dict(_assignment(value) for value in args.run)
    if not run_paths:
        parser.error("at least one --run is required")
    exchanges_by_model = {
        model: parse_transcript(Path(path)) for model, path in run_paths.items()
    }
    for value in args.override:
        model, scenario, path = _override_assignment(value)
        if model not in exchanges_by_model:
            parser.error(f"override references unknown model {model!r}")
        replacement = parse_transcript(Path(path)).get(scenario)
        if replacement is None:
            parser.error(f"override transcript has no scenario {scenario!r}")
        exchanges_by_model[model][scenario] = replacement

    from agents.llm_factory import create_judge_chat_model

    judge_model = create_judge_chat_model(timeout=180)
    results: dict[str, list[dict[str, Any]]] = {}
    for model, exchanges in exchanges_by_model.items():
        model_results: list[dict[str, Any]] = []
        for number, case in enumerate(selected_cases, start=1):
            scenario = str(case["source_test"])
            exchange = exchanges.get(scenario)
            if exchange is None:
                model_results.append(
                    {
                        "model": model,
                        "case_id": case["case_id"],
                        "domain": case["domain"],
                        "scenario": scenario,
                        "status": "not_executed",
                        "score": 0,
                        "reason": "Сценарий отсутствует в transcript (обычно pytest skip).",
                        "reference_answer": _reference_text(case),
                    }
                )
                continue
            print(f"Max judge: {model} / {case['case_id']}", flush=True)
            reference = ReferenceCase(
                number=number,
                scenario=scenario,
                query=exchange.query,
                reference_answer=_reference_text(case),
            )
            candidate = CandidateExchange(
                scenario=scenario,
                answer=exchange.answer,
                display_results=exchange.display_results,
                http_status=exchange.http_status,
                wall_seconds=0.0,
            )
            evaluation = _judge(reference, candidate, judge_model)
            item = {
                "model": model,
                "case_id": case["case_id"],
                "domain": case["domain"],
                "scenario": scenario,
                "query": exchange.query,
                "candidate_answer": exchange.answer,
                "candidate_display_results": exchange.display_results,
                "candidate_http_status": exchange.http_status,
                "reference_answer": reference.reference_answer,
                "transcript": exchange.transcript,
                **asdict(evaluation),
            }
            model_results.append(item)
        results[model] = model_results

    args.output_dir.mkdir(parents=True, exist_ok=True)
    json_path = args.output_dir / "reference_judge_results.json"
    report_path = args.output_dir / "REFERENCE_JUDGE_REPORT.md"
    json_path.write_text(
        json.dumps(results, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    report_path.write_text(
        _markdown_report(
            manifest_path=manifest_path,
            selected_cases=selected_cases,
            results=results,
        ),
        encoding="utf-8",
    )
    print(f"JSON: {json_path.resolve()}")
    print(f"Report: {report_path.resolve()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
