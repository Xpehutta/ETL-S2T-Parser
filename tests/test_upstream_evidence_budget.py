from __future__ import annotations

import json

from agents.contracts import EvidenceArtifact, WorkerOutcome
from agents.coordinator import (
    UPSTREAM_EVIDENCE_SERIALIZED_MAX_CHARS,
    _build_upstream_evidence_bundle,
    _worker_run_manifest,
)


def _run(
    step_id: str,
    *,
    status: str = "complete",
    evidence: list[EvidenceArtifact] | None = None,
):
    outcome = None
    if status in {"complete", "partial", "failed"}:
        outcome = WorkerOutcome(
            summary=f"{step_id} outcome",
            status=status,
            stop_reason=("tool_error" if status != "complete" else None),
            unmet_requirements=(
                [f"{step_id} unavailable"] if status != "complete" else []
            ),
            evidence=list(evidence or []),
        )
    return {
        "step_id": step_id,
        "depends_on": [],
        "terminal_status": status,
        "stop_reason": "tool_error" if status != "complete" else None,
        "outcome": outcome,
    }


def test_maximum_dag_evidence_stays_inside_serialized_budget():
    runs = [
        _run(
            f"step-{index}",
            evidence=[
                EvidenceArtifact(
                    evidence_id=f"evidence-{index}",
                    tool_name="run_sql",
                    compact_args={"query": "x" * 1800},
                    preview=str(index) * 7000,
                )
            ],
        )
        for index in range(8)
    ]

    bundle = _build_upstream_evidence_bundle(runs)
    serialized = json.dumps(
        bundle["evidence"],
        ensure_ascii=False,
        separators=(",", ":"),
    )

    assert len(serialized) <= UPSTREAM_EVIDENCE_SERIALIZED_MAX_CHARS
    assert bundle["evidence_budget"]["overflow"] is True
    assert bundle["evidence_budget"]["omitted_evidence_ids"]
    assert bundle["evidence_budget"]["preview_truncated_evidence_ids"]
    assert all(item["producer_step_id"] for item in bundle["evidence"])


def test_lazy_read_records_lineage_without_duplicate_upstream_evidence():
    original = EvidenceArtifact(
        evidence_id="evidence-original",
        tool_name="run_sql",
        preview='{"rows":[{"value":1}]}',
    )
    lazy_read = EvidenceArtifact(
        evidence_id="evidence-lazy-read",
        tool_name="read_previous_result",
        preview='{"rows":[{"value":1}]}',
        lineage_evidence_ids=["evidence-original"],
    )

    bundle = _build_upstream_evidence_bundle(
        [
            _run("producer", evidence=[original]),
            _run("consumer", evidence=[lazy_read]),
        ]
    )

    assert [item["evidence_id"] for item in bundle["evidence"]] == [
        "evidence-original"
    ]
    assert bundle["evidence_lineage"] == [
        {
            "producer_step_id": "consumer",
            "evidence_id": "evidence-lazy-read",
            "source_evidence_ids": ["evidence-original"],
        }
    ]


def test_lazy_read_extracts_single_and_batch_lineage():
    from agents.chat_graph import _tool_source_evidence_ids

    single = json.dumps(
        {
            "result_id": "result-one",
            "source_evidence_ids": ["evidence-one"],
            "result": {"rows": []},
        }
    )
    batch = json.dumps(
        {
            "results": [
                {
                    "result_id": "result-one",
                    "source_evidence_ids": ["evidence-one"],
                },
                {
                    "result_id": "result-two",
                    "source_evidence_ids": [
                        "evidence-two",
                        "evidence-one",
                    ],
                },
            ]
        }
    )

    assert _tool_source_evidence_ids(single) == ["evidence-one"]
    assert _tool_source_evidence_ids(batch) == [
        "evidence-one",
        "evidence-two",
    ]


def test_manifest_is_not_lost_when_evidence_overflows():
    runs = [
        _run(
            "large",
            evidence=[
                EvidenceArtifact(
                    evidence_id="evidence-large",
                    tool_name="run_sql",
                    compact_args={"query": "x" * 2000},
                    preview="v" * 20000,
                )
            ],
        ),
        _run("failed", status="failed"),
        {
            "step_id": "blocked",
            "depends_on": ["failed"],
            "terminal_status": "blocked_by_dependency",
            "stop_reason": "blocked_by_dependency",
            "outcome": None,
        },
    ]

    bundle = _build_upstream_evidence_bundle(runs)
    manifest = _worker_run_manifest(runs)

    assert bundle["evidence_budget"]["preview_truncated_evidence_ids"] == [
        "evidence-large"
    ]
    assert [(item["step_id"], item["status"]) for item in manifest] == [
        ("large", "complete"),
        ("failed", "failed"),
        ("blocked", "blocked_by_dependency"),
    ]
