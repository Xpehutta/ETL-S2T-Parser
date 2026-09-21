"""Typed contracts exchanged between workers and their coordinator."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any, Dict, List, Literal, Mapping, Optional

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationInfo,
    field_validator,
    model_validator,
)

ObservationStatus = Literal["complete", "continue", "reroute"]
UpstreamAction = Literal["pass", "reroute"]
WorkerOutcomeStatus = Literal["complete", "partial", "failed"]
WorkerStopReason = Literal[
    "no_results",
    "unresolved_entity",
    "ambiguous_entity",
    "missing_input",
    "missing_capability",
    "wrong_arguments",
    "tool_error",
    "budget_exhausted",
    "truncated_source",
    "observer_error",
]
WorkerCapability = Literal[
    "sql_read",
    "saved_result_read",
    "saved_result_aggregate",
    "semantic_search",
    "s2t_search",
    "s2t_read",
    "column_catalog_read",
    "graph_read",
    "excel_read",
    "general_read",
]
RerouteReason = Literal[
    "missing_capability",
    "unresolved_entity",
    "wrong_arguments",
    "truncated_result",
    "tool_error",
]
SqlRiskAspect = Literal[
    "row_filtering",
    "cardinality",
    "constraint_rejection",
    "value_changes",
    "write_semantics",
]
OperationPipeline = Literal[
    "agentic",
    "validation_protocol",
    "sql_risk_scope",
]
MAX_PLAN_STEPS = 8
_LEGACY_WORKER_STABLE_CONTEXT_MARKER = (
    "\n\nУстойчивые правила контекста:\n"
)
WORKER_PREVIOUS_RESULTS_MARKER = "\n\nРезультаты прошлых workers."
WORKER_ORIGINAL_TASK_MARKER = (
    "\n\nИсходная задача coordinator (immutable):\n"
)
WORKER_OPERATION_EXECUTION_MARKER = "\n\nOperation-skill текущей задачи:\n"
WORKER_OPERATION_COMPLETENESS_MARKER = (
    "\n\nOperation-skill проверки полноты:\n"
)


class SavedResultColumn(BaseModel):
    """One physical column exposed by a run-scoped dataset."""

    model_config = ConfigDict(extra="forbid")

    name: str
    sqlite_type: str


class PreviousResultSchema(BaseModel):
    """Compact table schema exposed with a lazy previous-result reference."""

    model_config = ConfigDict(extra="forbid")

    result_ref: str = Field(min_length=1)
    row_count: int = Field(ge=0)
    truncated: bool = False
    input_truncated: bool = False
    columns: List[SavedResultColumn] = Field(default_factory=list)


class PreviousResultReference(BaseModel):
    """Minimal lazy reference passed from one worker to later workers."""

    model_config = ConfigDict(extra="forbid")

    result_id: str = Field(min_length=1)
    description: str = Field(min_length=1, max_length=600)
    result_schema: Optional[PreviousResultSchema] = None

    @field_validator("result_id", "description")
    @classmethod
    def _strip_text(cls, value: str) -> str:
        clean_value = str(value or "").strip()
        if not clean_value:
            raise ValueError("previous result fields must not be blank")
        return clean_value


@dataclass(frozen=True)
class WorkerRequestParts:
    """Programmatic envelope around the current worker task."""

    current_task: str
    original_task: str = ""
    operation_execution_context: str = ""
    operation_completeness_context: str = ""
    previous_results: Optional[List[PreviousResultReference]] = None


def parse_worker_request(value: Any) -> WorkerRequestParts:
    """Coerce a typed request or treat an ordinary string as a literal task."""
    if isinstance(value, WorkerRequestParts):
        return value
    return WorkerRequestParts(current_task=str(value or "").strip())


def parse_legacy_worker_request(value: Any) -> WorkerRequestParts:
    """Explicitly decode the retired marker-delimited worker envelope."""
    full_text = str(value or "").strip()
    # Older direct worker ingress accepted an arbitrary conversation-context
    # suffix and forwarded it to both router and planner.  Keep recognizing the
    # old delimiter only as a fail-closed sanitization boundary: neither the
    # marker nor anything following it is part of a worker request anymore.
    task_and_context = full_text.split(
        _LEGACY_WORKER_STABLE_CONTEXT_MARKER,
        1,
    )[0]
    previous_results: Optional[List[PreviousResultReference]] = None

    if WORKER_PREVIOUS_RESULTS_MARKER in task_and_context:
        task_and_context, handoff_text = task_and_context.split(
            WORKER_PREVIOUS_RESULTS_MARKER,
            1,
        )
        json_start = handoff_text.find("{")
        if json_start < 0:
            raise ValueError("Legacy worker envelope has no previous-results JSON")
        try:
            decoded = json.loads(handoff_text[json_start:])
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            raise ValueError("Legacy worker envelope has invalid JSON") from exc
        if (
            not isinstance(decoded, Mapping)
            or set(decoded) != {"previous_results"}
            or not isinstance(decoded["previous_results"], list)
        ):
            raise ValueError(
                "Legacy worker envelope has invalid previous_results payload"
            )
        try:
            previous_results = [
                PreviousResultReference.model_validate(item)
                for item in decoded["previous_results"]
            ]
        except (TypeError, ValueError) as exc:
            raise ValueError(
                "Legacy worker envelope has invalid previous-result reference"
            ) from exc

    current_task = task_and_context
    operation_completeness_context = ""
    if WORKER_OPERATION_COMPLETENESS_MARKER in current_task:
        current_task, operation_completeness_context = current_task.split(
            WORKER_OPERATION_COMPLETENESS_MARKER,
            1,
        )

    operation_execution_context = ""
    if WORKER_OPERATION_EXECUTION_MARKER in current_task:
        current_task, operation_execution_context = current_task.split(
            WORKER_OPERATION_EXECUTION_MARKER,
            1,
        )

    original_task = ""
    if WORKER_ORIGINAL_TASK_MARKER in current_task:
        current_task, original_task_text = current_task.rsplit(
            WORKER_ORIGINAL_TASK_MARKER,
            1,
        )
        try:
            decoded_original_task = json.loads(original_task_text.strip())
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            raise ValueError("Legacy worker envelope has invalid JSON") from exc
        if not (
            isinstance(decoded_original_task, Mapping)
            and set(decoded_original_task) == {"original_task"}
            and isinstance(decoded_original_task["original_task"], str)
        ):
            raise ValueError(
                "Legacy worker envelope has invalid original_task payload"
            )
        original_task = decoded_original_task["original_task"]

    return WorkerRequestParts(
        current_task=current_task.strip(),
        original_task=original_task,
        operation_execution_context=operation_execution_context.strip(),
        operation_completeness_context=operation_completeness_context.strip(),
        previous_results=previous_results,
    )


class EvidenceFact(BaseModel):
    """One compact fact with explicit provenance."""

    model_config = ConfigDict(extra="forbid")

    text: str = Field(min_length=1)
    evidence_ids: List[str] = Field(default_factory=list, max_length=20)

    @field_validator("text")
    @classmethod
    def _strip_text(cls, value: str) -> str:
        clean_value = str(value or "").strip()
        if not clean_value:
            raise ValueError("fact text must not be blank")
        return clean_value

    @field_validator("evidence_ids", mode="before")
    @classmethod
    def _clean_evidence_ids(cls, value: Any) -> List[str]:
        if value is None:
            return []
        if not isinstance(value, list):
            raise ValueError("evidence_ids must be an array")
        return list(
            dict.fromkeys(
                clean_item
                for item in value
                if (clean_item := str(item or "").strip())
            )
        )


class Observation(BaseModel):
    """Cumulative worker assessment over accepted evidence."""

    model_config = ConfigDict(extra="forbid")

    status: ObservationStatus = Field(
        description=(
            "complete — нужные исходные данные получены; continue — нужен "
            "ещё один вызов текущей палитры; reroute — нужна новая палитра."
        )
    )
    gap: Optional[str] = Field(
        default=None,
        description=(
            "Одна краткая консолидированная строка обо всех незакрытых "
            "требованиях исходной task из текущего результата и prior_state. "
            "Не перечисляй одну причину и её следствия как разные проблемы. "
            "Null только при status=complete."
        ),
    )
    accepted_tool_call_ids: List[str] = Field(
        default_factory=list,
        max_length=20,
        description=(
            "Накопительный список идентификаторов успешных tool results, "
            "которые семантически подтверждают исходную task."
        ),
    )
    facts: List[EvidenceFact] = Field(
        default_factory=list,
        description=(
            "Подтверждённые накопительные факты. Каждый факт явно ссылается "
            "на evidence_id из принятых tool results."
        ),
    )
    limitations: List[str] = Field(
        default_factory=list,
        description=(
            "Ограничения, неоднозначности и непроверенные предположения "
            "результата. Не выбирай следующий инструмент."
        ),
    )
    reroute_reason: Optional[RerouteReason] = Field(
        default=None,
        description=(
            "Структурированная причина смены маршрута. Для исправления "
            "аргументов используй continue, а не reroute."
        ),
    )
    required_capabilities: List[WorkerCapability] = Field(
        default_factory=list,
        description=(
            "Недостающие возможности новой палитры; заполняются только когда "
            "текущие available_tools не могут закрыть gap."
        ),
    )

    @model_validator(mode="before")
    @classmethod
    def _discard_non_reroute_metadata(cls, value: Any) -> Any:
        """Treat status as authoritative for provider-added route metadata."""
        if not isinstance(value, Mapping):
            return value
        normalized = dict(value)
        for input_only_field in (
            "prior_state",
            "accepted_evidence",
            "tool_calls",
            "tool_results",
            "candidate_answer",
            "available_tools",
            "user_request",
            "previous_results",
        ):
            normalized.pop(input_only_field, None)
        if normalized.get("status") != "reroute":
            normalized.pop("reroute_reason", None)
            normalized.pop("required_capabilities", None)
        return normalized

    @field_validator(
        "accepted_tool_call_ids",
        "limitations",
        "required_capabilities",
        mode="before",
    )
    @classmethod
    def _remove_blank_list_items(
        cls,
        value: Any,
        info: ValidationInfo,
    ) -> List[str]:
        if value is None:
            return []
        if not isinstance(value, list):
            raise ValueError("observation list fields must be arrays")
        values = [
            clean_item
            for item in value
            if (clean_item := str(item or "").strip())
        ]
        if info.field_name == "required_capabilities":
            aliases = {
                "source_column_catalog_read": "column_catalog_read",
                "target_column_catalog_read": "column_catalog_read",
                "column_metadata_read": "column_catalog_read",
                "s2t_catalog_read": "s2t_read",
            }
            values = [aliases.get(item, item) for item in values]
        return values

    @field_validator("reroute_reason", mode="before")
    @classmethod
    def _unwrap_reroute_reason(cls, value: Any) -> Any:
        if not isinstance(value, Mapping):
            return value
        allowed = {
            "missing_capability",
            "unresolved_entity",
            "wrong_arguments",
            "truncated_result",
            "tool_error",
        }
        for key in ("type", "code", "reroute_reason"):
            candidate = str(value.get(key) or "").strip()
            if candidate in allowed:
                return candidate
        return value

    @field_validator("accepted_tool_call_ids")
    @classmethod
    def _deduplicate_accepted_tool_call_ids(
        cls,
        values: List[str],
    ) -> List[str]:
        return list(dict.fromkeys(values))

    @field_validator("gap", mode="before")
    @classmethod
    def _normalize_gap(cls, value: Any) -> Optional[str]:
        if value is None:
            return None
        clean_value = str(value).strip()
        if clean_value.casefold() == "null":
            return None
        return clean_value or None

    @model_validator(mode="after")
    def _gap_matches_status(self) -> "Observation":
        if self.status == "complete" and self.gap is not None:
            raise ValueError("gap must be null when status is complete")
        if self.status != "complete" and self.gap is None:
            raise ValueError(
                "gap must describe why observation is not complete"
            )
        if self.status != "reroute" and (
            self.reroute_reason is not None or self.required_capabilities
        ):
            raise ValueError(
                "reroute metadata is allowed only when status is reroute"
            )
        if self.reroute_reason in {
            "missing_capability",
            "unresolved_entity",
            "truncated_result",
        } and not self.required_capabilities:
            raise ValueError(
                "reroute reason requires required_capabilities"
            )
        return self


class EvidenceArtifact(BaseModel):
    """Accepted bounded tool evidence plus runtime-only references."""

    model_config = ConfigDict(extra="forbid")

    evidence_id: str = Field(min_length=1)
    tool_name: str = Field(min_length=1)
    compact_args: Dict[str, Any] = Field(default_factory=dict)
    preview: str = ""
    truncated: bool = False
    display_ref: Optional[str] = Field(default=None, exclude=True)
    dataset_ref: Optional[str] = Field(default=None, exclude=True)

    @field_validator("evidence_id", "tool_name")
    @classmethod
    def _strip_required_text(cls, value: str) -> str:
        clean_value = str(value or "").strip()
        if not clean_value:
            raise ValueError("evidence identifiers must not be blank")
        return clean_value

    @field_validator("display_ref", "dataset_ref", mode="before")
    @classmethod
    def _normalize_optional_ref(cls, value: Any) -> Optional[str]:
        if value is None:
            return None
        clean_value = str(value).strip()
        return clean_value or None


class SavedResultDescriptor(BaseModel):
    """Internal run-scoped descriptor of a materialized tabular result."""

    model_config = ConfigDict(extra="forbid")

    result_ref: str
    source_tool: str
    source_tool_call_id: Optional[str] = Field(default=None, exclude=True)
    worker_execution_id: Optional[str] = Field(default=None, exclude=True)
    row_count: int = Field(ge=0)
    source_total: Optional[int] = Field(default=None, ge=0)
    truncated: bool = False
    input_truncated: bool = False
    columns: List[SavedResultColumn] = Field(default_factory=list)


class WorkerOutcome(BaseModel):
    """Final worker result for upstream plus runtime-only lazy references."""

    model_config = ConfigDict(extra="forbid")

    summary: str
    status: WorkerOutcomeStatus = "complete"
    stop_reason: Optional[WorkerStopReason] = None
    unmet_requirements: List[str] = Field(default_factory=list)
    facts: List[EvidenceFact] = Field(default_factory=list)
    evidence: List[EvidenceArtifact] = Field(default_factory=list)
    datasets: List[SavedResultDescriptor] = Field(
        default_factory=list,
        exclude=True,
    )
    previous_results: List[PreviousResultReference] = Field(
        default_factory=list,
        exclude=True,
    )

    @field_validator("summary")
    @classmethod
    def _strip_summary(cls, value: str) -> str:
        clean_value = str(value or "").strip()
        if not clean_value:
            raise ValueError("worker summary must not be blank")
        return clean_value

    @field_validator("unmet_requirements", mode="before")
    @classmethod
    def _clean_unmet_requirements(cls, value: Any) -> List[str]:
        if value is None:
            return []
        if not isinstance(value, list):
            raise ValueError("unmet_requirements must be an array")
        return list(
            dict.fromkeys(
                clean_item
                for item in value
                if (clean_item := str(item or "").strip())
            )
        )

    @model_validator(mode="after")
    def _validate_status_and_provenance(self) -> "WorkerOutcome":
        if self.status == "complete" and self.unmet_requirements:
            raise ValueError(
                "complete worker outcome cannot have unmet_requirements"
            )
        if self.status != "complete" and self.stop_reason is None:
            raise ValueError(
                "partial or failed worker outcome requires stop_reason"
            )
        evidence_ids = [item.evidence_id for item in self.evidence]
        if len(evidence_ids) != len(set(evidence_ids)):
            raise ValueError("worker evidence_id values must be unique")
        known_evidence_ids = set(evidence_ids)
        unknown_fact_ids = sorted(
            {
                evidence_id
                for fact in self.facts
                for evidence_id in fact.evidence_ids
                if evidence_id not in known_evidence_ids
            }
        )
        if unknown_fact_ids:
            raise ValueError(
                "worker facts reference unknown evidence_id values: "
                + ", ".join(unknown_fact_ids)
            )

        dataset_refs = [item.result_ref for item in self.datasets]
        if len(dataset_refs) != len(set(dataset_refs)):
            raise ValueError("worker dataset refs must be unique")
        known_dataset_refs = set(dataset_refs)
        unknown_dataset_refs = sorted(
            {
                item.dataset_ref
                for item in self.evidence
                if item.dataset_ref is not None
                and item.dataset_ref not in known_dataset_refs
            }
        )
        if unknown_dataset_refs:
            raise ValueError(
                "worker evidence references unknown dataset_ref values: "
                + ", ".join(unknown_dataset_refs)
            )
        result_ids = [item.result_id for item in self.previous_results]
        if len(result_ids) != len(set(result_ids)):
            raise ValueError("worker previous result ids must be unique")
        return self

    def upstream_payload(self) -> Dict[str, Any]:
        """Serialize accepted evidence without worker interpretations."""
        return {
            "evidence": [
                {
                    "evidence_id": item.evidence_id,
                    "tool_name": item.tool_name,
                    "args": item.compact_args,
                    "preview": item.preview,
                    "truncated": item.truncated,
                    "displayable": item.display_ref is not None,
                }
                for item in self.evidence
            ],
        }

    def handoff_payload(self) -> Dict[str, Any]:
        """Serialize only lazy result references for later workers."""
        return {
            "previous_results": [
                item.model_dump(mode="json", exclude_none=True)
                for item in self.previous_results
            ],
        }


class PlanStep(BaseModel):
    """One worker task and its explicit DAG dependencies."""

    model_config = ConfigDict(extra="forbid")

    id: Optional[str] = Field(
        default=None,
        description=(
            "Уникальный стабильный ID шага. Для legacy-плана без ID "
            "coordinator создаёт последовательные step_1..step_N."
        ),
    )

    task: str = Field(
        min_length=1,
        description=(
            "Готовая задача одного worker на получение исходных данных. "
            "Производный анализ выполняет upstream coordinator."
        ),
    )
    depends_on: List[str] = Field(
        default_factory=list,
        description=(
            "ID шагов, результаты которых нужны этой task. Пустой список "
            "означает, что task готова к независимому запуску."
        ),
    )

    @field_validator("id")
    @classmethod
    def _validate_id(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        clean_value = value.strip()
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,63}", clean_value):
            raise ValueError(
                "id must use 1-64 ASCII letters, digits, '.', '_' or '-'"
            )
        return clean_value

    @field_validator("task")
    @classmethod
    def _strip_task(cls, value: str) -> str:
        clean_value = value.strip()
        if not clean_value:
            raise ValueError("task must not be blank")
        return clean_value

    @field_validator("depends_on")
    @classmethod
    def _validate_dependencies(cls, values: List[str]) -> List[str]:
        result: List[str] = []
        for value in values:
            clean_value = str(value or "").strip()
            if not re.fullmatch(
                r"[A-Za-z0-9][A-Za-z0-9_.-]{0,63}",
                clean_value,
            ):
                raise ValueError(
                    "depends_on IDs must use 1-64 ASCII letters, digits, "
                    "'.', '_' or '-'"
                )
            if clean_value in result:
                raise ValueError(
                    f"depends_on must not contain duplicate ID {clean_value!r}"
                )
            result.append(clean_value)
        return result


class WorkerPlan(BaseModel):
    """Validated worker DAG with a legacy linear-plan compatibility path."""

    model_config = ConfigDict(extra="forbid")

    steps: List[PlanStep] = Field(
        min_length=1,
        max_length=MAX_PLAN_STEPS,
        description=(
            "DAG worker tasks на получение данных. Независимые tasks могут "
            "выполняться конкурентно; зависимая task может лениво использовать "
            "принятые результаты только перечисленных depends_on."
        ),
    )

    @model_validator(mode="after")
    def _validate_and_normalize_dag(self) -> "WorkerPlan":
        has_ids = [step.id is not None for step in self.steps]
        if any(has_ids) and not all(has_ids):
            raise ValueError("all DAG steps must either provide id or omit it")

        if not any(has_ids):
            if any(step.depends_on for step in self.steps):
                raise ValueError("legacy steps without id cannot use depends_on")
            previous_ids: List[str] = []
            for index, step in enumerate(self.steps, start=1):
                step.id = f"step_{index}"
                # Preserve the old handoff contract exactly: every later
                # worker can see all accepted results from earlier workers.
                step.depends_on = list(previous_ids)
                previous_ids.append(step.id)

        ids = [str(step.id) for step in self.steps]
        duplicate_ids = sorted(
            step_id for step_id in set(ids) if ids.count(step_id) > 1
        )
        if duplicate_ids:
            raise ValueError(
                "duplicate DAG step ids: " + ", ".join(duplicate_ids)
            )

        known_ids = set(ids)
        for step in self.steps:
            assert step.id is not None
            if step.id in step.depends_on:
                raise ValueError(f"step {step.id!r} cannot depend on itself")
            missing = sorted(set(step.depends_on) - known_ids)
            if missing:
                raise ValueError(
                    f"step {step.id!r} has missing dependencies: "
                    + ", ".join(missing)
                )

        completed: set[str] = set()
        while len(completed) < len(self.steps):
            ready = [
                step
                for step in self.steps
                if step.id not in completed
                and set(step.depends_on).issubset(completed)
            ]
            if not ready:
                unresolved = [
                    str(step.id)
                    for step in self.steps
                    if step.id not in completed
                ]
                raise ValueError(
                    "worker plan contains a dependency cycle: "
                    + ", ".join(unresolved)
                )
            completed.update(str(step.id) for step in ready)
        return self

    def ready_groups(self) -> List[List[PlanStep]]:
        """Return deterministic topological layers in declared plan order."""

        completed: set[str] = set()
        groups: List[List[PlanStep]] = []
        while len(completed) < len(self.steps):
            ready = [
                step
                for step in self.steps
                if step.id not in completed
                and set(step.depends_on).issubset(completed)
            ]
            if not ready:  # Defensive: validation already rejects cycles.
                raise ValueError("worker plan contains a dependency cycle")
            groups.append(ready)
            completed.update(str(step.id) for step in ready)
        return groups


class UpstreamOutput(BaseModel):
    """Final answer and evidence selection assembled by upstream."""

    model_config = ConfigDict(extra="forbid")

    answer: str = Field(
        min_length=1,
        description=(
            "Готовый пользовательский ответ, сформированный по фактическим "
            "preview результатов tools."
        ),
    )
    used_evidence_ids: List[str] = Field(
        default_factory=list,
        description=(
            "Все evidence_id, факты которых использованы при формировании answer."
        ),
    )
    display_evidence_ids: List[str] = Field(
        default_factory=list,
        description=(
            "Evidence_id результатов, которые нужно показать отдельно и "
            "которые поддерживают сформированный ответ."
        ),
    )

    @field_validator("answer", mode="before")
    @classmethod
    def _serialize_structured_answer(cls, value: Any) -> Any:
        if isinstance(value, (Mapping, list, tuple, int, float, bool)):
            return json.dumps(value, ensure_ascii=False, separators=(",", ":"))
        return value

    @field_validator("answer")
    @classmethod
    def _strip_answer(cls, value: str) -> str:
        clean_value = value.strip()
        if not clean_value:
            raise ValueError("answer must not be blank")
        return clean_value

    @field_validator("used_evidence_ids", "display_evidence_ids")
    @classmethod
    def _clean_output_evidence_ids(cls, values: List[str]) -> List[str]:
        result: List[str] = []
        for value in values:
            clean_value = str(value or "").strip()
            if not clean_value:
                raise ValueError("evidence id lists must not contain blanks")
            if clean_value not in result:
                result.append(clean_value)
        return result

    @model_validator(mode="after")
    def _display_ids_are_used(self) -> "UpstreamOutput":
        unused_display_ids = sorted(
            set(self.display_evidence_ids) - set(self.used_evidence_ids)
        )
        if unused_display_ids:
            raise ValueError(
                "display_evidence_ids must be included in used_evidence_ids"
            )
        return self


class UpstreamDecision(BaseModel):
    """Data-sufficiency decision made before the upstream answer."""

    model_config = ConfigDict(extra="forbid")

    decision: UpstreamAction
    problem: str = ""

    @field_validator("problem", mode="before")
    @classmethod
    def _normalize_optional_text(cls, value: Any) -> str:
        if value is None:
            return ""
        return str(value)

    @field_validator("problem")
    @classmethod
    def _normalize_text(cls, value: str) -> str:
        clean_value = value.strip()
        return "" if clean_value.casefold() == "null" else clean_value


__all__ = [
    "EvidenceArtifact",
    "EvidenceFact",
    "MAX_PLAN_STEPS",
    "Observation",
    "ObservationStatus",
    "PlanStep",
    "PreviousResultReference",
    "PreviousResultSchema",
    "SavedResultColumn",
    "SavedResultDescriptor",
    "SqlRiskAspect",
    "OperationPipeline",
    "UpstreamOutput",
    "UpstreamAction",
    "UpstreamDecision",
    "WorkerRequestParts",
    "WorkerCapability",
    "WorkerOutcome",
    "WorkerOutcomeStatus",
    "WorkerStopReason",
    "WorkerPlan",
    "RerouteReason",
    "WORKER_ORIGINAL_TASK_MARKER",
    "WORKER_PREVIOUS_RESULTS_MARKER",
    "WORKER_OPERATION_COMPLETENESS_MARKER",
    "WORKER_OPERATION_EXECUTION_MARKER",
    "parse_legacy_worker_request",
    "parse_worker_request",
]
