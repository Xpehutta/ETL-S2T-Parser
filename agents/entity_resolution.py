"""Shared deterministic entity resolution for validation and agentic flows."""

from __future__ import annotations

import unicodedata
from difflib import SequenceMatcher
from typing import Any, Dict, List, Literal, Mapping, Optional, Sequence

from pydantic import BaseModel, ConfigDict, Field, model_validator

EntityType = Literal["file", "table"]
EntityRole = Literal["file", "source", "target"]
ResolutionStrategy = Literal["auto", "semantic"]
ResolutionMethod = Literal[
    "exact",
    "normalized_exact",
    "partial",
    "fuzzy",
    "semantic",
    "none",
]
ResolutionStatus = Literal["resolved", "ambiguous", "unresolved"]
CandidateCoverage = Literal["complete", "truncated"]

DEFAULT_FUZZY_THRESHOLD = 0.84
DEFAULT_FUZZY_GAP = 0.06
DEFAULT_SEMANTIC_THRESHOLD = 0.60
DEFAULT_SEMANTIC_GAP = 0.05
DEFAULT_SEMANTIC_LIMIT = 20
MAX_BATCH_ENTITIES = 50


class EntityMention(BaseModel):
    """One unresolved or potentially canonical user mention."""

    model_config = ConfigDict(extra="forbid")

    mention: str = Field(min_length=1, max_length=1000)
    entity_type: EntityType
    role: EntityRole
    strategy: ResolutionStrategy = "auto"
    file_id: Optional[int] = Field(default=None, gt=0)

    @model_validator(mode="after")
    def role_matches_type(self) -> "EntityMention":
        clean_mention = self.mention.strip()
        if not clean_mention:
            raise ValueError("mention must not be blank")
        if self.entity_type == "file" and self.role != "file":
            raise ValueError("file entity must use role='file'")
        if self.entity_type == "table" and self.role == "file":
            raise ValueError("table entity must use source or target role")
        if self.entity_type == "file" and self.file_id is not None:
            raise ValueError("file_id scope is only valid for table entities")
        self.mention = clean_mention
        return self


class ResolutionCandidate(BaseModel):
    """One canonical identity together with every retained provenance row."""

    model_config = ConfigDict(extra="forbid")

    canonical_name: str = Field(min_length=1)
    entity_type: EntityType
    role: EntityRole
    file_id: Optional[int] = None
    score: Optional[float] = None
    method: ResolutionMethod
    provenance: List[Dict[str, Any]] = Field(default_factory=list)


class EntityCandidateSet(BaseModel):
    """Lossless candidate identities returned by one resolution stage."""

    model_config = ConfigDict(extra="forbid")

    candidates: List[ResolutionCandidate] = Field(default_factory=list)
    coverage: CandidateCoverage = "complete"
    source: str = Field(min_length=1)
    total_candidates: int = Field(default=0, ge=0)
    source_result_id: Optional[str] = None
    threshold: Optional[float] = None
    minimum_gap: Optional[float] = None


# Public domain alias for resolver-specific candidate metadata.
CandidateSet = EntityCandidateSet


class EntityResolution(BaseModel):
    """Structured result which never hides unresolved ambiguity."""

    model_config = ConfigDict(extra="forbid")

    mention: str
    entity_type: EntityType
    role: EntityRole
    status: ResolutionStatus
    method: ResolutionMethod
    canonical_name: Optional[str] = None
    file_id: Optional[int] = None
    error_code: Optional[Literal["unresolved_entity", "ambiguous_entity"]] = None
    reason: str = ""
    candidate_set: EntityCandidateSet

    @model_validator(mode="after")
    def outcome_is_consistent(self) -> "EntityResolution":
        if self.status == "resolved":
            if not self.canonical_name:
                raise ValueError("resolved entity requires canonical_name")
            if self.error_code is not None:
                raise ValueError("resolved entity cannot have error_code")
        elif self.canonical_name is not None or self.file_id is not None:
            raise ValueError("unresolved entity cannot expose a selected identity")
        if self.status == "ambiguous" and self.error_code != "ambiguous_entity":
            raise ValueError("ambiguous entity requires ambiguous_entity error_code")
        if self.status == "unresolved" and self.error_code != "unresolved_entity":
            raise ValueError("unresolved entity requires unresolved_entity error_code")
        return self


def normalize_entity_name(value: Any) -> str:
    """Normalize spelling only for candidate comparison, never for output."""
    text = unicodedata.normalize("NFKC", str(value or ""))
    text = text.casefold().replace("ё", "е")
    return "".join(character for character in text if character.isalnum())


def _exact_identity(value: Any) -> str:
    return unicodedata.normalize("NFKC", str(value or "")).strip().casefold()


def _invoke(tool: Any, args: Dict[str, Any], callbacks: Sequence[Any]) -> Any:
    config = {"callbacks": list(callbacks)} if callbacks else None
    return tool.invoke(args, config=config) if config is not None else tool.invoke(args)


def _resolve_file_exact(args: Dict[str, Any], callbacks: Sequence[Any]) -> Any:
    # Imports stay lazy: agents.tools.__init__ exports the resolver tool and
    # would otherwise form a package-import cycle on a clean interpreter.
    from .tools.files import resolve_file

    return _invoke(resolve_file, args, callbacks)


def _list_file_candidates(callbacks: Sequence[Any]) -> Any:
    from .tools.files import list_files

    return _invoke(list_files, {}, callbacks)


def _semantic_search(args: Dict[str, Any], callbacks: Sequence[Any]) -> Any:
    from .tools.data import semantic_search_descriptions

    return _invoke(semantic_search_descriptions, args, callbacks)


def _source_result_id(payload: Mapping[str, Any]) -> Optional[str]:
    for field in ("result_id", "source_result_id"):
        value = str(payload.get(field) or "").strip()
        if value:
            return value
    saved = payload.get("saved_result")
    if isinstance(saved, Mapping):
        for field in ("result_id", "result_ref"):
            value = str(saved.get(field) or "").strip()
            if value:
                return value
    return None


def _candidate_identity(
    *,
    entity_type: EntityType,
    canonical_name: str,
    file_id: Optional[int],
) -> str:
    if entity_type == "file" and file_id is not None:
        return f"file:{file_id}"
    return _exact_identity(canonical_name)


def _aggregate_candidates(
    rows: Sequence[Mapping[str, Any]],
    *,
    entity_type: EntityType,
    role: EntityRole,
    method: ResolutionMethod,
    name_field: str,
    score_field: Optional[str] = None,
) -> List[ResolutionCandidate]:
    ordered: Dict[str, Dict[str, Any]] = {}
    for raw_row in rows:
        row = dict(raw_row)
        canonical_name = str(row.get(name_field) or "").strip()
        if not canonical_name:
            continue
        raw_file_id = row.get("file_id")
        try:
            file_id = int(raw_file_id) if raw_file_id is not None else None
        except (TypeError, ValueError):
            file_id = None
        identity = _candidate_identity(
            entity_type=entity_type,
            canonical_name=canonical_name,
            file_id=file_id,
        )
        score: Optional[float] = None
        if score_field is not None and row.get(score_field) is not None:
            try:
                score = float(row[score_field])
            except (TypeError, ValueError):
                score = None
        current = ordered.get(identity)
        if current is None:
            current = {
                "canonical_name": canonical_name,
                "entity_type": entity_type,
                "role": role,
                "file_id": file_id if entity_type == "file" else None,
                "score": score,
                "method": method,
                "provenance": [],
            }
            ordered[identity] = current
        elif score is not None and (
            current["score"] is None or score > current["score"]
        ):
            current["score"] = score
        # Deliberately append every source row. Repeated S2T/catalog rows are
        # evidence provenance, not duplicate candidate identities to discard.
        current["provenance"].append(row)
    return [ResolutionCandidate.model_validate(value) for value in ordered.values()]


def _candidate_set(
    candidates: Sequence[ResolutionCandidate],
    *,
    source: str,
    coverage: CandidateCoverage = "complete",
    total_candidates: Optional[int] = None,
    source_result_id: Optional[str] = None,
    threshold: Optional[float] = None,
    minimum_gap: Optional[float] = None,
) -> EntityCandidateSet:
    items = list(candidates)
    return EntityCandidateSet(
        candidates=items,
        coverage=coverage,
        source=source,
        total_candidates=(len(items) if total_candidates is None else total_candidates),
        source_result_id=source_result_id,
        threshold=threshold,
        minimum_gap=minimum_gap,
    )


def _resolved(
    request: EntityMention,
    method: ResolutionMethod,
    candidate_set: EntityCandidateSet,
    candidate: ResolutionCandidate,
) -> EntityResolution:
    return EntityResolution(
        mention=request.mention,
        entity_type=request.entity_type,
        role=request.role,
        status="resolved",
        method=method,
        canonical_name=candidate.canonical_name,
        file_id=candidate.file_id,
        reason=f"Однозначное совпадение методом {method}.",
        candidate_set=candidate_set,
    )


def _ambiguous(
    request: EntityMention,
    method: ResolutionMethod,
    candidate_set: EntityCandidateSet,
) -> EntityResolution:
    return EntityResolution(
        mention=request.mention,
        entity_type=request.entity_type,
        role=request.role,
        status="ambiguous",
        method=method,
        error_code="ambiguous_entity",
        reason=(
            "Найдено несколько допустимых канонических кандидатов; "
            "автоматический выбор запрещён."
        ),
        candidate_set=candidate_set,
    )


def _unresolved(
    request: EntityMention,
    method: ResolutionMethod,
    candidate_set: EntityCandidateSet,
    reason: str,
) -> EntityResolution:
    return EntityResolution(
        mention=request.mention,
        entity_type=request.entity_type,
        role=request.role,
        status="unresolved",
        method=method,
        error_code="unresolved_entity",
        reason=reason,
        candidate_set=candidate_set,
    )


def _table_rows(
    role: Literal["source", "target"],
    exact: Optional[str] = None,
) -> List[Dict[str, Any]]:
    from storage.database import get_db_connection

    column = f"{role}_table"
    where = f"NULLIF(TRIM({column}), '') IS NOT NULL"
    params: List[Any] = []
    if exact is not None:
        where += f" AND LOWER(TRIM({column})) = LOWER(TRIM(?))"
        params.append(str(exact).strip())
    conn = get_db_connection()
    try:
        rows = conn.execute(
            f"""
            SELECT id AS record_id, file_id, sheet_name, row_num,
                   TRIM({column}) AS canonical_name,
                   '{role}' AS table_role
            FROM s2t_transformations
            WHERE {where}
            ORDER BY id
            """,
            params,
        ).fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()


def _file_exact(
    request: EntityMention,
    callbacks: Sequence[Any],
) -> Optional[EntityResolution]:
    payload = _resolve_file_exact({"filename": request.mention}, callbacks)
    if not isinstance(payload, Mapping):
        return _unresolved(
            request,
            "exact",
            _candidate_set([], source="resolve_file"),
            "Exact file resolver вернул неизвестный формат.",
        )
    if not payload.get("error"):
        candidates = _aggregate_candidates(
            [payload],
            entity_type="file",
            role="file",
            method="exact",
            name_field="filename",
        )
        candidate_set = _candidate_set(candidates, source="resolve_file")
        if len(candidates) == 1:
            return _resolved(request, "exact", candidate_set, candidates[0])
    matches = payload.get("matches")
    if isinstance(matches, list) and matches:
        candidates = _aggregate_candidates(
            [row for row in matches if isinstance(row, Mapping)],
            entity_type="file",
            role="file",
            method="exact",
            name_field="filename",
        )
        candidate_set = _candidate_set(candidates, source="resolve_file")
        return _ambiguous(request, "exact", candidate_set)
    if payload.get("error") == "Uploaded file not found":
        return None
    return _unresolved(
        request,
        "exact",
        _candidate_set([], source="resolve_file"),
        str(payload.get("error") or "Exact file resolution failed."),
    )


def _exact_table(request: EntityMention) -> Optional[EntityResolution]:
    role = request.role
    assert role in {"source", "target"}
    rows = _table_rows(role, exact=request.mention)
    if not rows:
        return None
    candidates = _aggregate_candidates(
        rows,
        entity_type="table",
        role=role,
        method="exact",
        name_field="canonical_name",
    )
    candidate_set = _candidate_set(candidates, source="global_s2t_exact")
    if len(candidates) == 1:
        return _resolved(request, "exact", candidate_set, candidates[0])
    return _ambiguous(request, "exact", candidate_set)


def _candidate_universe(
    request: EntityMention,
    callbacks: Sequence[Any],
) -> List[ResolutionCandidate]:
    if request.entity_type == "file":
        payload = _list_file_candidates(callbacks)
        rows = payload if isinstance(payload, list) else []
        return _aggregate_candidates(
            [row for row in rows if isinstance(row, Mapping)],
            entity_type="file",
            role="file",
            method="none",
            name_field="filename",
        )
    assert request.role in {"source", "target"}
    rows = _table_rows(request.role)
    if request.file_id is not None:
        # Exact canonical S2T verification intentionally remains global.  A
        # file scope applies only after exact bypass, when fuzzy/partial and
        # semantic mentions must not be resolved against another upload.
        rows = [
            row
            for row in rows
            if row.get("file_id") is not None
            and int(row["file_id"]) == request.file_id
        ]
    return _aggregate_candidates(
        rows,
        entity_type="table",
        role=request.role,
        method="none",
        name_field="canonical_name",
    )


def _with_method(
    candidate: ResolutionCandidate,
    method: ResolutionMethod,
    *,
    score: Optional[float] = None,
) -> ResolutionCandidate:
    updates: Dict[str, Any] = {"method": method}
    if score is not None:
        updates["score"] = round(score, 6)
    return candidate.model_copy(update=updates)


def _normalized_or_partial(
    request: EntityMention,
    universe: Sequence[ResolutionCandidate],
) -> Optional[EntityResolution]:
    normalized_mention = normalize_entity_name(request.mention)
    if not normalized_mention:
        return None
    normalized = [
        _with_method(candidate, "normalized_exact")
        for candidate in universe
        if normalize_entity_name(candidate.canonical_name) == normalized_mention
    ]
    if normalized:
        candidate_set = _candidate_set(
            normalized,
            source="canonical_entity_universe",
        )
        if len(normalized) == 1:
            return _resolved(
                request,
                "normalized_exact",
                candidate_set,
                normalized[0],
            )
        return _ambiguous(request, "normalized_exact", candidate_set)

    partial = [
        _with_method(candidate, "partial")
        for candidate in universe
        if normalized_mention in normalize_entity_name(candidate.canonical_name)
    ]
    if not partial:
        return None
    candidate_set = _candidate_set(partial, source="canonical_entity_universe")
    if len(partial) == 1:
        return _resolved(request, "partial", candidate_set, partial[0])
    return _ambiguous(request, "partial", candidate_set)


def _fuzzy(
    request: EntityMention,
    universe: Sequence[ResolutionCandidate],
    *,
    threshold: float,
    minimum_gap: float,
) -> tuple[Optional[EntityResolution], EntityCandidateSet]:
    normalized_mention = normalize_entity_name(request.mention)
    scored: List[ResolutionCandidate] = []
    for candidate in universe:
        normalized_candidate = normalize_entity_name(candidate.canonical_name)
        if not normalized_mention or not normalized_candidate:
            continue
        score = SequenceMatcher(
            None,
            normalized_mention,
            normalized_candidate,
        ).ratio()
        scored.append(_with_method(candidate, "fuzzy", score=score))
    scored.sort(
        key=lambda item: (
            -(item.score or 0.0),
            item.canonical_name.casefold(),
            item.file_id or 0,
        )
    )
    if not scored:
        empty = _candidate_set(
            [],
            source="canonical_entity_universe",
            threshold=threshold,
            minimum_gap=minimum_gap,
        )
        return None, empty
    top_score = scored[0].score or 0.0
    plausible_cutoff = max(0.0, min(threshold, top_score - minimum_gap))
    plausible = [
        candidate
        for candidate in scored
        if (candidate.score or 0.0) >= plausible_cutoff
    ]
    candidate_set = _candidate_set(
        plausible,
        source="canonical_entity_universe",
        total_candidates=len(plausible),
        threshold=threshold,
        minimum_gap=minimum_gap,
    )
    if top_score < threshold:
        return None, candidate_set
    runner_up_score = scored[1].score if len(scored) > 1 else None
    if runner_up_score is not None and top_score - runner_up_score < minimum_gap:
        return _ambiguous(request, "fuzzy", candidate_set), candidate_set
    return _resolved(request, "fuzzy", candidate_set, scored[0]), candidate_set


def _semantic(
    request: EntityMention,
    callbacks: Sequence[Any],
    *,
    threshold: float,
    minimum_gap: float,
    limit: int,
    table_universe: Sequence[ResolutionCandidate] = (),
) -> EntityResolution:
    scope = {
        "file": "files",
        "source": "source_tables",
        "target": "target_tables",
    }[request.role]
    args: Dict[str, Any] = {
        "query": request.mention,
        "scope": scope,
        "limit": max(2, min(int(limit), 50)),
    }
    if request.entity_type == "table" and request.file_id is not None:
        args["file_id"] = request.file_id
    payload = _semantic_search(args, callbacks)
    if not isinstance(payload, Mapping) or payload.get("error"):
        return _unresolved(
            request,
            "semantic",
            _candidate_set(
                [],
                source="semantic_search_descriptions",
                threshold=threshold,
                minimum_gap=minimum_gap,
            ),
            str(
                payload.get("error")
                if isinstance(payload, Mapping)
                else "Semantic resolver вернул неизвестный формат."
            ),
        )
    raw_rows = [
        row
        for row in payload.get("rows", [])
        if isinstance(row, Mapping) and row.get("scope") == scope
    ]
    name_field = "filename" if request.entity_type == "file" else "name"
    candidates = _aggregate_candidates(
        raw_rows,
        entity_type=request.entity_type,
        role=request.role,
        method="semantic",
        name_field=name_field,
        score_field="score",
    )
    if request.entity_type == "table":
        # The semantic catalog proposes names; global S2T remains the
        # authoritative role-aware namespace used by the downstream readers.
        verified_names = {
            _exact_identity(candidate.canonical_name)
            for candidate in table_universe
        }
        candidates = [
            candidate
            for candidate in candidates
            if _exact_identity(candidate.canonical_name) in verified_names
        ]
    candidates.sort(
        key=lambda item: (
            -(item.score or 0.0),
            item.canonical_name.casefold(),
            item.file_id or 0,
        )
    )
    returned_rows = int(payload.get("returned_rows") or len(raw_rows))
    total_candidates = int(payload.get("total_candidates") or len(raw_rows))
    coverage: CandidateCoverage = (
        "truncated"
        if bool(payload.get("truncated")) or total_candidates > returned_rows
        else "complete"
    )
    candidate_set = _candidate_set(
        candidates,
        source="semantic_search_descriptions",
        coverage=coverage,
        total_candidates=total_candidates,
        source_result_id=_source_result_id(payload),
        threshold=threshold,
        minimum_gap=minimum_gap,
    )
    if not candidates or (candidates[0].score or 0.0) < threshold:
        return _unresolved(
            request,
            "semantic",
            candidate_set,
            "Нет semantic-кандидата выше порога принятия.",
        )
    top_score = candidates[0].score or 0.0
    runner_up_score = candidates[1].score if len(candidates) > 1 else None
    if runner_up_score is not None and top_score - runner_up_score < minimum_gap:
        return _ambiguous(request, "semantic", candidate_set)
    return _resolved(request, "semantic", candidate_set, candidates[0])


def resolve_entity(
    request: EntityMention | Mapping[str, Any],
    *,
    callbacks: Sequence[Any] = (),
    exact_checked: bool = False,
    fuzzy_threshold: float = DEFAULT_FUZZY_THRESHOLD,
    fuzzy_gap: float = DEFAULT_FUZZY_GAP,
    semantic_threshold: float = DEFAULT_SEMANTIC_THRESHOLD,
    semantic_gap: float = DEFAULT_SEMANTIC_GAP,
    semantic_limit: int = DEFAULT_SEMANTIC_LIMIT,
) -> EntityResolution:
    """Resolve one mention without selecting between ambiguous identities.

    ``exact_checked`` is reserved for callers that just used
    :func:`verify_exact_entity`; it prevents a duplicate exact backend call.
    """
    for name, value in (
        ("fuzzy_threshold", fuzzy_threshold),
        ("fuzzy_gap", fuzzy_gap),
        ("semantic_threshold", semantic_threshold),
        ("semantic_gap", semantic_gap),
    ):
        if not 0.0 <= float(value) <= 1.0:
            raise ValueError(f"{name} must be between 0 and 1")
    if int(semantic_limit) < 2:
        raise ValueError("semantic_limit must be at least 2")
    item = (
        request
        if isinstance(request, EntityMention)
        else EntityMention.model_validate(request)
    )
    if not exact_checked:
        exact = (
            _file_exact(item, callbacks)
            if item.entity_type == "file"
            else _exact_table(item)
        )
        if exact is not None:
            return exact

    universe = _candidate_universe(item, callbacks)
    normalized = _normalized_or_partial(item, universe)
    if normalized is not None and (
        normalized.method == "normalized_exact" or item.strategy == "auto"
    ):
        return normalized

    fuzzy_candidates = _candidate_set([], source="canonical_entity_universe")
    if item.strategy == "auto":
        fuzzy_result, fuzzy_candidates = _fuzzy(
            item,
            universe,
            threshold=fuzzy_threshold,
            minimum_gap=fuzzy_gap,
        )
        if fuzzy_result is not None:
            return fuzzy_result

    semantic_result = _semantic(
        item,
        callbacks,
        threshold=semantic_threshold,
        minimum_gap=semantic_gap,
        limit=semantic_limit,
        table_universe=universe if item.entity_type == "table" else (),
    )
    if (
        semantic_result.status == "unresolved"
        and not semantic_result.candidate_set.candidates
        and fuzzy_candidates.candidates
    ):
        return semantic_result.model_copy(update={"candidate_set": fuzzy_candidates})
    return semantic_result


def verify_exact_entity(
    request: EntityMention | Mapping[str, Any],
    *,
    callbacks: Sequence[Any] = (),
) -> Optional[EntityResolution]:
    """Verify a canonical identifier without entering approximate resolution.

    Callers use this fast path before deciding whether typo/partial/semantic
    resolution is needed.  It is intentionally limited to the existing exact
    file resolver and the exact role-specific global S2T namespace.
    """

    item = (
        request
        if isinstance(request, EntityMention)
        else EntityMention.model_validate(request)
    )
    return (
        _file_exact(item, callbacks)
        if item.entity_type == "file"
        else _exact_table(item)
    )


def resolve_entity_batch(
    requests: Sequence[EntityMention | Mapping[str, Any]],
    *,
    callbacks: Sequence[Any] = (),
) -> List[EntityResolution]:
    """Resolve up to fifty mentions in order with shared deterministic policy."""
    items = list(requests)
    if not items:
        raise ValueError("requests must contain at least one entity mention")
    if len(items) > MAX_BATCH_ENTITIES:
        raise ValueError(f"requests supports at most {MAX_BATCH_ENTITIES} entities")
    return [resolve_entity(item, callbacks=callbacks) for item in items]


__all__ = [
    "CandidateCoverage",
    "CandidateSet",
    "DEFAULT_FUZZY_GAP",
    "DEFAULT_FUZZY_THRESHOLD",
    "DEFAULT_SEMANTIC_GAP",
    "DEFAULT_SEMANTIC_LIMIT",
    "DEFAULT_SEMANTIC_THRESHOLD",
    "EntityMention",
    "EntityCandidateSet",
    "EntityResolution",
    "EntityRole",
    "EntityType",
    "MAX_BATCH_ENTITIES",
    "ResolutionCandidate",
    "ResolutionMethod",
    "ResolutionStatus",
    "normalize_entity_name",
    "resolve_entity",
    "resolve_entity_batch",
    "verify_exact_entity",
]
