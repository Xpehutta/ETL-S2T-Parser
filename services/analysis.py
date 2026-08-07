import logging
from typing import Any, Callable, Dict, List, Optional, Tuple

from agents.agent import get_model_name
from agents.sheet_group_classifier import classify_file_sheet_groups
from agents.summarizer_agent import ensure_file_description, summarize_file
from graph_storage import is_neo4j_configured
from processing.excel import convert_to_serializable
from services.graph_sync import sync_file_graph
from sheet_skills.s2t import S2TExtractionError, run_s2t_extraction_subagent
from sheet_skills.structured_metadata import extract_structured_metadata
from sheet_skills.table_catalog import extract_table_catalogs


logger = logging.getLogger(__name__)
ProgressCallback = Callable[[Dict[str, Any]], None]


def _emit_progress(
    callback: Optional[ProgressCallback],
    **updates: Any,
) -> None:
    if callback:
        callback(updates)


def try_generate_summary(file_id: int) -> Tuple[Optional[str], Optional[str]]:
    try:
        summary = summarize_file(file_id, save=True)
        if summary and str(summary).strip():
            return str(summary), None
        raise ValueError("LLM summary is empty")
    except Exception as exc:
        logger.error("Summary generation failed: %s", exc)
        return None, str(exc)


def try_generate_description(
    file_id: int,
    summary: Optional[str] = None,
    refresh: bool = False,
) -> Tuple[Optional[str], Optional[str]]:
    try:
        description = ensure_file_description(
            file_id,
            refresh=refresh,
            save=True,
            summary_override=summary,
        )
        if description and str(description).strip():
            return str(description), None
        raise ValueError("LLM description is empty")
    except Exception as exc:
        logger.error("Description generation failed: %s", exc)
        return None, str(exc)


def try_extract_s2t_transformations(
    file_id: int,
    sheet_group_analysis: Optional[Dict[str, Any]] = None,
) -> Tuple[int, Optional[str], Dict[str, Any]]:
    try:
        table_catalogs = extract_table_catalogs(file_id, sheet_group_analysis)
        structured_metadata = extract_structured_metadata(
            file_id,
            sheet_group_analysis,
        )
        report = run_s2t_extraction_subagent(
            file_id,
            sheet_group_analysis=sheet_group_analysis,
        )
        report["table_catalogs"] = table_catalogs
        report["structured_metadata"] = structured_metadata
        return int(report.get("verification", {}).get("count", 0)), None, report
    except S2TExtractionError as exc:
        logger.error("Useful-column extraction subagent failed: %s", exc)
        return 0, str(exc), exc.report
    except Exception as exc:
        logger.exception("Useful-column extraction failed unexpectedly")
        return 0, str(exc), {"status": "error", "error": str(exc)}


def try_sync_file_graph(
    file_id: int,
) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    if not is_neo4j_configured():
        message = (
            "Neo4j не настроен: добавьте NEO4J_URI, NEO4J_USERNAME, "
            "NEO4J_PASSWORD и NEO4J_DATABASE в .env. "
            "SQLite-анализ сохранён; lineage в Neo4j пропущен."
        )
        logger.warning("Neo4j synchronization skipped for file_id=%s", file_id)
        return None, message
    try:
        return sync_file_graph(file_id), None
    except Exception as exc:
        logger.exception("Neo4j synchronization failed")
        return None, str(exc)


def _public_sheets(sheets: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Exclude stored workbook rows from the HTTP response."""
    result = []
    for sheet in sheets:
        item = {
            "sheet_name": sheet["sheet_name"],
            "skip_reason": sheet.get("skip_reason"),
        }
        if sheet.get("header") is not None:
            item.update(
                {
                    "header": sheet["header"],
                    "columns": sheet.get("columns", []),
                    "data_preview": sheet.get("data_rows", [])[:3],
                }
            )
        result.append(item)
    return result


def finish_analysis(
    file_id: int,
    filename: str,
    sheets: List[Dict[str, Any]],
    progress_callback: Optional[ProgressCallback] = None,
) -> Dict[str, Any]:
    """Run post-storage analysis and build the shared upload response."""
    _emit_progress(
        progress_callback,
        status="running",
        phase="sheet_groups",
        percent=68,
        message="Классифицирую группы листов...",
        detail=file_id,
        file_id=file_id,
    )
    sheet_group_analysis = classify_file_sheet_groups(file_id, use_llm=True)

    _emit_progress(
        progress_callback,
        status="running",
        phase="s2t",
        percent=70,
        message="Собираю каталоги таблиц и S2T-трансформации...",
        detail=file_id,
        file_id=file_id,
    )
    count, extraction_error, extraction_report = try_extract_s2t_transformations(
        file_id,
        sheet_group_analysis=sheet_group_analysis,
    )

    _emit_progress(
        progress_callback,
        status="running",
        phase="summary",
        percent=84,
        message="Генерирую бизнес-саммари...",
        detail=file_id,
        file_id=file_id,
    )
    summary, summary_error = try_generate_summary(file_id)
    if summary is None:
        description, description_error = None, summary_error
    else:
        description, description_error = try_generate_description(file_id)
    response_sheets = _public_sheets(sheets)
    response = convert_to_serializable(
        {
            "filename": filename,
            "model_used": get_model_name(),
            "file_id": file_id,
            "summary": summary,
            "summary_error": summary_error,
            "description": description,
            "description_error": description_error,
            "s2t_transformations_count": count,
            "s2t_transformations_error": extraction_error,
            "s2t_extraction_report": extraction_report,
            "sheet_group_analysis": sheet_group_analysis,
            "sheets": response_sheets,
        }
    )
    graph_sync_report, graph_sync_error = try_sync_file_graph(file_id)
    response["graph_sync_report"] = graph_sync_report
    response["graph_sync_error"] = graph_sync_error
    _emit_progress(
        progress_callback,
        status="done",
        phase="done",
        percent=100,
        message="Анализ файла завершен",
        detail=f"Листов: {len(response_sheets)}, S2T transformations: {count}",
        file_id=file_id,
        filename=filename,
        s2t_transformations_count=count,
        s2t_transformations_error=extraction_error,
        s2t_extraction_report=extraction_report,
    )
    return response
