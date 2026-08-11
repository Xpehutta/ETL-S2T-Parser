"""Mechanical Excel parsing used by the HTTP upload layer."""

from __future__ import annotations

import datetime
import io
import logging
import zipfile
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

import numpy as np
import pandas as pd

from agents.agent import get_header_decision

logger = logging.getLogger(__name__)

ALLOWED_EXTENSIONS = {"xlsx", "xls", "xlsm"}
PREVIEW_ROWS = 10
MergedRange = Tuple[int, int, int, int]


def allowed_file(filename: str) -> bool:
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS


def convert_to_serializable(obj: Any) -> Any:
    if obj is None:
        return None
    if isinstance(obj, (datetime.datetime, datetime.date, pd.Timestamp)):
        return obj.isoformat()
    if isinstance(obj, np.datetime64):
        return None if pd.isna(obj) else str(obj)
    if isinstance(obj, dict):
        return {key: convert_to_serializable(value) for key, value in obj.items()}
    if isinstance(obj, (list, tuple, np.ndarray, pd.Series)):
        return [convert_to_serializable(value) for value in list(obj)]
    try:
        if pd.isna(obj):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(obj, np.integer):
        return int(obj)
    if isinstance(obj, np.floating):
        return float(obj)
    if isinstance(obj, np.bool_):
        return bool(obj)
    return obj


def _is_missing(value: Any) -> bool:
    if value is None:
        return True
    try:
        return bool(pd.isna(value))
    except (TypeError, ValueError):
        return False


def is_blank_header_value(value: Any) -> bool:
    if _is_missing(value):
        return True
    text = str(value).strip()
    lowered = text.lower()
    return (
        not text
        or lowered in {"none", "nan"}
        or lowered.startswith(("unnamed:", "untitled"))
    )


def _is_generated_on_value(value: Any) -> bool:
    return not _is_missing(value) and str(value).strip().lower().startswith("generated on")


def _generated_on_cutoff(headers: List[Any]) -> Optional[int]:
    for index, header in enumerate(headers):
        if not _is_generated_on_value(header):
            continue
        previous = index - 1
        while previous >= 0 and is_blank_header_value(headers[previous]):
            previous -= 1
        return previous + 1
    return None


def clean_header_values(headers: List[Any]) -> List[Any]:
    cleaned = list(headers)
    cutoff = _generated_on_cutoff(cleaned)
    cleaned = cleaned[:cutoff] if cutoff is not None else cleaned
    while cleaned and is_blank_header_value(cleaned[-1]):
        cleaned.pop()
    return cleaned


def _frame_rows(frame: pd.DataFrame) -> List[List[Any]]:
    return [
        [None if _is_missing(cell) else cell for cell in row]
        for row in frame.itertuples(index=False, name=None)
    ]


def _merged_ranges(excel_file: pd.ExcelFile, sheet_name: str) -> List[MergedRange]:
    """Return zero-based inclusive merged-cell coordinates for an openpyxl sheet."""
    workbook = getattr(excel_file, "book", None)
    if workbook is None:
        return []
    try:
        worksheet = workbook[sheet_name]
    except (KeyError, TypeError):
        return []

    merged_cells = getattr(worksheet, "merged_cells", None)
    ranges = getattr(merged_cells, "ranges", ()) if merged_cells is not None else ()
    return [
        (
            int(cell_range.min_row) - 1,
            int(cell_range.min_col) - 1,
            int(cell_range.max_row) - 1,
            int(cell_range.max_col) - 1,
        )
        for cell_range in ranges
    ]


def _hidden_rows(excel_file: pd.ExcelFile, sheet_name: str) -> Set[int]:
    """Return zero-based indexes of rows hidden in an openpyxl sheet."""
    workbook = getattr(excel_file, "book", None)
    if workbook is None:
        return set()
    try:
        worksheet = workbook[sheet_name]
    except (KeyError, TypeError):
        return set()

    row_dimensions = getattr(worksheet, "row_dimensions", None)
    if row_dimensions is None:
        return set()
    return {
        int(row_index) - 1
        for row_index, dimension in row_dimensions.items()
        if getattr(dimension, "hidden", False)
    }


def _expand_merged_values(
    frame: pd.DataFrame,
    merged_ranges: List[MergedRange],
    data_start: int,
) -> pd.DataFrame:
    """Copy each merged cell's top-left value into its covered data cells."""
    if frame.empty or not merged_ranges:
        return frame

    expanded = frame.copy()
    row_count, column_count = expanded.shape
    for first_row, first_column, last_row, last_column in merged_ranges:
        if (
            first_row >= row_count
            or first_column >= column_count
            or last_row < data_start
        ):
            continue
        value = expanded.iat[first_row, first_column]
        if _is_missing(value):
            continue
        for row_index in range(max(first_row, data_start), min(last_row + 1, row_count)):
            for column_index in range(first_column, min(last_column + 1, column_count)):
                expanded.iat[row_index, column_index] = value
    return expanded


def _header_rows_from_frame(header_data: pd.DataFrame, header_rows: int) -> List[List[Any]]:
    rows = [
        [None if is_blank_header_value(cell) else cell for cell in row]
        for row in _frame_rows(header_data.iloc[:header_rows])
    ]
    if not rows:
        return []

    width = max(map(len, rows))
    cutoffs = [cutoff for row in rows if (cutoff := _generated_on_cutoff(row)) is not None]
    if cutoffs:
        width = min(width, min(cutoffs))
    while width and all(is_blank_header_value(row[width - 1]) for row in rows):
        width -= 1
    return [row[:width] for row in rows]


def _fill_header_row(row: List[Any]) -> List[Any]:
    filled, previous = [], None
    for value in row:
        previous = previous if is_blank_header_value(value) else value
        filled.append(previous)
    return filled


def build_single_header_row(header_data: pd.DataFrame) -> List[Any]:
    rows = _header_rows_from_frame(header_data, 1)
    return _fill_header_row(clean_header_values(rows[0])) if rows else []


def build_nested_columns(header_data: pd.DataFrame, header_rows: int) -> List[List[Any]]:
    rows = [_fill_header_row(row) for row in _header_rows_from_frame(header_data, header_rows)]
    if not rows:
        return []
    return [
        [
            rows[row_index][column_index]
            for row_index in range(len(rows))
            if not is_blank_header_value(rows[row_index][column_index])
        ]
        for column_index in range(len(rows[0]))
    ]


def is_empty_or_irrelevant(preview_rows: List[List[Any]]) -> Tuple[bool, str]:
    if not preview_rows:
        return True, "Sheet is completely empty"
    if all(not row or all(cell is None for cell in row) for row in preview_rows):
        return True, "Sheet contains no data (all cells empty)"
    meaningful = sum(
        bool(cell.strip()) if isinstance(cell, str) else cell is not None
        for row in preview_rows[:5]
        for cell in row
    )
    return (False, "") if meaningful else (
        True,
        "Sheet contains only empty or whitespace cells",
    )


def _rows_empty(frame: pd.DataFrame, num_rows: int = 5) -> bool:
    sample = frame.iloc[:num_rows]
    return sample.empty or not bool(sample.notna().to_numpy().any())


def _resolve_header_decision(
    sheet_name: str,
    preview_rows: List[List[Any]],
) -> Tuple[int, int, bool]:
    decision = get_header_decision(sheet_name, preview_rows)
    logger.info(
        "Header decision for %s: start_row=%s, header_rows=%s",
        sheet_name,
        decision[0],
        decision[1],
    )
    return decision


def _emit_progress(
    callback: Optional[Callable[[Dict[str, Any]], None]],
    phase: str,
    percent: int,
    message: str,
    **details: Any,
) -> None:
    if callback:
        callback({"status": "running", "phase": phase, "percent": percent, "message": message, **details})


def _report_sheet_progress(
    callback: Optional[Callable[[Dict[str, Any]], None]],
    sheet_index: int,
    sheet_count: int,
    sheet_name: str,
    message: str,
    detail: str,
) -> None:
    _emit_progress(
        callback,
        "parse_sheet",
        10 + int((sheet_index / max(sheet_count, 1)) * 45),
        message,
        detail=detail,
        sheet_name=sheet_name,
        sheet_index=sheet_index,
        sheet_count=sheet_count,
    )


def _skip_sheet(
    sheets: List[Dict[str, Any]],
    callback: Optional[Callable[[Dict[str, Any]], None]],
    sheet_index: int,
    sheet_count: int,
    sheet_name: str,
    reason: str,
    detail: Optional[str] = None,
) -> None:
    sheets.append({"sheet_name": sheet_name, "skip_reason": reason})
    _report_sheet_progress(
        callback,
        sheet_index,
        sheet_count,
        sheet_name,
        "Лист пропущен...",
        detail or f"{sheet_name}: {reason}",
    )


def _columns_from_frame(
    frame: pd.DataFrame, start_row: int, header_rows: int
) -> Tuple[List[Any], Optional[str]]:
    if header_rows == 0:
        count = frame.shape[1] if not frame.iloc[start_row : start_row + 1].empty else 0
        columns = [f"Column_{index + 1}" for index in range(count)]
        return columns, None if columns else "No columns found"

    header_data = frame.iloc[start_row : start_row + header_rows]
    if header_data.empty or header_data.shape[1] == 0:
        return [], "No column headers found"
    columns = (
        build_single_header_row(header_data)
        if header_rows == 1
        else build_nested_columns(header_data, header_rows)
    )
    return columns, None


def _parse_loaded_sheet(
    frame: pd.DataFrame,
    sheet_name: str,
    merged_ranges: Optional[List[MergedRange]] = None,
    hidden_rows: Optional[Set[int]] = None,
    include_hidden_rows: bool = False,
) -> Dict[str, Any]:
    preview = _frame_rows(frame.iloc[:PREVIEW_ROWS])
    irrelevant, reason = is_empty_or_irrelevant(preview)
    if irrelevant:
        return {"skip_reason": reason}

    start, header_rows, nested = _resolve_header_decision(sheet_name, preview)
    data_start = start + header_rows

    columns, error = _columns_from_frame(frame, start, header_rows)
    if error:
        missing = "колонки" if header_rows == 0 else "заголовки"
        return {"skip_reason": error, "detail": f"{sheet_name}: не найдены {missing}"}

    data_frame = _expand_merged_values(frame, merged_ranges or [], data_start)
    data_positions = list(range(data_start, len(data_frame)))
    if not include_hidden_rows:
        hidden = hidden_rows or set()
        data_positions = [position for position in data_positions if position not in hidden]
    selected_data = data_frame.iloc[data_positions]
    if _rows_empty(selected_data, 5):
        return {
            "skip_reason": "No data rows after headers (first 5 rows empty)",
            "detail": f"{sheet_name}: нет строк данных после заголовков",
        }

    rows = _frame_rows(selected_data)
    return {
        "sheet": {
            "sheet_name": sheet_name,
            "skip_reason": None,
            "header": {
                "start_row": start,
                "row_count": header_rows,
                "nested": nested,
            },
            "columns": columns,
            "data_rows": rows,
            "data_row_numbers": [
                position - data_start for position in data_positions
            ],
        },
    }


def parse_excel_with_decisions(
    file_bytes: bytes,
    progress_callback: Optional[Callable[[Dict[str, Any]], None]] = None,
    include_hidden_rows: bool = False,
):
    sheets: List[Dict[str, Any]] = []

    excel_options = (
        {
            "engine": "openpyxl",
            "engine_kwargs": {"read_only": False},
        }
        if zipfile.is_zipfile(io.BytesIO(file_bytes))
        else {}
    )
    with pd.ExcelFile(io.BytesIO(file_bytes), **excel_options) as excel_file:
        sheet_names = list(excel_file.sheet_names)
        sheet_count = len(sheet_names)
        _emit_progress(
            progress_callback,
            "parse",
            10,
            "Читаю структуру Excel-книги...",
            detail=f"Найдено листов: {sheet_count}",
            sheet_count=sheet_count,
        )

        for sheet_index, sheet_name in enumerate(sheet_names, start=1):
            _emit_progress(
                progress_callback,
                "parse_sheet",
                10 + int(((sheet_index - 1) / max(sheet_count, 1)) * 45),
                "Анализирую лист...",
                detail=sheet_name,
                sheet_name=sheet_name,
                sheet_index=sheet_index,
                sheet_count=sheet_count,
            )
            progress_args = (
                progress_callback,
                sheet_index,
                sheet_count,
                sheet_name,
            )

            logger.info("Processing sheet: %s", sheet_name)
            frame = pd.read_excel(
                excel_file,
                sheet_name=sheet_name,
                header=None,
            )
            parsed = _parse_loaded_sheet(
                frame,
                sheet_name,
                merged_ranges=_merged_ranges(excel_file, sheet_name),
                hidden_rows=_hidden_rows(excel_file, sheet_name),
                include_hidden_rows=include_hidden_rows,
            )
            if parsed.get("skip_reason"):
                _skip_sheet(
                    sheets,
                    *progress_args,
                    parsed["skip_reason"],
                    parsed.get("detail"),
                )
                continue

            sheet = parsed["sheet"]
            sheets.append(sheet)
            data_rows = sheet["data_rows"]
            columns = parsed["sheet"]["columns"]
            _report_sheet_progress(
                *progress_args,
                "Лист разобран...",
                f"{sheet_name}: колонок {len(columns)}, строк {len(data_rows)}",
            )

    return sheets


__all__ = [
    "ALLOWED_EXTENSIONS",
    "allowed_file",
    "build_nested_columns",
    "build_single_header_row",
    "clean_header_values",
    "convert_to_serializable",
    "is_blank_header_value",
    "is_empty_or_irrelevant",
    "parse_excel_with_decisions",
]
