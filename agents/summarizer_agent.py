import json
import logging
import re
from typing import Any, Dict, List, Optional, Sequence, Tuple

from langchain_core.messages import BaseMessage, HumanMessage, SystemMessage
from langchain_core.output_parsers import StrOutputParser
from langchain_core.runnables import RunnableLambda

from storage.database import (
    get_db_connection,
    update_file_description,
    update_file_summary,
)
from .agent import chat_model

try:
    from langfuse import observe
    from .observability import get_callback_handler

    LANGFUSE_AVAILABLE = True
except ImportError:
    LANGFUSE_AVAILABLE = False

    def observe(*args, **kwargs):
        def decorator(func):
            return func

        return decorator

    def get_callback_handler():
        return None


logger = logging.getLogger(__name__)

MAX_SUBJECT_AREAS = 12
MAX_TABLE_DESCRIPTIONS = 20
MAX_VIEW_DESCRIPTIONS = 20
MAX_ATTRIBUTE_DESCRIPTIONS = 25
MAX_FIELD_DESCRIPTIONS = 20
MAX_METRIC_DESCRIPTIONS = 10
SUMMARY_TEXT_CHAR_LIMIT = 300
SYSTEM_PROMPT = (
    "Сделай краткое бизнес-саммари на русском языке по каталогу описаний "
    "таблиц, представлений, атрибутов и полей."
)
SUMMARY_OUTPUT_REQUIREMENTS = """
Сформируй один цельный абзац из 3–5 предложений объёмом не более 1200 символов.
Опирайся только на переданные описания таблиц, представлений, атрибутов и полей.
Сформулируй, какие предметные области, сущности и бизнес-процессы покрывает
спецификация, используя формулировки из описаний и обобщая их.
Не перечисляй типы артефактов документа (S2T-строки, журнал изменений, SQL,
внешние ключи, представления как класс объектов, исключённые листы).
Не описывай структуру документа, процесс интеграции или трансформации в
абстрактных терминах без опоры на конкретные описания.
Не описывай содержимое как уже загруженные бизнес-данные в хранилище.
Не придумывай отсутствующие факты.
Сразу начни с предметной области или бизнес-назначения, без фраз
«документ описывает», «спецификация предназначена» и «данные содержат».
Не упоминай запрос пользователя, JSON, Excel, файл, документ, листы,
выборку строк или процесс анализа.
Не используй Markdown-заголовки, списки, вступления и заключения.
""".strip()
SUMMARY_RESPONSE_FORMAT = {
    "type": "json_schema",
    "json_schema": {
        "name": "business_summary",
        "strict": True,
        "schema": {
            "type": "object",
            "properties": {
                "summary": {
                    "type": "string",
                    "minLength": 1,
                    "maxLength": 1200,
                }
            },
            "required": ["summary"],
            "additionalProperties": False,
        },
    },
}
SUBJECT_AREA_COLUMNS = ("Предметная область",)
VIEW_NAME_COLUMNS = ("Таблица", "Представление")
VIEW_DESCRIPTION_COLUMNS = ("Описание таблицы",)
TARGET_TABLE_COLUMNS = ("Таблица-приемник",)
TARGET_TABLE_DESCRIPTION_COLUMNS = ("Описание целевой таблицы",)
FIELD_NAME_COLUMNS = ("Поле приемника", "Поле", "Атрибут")
FIELD_DESCRIPTION_COLUMNS = (
    "Описание поля приемника",
    "Описание поля источника",
    "Описание поля",
    "Описание атрибута",
)
ENTITY_COLUMNS = ("Сущность",)
ATTRIBUTE_NAME_COLUMNS = ("Атрибут",)
METRIC_CODE_COLUMNS = ("Код выборки данных",)
METRIC_DESCRIPTION_COLUMNS = ("Описание",)
_SQL_COLUMN_RE = re.compile(r"\bsql\b", re.IGNORECASE)


def _summarizer_messages(inp: Dict[str, str]) -> List[BaseMessage]:
    return [
        SystemMessage(content=f"{SYSTEM_PROMPT}\n\n{SUMMARY_OUTPUT_REQUIREMENTS}"),
        HumanMessage(
            content=(
                f"{SUMMARY_OUTPUT_REQUIREMENTS}\n\n"
                "Верни только итоговое саммари без пояснений.\n\n"
                f"Данные:\n{inp['user_content']}"
            )
        ),
    ]


_summarizer_llm_chain = (
    RunnableLambda(_summarizer_messages)
    | chat_model
    | StrOutputParser()
)


def call_gigachat(user_content: str) -> str:
    """Invoke the configured chat model for summary-related text."""
    if getattr(chat_model, "supports_json_schema", False) is True:
        reply = chat_model.invoke(
            _summarizer_messages({"user_content": user_content}),
            response_format=SUMMARY_RESPONSE_FORMAT,
        )
        raw = StrOutputParser().invoke(reply).strip()
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise ValueError("LLM returned invalid structured summary JSON") from exc
        summary = payload.get("summary") if isinstance(payload, dict) else None
        if not isinstance(summary, str) or not summary.strip():
            raise ValueError("LLM returned an empty structured summary")
        return summary.strip()
    return _summarizer_llm_chain.invoke({"user_content": user_content}).strip()


def _file_text_fields(file_id: int) -> Dict[str, Any]:
    conn = get_db_connection()
    try:
        row = conn.execute(
            "SELECT file_id, filename, summary, description FROM files WHERE file_id = ?",
            (file_id,),
        ).fetchone()
    finally:
        conn.close()
    if not row:
        raise ValueError(f"File {file_id} not found")
    return dict(row)


def _sheet_columns(sheet_id: int, headers_json: Optional[str]) -> List[Dict[str, Any]]:
    try:
        parsed_headers = json.loads(headers_json or "[]")
    except (TypeError, json.JSONDecodeError):
        parsed_headers = []

    columns: List[Dict[str, Any]] = []
    for position, item in enumerate(parsed_headers):
        if not isinstance(item, dict):
            continue
        flat_name = str(item.get("flat") or "").strip()
        if not flat_name:
            path = item.get("path")
            if isinstance(path, list):
                flat_name = " > ".join(str(part) for part in path if part)
        if not flat_name:
            continue
        try:
            column_index = int(item.get("index", position))
        except (TypeError, ValueError):
            column_index = position
        columns.append(
            {
                "index": column_index,
                "name": flat_name,
                "column_id": column_index + 1,
            }
        )
    return sorted(columns, key=lambda column: column["index"])


def _column_id_by_name(columns: List[Dict[str, Any]]) -> Dict[str, int]:
    return {column["name"]: column["column_id"] for column in columns}


def _pick_named_column(
    column_names: Sequence[str],
    candidates: Tuple[str, ...],
) -> Optional[str]:
    names = set(column_names)
    for candidate in candidates:
        if candidate in names:
            return candidate
    return None


def _clean_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).replace("\r\n", "\n").strip()
    return text or None


def _compact_description(value: Any) -> Optional[str]:
    text = _clean_text(value)
    if text is None:
        return None
    if len(text) <= SUMMARY_TEXT_CHAR_LIMIT:
        return text
    return f"{text[: SUMMARY_TEXT_CHAR_LIMIT - 1]}…"


def _evenly_spaced_items(items: List[Any], limit: int) -> List[Any]:
    if len(items) <= limit:
        return items
    if limit <= 1:
        return items[:1]
    last_index = len(items) - 1
    picked = {
        items[round(index * last_index / (limit - 1))]
        for index in range(limit)
    }
    return [item for item in items if item in picked]


def _dedupe_records(
    records: List[Dict[str, Any]],
    key_fields: Tuple[str, ...],
) -> List[Dict[str, Any]]:
    seen = set()
    unique: List[Dict[str, Any]] = []
    for record in records:
        key = tuple(_clean_text(record.get(field)) or "" for field in key_fields)
        if not any(key) or key in seen:
            continue
        seen.add(key)
        unique.append(record)
    return unique


def _distinct_values(
    cursor: Any,
    sheet_id: int,
    column_id: int,
    limit: int,
) -> List[str]:
    rows = cursor.execute(
        """
        SELECT value
        FROM data
        WHERE sheet_id = ? AND column_id = ?
          AND IFNULL(TRIM(value), '') != ''
        GROUP BY IFNULL(value, '')
        ORDER BY MIN(row_num)
        LIMIT ?
        """,
        (sheet_id, column_id, limit),
    ).fetchall()
    return [
        text
        for text in (_clean_text(row["value"]) for row in rows)
        if text is not None
    ]


def _distinct_name_description(
    cursor: Any,
    sheet_id: int,
    name_column_id: int,
    description_column_id: int,
    limit: int,
) -> List[Dict[str, str]]:
    rows = cursor.execute(
        """
        SELECT
            d_name.value AS name,
            d_desc.value AS description
        FROM data AS d_name
        JOIN data AS d_desc
          ON d_name.sheet_id = d_desc.sheet_id
         AND d_name.row_num = d_desc.row_num
        WHERE d_name.sheet_id = ?
          AND d_name.column_id = ?
          AND d_desc.column_id = ?
          AND IFNULL(TRIM(d_name.value), '') != ''
          AND IFNULL(TRIM(d_desc.value), '') != ''
        GROUP BY IFNULL(d_name.value, ''), IFNULL(d_desc.value, '')
        ORDER BY MIN(d_name.row_num)
        LIMIT ?
        """,
        (sheet_id, name_column_id, description_column_id, limit),
    ).fetchall()
    return _dedupe_records(
        [
            {
                "name": _compact_description(row["name"]) or "",
                "description": _compact_description(row["description"]) or "",
            }
            for row in rows
        ],
        ("name", "description"),
    )


def _distinct_target_tables(
    cursor: Any,
    sheet_id: int,
    column_names: Sequence[str],
    column_ids: Dict[str, int],
    limit: int,
) -> List[Dict[str, str]]:
    table_column = _pick_named_column(column_names, TARGET_TABLE_COLUMNS)
    description_column = _pick_named_column(column_names, TARGET_TABLE_DESCRIPTION_COLUMNS)
    if not table_column or not description_column:
        return []

    area_column = _pick_named_column(column_names, SUBJECT_AREA_COLUMNS)
    if area_column:
        rows = cursor.execute(
            f"""
            SELECT
                d_area.value AS subject_area,
                d_table.value AS name,
                d_desc.value AS description
            FROM data AS d_table
            JOIN data AS d_desc
              ON d_table.sheet_id = d_desc.sheet_id
             AND d_table.row_num = d_desc.row_num
            LEFT JOIN data AS d_area
              ON d_table.sheet_id = d_area.sheet_id
             AND d_table.row_num = d_area.row_num
             AND d_area.column_id = ?
            WHERE d_table.sheet_id = ?
              AND d_table.column_id = ?
              AND d_desc.column_id = ?
              AND IFNULL(TRIM(d_table.value), '') != ''
              AND IFNULL(TRIM(d_desc.value), '') != ''
            GROUP BY
                IFNULL(d_area.value, ''),
                IFNULL(d_table.value, ''),
                IFNULL(d_desc.value, '')
            ORDER BY MIN(d_table.row_num)
            LIMIT ?
            """,
            (
                column_ids[area_column],
                sheet_id,
                column_ids[table_column],
                column_ids[description_column],
                limit,
            ),
        ).fetchall()
        return _dedupe_records(
            [
                {
                    "subject_area": _compact_description(row["subject_area"]),
                    "name": _compact_description(row["name"]) or "",
                    "description": _compact_description(row["description"]) or "",
                }
                for row in rows
            ],
            ("name", "description"),
        )

    return _distinct_name_description(
        cursor,
        sheet_id,
        column_ids[table_column],
        column_ids[description_column],
        limit,
    )


def _distinct_field_descriptions(
    cursor: Any,
    sheet_id: int,
    column_names: Sequence[str],
    column_ids: Dict[str, int],
    limit: int,
) -> List[Dict[str, str]]:
    table_column = _pick_named_column(column_names, TARGET_TABLE_COLUMNS)
    field_column = _pick_named_column(column_names, FIELD_NAME_COLUMNS)
    description_column = _pick_named_column(column_names, FIELD_DESCRIPTION_COLUMNS)
    if not field_column or not description_column:
        return []

    if table_column:
        rows = cursor.execute(
            f"""
            SELECT
                d_table.value AS table_name,
                d_field.value AS field_name,
                d_desc.value AS description
            FROM data AS d_field
            JOIN data AS d_desc
              ON d_field.sheet_id = d_desc.sheet_id
             AND d_field.row_num = d_desc.row_num
            LEFT JOIN data AS d_table
              ON d_field.sheet_id = d_table.sheet_id
             AND d_field.row_num = d_table.row_num
             AND d_table.column_id = ?
            WHERE d_field.sheet_id = ?
              AND d_field.column_id = ?
              AND d_desc.column_id = ?
              AND IFNULL(TRIM(d_field.value), '') != ''
              AND IFNULL(TRIM(d_desc.value), '') != ''
            GROUP BY
                IFNULL(d_table.value, ''),
                IFNULL(d_field.value, ''),
                IFNULL(d_desc.value, '')
            ORDER BY MIN(d_field.row_num)
            LIMIT ?
            """,
            (
                column_ids[table_column],
                sheet_id,
                column_ids[field_column],
                column_ids[description_column],
                limit,
            ),
        ).fetchall()
        return _dedupe_records(
            [
                {
                    "table": _compact_description(row["table_name"]),
                    "field": _compact_description(row["field_name"]) or "",
                    "description": _compact_description(row["description"]) or "",
                }
                for row in rows
            ],
            ("table", "field", "description"),
        )

    rows = cursor.execute(
        f"""
        SELECT
            d_field.value AS field_name,
            d_desc.value AS description
        FROM data AS d_field
        JOIN data AS d_desc
          ON d_field.sheet_id = d_desc.sheet_id
         AND d_field.row_num = d_desc.row_num
        WHERE d_field.sheet_id = ?
          AND d_field.column_id = ?
          AND d_desc.column_id = ?
          AND IFNULL(TRIM(d_field.value), '') != ''
          AND IFNULL(TRIM(d_desc.value), '') != ''
        GROUP BY IFNULL(d_field.value, ''), IFNULL(d_desc.value, '')
        ORDER BY MIN(d_field.row_num)
        LIMIT ?
        """,
        (
            sheet_id,
            column_ids[field_column],
            column_ids[description_column],
            limit,
        ),
    ).fetchall()
    return _dedupe_records(
        [
            {
                "field": _compact_description(row["field_name"]) or "",
                "description": _compact_description(row["description"]) or "",
            }
            for row in rows
        ],
        ("field", "description"),
    )


def _distinct_attributes(
    cursor: Any,
    sheet_id: int,
    column_names: Sequence[str],
    column_ids: Dict[str, int],
    limit: int,
) -> List[Dict[str, str]]:
    entity_column = _pick_named_column(column_names, ENTITY_COLUMNS)
    attribute_column = _pick_named_column(column_names, ATTRIBUTE_NAME_COLUMNS)
    description_column = _pick_named_column(column_names, FIELD_DESCRIPTION_COLUMNS)
    if not entity_column or not attribute_column or not description_column:
        return []

    rows = cursor.execute(
        f"""
        SELECT
            d_entity.value AS entity,
            d_attr.value AS attribute,
            d_desc.value AS description
        FROM data AS d_attr
        JOIN data AS d_desc
          ON d_attr.sheet_id = d_desc.sheet_id
         AND d_attr.row_num = d_desc.row_num
        JOIN data AS d_entity
          ON d_attr.sheet_id = d_entity.sheet_id
         AND d_attr.row_num = d_entity.row_num
        WHERE d_attr.sheet_id = ?
          AND d_entity.column_id = ?
          AND d_attr.column_id = ?
          AND d_desc.column_id = ?
          AND IFNULL(TRIM(d_entity.value), '') != ''
          AND IFNULL(TRIM(d_attr.value), '') != ''
          AND IFNULL(TRIM(d_desc.value), '') != ''
        GROUP BY
            IFNULL(d_entity.value, ''),
            IFNULL(d_attr.value, ''),
            IFNULL(d_desc.value, '')
        ORDER BY MIN(d_attr.row_num)
        LIMIT ?
        """,
        (
            sheet_id,
            column_ids[entity_column],
            column_ids[attribute_column],
            column_ids[description_column],
            limit,
        ),
    ).fetchall()
    return _dedupe_records(
        [
            {
                "entity": _compact_description(row["entity"]) or "",
                "attribute": _compact_description(row["attribute"]) or "",
                "description": _compact_description(row["description"]) or "",
            }
            for row in rows
        ],
        ("entity", "attribute", "description"),
    )


def _distinct_metrics(
    cursor: Any,
    sheet_id: int,
    column_names: Sequence[str],
    column_ids: Dict[str, int],
    limit: int,
) -> List[Dict[str, str]]:
    code_column = _pick_named_column(column_names, METRIC_CODE_COLUMNS)
    description_column = _pick_named_column(column_names, METRIC_DESCRIPTION_COLUMNS)
    if not code_column or not description_column:
        return []

    rows = cursor.execute(
        f"""
        SELECT
            d_code.value AS code,
            d_desc.value AS description
        FROM data AS d_code
        JOIN data AS d_desc
          ON d_code.sheet_id = d_desc.sheet_id
         AND d_code.row_num = d_desc.row_num
        WHERE d_code.sheet_id = ?
          AND d_code.column_id = ?
          AND d_desc.column_id = ?
          AND IFNULL(TRIM(d_code.value), '') != ''
          AND IFNULL(TRIM(d_desc.value), '') != ''
        GROUP BY IFNULL(d_code.value, ''), IFNULL(d_desc.value, '')
        ORDER BY MIN(d_code.row_num)
        LIMIT ?
        """,
        (
            sheet_id,
            column_ids[code_column],
            column_ids[description_column],
            limit,
        ),
    ).fetchall()
    return _dedupe_records(
        [
            {
                "code": _compact_description(row["code"]) or "",
                "description": _compact_description(row["description"]) or "",
            }
            for row in rows
        ],
        ("code", "description"),
    )


def _extract_sheet_semantics(
    cursor: Any,
    sheet_id: int,
    columns: List[Dict[str, Any]],
) -> Dict[str, List[Dict[str, str]]]:
    column_names = [column["name"] for column in columns]
    column_ids = _column_id_by_name(columns)
    extracted = {
        "subject_areas": [],
        "views": [],
        "tables": [],
        "attributes": [],
        "fields": [],
        "metrics": [],
    }

    subject_area_column = _pick_named_column(column_names, SUBJECT_AREA_COLUMNS)
    if subject_area_column:
        extracted["subject_areas"] = _distinct_values(
            cursor,
            sheet_id,
            column_ids[subject_area_column],
            MAX_SUBJECT_AREAS,
        )

    view_name_column = _pick_named_column(column_names, VIEW_NAME_COLUMNS)
    view_description_column = _pick_named_column(column_names, VIEW_DESCRIPTION_COLUMNS)
    if (
        view_name_column
        and view_description_column
        and not _pick_named_column(column_names, TARGET_TABLE_COLUMNS)
    ):
        extracted["views"] = _distinct_name_description(
            cursor,
            sheet_id,
            column_ids[view_name_column],
            column_ids[view_description_column],
            MAX_VIEW_DESCRIPTIONS,
        )

    if _pick_named_column(column_names, TARGET_TABLE_DESCRIPTION_COLUMNS):
        extracted["tables"] = _distinct_target_tables(
            cursor,
            sheet_id,
            column_names,
            column_ids,
            MAX_TABLE_DESCRIPTIONS,
        )
        extracted["fields"] = _distinct_field_descriptions(
            cursor,
            sheet_id,
            column_names,
            column_ids,
            MAX_FIELD_DESCRIPTIONS,
        )

    extracted["attributes"] = _distinct_attributes(
        cursor,
        sheet_id,
        column_names,
        column_ids,
        MAX_ATTRIBUTE_DESCRIPTIONS,
    )

    if _pick_named_column(column_names, METRIC_CODE_COLUMNS):
        extracted["metrics"] = _distinct_metrics(
            cursor,
            sheet_id,
            column_names,
            column_ids,
            MAX_METRIC_DESCRIPTIONS,
        )

    return extracted


def _load_persisted_table_descriptions(file_id: int) -> List[Dict[str, str]]:
    conn = get_db_connection()
    try:
        rows = []
        for table_name in ("target_tables", "source_tables"):
            table_rows = conn.execute(
                f"""
                SELECT table_name, description
                FROM {table_name}
                WHERE file_id = ?
                  AND IFNULL(TRIM(table_name), '') != ''
                  AND IFNULL(TRIM(description), '') != ''
                ORDER BY row_num
                """,
                (file_id,),
            ).fetchall()
            rows.extend(
                {
                    "name": _compact_description(row["table_name"]) or "",
                    "description": _compact_description(row["description"]) or "",
                    "catalog": table_name,
                }
                for row in table_rows
            )
    finally:
        conn.close()
    return _dedupe_records(rows, ("name", "description"))


def _merge_semantic_catalog(
    catalog: Dict[str, List[Any]],
    sheet_semantics: Dict[str, List[Any]],
) -> None:
    for key in ("subject_areas", "views", "tables", "attributes", "fields", "metrics"):
        catalog[key].extend(sheet_semantics.get(key) or [])

    catalog["subject_areas"] = list(
        dict.fromkeys(
            area
            for area in catalog["subject_areas"]
            if _clean_text(area) is not None
        )
    )[:MAX_SUBJECT_AREAS]
    catalog["views"] = _dedupe_records(catalog["views"], ("name", "description"))[
        :MAX_VIEW_DESCRIPTIONS
    ]
    catalog["tables"] = _evenly_spaced_items(
        _dedupe_records(catalog["tables"], ("name", "description")),
        MAX_TABLE_DESCRIPTIONS,
    )
    catalog["attributes"] = _evenly_spaced_items(
        _dedupe_records(catalog["attributes"], ("entity", "attribute", "description")),
        MAX_ATTRIBUTE_DESCRIPTIONS,
    )
    field_key = (
        ("table", "field", "description")
        if any(_clean_text(item.get("table")) for item in catalog["fields"])
        else ("field", "description")
    )
    catalog["fields"] = _evenly_spaced_items(
        _dedupe_records(catalog["fields"], field_key),
        MAX_FIELD_DESCRIPTIONS,
    )
    catalog["metrics"] = _dedupe_records(catalog["metrics"], ("code", "description"))[
        :MAX_METRIC_DESCRIPTIONS
    ]


def fetch_file_data(file_id: int) -> Dict[str, Any]:
    """Return a semantic catalog of table and attribute descriptions."""
    conn = get_db_connection()
    cursor = conn.cursor()
    catalog: Dict[str, List[Any]] = {
        "subject_areas": [],
        "views": [],
        "tables": [],
        "attributes": [],
        "fields": [],
        "metrics": [],
        "catalog_tables": [],
    }
    try:
        file_row = cursor.execute(
            "SELECT filename FROM files WHERE file_id = ?",
            (file_id,),
        ).fetchone()
        if not file_row:
            raise ValueError(f"File {file_id} not found")

        cursor.execute(
            """
            SELECT sheet_id, sheet_name, headers_json
            FROM file_sheet_headers
            WHERE file_id = ?
              AND EXISTS (
                  SELECT 1
                  FROM data
                  WHERE data.sheet_id = file_sheet_headers.sheet_id
              )
            ORDER BY sheet_name
            """,
            (file_id,),
        )
        header_rows = cursor.fetchall()

        for header_row in header_rows:
            columns = _sheet_columns(header_row["sheet_id"], header_row["headers_json"])
            sheet_semantics = _extract_sheet_semantics(
                cursor,
                header_row["sheet_id"],
                columns,
            )
            _merge_semantic_catalog(catalog, sheet_semantics)
    finally:
        conn.close()

    catalog["catalog_tables"] = _load_persisted_table_descriptions(file_id)

    return {
        "filename": file_row["filename"],
        "semantic_catalog": catalog,
        "final_summary": "",
    }


def build_summary_payload(snapshot: Dict[str, Any]) -> Dict[str, Any]:
    """Build the compact JSON payload sent to the summarizer LLM."""
    catalog = snapshot.get("semantic_catalog") or {}
    return {
        "focus": "table_and_attribute_descriptions",
        "filename": snapshot.get("filename"),
        "subject_areas": catalog.get("subject_areas") or [],
        "views": catalog.get("views") or [],
        "tables": catalog.get("tables") or [],
        "attributes": catalog.get("attributes") or [],
        "fields": catalog.get("fields") or [],
        "metrics": catalog.get("metrics") or [],
        "catalog_tables": catalog.get("catalog_tables") or [],
    }


@observe()
def summarize_snapshot(state: Dict[str, Any]) -> Dict[str, Any]:
    payload = json.dumps(
        build_summary_payload(state),
        ensure_ascii=False,
        default=str,
    )
    state["final_summary"] = call_gigachat(payload)
    return state


summarizer_chain = (
    RunnableLambda(fetch_file_data)
    | RunnableLambda(summarize_snapshot)
)


@observe()
def generate_summary(file_id: int) -> str:
    handler = get_callback_handler()
    if handler:
        chain_with_config = summarizer_chain.with_config(
            {
                "callbacks": [handler],
                "run_name": f"summarize_{file_id}",
            }
        )
        result = chain_with_config.invoke(file_id)
    else:
        result = summarizer_chain.invoke(file_id)
    return result["final_summary"]


def summarize_file(file_id: int, save: bool = True) -> str:
    summary = generate_summary(file_id)
    if save:
        update_file_summary(file_id, summary)
    return summary


def generate_description_from_summary(summary: str) -> str:
    prompt = f"""
Сформируй краткое описание на русском языке по готовому бизнес-саммари ниже.

Требования:
- 2–3 предложения, один короткий абзац;
- пиши о предметных областях, сущностях и бизнес-процессах, которые следуют
  из описаний таблиц и атрибутов;
- не описывай структуру документа, S2T-артефакты, SQL, внешние ключи
  или исключённые листы;
- не описывай это как уже загруженные бизнес-данные в хранилище;
- не упоминай Excel, файл, листы, загрузку, документ или рабочую книгу;
- не добавляй вводных фраз вида «данный файл содержит».

Готовое бизнес-саммари:
{summary}
"""
    return call_gigachat(prompt).strip()


def ensure_file_description(
    file_id: int,
    refresh: bool = False,
    save: bool = True,
    summary_override: Optional[str] = None,
) -> str:
    fields = _file_text_fields(file_id)
    cached_description = str(fields.get("description") or "").strip()
    if cached_description and not refresh:
        return cached_description

    summary = str(summary_override or "").strip() or str(fields.get("summary") or "").strip()
    if not summary:
        summary = summarize_file(file_id, save=save)

    description = generate_description_from_summary(summary)
    if save:
        update_file_description(file_id, description)
    return description


def generate_description_update_from_user_query(
    current_description: str,
    summary: str,
    user_query: str,
) -> str:
    prompt = f"""
Обнови краткое описание по уточнению пользователя.

Текущее краткое описание:
{current_description}

Текущее бизнес-саммари:
{summary}

Запрос пользователя:
{user_query}

Требования:
- верни только обновлённое краткое описание на русском языке;
- 2–4 предложения, один короткий абзац;
- опирайся на сохранённое описание, бизнес-саммари и факты из запроса пользователя;
- если пользователь уточняет или исправляет акцент описания, учти это;
- не описывай структуру документа, S2T-артефакты, SQL или исключённые листы;
- не описывай содержимое как уже загруженные бизнес-данные в хранилище;
- не упоминай Excel, файл, листы, загрузку, документ или рабочую книгу;
- не придумывай факты, которых нет в саммари или запросе пользователя.
"""
    return call_gigachat(prompt).strip()


def update_file_description_from_user_query(
    file_id: int,
    user_query: str,
    save: bool = True,
) -> str:
    request_text = str(user_query or "").strip()
    if not request_text:
        raise ValueError("user_query must be non-empty")

    base_description = ensure_file_description(file_id, refresh=False, save=save)
    fields = _file_text_fields(file_id)
    summary = str(fields.get("summary") or "").strip()
    updated_description = generate_description_update_from_user_query(
        current_description=base_description,
        summary=summary,
        user_query=request_text,
    )
    if save:
        update_file_description(file_id, updated_description)
    return updated_description
