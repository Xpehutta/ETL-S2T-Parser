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


def _sheet_columns(headers_json: Optional[str]) -> List[Dict[str, Any]]:
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


def _load_rows(
    cursor: Any,
    file_id: int,
    sheet_name: str,
) -> List[Dict[int, Any]]:
    rows: Dict[int, Dict[int, Any]] = {}
    for item in cursor.execute(
        """
        SELECT row_num, column_id, value
        FROM data
        WHERE file_id = ? AND table_name = ? COLLATE NOCASE
        ORDER BY row_num, id
        """,
        (file_id, sheet_name),
    ).fetchall():
        rows.setdefault(int(item["row_num"]), {})[int(item["column_id"])] = item["value"]
    return list(rows.values())


def _pick_column(column_ids: Dict[str, int], candidates: Sequence[str]) -> Optional[int]:
    return next((column_ids[name] for name in candidates if name in column_ids), None)


def _row_value(row: Dict[int, Any], column_id: Optional[int]) -> Optional[str]:
    return _compact_description(row.get(column_id)) if column_id is not None else None


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


def _extract_sheet_semantics(
    columns: List[Dict[str, Any]],
    rows: List[Dict[int, Any]],
) -> Dict[str, List[Any]]:
    column_ids = _column_id_by_name(columns)
    area_id = _pick_column(column_ids, SUBJECT_AREA_COLUMNS)
    view_id = _pick_column(column_ids, VIEW_NAME_COLUMNS)
    view_description_id = _pick_column(column_ids, VIEW_DESCRIPTION_COLUMNS)
    table_id = _pick_column(column_ids, TARGET_TABLE_COLUMNS)
    table_description_id = _pick_column(column_ids, TARGET_TABLE_DESCRIPTION_COLUMNS)
    field_id = _pick_column(column_ids, FIELD_NAME_COLUMNS)
    field_description_id = _pick_column(column_ids, FIELD_DESCRIPTION_COLUMNS)
    entity_id = _pick_column(column_ids, ENTITY_COLUMNS)
    attribute_id = _pick_column(column_ids, ATTRIBUTE_NAME_COLUMNS)
    metric_id = _pick_column(column_ids, METRIC_CODE_COLUMNS)
    metric_description_id = _pick_column(column_ids, METRIC_DESCRIPTION_COLUMNS)
    extracted: Dict[str, List[Any]] = {
        "subject_areas": [],
        "views": [],
        "tables": [],
        "attributes": [],
        "fields": [],
        "metrics": [],
    }
    for row in rows:
        area = _row_value(row, area_id)
        if area:
            extracted["subject_areas"].append(area)

        view = _row_value(row, view_id)
        view_description = _row_value(row, view_description_id)
        if view and view_description and table_id is None:
            extracted["views"].append({"name": view, "description": view_description})

        table = _row_value(row, table_id)
        table_description = _row_value(row, table_description_id)
        if table and table_description:
            record = {"name": table, "description": table_description}
            if area:
                record["subject_area"] = area
            extracted["tables"].append(record)

        field = _row_value(row, field_id)
        field_description = _row_value(row, field_description_id)
        if table_description_id is not None and field and field_description:
            record = {"field": field, "description": field_description}
            if table:
                record["table"] = table
            extracted["fields"].append(record)

        entity = _row_value(row, entity_id)
        attribute = _row_value(row, attribute_id)
        if entity and attribute and field_description:
            extracted["attributes"].append(
                {
                    "entity": entity,
                    "attribute": attribute,
                    "description": field_description,
                }
            )

        metric = _row_value(row, metric_id)
        metric_description = _row_value(row, metric_description_id)
        if metric and metric_description:
            extracted["metrics"].append(
                {"code": metric, "description": metric_description}
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

        header_rows = cursor.execute(
            """
            SELECT sheet_name, headers_json
            FROM file_sheet_headers AS headers
            WHERE file_id = ?
              AND EXISTS (
                  SELECT 1
                  FROM data
                  WHERE data.file_id = headers.file_id
                    AND data.table_name = headers.sheet_name COLLATE NOCASE
              )
            ORDER BY sheet_name
            """,
            (file_id,),
        ).fetchall()

        for header_row in header_rows:
            sheet_name = str(header_row["sheet_name"])
            sheet_semantics = _extract_sheet_semantics(
                _sheet_columns(header_row["headers_json"]),
                _load_rows(cursor, file_id, sheet_name),
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
