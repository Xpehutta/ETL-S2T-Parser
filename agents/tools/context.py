"""Runtime skills and lazily selected data schemas for the chat agent."""

import json
import os
from typing import Dict, Iterable, List, Literal, Optional, Tuple

from ..contracts import SqlRiskAspect
from ..experiment_flags import (
    OPERATION_SQL_RISK_ASPECTS_EXPERIMENT_ENV,
    experiment_flag_enabled,
)
from ..operation_protocols import (
    OPERATION_SQL_RISK_PROTOCOL_EXPERIMENT_ENV,
    configured_sql_risk_protocol,
    render_sql_risk_protocol,
)
from .common import PROJECT_ROOT

PROMPTS_DIR = PROJECT_ROOT / "agents" / "prompts"
CONFIG_DIR = PROJECT_ROOT / "config"
SchemaName = Literal[
    "SQLite ETL",
    "S2T-маппинг",
    "Excel-маппинги",
    "Neo4j lineage",
]

SCHEMA_CATALOG: Dict[str, str] = {
    "SQLite ETL": (
        "Реальные колонки публичных таблиц ETL-хранилища; нужна для составления "
        "произвольного run_sql (только read-only) и проверки физической "
        "структуры хранения."
    ),
    "S2T-маппинг": (
        "Поля S2T-кортежа, source/target-роли, алиасы заголовков S2T-листа "
        "и правила ETL-слоёв. Нужна для работы с сырой схемой/конфигурацией; "
        "готовые S2T-tools имеют собственные контракты."
    ),
    "Excel-маппинги": (
        "Группы и алиасы Excel-листов, роли их физических заголовков и "
        "настроенные цели извлечения."
    ),
    "Neo4j lineage": (
        "Labels, свойства и направления связей ETLTable/ETLColumn в Neo4j; "
        "нужна для произвольного run_cypher или ручного анализа графа, но не "
        "для готовых trace-tools."
    ),
}


OPERATION_SKILL_CATALOG: Dict[str, str] = {
    "Совместимость колонок": (
        "Явное сравнение типов, nullable или ключевых признаков точных "
        "source/target-колонок."
    ),
    "Анализ SQL-рисков": (
        "Явная оценка одного или нескольких SQL-аспектов: фильтрация "
        "строк, кардинальность, отклонение constraints, изменение "
        "значений или write semantics по сохранённому SQL/S2T-правилу."
    ),
    "Покрытие маппинга": (
        "Явный поиск разности между ограниченным каталогом колонок и "
        "S2T-полями той же роли."
    ),
    "Проектирование проверки": (
        "Составление методики или тест-протокола будущих проверок по "
        "подтверждённым правилам и метаданным."
    ),
}


_SQL_RISK_ASPECT_RULES: Dict[
    SqlRiskAspect,
    Dict[str, str],
] = {
    "row_filtering": {
        "plan": (
            "Запроси полный точный directed S2T mapping с фактическим SQL; "
            "не добавляй catalog metadata только ради оценки predicates."
        ),
        "planner": (
            "Прочитай mapping точной source→target пары без field/ID/file "
            "narrowing и сохрани полный SQL."
        ),
        "observer": (
            "Принимай только полный untruncated mapping обеих точных таблиц "
            "с фактическим правилом; безопасность не оценивай."
        ),
        "upstream_decision": (
            "Полного exact mapping достаточно для фактического или условного "
            "вывода об отсечении строк; не требуй metadata ради уверенности."
        ),
        "upstream": (
            "Оцени WHERE/HAVING/QUALIFY, JOIN ON и set/limit predicates; "
            "FALSE/UNKNOWN удаляет строку или группу, WHERE 1=1 не фильтрует."
        ),
    },
    "cardinality": {
        "plan": (
            "Запроси полный точный directed mapping; metadata читай только "
            "если пользователь явно запросил вывод по ключам."
        ),
        "planner": (
            "Прочитай mapping точной source→target пары без сужения по полю "
            "или предполагаемому механизму риска."
        ),
        "observer": (
            "Принимай только полный untruncated mapping с обеими таблицами и "
            "полным правилом; не назначай уровень риска."
        ),
        "upstream_decision": (
            "Exact mapping достаточен для структурного либо условного вывода "
            "о размножении/схлопывании; неизвестную уникальность назови границей."
        ),
        "upstream": (
            "Оцени JOIN multiplicity, DISTINCT, GROUP BY и дедупликацию. "
            "Без уникальности полных join keys размножение условно; прямой "
            "field mapping не доказывает 1:1. Не назначай качественный "
            "уровень риска и не утверждай фактические дубликаты без evidence "
            "о данных и уникальности. Не переноси сюда write semantics, PK "
            "или поведение повторного запуска: если они не запрошены и не "
            "прочитаны, итог ограничен условным механизмом кардинальности. "
            "Для LEFT JOIN ноль или одно совпадение справа дают одну строку; "
            "fan-out возникает только при нескольких совпадениях. Если в "
            "ответе называешь join-поля из mapping, выбери это mapping также "
            "для display. "
            "Конъюнкт `TRUE AND predicate` эквивалентен `predicate`: сам "
            "литерал TRUE не ослабляет условие JOIN."
        ),
    },
    "constraint_rejection": {
        "plan": (
            "После разрешения файла запроси одной самодостаточной worker task "
            "и полный exact directed S2T mapping, и column metadata обеих "
            "точных source.field→target.field endpoint-колонок в заданном "
            "file scope; не дели evidence по отдельным tasks."
        ),
        "planner": (
            "Для точной source_table.source_field→target_table.target_field "
            "пары сначала прочитай полный directed S2T mapping, затем обе "
            "ролевые column-записи одним exact-pair чтением без фильтра по "
            "ожидаемым type/PK/not_null. Table-only batch получает только "
            "имена таблиц без суффикса .field."
        ),
        "observer": (
            "Завершай только при полном untruncated exact directed S2T mapping "
            "и metadata ровно обеих endpoint-колонок с сохранёнными ролями и "
            "file scope; пустой каталог не доказывает отсутствие constraints."
        ),
        "upstream_decision": (
            "Для подтверждённого rejection нужны target constraint и точная "
            "source metadata либо выражение; target-only не доказывает безопасность."
        ),
        "upstream": (
            "Сопоставь точную field-пару: nullable→NOT NULL и несовпадение "
            "типов дают условный rejection/conversion до проверки значений."
        ),
    },
    "value_changes": {
        "plan": (
            "Запроси полный точный directed mapping с выражениями значений; "
            "сохрани названную source.field→target.field пару как scope анализа."
        ),
        "planner": (
            "Сохрани полный SQL/правило точной пары без сужения по одному полю "
            "или предполагаемому выражению."
        ),
        "observer": (
            "Принимай полный untruncated mapping с фактическими expressions; "
            "не подменяй отсутствующее выражение предположением."
        ),
        "upstream_decision": (
            "Exact mapping достаточен для вывода об изменениях, видимых в "
            "проекции точного target field; соседние проекции не создают gap."
        ),
        "upstream": (
            "Для source.field→target.field оцени только exact S2T-строку и "
            "внешнюю SQL-проекцию этого target field. CASE/COALESCE/CAST/"
            "арифметика в другом output alias не доказывают его изменение; "
            "прямая проекция означает, что механизм не обнаружен."
        ),
    },
    "write_semantics": {
        "plan": (
            "Запроси один полный exact directed mapping заданной пары; "
            "не планируй metadata или поиск в других таблицах."
        ),
        "planner": (
            "Прочитай один полный mapping точной source→target пары "
            "без field/ID/file narrowing; не заменяй write strategy ключом."
        ),
        "observer": (
            "Принимай полный untruncated exact mapping и завершай task даже "
            "без write statement: это terminal negative evidence, а не gap."
        ),
        "upstream_decision": (
            "Полный untruncated exact mapping достаточен для pass: без "
            "явного statement верни «не оценено», не reroute."
        ),
        "upstream": (
            "Различай append, overwrite, merge/upsert и conflict handling; "
            "без statement ответь «не оценено: в mapping write statement не "
            "сохранён»; PK/UNIQUE не доказывает режим."
        ),
    },
}

_SQL_RISK_REROUTE_PLAN_RULE = (
    "После reroute прошлое evidence удалено: новый план снова включает "
    "самодостаточную task чтения точного directed S2T mapping, даже если "
    "problem просит только дополняющие metadata; план только из дельты "
    "недостающих данных неполон."
)
_MAPPING_DEPENDENT_SQL_RISK_ASPECTS = {
    "row_filtering",
    "cardinality",
    "constraint_rejection",
    "value_changes",
    "write_semantics",
}


def _sql_risk_aspects_enabled() -> bool:
    return experiment_flag_enabled(
        OPERATION_SQL_RISK_ASPECTS_EXPERIMENT_ENV,
    )


def _sql_risk_aspect_context(
    aspects: Iterable[SqlRiskAspect],
    *,
    stage: str,
) -> str:
    protocol_candidate = configured_sql_risk_protocol(
        os.getenv(OPERATION_SQL_RISK_PROTOCOL_EXPERIMENT_ENV)
    )
    if protocol_candidate is not None:
        return render_sql_risk_protocol(
            protocol_candidate,
            aspects,
            stage=stage,
        )

    selected = [
        aspect
        for aspect in dict.fromkeys(aspects)
        if aspect in _SQL_RISK_ASPECT_RULES
    ]
    if not selected or not _sql_risk_aspects_enabled():
        return ""
    rules = [
        f"- `{aspect}`: {_SQL_RISK_ASPECT_RULES[aspect][stage]}"
        for aspect in selected
    ]
    if stage == "plan" and any(
        aspect in _MAPPING_DEPENDENT_SQL_RISK_ASPECTS
        for aspect in selected
    ):
        rules.insert(0, "- " + _SQL_RISK_REROUTE_PLAN_RULE)
    return (
        "## Анализ SQL-рисков\n"
        "Выбранные аспекты (не анализируй остальные): "
        + ", ".join(f"`{aspect}`" for aspect in selected)
        + "\n"
        + "\n".join(rules)
    )


def get_downstream_capability_context() -> str:
    """Return compact planning capabilities without concrete tool names."""
    return "\n".join(
        [
            "Доступные возможности чтения (описывай нужные данные, не инструмент):",
            "- точное разрешение имени файла, фильтры и списки файлов, листов, таблиц и колонок;",
            "- буквальный поиск таблиц и колонок по явно данному фрагменту;",
            "- смысловой поиск по описаниям, когда точное имя неизвестно;",
            "- точные и частичные S2T-строки, пары таблиц, правила и агрегации;",
            "- Additional objects по атрибутам или фрагменту, включая полный SQL;",
            "- lineage колонок и таблиц, пути, влияние и разбор явно данного SQL;",
            "- сырые значения Excel и read-only срезы хранилища;",
            "- полные сохранённые результаты прошлых workers и SQL-анализ их строк.",
        ]
    )


def get_downstream_table_context(*, read_native_comments: bool = True) -> str:
    """Return exact storage table names with compact planning descriptions."""
    from storage.database import (
        TABLE_COMMENTS,
        USER_FACING_TABLES,
        is_postgres_backend,
    )

    table_comments = TABLE_COMMENTS
    if read_native_comments and is_postgres_backend():
        from storage.postgres_schema import read_postgres_schema_metadata

        metadata = read_postgres_schema_metadata()
        table_comments = {
            table_name: str(table["comment"])
            for table_name, table in metadata.items()
        }

    return "\n".join(
        [
            "Реальные таблицы хранилища (справка, не список шагов; "
            "наличие таблицы не требует её чтения):"
        ]
        + [
            f"- `{name}` — {table_comments[name]}."
            for name in USER_FACING_TABLES
        ]
    )


def _prompt_text(filename: str) -> str:
    try:
        return (PROMPTS_DIR / filename).read_text(encoding="utf-8")
    except FileNotFoundError:
        return ""


def _format_backtick_list(names: Tuple[str, ...]) -> str:
    quoted = [f"`{name}`" for name in names]
    if len(quoted) <= 1:
        return "".join(quoted)
    return ", ".join(quoted[:-1]) + f" и {quoted[-1]}"


def get_sqlite_schema_cheatsheet() -> str:
    """Собрать блок активной SQL-схемы для prompt-ов агентов."""
    from storage.database import (
        INTERNAL_TABLES,
        STORAGE_SCHEMA_COLUMNS,
        STORAGE_SCHEMA_TABLE_ORDER,
        S2T_RECORD_FIELDS,
        TABLE_COMMENTS,
        USER_FACING_TABLES,
        is_postgres_backend,
    )

    postgres = is_postgres_backend()
    table_comments = TABLE_COMMENTS
    column_comments: Dict[str, Dict[str, str]] = {}
    if postgres:
        from storage.postgres_schema import read_postgres_schema_metadata

        metadata = read_postgres_schema_metadata()
        table_order = tuple(metadata)
        schema_columns = {
            table_name: tuple(table["columns"])
            for table_name, table in metadata.items()
        }
        table_comments = {
            table_name: str(table["comment"])
            for table_name, table in metadata.items()
        }
        column_comments = {
            table_name: {
                column_name: str(comment)
                for column_name, comment in table["columns"].items()
            }
            for table_name, table in metadata.items()
        }
    else:
        schema_columns = STORAGE_SCHEMA_COLUMNS
        table_order = STORAGE_SCHEMA_TABLE_ORDER

    rows = []
    for table_name in table_order:
        columns = schema_columns[table_name]
        role = "публичная" if table_name in USER_FACING_TABLES else "внутренняя"
        rendered_columns = (
            ", ".join(
                f"`{column}` — {column_comments[table_name][column]}"
                for column in columns
            )
            if postgres
            else ", ".join(f"`{column}`" for column in columns)
        )
        rows.append(
            f"| `{table_name}` | {role} | {table_comments[table_name]} | "
            f"{rendered_columns} |"
        )

    public_tables = _format_backtick_list(USER_FACING_TABLES)
    active_internal_tables = tuple(
        table_name for table_name in INTERNAL_TABLES if table_name in table_order
    )
    internal_tables = _format_backtick_list(active_internal_tables)
    internal_guidance = (
        f"- Внутренние таблицы упоминай только для явных вопросов про хранение или debug: {internal_tables}.\n"
        if active_internal_tables
        else ""
    )
    backend_label = "PostgreSQL" if postgres else "SQLite"
    system_catalog = "`pg_catalog`" if postgres else "`sqlite_master`"
    comment_guidance = (
        "- Описания таблиц и колонок прочитаны из `pg_catalog` через "
        "PostgreSQL `COMMENT ON`; используй именно их.\n"
        if postgres
        else ""
    )
    schema_source = (
        "Блок с таблицами, колонками и описаниями прочитан из `pg_catalog`."
        if postgres
        else (
            "Блок с таблицами и колонками сгенерирован из "
            "`storage/database.py`; не подменяй его устаревшей документацией."
        )
    )
    s2t_display_columns = _format_backtick_list(("row_num", *S2T_RECORD_FIELDS))
    return (
        f"## Актуальная схема {backend_label}\n\n"
        f"{schema_source}\n\n"
        "| Таблица | Роль | Комментарий | Колонки (реальные имена) |\n"
        "|---------|------|-------------|--------------------------|\n"
        + "\n".join(rows)
        + "\n\n"
        "## Публичная DDL-схема для обычных вопросов в чате\n"
        f"- Вопросы пользователя про \"таблицы\", \"DDL\" и \"схему\" трактуй как вопросы про публичный слой ETL/S2T, а не про все внутренние таблицы {backend_label}.\n"
        f"- По умолчанию показывай только публичные таблицы: {public_tables}.\n"
        + internal_guidance
        + comment_guidance
        + f"- Для `s2t_transformations` по умолчанию показывай только {s2t_display_columns}, если пользователь явно не просит сырой DDL.\n"
        f"- Не перечисляй {system_catalog} и служебные таблицы, если пользователь прямо не спрашивает про внутреннюю реализацию БД.\n"
    )


def _config_object(filename: str) -> Dict[str, object]:
    """Read one checked-in JSON config used as a schema source of truth."""
    return json.loads((CONFIG_DIR / filename).read_text(encoding="utf-8"))


def _compact_json(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def get_s2t_mapping_schema_cheatsheet() -> str:
    """Build the current S2T tuple and upload-column mapping schema."""
    from storage.database import S2T_RECORD_FIELDS

    column_mapping = _config_object("column_mapping.json")
    extraction = _config_object("usefull_col_extraction.json")
    table_layers = _config_object("table_layers.json")
    return (
        "## Схема S2T-маппинга\n\n"
        "Источник истины — текущие config JSON и storage/database.py.\n"
        "- Один сохранённый кортеж: "
        + ", ".join(f"`{field}`" for field in S2T_RECORD_FIELDS)
        + ".\n"
        "- `source_*` описывает вход, `target_*` — результат; "
        "`transformation_rule` хранит правило или SQL как текст.\n"
        "- `s2t_transformations` глобальна: её нельзя автоматически фильтровать "
        "по `file_id`. Пустой `target_table` недопустим при загрузке.\n"
        "- Алиасы заголовков S2T-листа: "
        + _compact_json(column_mapping.get("s2t", {}))
        + "\n- Цель извлечения S2T: "
        + _compact_json(extraction.get("s2t_transformations", {}))
        + "\n- Правила слоёв: "
        + _compact_json(table_layers)
    )


def get_excel_mapping_schema_cheatsheet() -> str:
    """Build current non-S2T sheet and column matching schemas."""
    column_mapping = _config_object("column_mapping.json")
    column_mapping.pop("s2t", None)
    extraction = _config_object("usefull_col_extraction.json")
    extraction.pop("s2t_transformations", None)
    return (
        "## Схемы Excel-маппингов\n\n"
        "Источник истины — текущие config JSON; имена ниже являются "
        "настроенными ролями и алиасами, а не найденными строками файла.\n"
        "- Группы и алиасы листов: "
        + _compact_json(_config_object("sheet_groups.json"))
        + "\n- Алиасы колонок по группам: "
        + _compact_json(column_mapping)
        + "\n- Целевые поля извлечения: "
        + _compact_json(extraction)
    )


def get_neo4j_schema_cheatsheet() -> str:
    """Return the public graph projection schema used by lineage tools."""
    return (
        "## Схема Neo4j lineage\n\n"
        "- Узел `ETLTable`: точное имя таблицы в свойстве `name`.\n"
        "- Узел `ETLColumn`: `key`, `table_name`, `name`; wildcard хранится "
        "как отдельная колонка с `name=\"*\"`.\n"
        "- `(:ETLColumn)-[:TRANSFORMS_TO]->(:ETLColumn)` — направленная "
        "колонковая связь.\n"
        "- `(:ETLTable)-[:TABLE_TRANSFORMS_TO]->(:ETLTable)` — направленная "
        "табличная связь; SQL правила может находиться на ребре.\n"
        "- Все узлы проекции имеют label `ETLProjection`; исходные факты "
        "остаются в основном SQL-хранилище."
    )


def load_schemas(sections: Iterable[str]) -> str:
    """Load only the exact data schemas selected by the router."""
    loaders = {
        "SQLite ETL": get_sqlite_schema_cheatsheet,
        "S2T-маппинг": get_s2t_mapping_schema_cheatsheet,
        "Excel-маппинги": get_excel_mapping_schema_cheatsheet,
        "Neo4j lineage": get_neo4j_schema_cheatsheet,
    }
    selected: List[str] = []
    for section in dict.fromkeys(str(item) for item in sections):
        loader = loaders.get(section)
        if loader is not None:
            selected.append(loader().strip())
    return "\n\n---\n\n".join(part for part in selected if part)


def load_skills(sections: Optional[Iterable[str]] = None) -> str:
    """Загрузить все либо только выбранные разделы runtime skills."""
    text = _prompt_text("skills.md")
    if sections is None or not text:
        return text

    requested = {section.strip().casefold() for section in sections}
    lines = text.splitlines()
    preamble: List[str] = []
    blocks: List[Tuple[str, List[str]]] = []
    current_name: Optional[str] = None
    current_lines: List[str] = []

    for line in lines:
        if line.startswith("## "):
            if current_name is not None:
                blocks.append((current_name, current_lines))
            current_name = line[3:].strip()
            current_lines = [line]
        elif current_name is None:
            preamble.append(line)
        else:
            current_lines.append(line)

    if current_name is not None:
        blocks.append((current_name, current_lines))

    selected_lines = list(preamble)
    for name, block_lines in blocks:
        if name.casefold() in requested:
            if selected_lines and selected_lines[-1] != "":
                selected_lines.append("")
            selected_lines.extend(block_lines)

    return "\n".join(selected_lines).strip()


def load_operation_skills(
    sections: Iterable[str],
    *,
    stage: Literal[
        "plan",
        "planner",
        "observer",
        "upstream_decision",
        "upstream",
    ],
    sql_risk_aspects: Optional[Iterable[SqlRiskAspect]] = None,
) -> str:
    """Load only the selected operation-skill rules for one LLM stage."""
    text = _prompt_text("operation_skills.md")
    if not text:
        return ""

    stage_titles = {
        "plan": "План данных",
        "planner": "Выполнение чтения",
        "observer": "Приёмка результата",
        "upstream_decision": "Проверка достаточности",
        "upstream": "Итоговый анализ",
    }
    requested = {
        str(section).strip().casefold()
        for section in sections
        if str(section).strip()
    }
    if not requested:
        return ""

    aspect_context = ""
    if "анализ sql-рисков" in requested:
        aspect_context = _sql_risk_aspect_context(
            sql_risk_aspects or (),
            stage=stage,
        )
        if aspect_context:
            requested.remove("анализ sql-рисков")

    selected: List[str] = []
    current_skill: Optional[str] = None
    current_stage: Optional[str] = None
    stage_lines: List[str] = []

    def flush_stage() -> None:
        if (
            current_skill is not None
            and current_skill.casefold() in requested
            and current_stage == stage_titles[stage]
        ):
            body = "\n".join(stage_lines).strip()
            if body:
                selected.append(f"## {current_skill}\n{body}")

    for line in text.splitlines():
        if line.startswith("## "):
            flush_stage()
            current_skill = line[3:].strip()
            current_stage = None
            stage_lines = []
        elif line.startswith("### "):
            flush_stage()
            current_stage = line[4:].strip()
            stage_lines = []
        elif current_stage is not None:
            stage_lines.append(line)
    flush_stage()

    if aspect_context:
        selected.append(aspect_context)

    if not selected:
        return ""
    return "Правила выбранных operation-skills:\n\n" + "\n\n".join(selected)


def load_chat_agent_context() -> str:
    """Загрузить runtime-контекст для Flask chat-agent."""
    return _prompt_text("chat_agent.md")


def load_upstream_analysis_context() -> str:
    """Загрузить постоянные правила анализа для upstream coordinator."""
    return "\n\n".join(
        part
        for part in (
            _prompt_text("upstream_analysis.md").strip(),
            _prompt_text("technical_fields.md").strip(),
        )
        if part
    )
