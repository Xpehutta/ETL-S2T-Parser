"""Runtime prompt and SQLite schema context for the chat agent."""

from typing import Iterable, List, Optional, Tuple

from .common import PROJECT_ROOT

PROMPTS_DIR = PROJECT_ROOT / "agents" / "prompts"


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
    """Собрать блок схемы SQLite для prompt-ов агентов из storage/database.py."""
    from storage.database import (
        INTERNAL_TABLES,
        STORAGE_SCHEMA_COLUMNS,
        STORAGE_SCHEMA_TABLE_ORDER,
        S2T_RECORD_FIELDS,
        USER_FACING_TABLES,
    )

    rows = []
    for table_name in STORAGE_SCHEMA_TABLE_ORDER:
        columns = STORAGE_SCHEMA_COLUMNS[table_name]
        role = "публичная" if table_name in USER_FACING_TABLES else "внутренняя"
        rows.append(
            f"| `{table_name}` | {role} | "
            + ", ".join(f"`{column}`" for column in columns)
            + " |"
        )

    public_tables = _format_backtick_list(USER_FACING_TABLES)
    internal_tables = _format_backtick_list(INTERNAL_TABLES)
    internal_guidance = (
        f"- Внутренние таблицы упоминай только для явных вопросов про хранение или debug: {internal_tables}.\n"
        if INTERNAL_TABLES
        else ""
    )
    s2t_display_columns = _format_backtick_list(("row_num", *S2T_RECORD_FIELDS))
    return (
        "\n\n---\n\n"
        "## Актуальная схема SQLite\n\n"
        "Блок с таблицами и колонками сгенерирован из `storage/database.py`; не подменяй его устаревшей документацией.\n\n"
        "| Таблица | Роль | Колонки (реальные имена) |\n"
        "|---------|------|--------------------------|\n"
        + "\n".join(rows)
        + "\n\n"
        "## Публичная DDL-схема для обычных вопросов в чате\n"
        "- Вопросы пользователя про \"таблицы\", \"DDL\" и \"схему\" трактуй как вопросы про публичный слой ETL/S2T, а не про все внутренние SQLite-таблицы.\n"
        f"- По умолчанию показывай только публичные таблицы: {public_tables}.\n"
        + internal_guidance
        + f"- Для `s2t_transformations` по умолчанию показывай только {s2t_display_columns}, если пользователь явно не просит сырой DDL.\n"
        "- Не перечисляй `sqlite_master` и служебные таблицы, если пользователь прямо не спрашивает про внутреннюю реализацию БД.\n"
    )


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


def load_chat_agent_context() -> str:
    """Загрузить runtime-контекст для Flask chat-agent."""
    return _prompt_text("chat_agent.md")
