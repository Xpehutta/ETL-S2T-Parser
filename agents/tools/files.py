"""Tools for uploaded files and their descriptions."""

from typing import Any, Dict, List

from langchain_core.tools import tool

from .common import file_meta

@tool(parse_docstring=True)
def list_files() -> List[Dict[str, Any]]:
    """Получить пользовательский каталог всех загруженных Excel-файлов.

    Возвращает имя файла, сохранённое краткое описание и время загрузки. Внутренний
    file_id намеренно не показывается в общем каталоге; для последующего обращения
    к конкретному файлу по точному имени предназначен resolve_file.
    """
    from storage.database import get_db_connection
    conn = get_db_connection()
    try:
        rows = conn.execute(
            """
            SELECT filename, description, upload_time
            FROM files
            ORDER BY upload_time DESC
            """
        ).fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()

@tool(parse_docstring=True)
def resolve_file(filename: str) -> Dict[str, Any]:
    """Разрешить точное имя загруженного файла в его числовой file_id.

    Поиск выполняется без учёта регистра. При отсутствии файла возвращает явную
    ошибку, а при нескольких загрузках с одинаковым именем — список совпадений,
    не выбирая одну запись самовольно.

    Args:
        filename: Полное имя загруженного файла с расширением.
    """
    from storage.database import get_db_connection

    clean_filename = (filename or "").strip()
    if not clean_filename:
        return {"error": "filename must be non-empty", "filename": filename}
    conn = get_db_connection()
    try:
        rows = conn.execute(
            """SELECT file_id, filename, upload_time
            FROM files
            WHERE filename = ? COLLATE NOCASE
            ORDER BY upload_time DESC""",
            (clean_filename,),
        ).fetchall()
    finally:
        conn.close()
    if not rows:
        return {"error": "Uploaded file not found", "filename": clean_filename}
    if len(rows) > 1:
        return {
            "error": "Multiple uploads have this filename",
            "filename": clean_filename,
            "matches": [dict(row) for row in rows],
        }
    return dict(rows[0])


@tool(parse_docstring=True)
def get_file_description(file_id: int) -> Dict[str, Any]:
    """
    Получить сохранённое краткое описание конкретного загруженного файла.

    Read-only: читает files.description и files.summary по реальному числовому
    file_id без LLM-вызовов и без записи в БД. Если описание ещё не создано,
    сообщи об этом явно; для генерации или обновления используй мутирующие
    инструменты после явного запроса пользователя. Логические имена ETL-таблиц
    не являются идентификаторами файлов.

    Args:
        file_id: Числовой идентификатор загрузки из UI или resolve_file.
    """
    from storage.database import get_db_connection

    conn = get_db_connection()
    try:
        row = conn.execute(
            """
            SELECT file_id, filename, upload_time, summary, description
            FROM files
            WHERE file_id = ?
            """,
            (file_id,),
        ).fetchone()
    finally:
        conn.close()

    if not row:
        return {"error": f"File not found: {file_id}", "file_id": file_id}

    meta = dict(row)
    description = str(meta.get("description") or "").strip()
    summary = str(meta.get("summary") or "").strip()
    meta["description"] = description or None
    meta["summary"] = summary or None

    result: Dict[str, Any] = {
        "file": meta,
        "file_id": file_id,
        "description": description or None,
        "summary": summary or None,
        "description_present": bool(description),
        "summary_present": bool(summary),
    }

    if not description:
        result["missing_description"] = True
        if summary:
            result["hint"] = (
                "Краткое описание ещё не сохранено. Для генерации нужен "
                "явный запрос пользователя на обновление описания."
            )
        else:
            result["hint"] = (
                "Краткое описание и бизнес-саммари ещё не сохранены. "
                "Сообщи пользователю, что данные появятся после завершения "
                "анализа файла или после явного запроса на обновление."
            )

    return result

@tool(parse_docstring=True)
def update_file_description(file_id: int, description: str) -> Dict[str, Any]:
    """Перезаписать сохранённое описание файла по явному запросу пользователя.

    Это мутирующий инструмент: обновляет files.description и не должен вызываться
    для обычного чтения или автоматического уточнения ответа.

    Args:
        file_id: Числовой идентификатор существующей загрузки.
        description: Непустой утверждённый пользователем текст нового описания.
    """
    from storage.database import update_file_description as db_update_file_description

    text = str(description or "").strip()
    if not text:
        return {"error": "description must be non-empty", "file_id": file_id}

    db_update_file_description(file_id, text)
    return {
        "file": file_meta(file_id),
        "file_id": file_id,
        "description": text,
        "updated": True,
    }


@tool(parse_docstring=True)
def update_table_info_from_user_query(file_id: int, user_query: str) -> Dict[str, Any]:
    """
    Обновить описание файла фактами из явного пользовательского уточнения.

    Это мутирующий инструмент. Сначала проверяет существование строки files,
    использует уже сохранённое базовое описание, затем просит LLM переработать его
    с учётом текста пользователя и сохраняет результат.

    Args:
        file_id: Числовой идентификатор существующей загрузки.
        user_query: Явное уточнение пользователя, которое требуется сохранить.
    """
    request_text = str(user_query or "").strip()
    if not request_text:
        return {"error": "user_query must be non-empty", "file_id": file_id}

    from agents.summarizer_agent import update_file_description_from_user_query
    from storage.database import get_db_connection

    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT file_id, filename, upload_time FROM files WHERE file_id = ?",
        (file_id,),
    )
    row = cursor.fetchone()
    conn.close()
    if not row:
        return {
            "error": f"File not found in files: {file_id}",
            "file_id": file_id,
        }

    updated_description = update_file_description_from_user_query(
        file_id,
        request_text,
        save=True,
    )
    meta = dict(row)
    meta["description"] = updated_description
    return {
        "file": meta,
        "file_id": file_id,
        "description": updated_description,
        "updated": True,
        "source": "user_query",
        "sequence": [
            "verified_files_row",
            "ensured_generated_description",
            "updated_description_from_user_query",
        ],
    }
