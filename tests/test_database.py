import pytest
import json
import datetime
from config.useful_columns import get_usefull_col_extraction_target
from storage.database import (
    CORE_TABLES,
    DATA_COLUMNS,
    DatabaseSchemaError,
    FILES_COLUMNS,
    FILE_SHEET_HEADER_COLUMNS,
    INTERNAL_TABLES,
    LEGACY_TABLES,
    REMOVED_WORKBOOK_TABLES,
    S2T_TRANSFORMATION_COLUMNS,
    SOURCE_TABLE_COLUMNS,
    STORAGE_SCHEMA_COLUMNS,
    STORAGE_SCHEMA_TABLE_ORDER,
    TARGET_TABLE_COLUMNS,
    USER_FACING_TABLES,
    clear_all_data,
    get_file,
    init_db,
    store_excel_data,
    get_db_connection,
    update_file_description,
    update_file_summary,
    update_file_result_json,
    get_column_id_by_name,
    get_sheet_id,
    get_columns_by_sheet,
)

def test_init_db(temp_db):
    # temp_db fixture provides a connection; tables should exist
    cursor = temp_db.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = {row[0] for row in cursor.fetchall()}
    assert tables == set(CORE_TABLES)
    cursor.execute("PRAGMA table_info(files)")
    file_columns = [row[1] for row in cursor.fetchall()]
    assert file_columns == list(FILES_COLUMNS)
    cursor.execute("PRAGMA table_info(s2t_transformations)")
    s2t_columns = [row[1] for row in cursor.fetchall()]
    assert s2t_columns == list(S2T_TRANSFORMATION_COLUMNS)
    cursor.execute("PRAGMA table_info(file_sheet_headers)")
    header_columns = [row[1] for row in cursor.fetchall()]
    assert header_columns == list(FILE_SHEET_HEADER_COLUMNS)
    for table_name, expected_columns in (
        ("source_tables", SOURCE_TABLE_COLUMNS),
        ("target_tables", TARGET_TABLE_COLUMNS),
    ):
        cursor.execute(f"PRAGMA table_info({table_name})")
        catalog_columns = [row[1] for row in cursor.fetchall()]
        assert catalog_columns == list(expected_columns)
    cursor.execute("PRAGMA table_info(data)")
    data_columns = [row[1] for row in cursor.fetchall()]
    assert data_columns == list(DATA_COLUMNS)
    assert not (tables & set(LEGACY_TABLES + REMOVED_WORKBOOK_TABLES))


def test_storage_schema_constants_cover_current_tables():
    assert set(STORAGE_SCHEMA_COLUMNS) == set(CORE_TABLES)
    assert set(STORAGE_SCHEMA_TABLE_ORDER) == set(CORE_TABLES)
    assert set(USER_FACING_TABLES + INTERNAL_TABLES) == set(CORE_TABLES)
    assert USER_FACING_TABLES == (
        "files",
        "file_sheet_headers",
        "source_tables",
        "target_tables",
        "s2t_transformations",
        "data",
    )
    assert INTERNAL_TABLES == ()
    assert tuple(get_usefull_col_extraction_target("source_tables")["fields"]) == tuple(
        SOURCE_TABLE_COLUMNS[5:-1]
    )
    assert tuple(get_usefull_col_extraction_target("target_tables")["fields"]) == tuple(
        TARGET_TABLE_COLUMNS[5:-1]
    )
    assert tuple(get_usefull_col_extraction_target("s2t_transformations")["fields"]) == tuple(
        S2T_TRANSFORMATION_COLUMNS[5:-1]
    )


def test_clear_all_data_deletes_every_row_and_keeps_schema(temp_db):
    cursor = temp_db.cursor()
    cursor.execute(
        """
        INSERT INTO files (file_id, filename, upload_time)
        VALUES (10, 'mapping.xlsx', '2026-07-27')
        """
    )
    cursor.execute(
        """
        INSERT INTO file_sheet_headers
        (sheet_id, file_id, sheet_name, skipped, columns_count)
        VALUES (20, 10, 'S2T', 0, 1)
        """
    )
    cursor.execute(
        """
        INSERT INTO data
        (id, sheet_id, table_name, row_num, column_id, value)
        VALUES (30, 20, 'S2T', 0, 1, 'value')
        """
    )
    cursor.execute(
        """
        INSERT INTO source_tables
        (id, file_id, sheet_id, sheet_name, row_num, table_name, description)
        VALUES (40, 10, 20, 'Source', 0, 'src', 'source')
        """
    )
    cursor.execute(
        """
        INSERT INTO target_tables
        (id, file_id, sheet_id, sheet_name, row_num, table_name, description)
        VALUES (50, 10, 20, 'Target', 0, 'tgt', 'target')
        """
    )
    cursor.execute(
        """
        INSERT INTO s2t_transformations
        (id, file_id, sheet_id, sheet_name, row_num,
         target_table, target_field, source_table, source_field)
        VALUES (60, 10, 20, 'S2T', 0, 'tgt', 'id', 'src', 'id')
        """
    )
    temp_db.commit()

    deleted = clear_all_data()

    assert deleted == {
        "files": 1,
        "file_sheet_headers": 1,
        "source_tables": 1,
        "target_tables": 1,
        "s2t_transformations": 1,
        "data": 1,
    }
    tables = {
        row[0]
        for row in temp_db.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        ).fetchall()
    }
    assert tables == set(CORE_TABLES)
    for table_name in CORE_TABLES:
        assert temp_db.execute(
            f'SELECT COUNT(*) FROM "{table_name}"'
        ).fetchone()[0] == 0


def test_init_db_drops_legacy_tables(temp_db):
    cursor = temp_db.cursor()
    for table_name in LEGACY_TABLES + REMOVED_WORKBOOK_TABLES:
        cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} (id TEXT)")
    temp_db.commit()

    init_db()

    cursor = temp_db.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = {row[0] for row in cursor.fetchall()}
    assert tables == set(CORE_TABLES)


def test_init_db_rejects_old_table_catalog_schema_without_mutating_data(temp_db):
    cursor = temp_db.cursor()
    cursor.execute(
        "INSERT INTO files (filename, upload_time, model_used) VALUES (?, ?, ?)",
        ("legacy.xlsx", "2025-01-01", "model"),
    )
    cursor.execute("DROP TABLE source_tables")
    cursor.execute(
        """
        CREATE TABLE source_tables (
            id TEXT PRIMARY KEY,
            file_id TEXT,
            table_name TEXT,
            description TEXT
        )
        """
    )
    cursor.executemany(
        """
        INSERT INTO source_tables (id, file_id, table_name, description)
        VALUES (?, ?, ?, ?)
        """,
        [
            ("old-1", "1", "src_same", "Одинаковое описание"),
            ("old-2", "1", "src_same", "Одинаковое описание"),
        ],
    )
    temp_db.commit()

    with pytest.raises(DatabaseSchemaError, match="Несовместимая схема SQLite"):
        init_db()

    cursor = temp_db.cursor()
    cursor.execute(
        """
        SELECT file_id, table_name, description
        FROM source_tables
        ORDER BY rowid
        """
    )
    assert [tuple(row) for row in cursor.fetchall()] == [
        ("1", "src_same", "Одинаковое описание"),
        ("1", "src_same", "Одинаковое описание"),
    ]
    assert cursor.execute(
        "SELECT name FROM sqlite_master WHERE name LIKE '%_numeric'"
    ).fetchall() == []


def test_init_db_legacy_pre_refactor_schema_includes_recovery_hint(temp_db):
    cursor = temp_db.cursor()
    cursor.execute("DROP TABLE IF EXISTS files")
    cursor.execute(
        """
        CREATE TABLE files (
            file_hash TEXT PRIMARY KEY,
            filename TEXT,
            model_used TEXT,
            upload_time TEXT,
            summary TEXT,
            result_json TEXT
        )
        """
    )
    cursor.execute("DROP TABLE IF EXISTS data")
    cursor.execute(
        """
        CREATE TABLE data (
            id INTEGER PRIMARY KEY,
            sheet_hash TEXT,
            row_num INTEGER,
            column_hash TEXT,
            value TEXT
        )
        """
    )
    temp_db.commit()

    with pytest.raises(DatabaseSchemaError, match="legacy-схема") as exc_info:
        init_db()

    message = str(exc_info.value)
    assert "files.file_hash" in message
    assert ".legacy.bak" in message


def test_init_db_rejects_legacy_s2t_column_names_without_mutating_data(temp_db):
    cursor = temp_db.cursor()
    cursor.execute(
        "INSERT INTO files (filename, upload_time, model_used) VALUES (?, ?, ?)",
        ("legacy-s2t.xlsx", "2025-01-01", "model"),
    )
    file_id = cursor.lastrowid
    cursor.execute("DROP TABLE s2t_transformations")
    cursor.execute(
        """
        CREATE TABLE s2t_transformations (
            id INTEGER PRIMARY KEY,
            file_id INTEGER,
            sheet_id INTEGER,
            sheet_name TEXT,
            row_num INTEGER,
            target_table TEXT,
            target_column TEXT,
            source_table TEXT,
            source_column TEXT,
            transformation_rule TEXT,
            raw_json TEXT
        )
        """
    )
    cursor.execute(
        """
        INSERT INTO s2t_transformations
        (file_id, sheet_name, row_num, target_table, target_column,
         source_table, source_column, transformation_rule, raw_json)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            file_id,
            "S2T",
            1,
            "target_table",
            "target_id",
            "source_table",
            "source_id",
            "copy",
            '{"legacy": true}',
        ),
    )
    temp_db.commit()

    with pytest.raises(DatabaseSchemaError, match="Несовместимая схема SQLite"):
        init_db()

    columns = [
        row[1] for row in cursor.execute("PRAGMA table_info(s2t_transformations)")
    ]
    assert "target_column" in columns
    assert "source_column" in columns
    assert "target_field" not in columns
    assert "source_field" not in columns
    row = cursor.execute(
        """
        SELECT target_table, target_column, source_table, source_column,
               transformation_rule, raw_json
        FROM s2t_transformations
        """
    ).fetchone()
    assert tuple(row) == (
        "target_table",
        "target_id",
        "source_table",
        "source_id",
        "copy",
        '{"legacy": true}',
    )


def test_init_db_drops_removed_workbook_tables(temp_db):
    cursor = temp_db.cursor()
    cursor.execute(
        """
        CREATE TABLE sheets (
            sheet_id TEXT PRIMARY KEY,
            file_id TEXT,
            sheet_name TEXT,
            header_start_row INTEGER,
            header_rows_count INTEGER,
            nested_structure INTEGER,
            skipped INTEGER DEFAULT 0,
            skip_reason TEXT
        )
        """
    )
    cursor.execute(
        """
        CREATE TABLE columns (
            column_id TEXT PRIMARY KEY,
            sheet_id TEXT,
            column_index INTEGER,
            column_name_flat TEXT,
            column_header TEXT
        )
        """
    )
    cursor.execute(
        """
        INSERT INTO sheets
        (sheet_id, file_id, sheet_name, header_start_row, header_rows_count, nested_structure, skipped)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        ("sh_old", "fh_old", "OldSheet", 2, 2, 1, 0),
    )
    cursor.execute(
        """
        INSERT INTO columns
        (column_id, sheet_id, column_index, column_name_flat, column_header)
        VALUES (?, ?, ?, ?, ?)
        """,
        ("c_old", "sh_old", 0, "Parent > Child", json.dumps(["Parent", "Child"])),
    )
    temp_db.commit()

    init_db()

    tables = {
        row[0]
        for row in cursor.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
    }
    assert not (tables & set(REMOVED_WORKBOOK_TABLES))


def test_store_excel_data(temp_db, sample_excel_bytes):
    sheets = [{
        "sheet_name": "Sheet1",
        "skip_reason": None,
        "header": {"start_row": 0, "row_count": 1, "nested": False},
        "columns": ["Name", "Age"],
        "data_rows": [["Alice", 30], ["Bob", 25]],
    }]
    file_id = store_excel_data(sample_excel_bytes, "test.xlsx", "GigaChat-Pro", sheets)
    assert file_id is not None
    # Verify data was inserted
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT filename FROM files WHERE file_id = ?", (file_id,))
    row = cursor.fetchone()
    assert row["filename"] == "test.xlsx"
    cursor.execute(
        """
        SELECT sheet_name, columns_count, headers_json, headers_flat
        FROM file_sheet_headers
        WHERE file_id = ?
        """,
        (file_id,),
    )
    header_row = cursor.fetchone()
    assert header_row["sheet_name"] == "Sheet1"
    assert header_row["columns_count"] == 2
    assert "Name" in header_row["headers_flat"]
    assert json.loads(header_row["headers_json"])[0]["flat"] == "Name"
    cursor.execute("SELECT DISTINCT table_name FROM data WHERE sheet_id = ?", (get_sheet_id(file_id, "Sheet1"),))
    data_table_names = {row["table_name"] for row in cursor.fetchall()}
    assert data_table_names == {"Sheet1"}
    conn.close()


def test_store_excel_data_preserves_filtered_source_row_numbers(
    temp_db, sample_excel_bytes
):
    sheets = [{
        "sheet_name": "Sheet1",
        "skip_reason": None,
        "header": {"start_row": 0, "row_count": 1, "nested": False},
        "columns": ["Name"],
        "data_rows": [["Alice"], ["Bob"]],
        "data_row_numbers": [0, 3],
    }]

    file_id = store_excel_data(
        sample_excel_bytes,
        "filtered.xlsx",
        "test-model",
        sheets,
    )

    conn = get_db_connection()
    row_numbers = [
        row["row_num"]
        for row in conn.execute(
            """
            SELECT DISTINCT data.row_num
            FROM data
            JOIN file_sheet_headers
              ON file_sheet_headers.sheet_id = data.sheet_id
            WHERE file_sheet_headers.file_id = ?
            ORDER BY data.row_num
            """,
            (file_id,),
        ).fetchall()
    ]
    conn.close()
    assert row_numbers == [0, 3]


def test_get_file_returns_complete_record(temp_db, sample_excel_bytes):
    file_id = store_excel_data(
        sample_excel_bytes,
        "record.xlsx",
        "test-model",
        [],
    )

    record = get_file(file_id)

    assert record is not None
    assert tuple(record) == FILES_COLUMNS
    assert record["file_id"] == file_id
    assert record["filename"] == "record.xlsx"
    assert record["model_used"] == "test-model"
    assert get_file(file_id + 1) is None


def test_store_excel_preserves_summary_and_result_json_on_reupload(
    temp_db, sample_excel_bytes, mock_embeddings
):
    sheets = [
        {
            "sheet_name": "Sheet1",
            "skip_reason": None,
            "header": {"start_row": 0, "row_count": 1, "nested": False},
            "columns": ["A"],
            "data_rows": [["1"]],
        }
    ]
    fh = store_excel_data(sample_excel_bytes, "t.xlsx", "m1", sheets)
    update_file_summary(fh, "preserved summary")
    update_file_description(fh, "preserved description")
    update_file_result_json(fh, '{"kept": true}')

    store_excel_data(
        sample_excel_bytes,
        "t.xlsx",
        "m2",
        sheets,
        file_id=fh,
    )

    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT summary, description, description_embedding, result_json, model_used
        FROM files
        WHERE file_id = ?
        """,
        (fh,),
    )
    row = cursor.fetchone()
    conn.close()
    assert row["summary"] == "preserved summary"
    assert row["description"] == "preserved description"
    assert row["description_embedding"] == b"embedding:preserved description"
    assert json.loads(row["result_json"]) == {"kept": True}
    assert row["model_used"] == "m2"


def test_equal_file_uploads_get_separate_numeric_ids(temp_db, sample_excel_bytes):
    sheets = [
        {
            "sheet_name": "Sheet1",
            "skip_reason": None,
            "header": {"start_row": 0, "row_count": 1, "nested": False},
            "columns": ["A"],
            "data_rows": [["same"], ["same"]],
        }
    ]

    first_id = store_excel_data(sample_excel_bytes, "same.xlsx", "m", sheets)
    second_id = store_excel_data(sample_excel_bytes, "same.xlsx", "m", sheets)

    assert isinstance(first_id, int)
    assert isinstance(second_id, int)
    assert first_id != second_id
    count = temp_db.execute("SELECT COUNT(*) FROM files").fetchone()[0]
    assert count == 2


def test_update_file_summary(temp_db):
    # First insert a file manually
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("INSERT INTO files (filename, upload_time, model_used) VALUES (?, ?, ?)",
                   ("test.xlsx", "2025-01-01", "model"))
    file_id = cursor.lastrowid
    conn.commit()
    conn.close()
    update_file_summary(file_id, "Test summary")
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT summary FROM files WHERE file_id = ?", (file_id,))
    row = cursor.fetchone()
    assert row["summary"] == "Test summary"
    conn.close()


def test_update_file_description(temp_db, mock_embeddings):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO files (filename, upload_time, model_used) VALUES (?, ?, ?)",
        ("desc.xlsx", "2025-01-01", "model"),
    )
    file_id = cursor.lastrowid
    conn.commit()
    conn.close()

    update_file_description(file_id, "Short file description")

    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT description, description_embedding
        FROM files
        WHERE file_id = ?
        """,
        (file_id,),
    )
    row = cursor.fetchone()
    conn.close()
    assert row["description"] == "Short file description"
    assert row["description_embedding"] == b"embedding:Short file description"


def test_update_file_result_json(temp_db):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO files (filename, upload_time, model_used) VALUES (?, ?, ?)",
        ("f.xlsx", "2025-01-01", "m"),
    )
    file_id = cursor.lastrowid
    conn.commit()
    conn.close()
    payload = {"ok": True, "n": 1}
    update_file_result_json(file_id, json.dumps(payload))
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT result_json FROM files WHERE file_id = ?", (file_id,))
    row = cursor.fetchone()
    conn.close()
    assert json.loads(row["result_json"]) == payload


def test_get_column_id_by_name_segment_short_token(temp_db):
    """Schema matcher often emits short Excel aliases (e.g. dto) vs nested headers."""
    sheets = [
        {
            "sheet_name": "Meta",
            "skip_reason": None,
            "header": {"start_row": 0, "row_count": 1, "nested": False},
            "columns": ["Метаданные > DTO", "Other"],
            "data_rows": [["v1", "v2"]],
        }
    ]
    fh = store_excel_data(
        b"",
        "meta.xlsx",
        "model",
        sheets,
        max_rows_per_sheet=10,
    )
    sh = get_sheet_id(fh, "Meta")
    cid = get_column_id_by_name(sh, "dto")
    assert cid is not None
    flat = next(row["column_name_flat"] for row in get_columns_by_sheet(sh) if row["column_id"] == cid)
    assert "DTO" in flat


def test_get_column_id_by_name_and_nested_partial(temp_db):
    sheets = [
        {
            "sheet_name": "S1",
            "skip_reason": None,
            "header": {"start_row": 0, "row_count": 1, "nested": False},
            "columns": ["Parent > Child", "Plain"],
            "data_rows": [["v1", "v2"]],
        }
    ]
    fh = store_excel_data(
        b"",
        "minimal.xlsx",
        "model",
        sheets,
        max_rows_per_sheet=10,
    )
    sh = get_sheet_id(fh, "S1")
    assert get_column_id_by_name(sh, "Parent > Child") is not None
    assert get_column_id_by_name(sh, "zzz_nonexistent_column") is None
    cid = get_column_id_by_name(sh, "Child")
    assert cid is not None


def test_store_nested_column_headers(temp_db, sample_excel_bytes):
    sheets = [
        {
            "sheet_name": "Sheet1",
            "skip_reason": None,
            "header": {"start_row": 0, "row_count": 2, "nested": True},
            "columns": [["H1", "a"], ["H1", "b"]],
            "data_rows": [[1, 2]],
        }
    ]
    file_id = store_excel_data(
        sample_excel_bytes,
        "nest.xlsx",
        "model",
        sheets,
    )
    flats = [
        row["column_name_flat"]
        for row in get_columns_by_sheet(get_sheet_id(file_id, "Sheet1"))
    ]
    assert "H1 > a" in flats
    assert "H1 > b" in flats


def test_store_nested_column_headers_with_datetime(temp_db, sample_excel_bytes):
    header_date = datetime.datetime(2026, 7, 19, 12, 30)
    sheets = [
        {
            "sheet_name": "Sheet1",
            "skip_reason": None,
            "header": {"start_row": 0, "row_count": 2, "nested": True},
            "columns": [["Period", header_date]],
            "data_rows": [[1]],
        }
    ]
    file_id = store_excel_data(
        sample_excel_bytes,
        "date_header.xlsx",
        "model",
        sheets,
    )
    row = get_columns_by_sheet(get_sheet_id(file_id, "Sheet1"))[0]

    assert json.loads(row["column_header"]) == ["Period", "2026-07-19 12:30:00"]
