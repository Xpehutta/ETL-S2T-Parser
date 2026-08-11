import pytest
import json
import datetime
from config.useful_columns import get_usefull_col_extraction_target
from storage.database import (
    ADDITIONAL_OBJECT_COLUMNS,
    CORE_TABLES,
    DATA_COLUMNS,
    DatabaseSchemaError,
    FILES_COLUMNS,
    FILE_SHEET_HEADER_COLUMNS,
    INTERNAL_TABLES,
    PXF_TO_A_COLUMNS,
    S2T_TRANSFORMATION_COLUMNS,
    S2T_FIELDS,
    S2T_LAYER_FIELDS,
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
    get_columns_by_sheet,
    migrate_s2t_layer_columns,
)
from storage.s2t import backfill_s2t_layers

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
        ("additional_objects", ADDITIONAL_OBJECT_COLUMNS),
        ("pxf_to_a", PXF_TO_A_COLUMNS),
    ):
        cursor.execute(f"PRAGMA table_info({table_name})")
        catalog_columns = [row[1] for row in cursor.fetchall()]
        assert catalog_columns == list(expected_columns)
    cursor.execute("PRAGMA table_info(data)")
    data_columns = [row[1] for row in cursor.fetchall()]
    assert data_columns == list(DATA_COLUMNS)


def test_store_excel_data_preserves_long_cell_values(temp_db):
    sql = "SELECT '" + ("x" * 1500) + "'"
    file_id = store_excel_data(
        "long_sql.xlsx",
        "model",
        [
            {
                "sheet_name": "Additional objects",
                "skip_reason": None,
                "header": {"start_row": 0, "row_count": 1, "nested": False},
                "columns": ["name", "SQL"],
                "data_rows": [["long_object", sql]],
            }
        ],
    )

    stored = temp_db.execute(
        """
        SELECT value
        FROM data
        WHERE file_id = ? AND table_name = 'Additional objects' AND column_id = 2
        """,
        (file_id,),
    ).fetchone()[0]
    assert stored == sql


def test_storage_schema_constants_cover_current_tables():
    assert set(STORAGE_SCHEMA_COLUMNS) == set(CORE_TABLES)
    assert set(STORAGE_SCHEMA_TABLE_ORDER) == set(CORE_TABLES)
    assert set(USER_FACING_TABLES + INTERNAL_TABLES) == set(CORE_TABLES)
    assert USER_FACING_TABLES == (
        "files",
        "file_sheet_headers",
        "source_tables",
        "target_tables",
        "additional_objects",
        "pxf_to_a",
        "s2t_transformations",
        "data",
    )
    assert INTERNAL_TABLES == ()
    assert tuple(get_usefull_col_extraction_target("source_tables")["fields"]) == tuple(
        SOURCE_TABLE_COLUMNS[4:-1]
    )
    assert tuple(get_usefull_col_extraction_target("target_tables")["fields"]) == tuple(
        TARGET_TABLE_COLUMNS[4:-1]
    )
    assert tuple(get_usefull_col_extraction_target("additional_objects")["fields"]) == tuple(
        ADDITIONAL_OBJECT_COLUMNS[4:]
    )
    assert tuple(get_usefull_col_extraction_target("pxf_to_a")["fields"]) == tuple(
        PXF_TO_A_COLUMNS[4:]
    )
    assert tuple(get_usefull_col_extraction_target("s2t_transformations")["fields"]) == tuple(
        S2T_FIELDS
    )
    assert S2T_TRANSFORMATION_COLUMNS[4:] == S2T_FIELDS + S2T_LAYER_FIELDS


def test_explicit_s2t_layer_migration_preserves_and_backfills_rows(temp_db):
    fields_sql = ", ".join(f'"{field}" TEXT' for field in S2T_FIELDS)
    temp_db.execute("DROP TABLE s2t_transformations")
    temp_db.execute(
        f"""
        CREATE TABLE s2t_transformations (
            id INTEGER PRIMARY KEY,
            file_id INTEGER,
            sheet_name TEXT,
            row_num INTEGER,
            {fields_sql}
        )
        """
    )
    temp_db.execute(
        """
        INSERT INTO s2t_transformations
        (id, file_id, sheet_name, row_num, source_table, target_table)
        VALUES (1, 10, 'S2T', 1, 'source_without_prefix', 'target_without_prefix')
        """
    )
    temp_db.execute(
        """
        INSERT INTO s2t_transformations
        (id, file_id, sheet_name, row_num, source_table, target_table)
        VALUES (2, 10, 'Additional objects', 2, 'known_table', 'b_target')
        """
    )
    temp_db.commit()

    assert migrate_s2t_layer_columns() == {
        "changed": True,
        "columns_added": ["source_layer", "target_layer"],
    }
    assert migrate_s2t_layer_columns()["changed"] is False
    assert backfill_s2t_layers() == {
        "file_id": None,
        "rows": 2,
        "updated": 2,
        "resolved_source": 1,
        "resolved_target": 2,
    }
    rows = temp_db.execute(
        "SELECT source_layer, target_layer FROM s2t_transformations ORDER BY id"
    ).fetchall()
    assert [tuple(row) for row in rows] == [("B", "T"), (None, "B")]


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
        (file_id, sheet_name, skipped, columns_count)
        VALUES (10, 'S2T', 0, 1)
        """
    )
    cursor.execute(
        """
        INSERT INTO data
        (id, file_id, table_name, row_num, column_id, value)
        VALUES (30, 10, 'S2T', 0, 1, 'value')
        """
    )
    cursor.execute(
        """
        INSERT INTO source_tables
        (id, file_id, sheet_name, row_num, table_name, description)
        VALUES (40, 10, 'Source', 0, 'src', 'source')
        """
    )
    cursor.execute(
        """
        INSERT INTO target_tables
        (id, file_id, sheet_name, row_num, table_name, description)
        VALUES (50, 10, 'Target', 0, 'tgt', 'target')
        """
    )
    cursor.execute(
        """
        INSERT INTO s2t_transformations
        (id, file_id, sheet_name, row_num,
         target_table, target_field, source_table, source_field)
        VALUES (60, 10, 'S2T', 0, 'tgt', 'id', 'src', 'id')
        """
    )
    cursor.execute(
        """
        INSERT INTO additional_objects
        (id, file_id, sheet_name, row_num, name, sql)
        VALUES (70, 10, 'Additional objects', 0, 'view_a', 'SELECT 1')
        """
    )
    cursor.execute(
        """
        INSERT INTO pxf_to_a
        (id, file_id, sheet_name, row_num,
         external_a_table, materialized_storage, replica_table, sod)
        VALUES (80, 10, 'pxf_to_a', 0, 'ext_a', 'mat_a', 'replica_a', 'SOD')
        """
    )
    cursor.execute("ALTER TABLE additional_objects ADD COLUMN obsolete TEXT")
    temp_db.commit()

    deleted = clear_all_data()

    assert deleted == {
        "files": 1,
        "file_sheet_headers": 1,
        "source_tables": 1,
        "target_tables": 1,
        "additional_objects": 1,
        "pxf_to_a": 1,
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
    for table_name, expected_columns in STORAGE_SCHEMA_COLUMNS.items():
        assert temp_db.execute(
            f'SELECT COUNT(*) FROM "{table_name}"'
        ).fetchone()[0] == 0
        actual_columns = tuple(
            row[1]
            for row in temp_db.execute(
                f'PRAGMA table_info("{table_name}")'
            ).fetchall()
        )
        assert actual_columns == tuple(expected_columns)


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


def test_init_db_rejects_incompatible_identifier_schema(temp_db):
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

    with pytest.raises(DatabaseSchemaError) as exc_info:
        init_db()

    message = str(exc_info.value)
    assert "files" in message
    assert "file_hash" in message


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


def test_store_excel_data(temp_db):
    sheets = [{
        "sheet_name": "Sheet1",
        "skip_reason": None,
        "header": {"start_row": 0, "row_count": 1, "nested": False},
        "columns": ["Name", "Age"],
        "data_rows": [["Alice", 30], ["Bob", 25]],
    }]
    file_id = store_excel_data("test.xlsx", "GigaChat-Pro", sheets)
    assert file_id is not None
    # Verify data was inserted
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT filename FROM files WHERE file_id = ?", (file_id,))
    row = cursor.fetchone()
    assert row["filename"] == "test.xlsx"
    cursor.execute(
        """
        SELECT sheet_name, columns_count, headers_json
        FROM file_sheet_headers
        WHERE file_id = ?
        """,
        (file_id,),
    )
    header_row = cursor.fetchone()
    assert header_row["sheet_name"] == "Sheet1"
    assert header_row["columns_count"] == 2
    assert json.loads(header_row["headers_json"])[0]["flat"] == "Name"
    cursor.execute(
        "SELECT DISTINCT table_name FROM data WHERE file_id = ?",
        (file_id,),
    )
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
              ON file_sheet_headers.file_id = data.file_id
             AND file_sheet_headers.sheet_name = data.table_name
            WHERE file_sheet_headers.file_id = ?
            ORDER BY data.row_num
            """,
            (file_id,),
        ).fetchall()
    ]
    conn.close()
    assert row_numbers == [0, 3]


def test_store_excel_data_keeps_rows_beyond_previous_limit(temp_db):
    rows = [[f"value_{index}"] for index in range(1001)]
    file_id = store_excel_data(
        "all_rows.xlsx",
        "test-model",
        [
            {
                "sheet_name": "Sheet1",
                "skip_reason": None,
                "header": {"start_row": 0, "row_count": 1, "nested": False},
                "columns": ["Value"],
                "data_rows": rows,
            }
        ],
    )

    count, last_row = temp_db.execute(
        "SELECT COUNT(*), MAX(row_num) FROM data WHERE file_id = ?",
        (file_id,),
    ).fetchone()

    assert (count, last_row) == (1001, 1000)


def test_get_file_returns_complete_record(temp_db):
    file_id = store_excel_data(
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


def test_equal_file_uploads_get_separate_numeric_ids(temp_db):
    sheets = [
        {
            "sheet_name": "Sheet1",
            "skip_reason": None,
            "header": {"start_row": 0, "row_count": 1, "nested": False},
            "columns": ["A"],
            "data_rows": [["same"], ["same"]],
        }
    ]

    first_id = store_excel_data("same.xlsx", "m", sheets)
    second_id = store_excel_data("same.xlsx", "m", sheets)

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


def test_store_nested_column_headers(temp_db):
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
        "nest.xlsx",
        "model",
        sheets,
    )
    flats = [
        row["column_name_flat"]
        for row in get_columns_by_sheet(file_id, "Sheet1")
    ]
    assert "H1 > a" in flats
    assert "H1 > b" in flats


def test_store_nested_column_headers_with_datetime(temp_db):
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
        "date_header.xlsx",
        "model",
        sheets,
    )
    row = get_columns_by_sheet(file_id, "Sheet1")[0]

    assert json.loads(row["column_header"]) == ["Period", "2026-07-19 12:30:00"]
