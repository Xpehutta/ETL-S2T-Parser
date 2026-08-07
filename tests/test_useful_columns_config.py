import json

import pytest

import config.column_mapping as column_mapping_config
import config.sheet_groups as sheet_groups
from config.useful_columns import (
    clear_usefull_col_extraction_cache,
    get_usefull_col_extraction_target,
)


@pytest.fixture(autouse=True)
def _reset_config_caches():
    clear_usefull_col_extraction_cache()
    column_mapping_config.clear_column_mapping_cache()
    sheet_groups.clear_sheet_groups_cache()
    yield
    clear_usefull_col_extraction_cache()
    column_mapping_config.clear_column_mapping_cache()
    sheet_groups.clear_sheet_groups_cache()


def test_usefull_col_extraction_config_defines_s2t_target():
    target = get_usefull_col_extraction_target("s2t_transformations")

    assert target == {
        "sheet_group": "s2t",
        "fields": [
            "target_field",
            "source_field",
            "target_table",
            "source_table",
            "transformation_rule",
        ],
    }


@pytest.mark.parametrize("target_name", ["source_tables", "target_tables"])
def test_usefull_col_extraction_config_defines_table_catalog_targets(target_name):
    target = get_usefull_col_extraction_target(target_name)

    assert target == {
        "sheet_group": target_name,
        "fields": ["table_name", "description"],
    }


@pytest.mark.parametrize(
    "target_name, fields",
    [
        ("additional_objects", ["name", "sql"]),
        (
            "pxf_to_a",
            [
                "external_a_table",
                "materialized_storage",
                "replica_table",
                "sod",
            ],
        ),
    ],
)
def test_usefull_col_extraction_config_defines_structured_metadata_targets(
    target_name,
    fields,
):
    assert get_usefull_col_extraction_target(target_name) == {
        "sheet_group": target_name,
        "fields": fields,
    }


def test_usefull_col_extraction_config_validates_fields_against_sheet_group_mapping(
    tmp_path, monkeypatch
):
    config_path = tmp_path / "usefull_col_extraction.json"
    column_mapping_path = tmp_path / "column_mapping.json"
    sheet_groups_path = tmp_path / "sheet_groups.json"

    config_path.write_text(
        json.dumps(
            {
                "s2t_transformations": {
                    "sheet_group": "s2t",
                    "fields": [
                        "target_field",
                        "source_field",
                        "target_table",
                        "source_table",
                        "transformation_rule",
                    ],
                }
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    column_mapping_path.write_text(
        json.dumps(
            {
                "s2t": {
                    "target_table": ["Target Table"],
                    "target_field": ["Target Column"],
                    "source_table": ["Source Table"],
                    "source_field": ["Source Column"],
                    "transformation_rule": ["SQL Transform"],
                }
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    sheet_groups_path.write_text(
        json.dumps({"s2t": ["S2T"]}, ensure_ascii=False),
        encoding="utf-8",
    )

    monkeypatch.setattr(column_mapping_config, "COLUMN_MAPPING_PATH", column_mapping_path)
    monkeypatch.setattr(sheet_groups, "SHEET_GROUPS_PATH", sheet_groups_path)
    column_mapping_config.clear_column_mapping_cache()
    sheet_groups.clear_sheet_groups_cache()
    clear_usefull_col_extraction_cache()

    target = get_usefull_col_extraction_target("s2t_transformations", path=str(config_path))

    assert target == {
        "sheet_group": "s2t",
        "fields": [
            "target_field",
            "source_field",
            "target_table",
            "source_table",
            "transformation_rule",
        ],
    }


def test_usefull_col_extraction_config_rejects_unknown_column_mapping_group(tmp_path, monkeypatch):
    config_path = tmp_path / "usefull_col_extraction.json"
    column_mapping_path = tmp_path / "column_mapping.json"
    sheet_groups_path = tmp_path / "sheet_groups.json"

    config_path.write_text(
        json.dumps(
            {
                "s2t_transformations": {
                    "sheet_group": "missing_group",
                    "fields": ["target_table"],
                }
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    column_mapping_path.write_text(
        json.dumps({"s2t": {"target_table": ["Target Table"]}}, ensure_ascii=False),
        encoding="utf-8",
    )
    sheet_groups_path.write_text(
        json.dumps({"missing_group": ["Missing"]}, ensure_ascii=False),
        encoding="utf-8",
    )

    monkeypatch.setattr(column_mapping_config, "COLUMN_MAPPING_PATH", column_mapping_path)
    monkeypatch.setattr(sheet_groups, "SHEET_GROUPS_PATH", sheet_groups_path)
    column_mapping_config.clear_column_mapping_cache()
    sheet_groups.clear_sheet_groups_cache()
    clear_usefull_col_extraction_cache()

    with pytest.raises(ValueError, match="has no column_mapping group"):
        get_usefull_col_extraction_target("s2t_transformations", path=str(config_path))


def test_usefull_col_extraction_config_rejects_unknown_mapping_field(tmp_path, monkeypatch):
    config_path = tmp_path / "usefull_col_extraction.json"
    column_mapping_path = tmp_path / "column_mapping.json"
    sheet_groups_path = tmp_path / "sheet_groups.json"

    config_path.write_text(
        json.dumps(
            {
                "s2t_transformations": {
                    "sheet_group": "s2t",
                    "fields": ["missing_field"],
                }
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    column_mapping_path.write_text(
        json.dumps({"s2t": {"target_field": ["Target Column"]}}, ensure_ascii=False),
        encoding="utf-8",
    )
    sheet_groups_path.write_text(
        json.dumps({"s2t": ["S2T"]}, ensure_ascii=False),
        encoding="utf-8",
    )

    monkeypatch.setattr(column_mapping_config, "COLUMN_MAPPING_PATH", column_mapping_path)
    monkeypatch.setattr(sheet_groups, "SHEET_GROUPS_PATH", sheet_groups_path)
    column_mapping_config.clear_column_mapping_cache()
    sheet_groups.clear_sheet_groups_cache()
    clear_usefull_col_extraction_cache()

    with pytest.raises(ValueError, match="unknown mapping fields"):
        get_usefull_col_extraction_target("s2t_transformations", path=str(config_path))


def test_usefull_col_extraction_config_rejects_old_fields_object(tmp_path):
    config_path = tmp_path / "usefull_col_extraction.json"
    config_path.write_text(
        json.dumps(
            {
                "s2t_transformations": {
                    "sheet_group": "s2t",
                    "fields": {"target_field": "target_field"},
                }
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="must define fields as a list"):
        get_usefull_col_extraction_target("s2t_transformations", path=str(config_path))


def test_usefull_col_extraction_config_requires_explicit_sheet_group(tmp_path):
    config_path = tmp_path / "usefull_col_extraction.json"
    config_path.write_text(
        json.dumps(
            {
                "source_tables": {
                    "fields": ["table_name", "description"],
                }
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="must define sheet_group"):
        get_usefull_col_extraction_target("source_tables", path=str(config_path))
