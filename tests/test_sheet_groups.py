from config.sheet_groups import (
    find_sheet_group_alias,
    group_for_sheet,
    load_sheet_groups,
    normalize_sheet_name,
    sheet_name_in_group,
)


def test_normalize_sheet_name_handles_case_spaces_and_underscores():
    assert normalize_sheet_name(" Source_Tables ") == normalize_sheet_name("source tables")
    assert normalize_sheet_name("json кап802 -> hadoop кап818") == normalize_sheet_name(
        "json кап802->hadoop кап818"
    )


def test_sheet_group_aliases_include_source_tables_and_s2t():
    assert sheet_name_in_group("Source tables", "source_tables")
    assert sheet_name_in_group("source_tables", "source_tables")
    assert sheet_name_in_group("SourceToTarget", "s2t")
    assert sheet_name_in_group("S2T", "s2t")
    assert sheet_name_in_group("s2t", "s2t")


def test_group_for_sheet_resolves_common_aliases():
    assert group_for_sheet("Additional_Objects") == "additional_objects"
    assert group_for_sheet("PXF -> A") == "pxf_to_a"
    assert group_for_sheet("История изменений документа") == "change_log"
    assert group_for_sheet("Target Colums") == "target_columns"


def test_find_sheet_group_alias_returns_matched_alias():
    match = find_sheet_group_alias("source_tables")

    assert match is not None
    assert match["group"] == "source_tables"
    assert match["alias"] == "source_tables"


def test_unconfigured_layer_transition_names_are_not_sheet_groups():
    groups = load_sheet_groups()

    assert "pxf_to_a" in groups
    for group in ("a_to_b", "b_to_s", "a_to_b_to_s"):
        assert group not in groups
    for sheet_name in ("a2b_columns", "b2s", "A=B-S"):
        assert group_for_sheet(sheet_name) is None

def test_sheet_group_aliases_do_not_collide():
    groups = load_sheet_groups()
    seen = {}
    collisions = []
    for group, aliases in groups.items():
        for alias in aliases + [group]:
            norm = normalize_sheet_name(alias)
            previous = seen.setdefault(norm, group)
            if previous != group:
                collisions.append((norm, previous, group))

    assert collisions == []
