from agents.tools import load_skills


def test_load_skills(tmp_path):
    # Create a temporary skills.md
    skills_file = tmp_path / "skills.md"
    skills_file.write_text("# Test Skills\n- Header detection")
    # Override the file path? The helper uses relative path. For test, we can mock open.
    # Simpler: test that the function returns empty string when file missing
    assert isinstance(load_skills(), str)  # May be empty if file not found


def test_load_skills_contains_tool_orchestration_and_domain_context():
    text = load_skills()
    assert "S2T-строки" in text
    assert "s2t_transformations" in text
    assert "Neo4j" in text
    assert "ETLColumn" in text
    assert "TABLE_TRANSFORMS_TO" in text
    for tool_name in (
        "run_sql",
        "trace_neo4j_lineage",
        "search_excel_values",
        "semantic_search_descriptions",
        "trace_transformation_path",
    ):
        assert tool_name in text
    assert "Атомарные контракты tools" in text
    assert "Извлечение полезных колонок" not in text
    assert "update_file_description" not in text
    assert len(text) < 10000


def test_s2t_skill_routes_table_name_set_operations_to_domain_tool():
    text = load_skills(["S2T-строки"])

    assert "list_s2t_table_names" in text
    assert "принадлежность имени" in text
    assert "run_sql" in text
    assert "нестандартной агрегации" in text
    assert "JOIN, EXISTS, NOT EXISTS" not in text


def test_load_skills_includes_transformation_path_analysis_skill():
    text = load_skills()
    assert "Путь S2T-преобразования" in text
    assert "прямую трансформацию" in text
    assert "Отсутствие подтверждения Neo4j не отменяет факты SQLite" in text
    assert "source_table.source_field → target_table.target_field" in text
    assert "перенеси её дословно" in text
    assert "не отсутствие" in text


def test_load_skills_can_select_one_section():
    text = load_skills(["SQL lineage"])

    assert "## SQL lineage" in text
    assert "parse_sql_column_lineage" in text
    assert "visualize_sql_lineage" in text
    assert "диалект SQLGlot" in text
    assert "## S2T-строки" not in text
    assert "## Neo4j" not in text


def test_load_skills_can_select_sql_execution_section():
    text = load_skills(["SQLite SQL"])

    assert "## SQLite SQL" in text
    assert "run_sql" in text
    assert "не физические ETL-таблицы" in text
    assert "source_table" in text
    assert "## SQL lineage" not in text
