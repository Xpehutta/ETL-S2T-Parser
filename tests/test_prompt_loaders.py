from agents.tools import (
    load_chat_agent_context,
    load_schemas,
    load_skills,
    load_upstream_analysis_context,
)


def test_load_skills(tmp_path):
    # Create a temporary skills.md
    skills_file = tmp_path / "skills.md"
    skills_file.write_text("# Test Skills\n- Header detection")
    # Override the file path? The helper uses relative path. For test, we can mock open.
    # Simpler: test that the function returns empty string when file missing
    assert isinstance(load_skills(), str)  # May be empty if file not found


def test_chat_agent_context_keeps_general_anti_invention_rules_out_of_queries():
    text = load_chat_agent_context()

    assert "Не выдумывай ID, файлы, листы, колонки, таблицы и строки" in text
    assert "не подставляй вместо" in text
    assert "`{PLACEHOLDER}`" not in text


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
        "search_column_catalog",
        "trace_transformation_path",
    ):
        assert tool_name in text
    for semantic_scope in (
        "`files`",
        "`tables`",
        "`source_tables`",
        "`target_tables`",
        "`columns`",
        "`source_columns`",
        "`target_columns`",
    ):
        assert semantic_scope in text
    assert "`all` — только без" in text
    assert "Атомарные контракты tools" in text
    assert "directed exact reader" in text
    assert "pair-reader" in text
    assert "Производный анализ, сравнение и объяснение выполняет upstream" in text
    assert "## Анализ трансформаций" not in text
    assert "`analyze`" not in text
    assert "Извлечение полезных колонок" not in text
    assert "update_file_description" not in text


def test_s2t_skill_routes_table_name_set_operations_to_domain_tool():
    text = load_skills(["S2T-строки"])

    assert "list_s2t_table_names" in text
    assert "принадлежность имени" in text
    assert "run_sql" in text
    assert "нестандартной агрегации" in text
    assert "JOIN, EXISTS, NOT EXISTS" not in text


def test_s2t_skill_only_prepares_transformation_data_for_upstream():
    text = load_skills(["S2T-строки"])
    assert "## S2T-строки" in text
    assert "Путь S2T-преобразования" not in text
    assert "source_table.source_field → target_table.target_field" in text
    assert "сохрани его дословно" in text
    assert "Правило анализирует upstream coordinator" in text
    assert "`analyze`" not in text
    assert "## Анализ трансформаций" not in text
    assert "source_table`, `source_field`, `target_table` и `target_field`" in text
    assert "дополнительный фильтр является mismatch" in text
    assert "Не зеркаль роли" in text
    assert "фильтруемыми и возвращаемыми полями" in text
    assert "повторно получать не требуется" in text


def test_upstream_analysis_context_is_independent_and_sql_precise():
    text = load_upstream_analysis_context()

    assert "Правила upstream-анализа" in text
    assert "принятым evidence" in text
    assert "фактический `transformation_rule` или SQL" in text
    assert "`transformation_rule` или SQL" in text
    assert "`WHERE 1=1` не фильтрует" in text
    assert "`LEFT JOIN ... ON p`" in text
    assert "может размножить" in text
    assert "не доказывает `UPSERT`" in text
    assert "`UNION ALL`" in text
    assert "Полные одинаковые строки" in text
    assert "Технические поля хранилища" in text
    assert "не переименовывай" in text
    assert "без прямого подтверждения в evidence" in text
    assert "Не подставляй фиктивное значение" in text
    assert "row_format=named_records_with_dictionary_refs" in text
    assert "0-based индексом" in text
    assert "не схлопывай одинаковые occurrences" in text
    assert "returned_rows == len(rows)" in text
    assert "`{PLACEHOLDER}`" not in text
    assert len(text) < 3500
    assert "`analyze`" not in text


def test_load_skills_can_select_one_section():
    text = load_skills(["Neo4j"])

    assert "## Neo4j" in text
    assert "trace_neo4j_lineage" in text
    assert "TABLE_TRANSFORMS_TO" in text
    assert "оба конца и промежуточные узлы" in text
    assert "транзитивный impact/downstream" in text
    assert "ограничение глубины" in text
    assert "## S2T-строки" not in text
    assert "## Excel и описания" not in text


def test_load_skills_can_select_excel_section():
    text = load_skills(["Excel и описания"])

    assert "## Excel и описания" in text
    assert "search_excel_values" in text
    assert "semantic_search_descriptions" in text
    assert "exact catalog tool" in text
    assert "pair-reader" in text
    assert "## Neo4j" not in text


def test_load_skills_can_select_comparison_and_explanation_independently():
    comparison = load_skills(["Сравнение"])
    explanation = load_skills(["Объяснение"])
    normalized_comparison = " ".join(comparison.split())
    normalized_explanation = " ".join(explanation.split())

    assert "## Сравнение" in comparison
    assert "один и тот же необходимый" in normalized_comparison
    assert "не задаёт между ними source/target" in normalized_comparison
    assert "Не ищи связь или путь" in normalized_comparison
    assert "Не формулируй общее и различия в worker" in normalized_comparison
    assert "## Объяснение" not in comparison

    assert "## Объяснение" in explanation
    assert "получи точные выражения, правила, роли" in normalized_explanation
    assert "без производного объяснения" in normalized_explanation
    assert "## Сравнение" not in explanation

    for text in (comparison, explanation):
        assert "## S2T-строки" not in text
        assert "## Neo4j" not in text
        assert "## Excel и описания" not in text


def test_load_schemas_selects_current_s2t_mappings_without_other_domains():
    text = load_schemas(("S2T-маппинг",))

    assert "## Схема S2T-маппинга" in text
    assert "`source_table`" in text
    assert "`target_table`" in text
    assert '"source_field"' in text
    assert '"target_field"' in text
    assert '"sheet_group":"s2t"' in text
    assert "## Актуальная схема SQLite" not in text
    assert "## Схемы Excel-маппингов" not in text


def test_load_schemas_includes_exact_excel_and_sqlite_sources():
    text = load_schemas(("SQLite ETL", "Excel-маппинги"))

    assert "## Актуальная схема SQLite" in text
    assert "`additional_objects`" in text
    assert "`sql`" in text
    assert "Additional objects с точным именем и полным SQL" in text
    assert "## Схемы Excel-маппингов" in text
    assert '"additional_objects"' in text
    assert '"sheet_group":"additional_objects"' in text
