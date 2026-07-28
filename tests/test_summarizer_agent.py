import pytest
from unittest.mock import patch
from agents.summarizer_agent import (
    SYSTEM_PROMPT,
    SUMMARY_OUTPUT_REQUIREMENTS,
    build_summary_payload,
    ensure_file_description,
    fetch_file_data,
    generate_description_from_summary,
    generate_description_update_from_user_query,
    generate_summary,
    summarize_file,
    update_file_description_from_user_query,
    _evenly_spaced_items,
)


@patch("agents.summarizer_agent.summarizer_chain")
def test_generate_summary(mock_chain):
    mock_chain.invoke.return_value = {"final_summary": "Final summary."}

    summary = generate_summary(1)
    assert summary == "Final summary."
    mock_chain.invoke.assert_called_once_with(1)


def test_build_summary_payload_uses_semantic_catalog():
    snapshot = {
        "filename": "summary.xlsx",
        "semantic_catalog": {
            "subject_areas": ["Нерисковые продукты ЮЛ"],
            "views": [{"name": "v_dcc_agrmnt", "description": "Договора"}],
            "tables": [
                {
                    "subject_area": "Нерисковые продукты ЮЛ",
                    "name": "t_dcc_agrmnt_300",
                    "description": "Договора НЦБ",
                }
            ],
            "attributes": [
                {
                    "entity": "Договор",
                    "attribute": "Идентификатор договора",
                    "description": "Id договора в АХД",
                }
            ],
            "fields": [
                {
                    "table": "t_dcc_agrmnt_300",
                    "field": "agrmnt_id",
                    "description": "Идентификатор договора",
                }
            ],
            "metrics": [
                {
                    "code": "metric_11",
                    "description": "Средние входящие остатки",
                }
            ],
            "catalog_tables": [],
        },
    }

    payload = build_summary_payload(snapshot)
    assert payload["focus"] == "table_and_attribute_descriptions"
    assert payload["subject_areas"] == ["Нерисковые продукты ЮЛ"]
    assert payload["views"][0]["description"] == "Договора"
    assert payload["tables"][0]["description"] == "Договора НЦБ"
    assert payload["attributes"][0]["entity"] == "Договор"
    assert "skipped_sheets" not in payload
    assert "sheets" not in payload
    assert "описаний таблиц" in SYSTEM_PROMPT
    assert "S2T-строки" in SUMMARY_OUTPUT_REQUIREMENTS


def test_fetch_file_data_extracts_descriptions_from_sheets(temp_db, sample_excel_bytes):
    from storage.database import store_excel_data

    file_id = store_excel_data(
        sample_excel_bytes,
        "summary.xlsx",
        "model",
        [
            {
                "sheet_name": "Представления",
                "skip_reason": None,
                "header": {"start_row": 0, "row_count": 1, "nested": False},
                "columns": ["Таблица", "Описание таблицы"],
                "data_rows": [
                    ["v_dcc_agrmnt", "Договора"],
                    ["v_dcc_client", "Клиенты"],
                ],
            },
            {
                "sheet_name": "S2T таблицы",
                "skip_reason": None,
                "header": {"start_row": 0, "row_count": 1, "nested": False},
                "columns": [
                    "Предметная область",
                    "Описание целевой таблицы",
                    "Таблица-приемник",
                    "Поле приемника",
                    "Описание поля приемника",
                ],
                "data_rows": [
                    [
                        "Нерисковые продукты ЮЛ",
                        "Договора НЦБ",
                        "t_dcc_agrmnt_300",
                        "agrmnt_id",
                        "Идентификатор договора",
                    ],
                    [
                        "Нерисковые продукты ЮЛ",
                        "Договора НЦБ",
                        "t_dcc_agrmnt_300",
                        "host_client_id",
                        "Идентификатор клиента",
                    ],
                ],
            },
            {
                "sheet_name": "Атрибуты",
                "skip_reason": "Manually skipped by user",
                "header": {"start_row": 0, "row_count": 1, "nested": False},
                "columns": ["Сущность", "Атрибут", "Описание атрибута"],
                "data_rows": [
                    ["Договор", "Номер договора", "Номер договора в АХД"],
                ],
            },
        ],
    )

    snapshot = fetch_file_data(file_id)
    catalog = snapshot["semantic_catalog"]

    assert snapshot["filename"] == "summary.xlsx"
    assert catalog["subject_areas"] == ["Нерисковые продукты ЮЛ"]
    assert catalog["views"] == [
        {"name": "v_dcc_agrmnt", "description": "Договора"},
        {"name": "v_dcc_client", "description": "Клиенты"},
    ]
    assert catalog["tables"] == [
        {
            "subject_area": "Нерисковые продукты ЮЛ",
            "name": "t_dcc_agrmnt_300",
            "description": "Договора НЦБ",
        }
    ]
    assert len(catalog["fields"]) == 2
    assert catalog["attributes"] == [
        {
            "entity": "Договор",
            "attribute": "Номер договора",
            "description": "Номер договора в АХД",
        }
    ]


def test_fetch_file_data_includes_skipped_sheet_when_rows_exist(temp_db, sample_excel_bytes):
    from storage.database import store_excel_data

    file_id = store_excel_data(
        sample_excel_bytes,
        "summary.xlsx",
        "model",
        [
            {
                "sheet_name": "Атрибуты",
                "skip_reason": "Manually skipped by user",
                "header": {"start_row": 0, "row_count": 1, "nested": False},
                "columns": ["Сущность", "Атрибут", "Описание атрибута"],
                "data_rows": [
                    ["Договор", "Клиент", "Идентификатор клиента"],
                ],
            }
        ],
    )

    catalog = fetch_file_data(file_id)["semantic_catalog"]
    assert catalog["attributes"][0]["description"] == "Идентификатор клиента"


def test_evenly_spaced_items_picks_edges_and_middle():
    assert _evenly_spaced_items([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 5) == [1, 3, 5, 8, 10]


@patch("agents.summarizer_agent.call_gigachat")
def test_description_prompts_focus_on_domain_descriptions(mock_call_gigachat):
    mock_call_gigachat.return_value = "Описание"

    generate_description_from_summary("Саммари")
    generate_description_update_from_user_query("Описание", "Саммари", "Уточнение")

    prompts = [call.args[0] for call in mock_call_gigachat.call_args_list]
    assert "описаний таблиц и атрибутов" in prompts[0]
    assert "S2T-артефакты" in prompts[0]
    assert "S2T-артефакты" in prompts[1]
    assert all("вЂ" not in prompt and "РЎС" not in prompt for prompt in prompts)


@patch("agents.summarizer_agent.generate_summary")
@patch("agents.summarizer_agent.update_file_summary")
def test_summarize_file(mock_update, mock_generate):
    mock_generate.return_value = "Generated summary"
    result = summarize_file(1, save=True)
    assert result == "Generated summary"
    mock_update.assert_called_once_with(1, "Generated summary")


@patch("agents.summarizer_agent._file_text_fields")
def test_ensure_file_description_uses_cached_value(mock_file_fields):
    mock_file_fields.return_value = {
        "file_id": 1,
        "filename": "test.xlsx",
        "summary": "Long summary",
        "description": "Cached description",
    }

    result = ensure_file_description(1)

    assert result == "Cached description"


@patch("agents.summarizer_agent.update_file_description")
@patch("agents.summarizer_agent.generate_description_from_summary")
@patch("agents.summarizer_agent._file_text_fields")
def test_ensure_file_description_generates_and_saves(
    mock_file_fields,
    mock_generate_description,
    mock_update_description,
):
    mock_file_fields.return_value = {
        "file_id": 1,
        "filename": "test.xlsx",
        "summary": "Long summary",
        "description": None,
    }
    mock_generate_description.return_value = "Short description"

    result = ensure_file_description(1)

    assert result == "Short description"
    mock_generate_description.assert_called_once_with("Long summary")
    mock_update_description.assert_called_once_with(1, "Short description")


@patch("agents.summarizer_agent.update_file_description")
@patch("agents.summarizer_agent.generate_description_update_from_user_query")
@patch("agents.summarizer_agent._file_text_fields")
@patch("agents.summarizer_agent.ensure_file_description")
def test_update_file_description_from_user_query(
    mock_ensure_description,
    mock_file_fields,
    mock_generate_updated_description,
    mock_update_description,
):
    mock_ensure_description.return_value = "Current description"
    mock_file_fields.return_value = {
        "file_id": 1,
        "filename": "test.xlsx",
        "summary": "Long summary",
        "description": "Current description",
    }
    mock_generate_updated_description.return_value = "Updated description"

    result = update_file_description_from_user_query(
        1,
        "Добавь акцент на кредитные договоры",
    )

    assert result == "Updated description"
    mock_ensure_description.assert_called_once_with(1, refresh=False, save=True)
    mock_generate_updated_description.assert_called_once_with(
        current_description="Current description",
        summary="Long summary",
        user_query="Добавь акцент на кредитные договоры",
    )
    mock_update_description.assert_called_once_with(1, "Updated description")
