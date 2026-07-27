# Описание проекта для передачи GPT

Этот файл дает компактный актуальный контекст по проекту ETL S2T Agent. Основной рабочий документ для Codex остается `AGENTS.md`, но этот файл можно передавать другой GPT-модели как краткое описание архитектуры и ближайших решений.

## Назначение

ETL S2T Agent - Flask-приложение для разбора Excel-книг с Source-to-Target маппингами. Приложение:

- загружает Excel-файл и определяет структуру заголовков листов, включая многоуровневые headers;
- сохраняет файл, листы, колонки и значения ячеек в SQLite;
- извлекает минимальную S2T-таблицу трансформаций через внутренний subagent;
- показывает трансформации в UI и отвечает на вопросы через чат-агента;
- при наличии LLM-ключей может генерировать бизнес-саммари по загруженному файлу.

Основной UI сейчас: `/chat_app`, однопользовательский чат без серверных пользовательских сессий. Классический `/` оставлен как вспомогательный экран загрузки и просмотра.

## Текущий Пайплайн

1. `POST /upload` получает Excel.
2. `parse_excel_with_decisions` определяет skipped sheets, строки заголовков и nested headers.
3. `store_excel_data` пишет структуру и данные в SQLite.
4. `classify_file_sheet_groups(file_id)` сопоставляет листы с группами из `config/sheet_groups.json`.
5. `run_s2t_extraction_subagent(file_id)` запускает `usefull_col_extraction` и пишет `source_tables`, `target_tables` и `s2t_transformations`.
6. `summarize_file(file_id)` передаёт LLM названия колонок и первые 5 строк каждого непропущенного листа одним запросом.
7. UI/чат читают данные через current tools и read-only SQL.

## Актуальная SQLite-Схема

Для текущей задачи реально нужны пользовательски значимые таблицы:

- `files` - загруженные файлы, `file_id`, filename, summary, result_json.
- `file_sheet_headers` - что найдено по листам и headers: одна строка на лист, `headers_json`, `headers_flat`.
- `source_tables` - построчный каталог таблиц-источников: `table_name`, `description`.
- `target_tables` - построчный каталог таблиц-приёмников: `table_name`, `description`.
- `s2t_transformations` - итоговая полезная таблица с извлеченными S2T-строками.
- `data` - публичные значения строк Excel по `sheet_id`, `table_name`, `row_num`, `column_id` и `value`.

`data` хранит `table_name` — имя листа/таблицы Excel, поэтому пользовательский агент может анализировать значения через read-only SQL без дополнительной таблицы листов.

Таблицы с деталями разбора Excel:

- `file_sheet_headers` - листы файла, skipped/header metadata и multi-level headers.
- `data` - значения ячеек по `sheet_id`, `table_name`, `row_num`, `column_id`.

Пользовательски и для обычных вопросов про DDL показываем только:

- `files`
- `file_sheet_headers`
- `source_tables`
- `target_tables`
- `s2t_transformations`
- `data`

Для просмотра листов и заголовков используй `file_sheet_headers`, а для анализа сохранённых значений строк — публичную таблицу `data`.

Удаленные legacy-таблицы:

- `relationships`
- `embeddings`
- `column_mappings`
- `additions`

`source_tables`, `target_tables` и `s2t_transformations` сохраняют каждую исходную строку отдельно. Одинаковые бизнес-значения не схлопываются.

## S2T Transformations v1

Таблица `s2t_transformations` хранит только минимальные поля:

- `id`
- `file_id`
- `sheet_id`
- `sheet_name`
- `row_num`
- `target_table`
- `target_field`
- `source_table`
- `source_field`
- `transformation_rule`
- `raw_json`

В отображении пользователю показываются:

- `row`
- `target_table`
- `target_field`
- `source_table`
- `source_field`
- `transformation`

Не показываем и не заполняем в v1: `table_sql`, типы, описания таблиц, флаги, старые catalog-поля.

## S2T Subagent

Sheet skill живет в `sheet_skills/s2t.py` и работает по шагам:

1. load target config из `usefull_col_extraction.json`;
2. plan column mapping по `column_mapping.json`;
3. self-check выбранных колонок;
4. write через allowlisted tool;
5. verify записанные строки.

Subagent не получает общий `run_sql`. Его tools:

- `write_s2t_transformations_from_plan(file_id, sheet_mappings)` - mutating;
- `verify_s2t_transformations(file_id)` - read-only.

Write-tool принимает не готовые строки от LLM, а mapping колонок по `column_id`. Строки строятся самим tool из SQLite `data`.

## Chat Agent И Tools

Текущий чатовый путь: Flask `/chat` -> `agent_chat` -> LangGraph (`planner -> prepare_tool -> tools -> observer -> planner/responder`) -> ответ.

Planner возвращает только строгий JSON одного из двух видов: `tool` или `final`.
Инструменты представлены стандартными LangChain `BaseTool`: Pydantic-схема аргументов
строится из аннотаций Python-функции. Приложение валидирует имя и `args_schema`,
выполняет `tool.invoke()` и передает в следующий шаг только фактический результат.
Текстовый ReAct, keyword-routing и provider-specific fallback-парсеры не используются.

История диалога хранится только в `sessionStorage` текущей вкладки. `/chat` принимает
ограниченный контекст из сообщений с ролями `user`/`assistant`; серверная сессия и
запись истории в SQLite не используются.

Подключенные для чата tools должны оставаться минимальными:

- `run_sql` - read-only SQL;
- `list_files`;
- `resolve_file`;
- `list_sheets`;
- `list_columns`;
- `list_file_sheet_headers`;
- `list_s2t_transformations`;
- `search_s2t_transformations`;

Runtime skill `s2t_table_summary_skill` позволяет агенту суммаризировать логические
таблицы-приёмники, таблицы-источники и связи между ними по фактам из
`s2t_transformations`, используя самостоятельно выбранный read-only SQL.
- `list_sheet_group_classifications`.

Старые tools не подключать:

- `mapping_overview`;
- `search_column_mappings`;
- `list_target_table_columns`;
- lineage helpers;
- semantic similarity helpers.

Важно: если пользователь спрашивает про "таблицы", чаще всего он имеет в виду ETL source/target таблицы внутри `s2t_transformations`, а не физические SQLite-таблицы.

## Главные Файлы

- `app.py` - Flask routes, upload, corrections, progress, summary, S2T transformations API, chat.
- `storage/database.py` - SQLite schema, migrations, `store_excel_data`.
- `storage/s2t.py` - current S2T storage helpers.
- `processing/excel.py` - механический разбор Excel.
- `sheet_skills/s2t.py` - внутренний sheet skill извлечения S2T.
- `sheet_skills/table_catalog.py` - sheet skill каталогов source/target.
- `config/` - JSON-конфигурации и их загрузчики.
- `agents/agent.py` - header detection и входная точка chat agent.
- `agents/chat_graph.py` - многошаговый StateGraph для выбора, валидации и выполнения tools.
- `agents/tools/` - тематические `@tool`-модули и явный LangChain `BaseTool` registry.
- `agents/prompts/skills.md` - prompt-skills приложения.
- Описания и схемы tools формируются декоратором `@tool(parse_docstring=True)` из русских docstring и типизированных сигнатур.
- `config/sheet_groups.json` - aliases групп листов.
- `column_mapping.json` - aliases колонок для S2T extraction.
- `templates/chat_app.html` - chat-first UI.
- `templates/index.html` - классический UI.

## Ближайший План

1. Довести S2T extraction до устойчивых multi-level headers на реальных файлах.
2. Добавить понятный progress/report по шагам subagent в UI.
3. Стабилизировать summaries: таймауты и явное отображение ошибок провайдера.
4. Добавить tests на текущую минимальную схему и S2T extraction.
5. Позже отдельно решить, нужен ли новый слой для `additional_objects`; если нужен, добавлять как новую таблицу и новый skill, а не возвращать старый catalog слой.
