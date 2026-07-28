# AGENTS.md

## О проекте

ETL S2T Parser - приложение для разбора Excel-файлов с S2T-маппингами: определяет структуру заголовков, сохраняет метаданные и примеры строк в SQLite, строит бизнес-саммари через настроенный LLM provider, сопоставляет листы с целевой S2T-схемой и отвечает на вопросы через инструментального агента.

## Основные команды

- Запуск Flask UI: `uv run python app.py`
- Тесты: `pytest tests/ -q`
- Тесты с покрытием: `pytest tests/ --cov=. --cov-config=.coveragerc`

## Карта архитектуры

- `app.py` - тонкий Flask API: маршруты загрузки, коррекций, preview headers, progress, summary, S2T transformations и chat.
- `processing/excel.py` - механический разбор загруженного Excel: preview, существующее определение заголовков, построение колонок и чтение сырых строк без доменной очистки.
- `agents/agent.py` - определение заголовков Excel и входная точка chat agent.
- `agents/chat_graph.py` - общий многошаговый LangGraph с нативным tool calling, `ToolNode`, observer и отдельным responder.
- `agents/summarizer_agent.py` - цепочка бизнес-саммаризации файла из SQLite.
- `sheet_skills/s2t.py` - sheet skill `usefull_col_extraction`: inspect, deterministic/LLM matching, валидация строк и построение записей S2T.
- `sheet_skills/table_catalog.py` - sheet skill каталогов: построчно сохраняет `table_name` и `description` из групп `source_tables`/`target_tables`.
- `storage/s2t.py` - транзакционная запись, очистка, чтение, поиск/агрегация и проверка сохранённых S2T-записей.
- `storage/database.py` - актуальная публичная SQLite-схема: `files`, `file_sheet_headers`, `source_tables`, `target_tables`, `s2t_transformations`, `data`.
- `graph_storage/` - изолированные настройки и lifecycle Neo4j driver.
- `services/graph_sync.py` - пересобирает колонковую Neo4j-проекцию одного файла: только `ETLColumn` и `TRANSFORMS_TO`; остальные факты остаются в SQLite.
- `config/` - загрузчики и JSON-конфигурации sheet groups, column mappings и useful-column extraction.
- `agents/tools/` - тематические модули с декорированными `@tool` и отдельный read-only/write registry.
- `agents/observability.py` - необязательная интеграция Langfuse.
- `agents/prompts/` - runtime skills и контекст chat-agent.
- `templates/index.html` - основной веб-интерфейс.

## Правила для доработок

- Сначала проверяй существующие паттерны проекта; не вводи новый фреймворк без явной пользы.
- Храни факты о файлах, листах, колонках и маппингах в SQLite через существующий слой `storage/database.py`.
- Не придумывай `file_id`, имена листов, колонок, ETL source/target tables или S2T rows: получай их через инструменты или SQL.
- Для логических ETL/S2T-таблиц вида `t_*` не используй `PRAGMA` как для физических SQLite-таблиц; ищи их в `s2t_transformations.target_table` и связанных строках трансформаций.
- Перед добавлением нового инструмента агента используй `@tool(parse_docstring=True)`, русский docstring и типы; затем явно добавляй готовый `BaseTool` в `agents/tools/registry.py` и обновляй тесты.
- Runtime LLM-provider для агентной части и S2T matching настраивается через `LLM_PROVIDER`; по умолчанию используется `gigachat`. Альтернативы: OpenAI-compatible `openrouter` или локальный `ollama`. Не добавляй молчаливые non-LLM fallback-записи: если LLM не дал валидный план, возвращай ошибку.
- Unit-тесты могут мокать транспорт LLM для проверки обвязки, но это не считается проверкой качества модели. Реальные проверки модели выноси в отдельные integration/smoke тесты с явным запуском.
- Сохраняй русские промпты и документацию в UTF-8. Не переписывай русские строки только из-за mojibake в консоли PowerShell.
- Summarizer должен делать один LLM-вызов по названиям колонок и максимум пяти первым строкам каждого непропущенного листа; не добавляй доменные эвристики и промежуточные подсказки.

## Правила для агентной части

- Единый диалог должен быть read-only по умолчанию: отвечать про файлы, листы, заголовки, summary и S2T transformations.
- Мутации вроде загрузки файла, повторного S2T refresh или очистки `s2t_transformations` должны идти только через явное действие пользователя или подтверждение.
- Перед `usefull_col_extraction` запускай sheet-group subagent: exact/fuzzy по `config/sheet_groups.json`, затем LLM только для несматченных листов, затем запись новых алиасов в текущий `config/sheet_groups.json`. Шаг извлечения полезных колонок не должен сам решать группу листа по имени.
- Запись `s2t_transformations` выполняется через target `s2t_transformations` в `usefull_col_extraction.json`: subagent выбирает колонки в два шага - сначала exact/fuzzy по `column_mapping.json`, затем настроенный OpenAI-compatible LLM только для листов, где сматчились не все настроенные роли.
- `usefull_col_extraction` должен отправлять в настроенный LLM максимум один запрос на один неполностью сматченный лист и просить компактный ответ `column_roles`: каждой колонке по плоскому имени `column_name` сопоставляется ключ `mapping_field` из настроенной группы `column_mapping_json` или `null`.
- Не отправляй LLM внутренние ID (`file_id`, `sheet_id`, `column_id`), `column_index`, полный `header_path`, valid/critical/nullable роли, `role_to_column_mapping_field` и эвристические подсказки для matching; в prompt достаточно `sheet_name`, настроенного `column_mapping_json`, плоского `column_name` и samples.
- Exact/fuzzy matching можно использовать внутри Python для evidence и первичного сопоставления. В prompt это не отправлять; если deterministic pass нашел все v1-роли, LLM для этого листа не вызывается.
- Если `usefull_col_extraction` подтвердил header с ролью не exact-ом, добавляй фактическое название header в `column_mapping.json` для соответствующего поля, без дублей.
- Для вопросов по маппингам и трансформациям используй `search_s2t_transformations`, `list_s2t_transformations` или read-only SQL по `s2t_transformations`; Neo4j используй только для lineage колонок.
- Не дедуплицируй строки `source_tables`, `target_tables` и `s2t_transformations`: одинаковые строки исходного Excel являются отдельными фактами.
- Если в строке S2T отсутствует `target_table`, возвращай явную ошибку с листом и номером строки до начала транзакции; существующие `s2t_transformations` не изменяй.
- Если активен текущий файл в UI, передавай его `file_id` только файловым tools. Просмотр, поиск и суммаризация глобальной `s2t_transformations` никогда не ограничиваются `file_id`.

## Проверка изменений

- Для изменений Flask routes и UI-чата обновляй `tests/test_app.py`.
- Для tools и агентной логики обновляй `tests/test_agent_tools.py`.
- Для хранения S2T-строк обновляй `tests/test_database.py` и `tests/test_s2t_transformations.py`.
- После изменений запускай минимум релевантные тесты, а перед крупным merge - `pytest tests/ -q`.

## Актуальные уточнения агентной части

- Когда пользователь спрашивает про "таблицы", по умолчанию имеются в виду таблицы ETL/S2T-слоев и сохраненные результаты анализа этих слоев, прежде всего `s2t_transformations`, а не физические служебные таблицы SQLite.
- `data` входит в публичную схему и доступна для read-only анализа сохранённых значений строк Excel.
- Для обычных вопросов про DDL/схему показывай публичную ETL-схему: `files`, `file_sheet_headers`, `source_tables`, `target_tables`, `s2t_transformations` и `data`.

## Актуальная SQLite-схема

- Для текущей задачи показывай пользовательски значимые таблицы: `files`, `file_sheet_headers`, `source_tables`, `target_tables`, `s2t_transformations`, `data`.
- `source_tables` и `target_tables` хранят `id`, `file_id`, `sheet_id`, `sheet_name`, `row_num`, `table_name`, `description`; одинаковые исходные строки сохраняются отдельно.
- `data` — публичная таблица для анализа и добора значений: `id`, `sheet_id`, `table_name`, `row_num`, `column_id`, `value`.
- `table_name` в `data` хранит имя листа/таблицы Excel, чтобы анализировать значения без лишнего join.
- Для обычных вопросов пользователя про таблицы, DDL или схему показывай `files`, `file_sheet_headers`, `source_tables`, `target_tables`, `s2t_transformations` и `data`.
