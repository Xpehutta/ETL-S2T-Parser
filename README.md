# ETL S2T Agent

[![Python 3.12+](https://img.shields.io/badge/Python-3.12%2B-blue.svg)](https://www.python.org/)
[![Flask 3](https://img.shields.io/badge/Flask-3.x-green.svg)](https://flask.palletsprojects.com/)
[![LangGraph](https://img.shields.io/badge/agents-LangGraph-orange.svg)](https://www.langchain.com/langgraph)

ETL S2T Agent — chat-first приложение для загрузки и анализа Excel-файлов с Source-to-Target-маппингами. Оно сохраняет исходные факты в PostgreSQL (с SQLite fallback для локальной разработки), извлекает S2T- и SQL-lineage, при наличии Neo4j строит графовую проекцию и отвечает на вопросы через многоагентный LangGraph.

> **Актуальное состояние функций:** [отчёт от 15 сентября 2026 года](LIVE_AGENT_ADDED_FEATURES_AND_WORD_9_REPORT_2026-09-15.md). Он описывает добавленные возможности и статус девяти основных Word-требований.

> **Актуальный benchmark локальных моделей:** [полный CORE29-отчёт от 18 сентября 2026 года](CORE29_TOP2_FULL_REPORT.md). Он сравнивает лучшие локальные модели на основном Word‑9 и расширенном наборе из 22 сценариев.

Исторические демонстрации и отчёты предыдущих прогонов собраны в
[`docs/history/`](docs/history/README.md) и не описывают текущее поведение.

## Возможности

- загрузка `.xlsx`, `.xls` и `.xlsm` из единого интерфейса чата;
- автоматический выбор строки заголовка CatBoost-моделью;
- сохранение заголовков и значений Excel без обрезки в основном SQL-хранилище;
- классификация листов и настраиваемое сопоставление колонок;
- извлечение S2T, каталогов таблиц, PXF-маппингов и дополнительных объектов;
- разбор SQL дополнительных объектов через SQLGlot;
- read-only вопросы к PostgreSQL/SQLite и Neo4j на естественном языке;
- полные табличные результаты в отдельном scrollable-блоке, а не в тексте чата;
- сравнение многоагентного режима с базовым одноагентным режимом;
- метрики времени, LLM-вызовов, инструментов и токенов для live-сценариев.

## Архитектура

Настроенное SQL-хранилище является источником исходных фактов: PostgreSQL в
рабочем окружении либо SQLite fallback. Neo4j хранит только производную
проекцию lineage и может быть отключён.

### Загрузка Excel

```mermaid
flowchart LR
    UI["Chat-first UI"] --> API["POST /upload"]
    API --> PARSE["Механический разбор Excel"]
    PARSE --> HEADER["CatBoost: строка заголовка"]
    HEADER --> STORE[("PostgreSQL / SQLite fallback")]
    STORE --> GROUPS["Классификация групп листов"]
    GROUPS --> SKILLS["Sheet skills"]
    SKILLS --> STORE
    STORE --> SUMMARY["Summary и description"]
    STORE --> GRAPH["Neo4j projection (опционально)"]
```

`processing/excel.py` читает каждый лист один раз, сохраняет исходные номера строк, разворачивает объединённые ячейки данных и по умолчанию исключает скрытые строки. Включить их можно при загрузке в интерфейсе. Ответ загрузки содержит `data_row_count` для каждого листа и `total_data_row_count` для всей книги; это число разобранных строк данных после фильтрации скрытых строк, независимо от числа заполненных ячеек, а одинаковые строки считаются отдельно.

Строка заголовка выбирается среди первых десяти строк моделью из `models/catboost_header_model.cbm`. Кандидаты с тремя и более пустыми/`Untitled` значениями исключаются, если остаются менее разреженные строки. При ошибке CatBoost используется настроенный LLM-provider.

После механического разбора:

1. лист сопоставляется с группой из `config/sheet_groups.json`;
2. колонки сначала сопоставляются детерминированно по `config/column_mapping.json`;
3. LLM вызывается только для листа с неполным сопоставлением;
4. результат валидируется и транзакционно записывается в целевую таблицу.

Одинаковые строки Excel не дедуплицируются. Если в непустой S2T-строке отсутствует `target_table`, запись завершается явной ошибкой до начала транзакции. Строки без единого S2T-значения не считаются бизнес-строками.

### Многоагентный чат

По умолчанию `CHAT_AGENT_MODE=multiagent`.

```mermaid
flowchart TD
    Q["Запрос + история"] --> S["Supervisor"]
    S -->|данные не нужны| A["Прямой ответ"]
    S -->|нужны данные| OR["Operation router"]
    OR -->|обычный запрос| C["Downstream plan: 1–8 read tasks"]
    C --> W["Последовательные workers"]
    W --> R["Router: tools + retrieval skills + schemas"]
    R --> P["Planner → read-only tool"]
    P --> O["Observer каждого tool result"]
    O -->|continue| P
    O -->|reroute| R
    O -->|complete| F["WorkerOutcome + accepted evidence"]
    F -->|следующая task| W
    F --> U["Upstream data decision"]
    U -->|reroute, максимум один раз| C
    U -->|pass| UA["Upstream answer + display selection"]
    OR -->|тест-протокол| VRAW["LLM: RawTestProtocolContract"]
    VRAW --> VREV["LLM: completeness + role review"]
    VREV --> VRES["Shared entity resolution"]
    VRES --> VC["Resolved contract → dependency readers → Phase 0–3"]
    OR -->|SQL-risk scope, opt-in| SX["LLM: mode + exact directed scope"]
    SX --> SR["Exact readers → run-scoped evidence"]
    SR --> SS["SQLGlot: neutral structure"]
    SS --> SA["LLM: risk conclusion + answer + display"]
    VC --> UI
    SA --> UI
    UA --> UI
```

Основные контракты:

- supervisor отдельно формирует исполнимую `task` и устойчивый `context`;
- после supervisor operation router один раз выбирает общий agentic-поток либо
  специализированный pipeline. Для `sql_risk_scope` он выбирает только pipeline:
  mode, exact source/target и optional `file_id` извлекаются отдельным native
  LLM-вызовом уже внутри ветки;
- неявного «активного файла» нет. В общем agentic-потоке downstream при
  необходимости создаёт отдельную задачу разрешения имени файла, а worker
  выбирает public `resolve_file` и передаёт принятый `file_id` следующему worker через
  lazy result reference. Coordinator не извлекает идентификаторы из текста и не
  переписывает model-owned план;
- в общем потоке downstream сразу создаёт полный план из 1–8 задач чтения;
  workers выполняются последовательно и могут лениво прочитать принятые
  результаты предыдущих workers по коротким `result_id`;
- worker получает текущую задачу и короткие ссылки на все принятые результаты
  предыдущих шагов текущего цикла; router выбирает tools, retrieval-skills и
  schemas только для операции текущей task. Planner дополнительно получает
  исходную coordinator-task как immutable справочник точных литералов, но не
  может менять по ней операцию или dataset текущего шага;
- planner видит текущую задачу, immutable справочник, последний обмен с
  инструментом и накопительную observer-выжимку; observer оценивает полноту
  только текущей task;
- observer вызывается после каждого data-tool result и возвращает только
  `complete`, `continue` или `reroute`; невалидная структура повторно
  запрашивается на том же payload без повторного data-tool;
- полные результаты инструментов не копируются в историю worker: там остаётся ограниченный preview;
- upstream получает только `original_task` и принятые evidence: `evidence_id`, tool name, args, preview, `truncated` и булевый признак `displayable`; внутренний `display_ref`, worker summary, facts, limitations и runtime refs туда не передаются;
- принятые полные tool results сохраняются под run-scoped `result_id` и читаются через `read_previous_result` только когда краткого description недостаточно; табличные SQLite-результаты дополнительно материализуются во временной relation `result`, доступной через `query_saved_result`;
- если tool вернул только preview, схема сохранённого результата содержит `truncated=true`, поэтому его нельзя использовать как полный исходный набор;
- upstream сначала линейно вызывает `submit_upstream_data_decision`: обязательное поле `decision` равно `pass` или `reroute`, а `problem` служит необязательным пояснением. `reroute` запускает чистый повтор чтения со сбросом результатов прошлого цикла; `pass` переводит управление к отдельному `submit_upstream_answer`. Только этот второй вызов выполняет производный SQL/S2T-анализ по исходной task и evidence, формирует обязательный `answer` и опционально выбирает evidence IDs для UI. Отдельного semantic reviewer/repair нет;
- полные данные разрешаются по ссылкам только на границе HTTP-ответа.

Worker завершается самим planner только через native `finish_worker(summary)`. Обычный финальный текст отклоняется; полноту исходных данных определяет тот же structured observer.

Для повторяемых операций предусмотрены специализированные маршруты:

- `validation_protocol`: LLM извлекает только пользовательский
  `RawTestProtocolContract`, а отдельный native LLM-review сверяет его полноту
  и роли с исходной задачей; общий resolver подтверждает source/target и
  формирует канонический `ResolvedTestProtocolContract`, после чего
  dependency-based readers и deterministic compiler строят Greenplum
  SQL-шаблоны без исполнения SQL во внешней БД. Ошибка extraction, unresolved
  или ambiguous entity возвращается как структурированный validation-status и
  не переключает запрос молча в agentic-поток;
- `sql_risk_scope` (opt-in): внутренний native LLM с учётом устойчивого context
  извлекает один closed mode и точный directed scope только из исходной task,
  exact readers получают данные, SQLGlot строит
  нейтральную структуру, а второй native LLM делает вывод о риске, формирует
  ответ и выбирает display evidence. Эта ветка не запускает обычные
  downstream/workers/upstream и не откатывается молча в agentic-поток.

### Validation protocol

Специализированный pipeline отделяет пользовательские упоминания от
канонических сущностей:

```text
RawTestProtocolContract
→ model-owned completeness/role review
→ shared entity resolution
→ ResolvedTestProtocolContract
→ dependency-based exact readers
→ SQLGlot NormalizedTransformation
→ declarative check registry
→ Phase 0–3 protocol
```

Первый native LLM-вызов явно выбирает `file_scope_kind` (`file_id`,
`file_mention` либо подтверждённое отсутствие scope) и сохраняет отдельные
literal source/target mentions. Код не извлекает эти значения из естественного
языка и не переписывает model-owned контракт. Отдельный native LLM-review
сверяет, что extraction не потерял явно заданный файл, load, check, mode, key
или роль source/target. При замечаниях разрешён один повтор extraction; второй
отказ либо ошибка review возвращает structured failure без agentic fallback.

Exact identifier сначала проверяется без approximate search. Общий resolver
запускается только для неподтверждённого typo, partial или semantic mention;
неоднозначный кандидат не выбирается автоматически. Файл необязателен: S2T-only
checks компилируются без него, а check, которому нужен file-scoped catalog,
получает `partial`/`unavailable`, не обрушая весь протокол.

Режимы протокола:

- `explicit` — только явно запрошенные checks;
- `standard` — `row_count`, `key_uniqueness`, `required_null_rate` и
  `transformation_correctness`;
- `exhaustive` — все 13 checks.

| Фаза | Содержимое |
|---|---|
| Phase 0 — static/preflight | mapping coverage, наличие target fields, mapped fields вне каталога, unmapped required fields, requested sources, разбор transformation SQL, ambiguity и согласованность projection |
| Phase 1 — smoke | `row_count`, `key_uniqueness`, `required_null_rate`, `schema_compatibility`, `expected_required_nulls`, `duplicate_actual` |
| Phase 2 — reconciliation | `transformation_correctness`, `key_reconciliation`, `aggregate_reconciliation` |
| Phase 3 — diagnostics | `missing_rows`, `extra_rows`, `field_mismatch`, `duplicate_expected` |

Явный comparison key имеет приоритет над target PK. Expression projections
(`COALESCE`, `CASE`, `CAST`, арифметика и aliases) нормализуются через SQLGlot.
Шаблоны разделяют `{{SOURCE_SCOPE_PREDICATE}}` и
`{{TARGET_SCOPE_PREDICATE}}`. Output содержит статусы checks
`ready|partial|unavailable`, общий статус
`ready|partial_protocol|unavailable`, issues, preflight и сводку фаз.

Для impact по колонке `trace_neo4j_lineage` возвращает точные
`transformation_id`, а `get_s2t_rules_by_ids` одним параметризованным чтением
получает соответствующие S2T-правила. Planner не генерирует SQL для этого
перехода между Neo4j и основным SQL-хранилищем.

Режим `single_agent` сохранён как базовая линия для live-сравнений:

```ini
CHAT_AGENT_MODE=single_agent
```

## Хранилище

При заданном `DATABASE_URL` основное хранилище работает в PostgreSQL. Без него
сохраняется совместимый локальный fallback `excel_data.db`. В PostgreSQL
назначение каждой таблицы и колонки записывается нативно через
`COMMENT ON TABLE` и `COMMENT ON COLUMN` и доступно в системном каталоге.

Минимальная конфигурация:

```ini
DATABASE_URL=postgresql://etl_user:change_me@localhost:5432/etl_s2t
POSTGRES_SCHEMA=public
POSTGRES_STATEMENT_TIMEOUT_MS=30000
```

Одноразовая проверяемая миграция существующей SQLite-базы:

```bash
uv run python scripts/migrate_sqlite_to_postgres.py --source excel_data.db --dry-run
uv run python scripts/migrate_sqlite_to_postgres.py --source excel_data.db
```

Миграция проверяет число строк и SHA-256 нормализованных данных каждой таблицы
до фиксации транзакции. Непустая целевая схема отклоняется; `--replace` нужно
указывать явно только для заранее проверенной целевой схемы. Временное
run-scoped хранилище результатов агента остаётся SQLite и не входит в миграцию.

| Таблица | Назначение |
|---|---|
| `files` | загрузки, summary и description |
| `file_sheet_headers` | решения по заголовкам и плоские имена колонок |
| `data` | исходные значения Excel с `file_id`, листом, строкой и колонкой |
| `source_tables` | построчный каталог таблиц-источников |
| `target_tables` | построчный каталог целевых таблиц |
| `source_columns` | колонки источников: заголовки определяются по aliases из `column_mapping.json` и, если ролей не хватает, одной LLM-проверкой; хранятся тип, PK, not-null, описание и embedding из технического имени плюс описания; специализированный лист дополняется сырым S2T |
| `target_columns` | целевые колонки: заголовки определяются по aliases из `column_mapping.json` и, если ролей не хватает, одной LLM-проверкой; хранятся тип, PK, not-null, описание и embedding из технического имени плюс описания; специализированный лист дополняется сырым S2T |
| `additional_objects` | имя и полный SQL дополнительного объекта |
| `pxf_to_a` | внешняя, материализованная и репличная таблицы, СОД |
| `s2t_transformations` | общая таблица колонковых ETL-связей и правил |

Если на специализированном листе колонок отсутствует имя таблицы, оно
подставляется только при однозначном совпадении `column_name` на сыром S2T-листе.
Имя предыдущей строки не наследуется, а неоднозначность остаётся явной в отчёте.

`s2t_transformations` содержит как строки исходных S2T-листов, так и связи, извлечённые из `additional_objects.sql`. Для дополнительных объектов SQLGlot обрабатывает CTE, вложенные SELECT и set-операции; ошибки одного объекта попадают в отчёт и не останавливают остальные.

`source_layer` и `target_layer` определяются по группе листа правилами из `config/table_layers.json`, а не по имени таблицы и не через LLM.

### Профиль embedding-индекса

Семантический поиск хранит внутренние метаданные индекса: модель, optional
revision, явный query/document-профиль, нормализацию и размерность. Для модели
по умолчанию используется профиль `multilingual-e5-v1` (`query: ` для запроса,
`passage: ` для документов). Для любой явно заданной `EMBEDDING_MODEL` нужно
также явно задать `EMBEDDING_PROFILE`; доступен нейтральный
`plain-normalized-v1`, который не добавляет префиксы.

Старые embedding blobs без этих метаданных и векторы от другой конфигурации не
смешиваются с новым запросом. После смены модели, revision или профиля выполните
явную переиндексацию из корня проекта:

```bash
uv run python scripts/reindex_description_embeddings.py
```

Команда атомарно перестраивает descriptions для `files`, каталогов таблиц и
каталогов колонок. Read-only chat эту миграцию не запускает.

### Neo4j

При настроенном подключении `services/graph_sync.py` пересобирает проекцию одного файла:

- `ETLColumn` и `TRANSFORMS_TO` — lineage колонок;
- `ETLTable` и `TABLE_TRANSFORMS_TO` — lineage таблиц.

Если Neo4j выключен или недоступен, анализ основного SQL-хранилища сохраняется,
а ошибка синхронизации возвращается отдельно. Для вопросов по S2T и
трансформациям используется PostgreSQL/SQLite backend; Neo4j предназначен для
путей и lineage.

## Быстрый запуск

Требования:

- Python 3.12+;
- [uv](https://docs.astral.sh/uv/);
- GigaChat, OpenRouter или локальный Ollama;
- Neo4j 5+ — только для графовых сценариев.

```bash
git clone https://github.com/Sasyami/ETL-S2T-Parser.git
cd ETL-S2T-Parser
uv sync
```

Скопируйте `.env.example` в `.env`, заполните выбранный provider и запустите:

```bash
uv run python app.py
```

Интерфейс будет доступен на `http://127.0.0.1:5000`. Пути `/` и `/chat_app` открывают один и тот же chat-first экран с загрузкой файла, прогрессом анализа, чатом и просмотром полной таблицы трансформаций.

## Настройка LLM

По умолчанию используется GigaChat.

Все проектные boolean-переменные окружения принимают только `0` или `1`;
текстовые aliases (`true`, `false`, `on`, `off` и подобные) считаются ошибкой
конфигурации. Это относится к live/judge, metrics, Langfuse, SSL/reasoning и
всем бинарным флагам с суффиксом `_EXPERIMENT`. Многовариантный
`OPERATION_SQL_RISK_PROTOCOL_EXPERIMENT` остаётся enum-selector.

При обновлении старого `.env` замените сохранённые boolean aliases явно:
`false` → `0`, `true` → `1`. В частности, прежнее
`GIGACHAT_VERIFY_SSL=false` должно стать `GIGACHAT_VERIFY_SSL=0`. Приложение
намеренно не переписывает пользовательский `.env` и завершает запуск с именем
ошибочной переменной до сетевого запроса.

### GigaChat

```ini
LLM_PROVIDER=gigachat
GIGACHAT_API_KEY=your_key
GIGACHAT_MODEL=GigaChat
GIGACHAT_API_URL=https://api.giga.chat/v1
GIGACHAT_SCOPE=GIGACHAT_API_PERS
GIGACHAT_VERIFY_SSL=0
GIGACHAT_TIMEOUT=120
```

Вместо `GIGACHAT_API_KEY` поддерживаются `GIGACHAT_CREDENTIALS` и `GIGACHAT_EMBEDDINGS_CREDENTIALS`.

### Ollama

Модель должна поддерживать native tool calling и structured output.

```bash
ollama pull qwen3.5:9b
```

```ini
LLM_PROVIDER=ollama
OLLAMA_MODEL=qwen3.5:9b
OLLAMA_BASE_URL=http://localhost:11434
OLLAMA_NUM_CTX=16384
OLLAMA_TIMEOUT=120
OLLAMA_TEMPERATURE=0
OLLAMA_REASONING=0
# auto | native | text; auto uses text tool calls for deepseek-r1 and llama3.1
OLLAMA_TOOL_CALL_MODE=auto
```

### OpenRouter

```ini
LLM_PROVIDER=openrouter
OPENROUTER_API_KEY=your_key
OPENROUTER_MODEL=openrouter/free
OPENROUTER_BASE_URL=https://openrouter.ai/api/v1
OPENROUTER_TIMEOUT=120
OPENROUTER_TEMPERATURE=0
```

### Neo4j

```ini
NEO4J_URI=neo4j://localhost:7687
NEO4J_USERNAME=neo4j
NEO4J_PASSWORD=change_me
NEO4J_DATABASE=neo4j
```

## Конфигурация извлечения

| Файл | Назначение |
|---|---|
| `config/sheet_groups.json` | группы листов и их алиасы |
| `config/column_mapping.json` | роли и варианты названий Excel-колонок |
| `config/usefull_col_extraction.json` | группа листа, целевая таблица SQL-хранилища и поля |
| `config/table_layers.json` | переходы ETL-слоёв по группам листов |

Новые подтверждённые алиасы листов и заголовков добавляются в текущие JSON-конфигурации без дублей.

## HTTP API

| Метод | Путь | Назначение |
|---|---|---|
| `GET` | `/`, `/chat_app` | chat-first UI |
| `POST` | `/upload` | загрузка и полный анализ Excel |
| `GET` | `/analysis_progress/<upload_id>` | прогресс загрузки |
| `POST` | `/chat` | запрос к выбранному агентному режиму |
| `GET` | `/summary/<file_id>` | summary файла |
| `GET` | `/description/<file_id>` | краткое описание файла |
| `GET` | `/transformations` | глобальная таблица S2T |
| `GET` | `/transformations/<file_id>` | S2T указанного файла |
| `DELETE` | `/transformations/<file_id>` | явная очистка S2T файла |
| `DELETE` | `/storage` | явная полная очистка хранилищ |
| `GET` | `/sheet_groups/<file_id>/classify` | классификация листов |
| `GET` | `/exports/...` | скачивание полных результатов |

История чата хранится в `sessionStorage` браузера и передаётся в `/chat`. В SQLite история не записывается.

## Тесты

Обычный набор не обращается к реальной модели:

```bash
pytest tests/ -q
pytest tests/ --cov=. --cov-config=.coveragerc
```

### Live-сценарии

Live-тесты используют реальный Flask `/chat`, выбранный provider и запущенный
Neo4j для графовых сценариев. SQLite берётся из `LIVE_AGENT_DB_PATH`, если
переменная задана, иначе из workspace `excel_data.db`; путь должен указывать на
существующий файл. Таймаут одного локального HTTP `/chat`-обмена задаётся
положительным конечным числом секунд в `LIVE_AGENT_HTTP_TIMEOUT` (по умолчанию
300). Supervisor, coordinator, workers, router, tools, observer, upstream
decision и upstream answer не
подменяются. Запросы выполняются строго последовательно, без batching и
параллельного pytest.

Опциональный `--llm-judge` после каждого ответа отдельным LLM-вызовом оценивает текущий запрос, role-aware историю, публичный answer и display-results, записывает semantic verdict в transcript/comparison report и валидирует сценарий: `failed` или ошибка judge переводят pytest-тест в failed после выполнения его обычных проверок. Пользовательские сообщения истории считаются условиями задачи, а неподтверждённый текст assistant — нет. `LLM_JUDGE_PROVIDER` может независимо выбрать provider judge; например, локального Ollama-агента можно оценивать через `LLM_JUDGE_PROVIDER=gigachat` и `GIGACHAT_JUDGE_MODEL=GigaChat-2-Max`.

```powershell
$env:RUN_LIVE_AGENT_SCENARIOS = "1"
$env:LIVE_AGENT_MODE = "multiagent"
$env:LLM_PROVIDER = "ollama"
$env:OLLAMA_MODEL = "qwen3.5:9b"
$env:LLM_JUDGE_PROVIDER = "gigachat"
$env:GIGACHAT_JUDGE_MODEL = "GigaChat-2-Max"
$env:LIVE_AGENT_DB_PATH = "C:\path\to\live-excel-data.db"
$env:LIVE_AGENT_TRANSCRIPT_PATH = ".test_runs/live-agent.md"
pytest tests/test_live_agent_scenarios.py -q
```

Live-сценарии проверяют обычный диалог, SQLite-count, историю supervisor,
scrollable-результаты, последовательную передачу между workers, точные S2T-пары,
Neo4j-пути, validation-протоколы, shared entity resolution и каталоговые
вопросы. History-набор отдельно
проверяет однозначную ссылку, отказ от неоднозначной ссылки, недоверие к
неподтверждённому предположению assistant и приоритет последнего пользовательского
правила. Неверные или неполные факты, отсутствие требуемого источника и
инфраструктурные ошибки делают сценарий failed. Отклонения display/UI записываются
как presentation warnings, а превышения времени, LLM-вызовов, tools и токенов —
как efficiency warnings; сами по себе они сценарий не роняют.

Каждый сценарий входит ровно в одну смысловую группу:

| `--group` | Pytest marker | Что проверяет |
|---|---|---|
| `smoke` | `live_smoke` | прямой ответ и базовый запрос к данным |
| `history` | `live_history` | разрешение ссылок и правила истории supervisor |
| `display` | `live_display` | полные и scrollable результаты |
| `handoff` | `live_handoff` | зависимые workers и передача результатов |
| `graph` | `live_graph` | Neo4j lineage и точные пути |
| `validation` | `live_validation` | анализ рисков и validation-протоколы |
| `resolution` | `live_resolution` | validation-resolver и его изоляция от agentic; model-owned retrieval кандидатов, exact bypass и ambiguity |
| `catalog` | `live_catalog` | S2T-каталог, semantic search и impact analysis |

Локально группу можно выбрать обычным pytest marker:

```bash
RUN_LIVE_AGENT_SCENARIOS=1 \
LIVE_AGENT_MODE=multiagent \
pytest tests/test_live_agent_scenarios.py -m live_history -q
```

Для последовательного сравнения режимов:

```bash
uv run python scripts/run_live_agent_benchmark.py \
  --provider ollama \
  --model qwen3.5:9b \
  --modes multiagent \
  --group history
```

`--group` можно повторять: `--group history --group handoff` объединяет группы
через OR. Вместе с `--scenario` группа служит дополнительным фильтром точного
сценария. Без `--group` benchmark по-прежнему запускает весь live-набор.

Отчёты записываются в `.test_runs/` и не попадают в git.

### Эксперименты E1–E5

`scripts/run_multiagent_experiments.py` последовательно запускает ограниченную
матрицу сценариев поверх того же real-HTTP benchmark и сохраняет transcript,
JUnit и сводный Markdown-отчёт.
Матрица считается неполной и возвращает ненулевой exit code, если хотя бы один
её сценарий пропущен либо не выполнен (например, из-за отсутствующей live DB).
| Эксперимент | Что сравнивается | Управляющие flags/env |
|---|---|---|
| E1 | capability-based reroute и разделение selector/arguments | `WORKER_CAPABILITY_REROUTE_EXPERIMENT`, `WORKER_SPLIT_TOOL_CALL_EXPERIMENT` |
| E2 | выбор SQL-risk аспектов | `OPERATION_SQL_RISK_ASPECTS_EXPERIMENT` |
| E3 | modes, preflight, 13 checks, expressions, keys и phases | текущий deterministic compiler |
| E4 | минимальные dependency-based readers | текущий dependency planner |
| E5 | validation-resolver и model-owned agentic candidate selection | resolver isolation |

```bash
uv run python scripts/run_multiagent_experiments.py \
  --experiment E1 \
  --experiment E5 \
  --provider gigachat \
  --model GigaChat-3-Ultra \
  --llm-judge
```

`--experiment` можно повторять; без него запускаются E1–E5. Доступны также
`--pytest-arg`, `--output-dir` и `--dry-run`.
`scripts/run_live_agent_benchmark.py` дополнительно принимает `--modes`,
`--scenario`, `--group` и `--allow-failures`.

### Независимый multiagent holdout

`scripts/run_multiagent_holdout.py` сравнивает две заранее зафиксированные
multiagent-конфигурации на десяти сценариях, не входящих в E1–E5. Каждый сценарий
выполняется в обеих конфигурациях, порядок AB/BA чередуется. И агент, и
обязательный semantic judge используют `GigaChat-2-Max`; успешным считается
только результат, одновременно прошедший детерминированные проверки и judge.
Runner проверяет SHA256 одной read-only SQLite-базы до и после каждого из 20
HTTP-обменов, не запускает Neo4j и fail-closed отклоняет skip, неполную judge
telemetry или изменение fixture.

```bash
# Проверка матрицы и fixture без HTTP/LLM
uv run python scripts/run_multiagent_holdout.py \
  --db-path .test_runs/synthetic_live.db \
  --dry-run

# Полный последовательный A/B-прогон
uv run python scripts/run_multiagent_holdout.py \
  --db-path .test_runs/synthetic_live.db \
  --output-dir .test_runs/holdout-max
```

Набор, критерии принятия и порядок фиксируются в preregistration до первого
вызова. Отрицательный результат не даёт runner-у заменить сценарии, ослабить
проверки или объявить экономию токенов улучшением при провале качества.

### Изолированные эксперименты operation-протоколов

`scripts/run_operation_protocol_experiments.py` сравнивает текущий typed
SQL-risk protocol с 20 заранее заданными prompt-only вариантами: пять аспектов
(`row_filtering`, `cardinality`, `constraint_rejection`, `value_changes`,
`write_semantics`) × четыре общие protocol family (`minimal_artifact`,
`evidence_ledger`, `epistemic_state_machine`, `decision_table`). Каждый вариант
получает свой paired baseline на том же live-сценарии; порядок AB/BA чередуется.
Обе руки используют только multiagent, `GigaChat-2-Max` и обязательный
`GigaChat-2-Max` semantic judge.

```bash
# Проверка фиксированной матрицы, commit и fixture без HTTP/LLM
uv run python scripts/run_operation_protocol_experiments.py \
  --db-path .test_runs/synthetic_live.db \
  --dry-run

# 20 пар, то есть 40 последовательных /chat-обменов
uv run python scripts/run_operation_protocol_experiments.py \
  --db-path .test_runs/synthetic_live.db \
  --output-dir .test_runs/operation-protocol-experiments
```

Изолированные arms по умолчанию запускаются тем же Python, которым запущен
runner; другой интерпретатор можно передать через `--python-executable`.
Версионируемый pytest-plugin с guard неизменности synthetic fixture находится в
`tests/support/synthetic_live_support.py`; пустая локальная заглушка preflight не
проходит. Завершение по timeout или interrupt останавливает всё дерево процессов
платформенным способом на Windows и POSIX до удаления временного clone.

Перед первым вызовом runner фиксирует committed HEAD, SHA протокольного bundle,
SQLite и synthetic plugin. Каждый эксперимент выполняется в отдельном локальном
`--no-hardlinks` clone и на двух disposable копиях БД; в `finally` clone и копии
удаляются, а rollback certificate сохраняется. Внешний Langfuse отключён.
Неоткатываемы только уже потраченные provider tokens и provider-side логи.
Runner не переносит победивший вариант в default: E2-сценарии являются
development evidence, поэтому потенциальному победителю нужен новый независимый
holdout.

Все 20 ячеек, включая `value_changes` и `write_semantics`, проходят
полный model-owned upstream decision и upstream answer. Runner фиксирует
`OPERATION_SQL_RISK_SCOPE_EVIDENCE_EXPERIMENT=0`, поэтому отдельная scope-ветка
с внутренними extraction/assessment вызовами не входит в эту preregistered
популяцию.

Confirmatory Max/Max-run `20260910_021405` завершил все 20 пар и откаты, но ни
одно семейство не прошло preregistered gate. Combined score изменился с 7/20 до
9/20, при этом semantic score снизился с 16/20 до 15/20, candidate agent tokens
выросли на 76,3%, а `epistemic_state_machine` дал regression с HTTP 500.
Продвигать варианты нельзя: результаты этого исторического эксперимента не
описывают текущий runtime.

Для development-проверки SQL-risk operation scope доступен отдельный
opt-in `OPERATION_SQL_RISK_SCOPE_EVIDENCE_EXPERIMENT=1`.
Operation scope не является operation skill: router возвращает
`pipeline=sql_risk_scope` и `skills=[]`; поле risk-aspects в default router
schema отсутствует, execution mode верхний router не выбирает. После router внутренний native LLM-вызов
`submit_sql_risk_scope` с учётом устойчивого context извлекает один mode из `row_filtering`,
`conditional_cardinality`, `nullable_constraint`, `value_changes` и
`write_semantics`, точную направленную source → target пару и optional
`file_id`. Идентификаторы и attestations берутся только из исходной task; context
может уточнять лишь intent и терминологию. Код только проверяет typed schema и дословное происхождение этих
значений из исходной task; literal/NL parser, regex/keyword-классификации и
эвристического исправления identifiers нет. После одного невалидного ответа
допускается один repair-вызов той же модели.

Прямое сравнение каталоговых `data_type`, `not_null` или ключевых признаков
двух колонок, включая совместимость nullable source с NOT NULL target, идёт по
обычному agentic-профилю `Совместимость колонок`. `nullable_constraint` внутри
scope используется только когда вопрос требует анализа сохранённой
SQL-проекции, выражения или predicate, способных породить NULL.

Contract задаёт fixed exact readers. Полные results сохраняются в run-scoped
store, а SQLGlot формирует нейтральный structural bundle с полными predicates,
joins, projections, DML targets и parse diagnostics — без семантического
вывода о риске. Затем второй native LLM-вызов `submit_sql_risk_assessment`
получает исходную task, устойчивый context, exact evidence и весь bundle, самостоятельно формирует
risk conclusion, пользовательский ответ и display selection. Код проверяет
полноту provenance и допускает один assessment repair. Эта ветка не вызывает
downstream planner/model, workers, worker planner, observer либо обычные
upstream decision/answer. Invalid extraction/assessment, reader/structure
error или incomplete evidence возвращают structured `unavailable` с
`silent_fallback=false`; молчаливого перехода в agentic pipeline нет. Default
выключен.

Прежние development-прототипы с code-owned risk verdicts и NL/literal parsers
удалены. Связанные отчёты остаются историческими артефактами и не описывают
текущий runtime. В актуальной ветке вывод о риске принадлежит только внутреннему
LLM assessment; scope default по-прежнему выключен.

## Структура проекта

```text
app.py                         Flask API и выбор режима чата
processing/excel.py            механический разбор Excel
storage/database.py            схема и хранение исходных данных
storage/s2t.py                 операции с S2T transformations
sheet_skills/                  обработчики групп Excel-листов
services/analysis.py           post-upload pipeline
services/graph_sync.py         проекция SQL-хранилища → Neo4j
graph_storage/                 lifecycle и настройки Neo4j
agents/supervisor.py           верхний LangGraph
agents/coordinator.py          выбор pipeline, downstream/workers/upstream
agents/sql_risk_scope_extraction.py  native LLM contract режима и exact scope
agents/sql_risk_operation_pipeline.py  orchestration exact readers → structure → assessment
agents/sql_risk_structure.py     нейтральный SQLGlot structural bundle
agents/sql_risk_assessment.py    native LLM contract вывода, ответа и display
agents/worker.py               worker runtime и работа с зависимостями
agents/chat_graph.py           planner/tool/observer loop
agents/entity_resolution.py    внутренний exact/partial/fuzzy/semantic resolver validation
agents/test_protocol_contract_review.py  model-owned review полноты/ролей Raw contract
agents/test_protocol_resolution.py  Raw → Resolved validation contract
agents/validation_protocol.py  dependency-based readers test protocol
agents/transformation_ast.py   SQLGlot-нормализация transformation
agents/test_protocol.py        phased compiler 13 SQL checks
agents/tools/routing.py        LLM router tools и skills
agents/tools/saved_results.py  run-scoped результаты и read-only relation
agents/tools/                  read-only/write registries и tools
agents/prompts/                runtime prompts и skills
agents/run_metrics.py          метрики live-запусков
config/                        JSON-конфигурации извлечения
templates/chat_app.html        единый интерфейс
scripts/                       live benchmark и E1–E5/holdout/protocol runners
docs/history/                  архив старых демонстраций и live-отчётов
tests/                         unit, integration и live tests
samples/                       примеры S2T Excel
```

## Логи и трассировка

Логи пишутся в консоль и в ротационный UTF-8 файл `logs/agent.log`. Уровень и размер задаются через `LOG_LEVEL`, `LOG_FILE`, `LOG_MAX_BYTES` и `LOG_BACKUP_COUNT`.

Langfuse необязателен. Для включения задайте `LANGFUSE_ENABLED=1`, `LANGFUSE_PUBLIC_KEY` и `LANGFUSE_SECRET_KEY`.

## Безопасность данных

- чат read-only по умолчанию;
- mutation-tools находятся в отдельном registry;
- очистка и повторная запись требуют явного действия пользователя;
- свободный SQL и Cypher ограничены read-only операциями;
- полные tool results не размножаются в LLM-контексте;
- Основное SQL-хранилище остаётся источником истины даже при включённом Neo4j.
