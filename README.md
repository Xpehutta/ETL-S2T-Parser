# ETL S2T Parser

[![Python 3.12+](https://img.shields.io/badge/Python-3.12%2B-blue.svg)](https://www.python.org/)
[![Flask](https://img.shields.io/badge/Flask-3.x-green.svg)](https://flask.palletsprojects.com/)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

ETL S2T Parser — веб-приложение для разбора Excel-файлов с Source-to-Target-маппингами, описаниями таблиц и правилами преобразований.

Приложение:

- определяет заголовки и читает многоуровневые Excel-листы;
- сохраняет структуру файла и значения строк в SQLite;
- классифицирует листы и извлекает S2T-трансформации по настраиваемым схемам;
- строит краткое бизнес-описание загруженного файла;
- создаёт локальные эмбеддинги описаний файлов и таблиц;
- проецирует связи между колонками в Neo4j;
- отвечает на вопросы через инструментального LangGraph-агента.

## Как устроена обработка

```mermaid
flowchart LR
    U["Excel-файл"] --> P["Механический разбор листов"]
    P --> H["Определение заголовков"]
    H --> DB[("SQLite")]
    DB --> C["Классификация групп листов"]
    C --> S["Sheet skills"]
    S --> DB
    DB --> M["Суммаризация и описания"]
    M --> DB
    DB --> G[("Neo4j: lineage колонок")]
    DB --> A["LangGraph-агент"]
    G --> A
```

### 1. Разбор Excel

`processing/excel.py` один раз читает каждый лист, применяет найденное или исправленное пользователем решение о заголовке, разворачивает объединённые ячейки заголовков и строит плоские имена колонок. Скрытые строки по умолчанию не загружаются; в интерфейсе их можно явно включить.

Определение положения и глубины заголовка находится в `agents/agent.py`. Результат можно проверить и скорректировать до повторной записи файла.

### 2. Хранение исходных фактов

SQLite — основной источник данных приложения. В неё записываются:

- метаданные файла;
- листы и распознанные заголовки;
- значения ячеек с исходными номерами строк;
- каталоги source- и target-таблиц;
- строки S2T-трансформаций.

Одинаковые строки не дедуплицируются: каждая строка исходного Excel остаётся отдельным фактом.

### 3. Классификация листов и sheet skills

Сначала `agents/sheet_group_classifier.py` определяет группу каждого листа:

1. точное или нечёткое совпадение по `config/sheet_groups.json`;
2. один LLM-вызов только для несопоставленных листов;
3. сохранение подтверждённых новых алиасов в конфигурацию.

После классификации запускается подходящий обработчик из `sheet_skills/`:

- `sheet_skills/s2t.py` сопоставляет колонки и строит строки `s2t_transformations`;
- `sheet_skills/table_catalog.py` сохраняет названия и описания source/target-таблиц.

Для S2T сначала используется сопоставление по `config/column_mapping.json`. Если найдены не все настроенные поля, модель получает один компактный запрос для этого листа. Если валидный план не получен, обработка завершается ошибкой без молчаливой записи неполных данных.

Отсутствие `target_table` в любой извлекаемой строке считается ошибкой до начала транзакции. Уже сохранённые трансформации при этом не изменяются.

### 4. Суммаризация и эмбеддинги

`agents/summarizer_agent.py` делает один LLM-вызов: детерминированно извлекает описания таблиц, представлений, атрибутов и полей из сохранённых листов и передаёт их в модель как семантический каталог.

При сохранении описаний создаются локальные эмбеддинги:

- `files.description`;
- `source_tables.description`;
- `target_tables.description`.

Модель задаётся переменной `EMBEDDING_MODEL`. Векторы хранятся в BLOB-полях соответствующих SQLite-таблиц, отдельная таблица эмбеддингов не используется.

### 5. Графовая проекция

После успешной записи S2T-результата `services/graph_sync.py` может пересобрать в Neo4j проекцию lineage выбранного файла:

- узлы `ETLColumn` представляют колонки логических таблиц;
- связи `TRANSFORMS_TO` представляют переход source-колонки в target-колонку.

Названия файлов, листов, таблиц, описания и сами строки трансформаций остаются в SQLite. Отсутствие узла или связи в Neo4j не доказывает отсутствие факта в SQLite.

### 6. Инструментальный агент

`agents/chat_graph.py` реализует цикл:

```mermaid
flowchart LR
    Q["Вопрос"] --> P["Planner"]
    P -->|вызов инструмента| T["ToolNode"]
    T --> O["Observer"]
    O --> P
    P -->|инструменты больше не нужны| R["Responder"]
    R --> A["Ответ"]
```

Planner выбирает только зарегистрированные инструменты. Observer фиксирует полученные факты и ограничения, а отдельный responder формирует итоговый ответ.

Маршрутизация источников разделена:

- строки, маппинги, правила и таблица трансформаций читаются из SQLite;
- lineage, пути, upstream/downstream и impact analysis читаются из Neo4j;
- отсутствие данных в Neo4j не подменяет проверку SQLite.

Диалог read-only по умолчанию. Инструменты изменения данных находятся в отдельном registry и не выдаются обычному чату.

## Структура проекта

```text
app.py                         Flask API и запуск приложения
processing/excel.py            механический разбор Excel
storage/database.py            схема и базовые операции SQLite
storage/s2t.py                 запись, чтение и поиск S2T
graph_storage/                 конфигурация, подключение и чтение Neo4j
services/analysis.py           запуск анализа после сохранения файла
services/embeddings.py         локальное эмбеддирование описаний
services/graph_sync.py         проекция SQLite → Neo4j
agents/agent.py                определение заголовков и вход в чат
agents/chat_graph.py           LangGraph planner/tools/observer/responder
agents/summarizer_agent.py     однопроходная суммаризация
agents/sheet_group_classifier.py
agents/tools/                  тематические @tool и registry
agents/prompts/                системные инструкции агента
sheet_skills/s2t.py            извлечение S2T
sheet_skills/table_catalog.py  каталоги source/target-таблиц
config/                        JSON-схемы и их загрузчики
templates/                     веб-интерфейс
tests/                         автоматические проверки
samples/                       примеры Excel-файлов
```

## Быстрый запуск

### Требования

- Python 3.12 или новее;
- [uv](https://docs.astral.sh/uv/);
- доступ хотя бы к одному LLM-provider: GigaChat, OpenRouter или Ollama;
- Neo4j — только если нужна графовая проекция и lineage-запросы.

### Установка

```bash
git clone https://github.com/Xpehutta/ETL-S2T-Parser.git
cd ETL-S2T-Parser
uv sync
```

Создайте `.env` в корне проекта, затем запустите:

```bash
uv run python app.py
```

Основной интерфейс откроется по адресу `http://127.0.0.1:5000`, чат — по адресу `http://127.0.0.1:5000/chat_app`.

## Настройка LLM

По умолчанию приложение использует **GigaChat** (`LLM_PROVIDER=gigachat`). Для другого backend задайте `LLM_PROVIDER`.

### GigaChat (по умолчанию)

```ini
LLM_PROVIDER=gigachat
GIGACHAT_API_KEY=your_key
GIGACHAT_MODEL=GigaChat
GIGACHAT_API_URL=https://gigachat.devices.sberbank.ru/api/v1
GIGACHAT_SCOPE=GIGACHAT_API_PERS
GIGACHAT_VERIFY_SSL=false
GIGACHAT_TIMEOUT=120

GIGACHAT_HEADER_TIMEOUT=20
GIGACHAT_HEADER_RETRY_ATTEMPTS=1
GIGACHAT_HEADER_PREVIEW_ROWS=4
```

Вместо `GIGACHAT_API_KEY` также поддерживаются `GIGACHAT_CREDENTIALS` и `GIGACHAT_EMBEDDINGS_CREDENTIALS`. Переменная `MODEL` используется как fallback для `GIGACHAT_MODEL`.

### Ollama

Используемая модель должна поддерживать native tool calling, иначе чат-агент не сможет вызывать инструменты.

```bash
ollama pull qwen2.5:7b
```

```ini
LLM_PROVIDER=ollama
OLLAMA_MODEL=qwen2.5:7b
OLLAMA_BASE_URL=http://localhost:11434
OLLAMA_NUM_CTX=16384
OLLAMA_TIMEOUT=120
OLLAMA_TEMPERATURE=0
OLLAMA_REASONING=false
```

### OpenRouter

```ini
LLM_PROVIDER=openrouter
OPENROUTER_API_KEY=sk-or-v1-...
OPENROUTER_MODEL=openrouter/free
OPENROUTER_BASE_URL=https://openrouter.ai/api/v1
OPENROUTER_TIMEOUT=120
OPENROUTER_TEMPERATURE=0
```

## Настройка эмбеддингов

Эмбеддинги считаются локально через Sentence Transformers. Например:

```ini
EMBEDDING_MODEL=intfloat/multilingual-e5-small
```

При первом использовании библиотека загрузит модель в локальный кеш.

## Настройка Neo4j

Neo4j запускается отдельным процессом и не обязан находиться внутри Flask-приложения. Пример запуска через Docker:

```bash
docker run --name etl-s2t-neo4j \
  -p 7474:7474 \
  -p 7687:7687 \
  -e NEO4J_AUTH=neo4j/change_me \
  -d neo4j:5
```

Добавьте подключение в `.env`:

```ini
NEO4J_URI=neo4j://localhost:7687
NEO4J_USERNAME=neo4j
NEO4J_PASSWORD=change_me
NEO4J_DATABASE=neo4j
```

Веб-интерфейс Neo4j Browser будет доступен по адресу `http://localhost:7474`.

Если Neo4j не настроен или недоступен, SQLite-анализ остаётся сохранённым, а ошибка синхронизации возвращается отдельно.

## Конфигурация извлечения

Логика извлечения задаётся данными, а не зашивается в обработчик:

| Файл | Назначение |
|---|---|
| `config/sheet_groups.json` | группы листов и допустимые имена/алиасы |
| `config/column_mapping.json` | поля группы и варианты заголовков Excel |
| `config/usefull_col_extraction.json` | целевые SQLite-таблицы, группа листа и список записываемых полей |

Пример целевого описания:

```json
{
  "s2t_transformations": {
    "sheet_group": "s2t",
    "fields": [
      "target_field",
      "source_field",
      "target_table",
      "source_table",
      "transformation_rule"
    ]
  }
}
```

Имена полей в `fields` одновременно определяют ожидаемые роли из `column_mapping.json` и пользовательские колонки целевой SQLite-таблицы.

## SQLite-схема

По умолчанию база создаётся в `excel_data.db`.

| Таблица | Содержимое |
|---|---|
| `files` | файл, модель, время загрузки, summary, description и embedding |
| `file_sheet_headers` | лист, решение о заголовке и плоские имена колонок |
| `data` | значения ячеек: лист, исходная строка, колонка и значение |
| `source_tables` | строки каталога таблиц-источников и embeddings описаний |
| `target_tables` | строки каталога таблиц-приёмников и embeddings описаний |
| `s2t_transformations` | построчные source-to-target-маппинги и правила |

При несовместимой старой схеме приложение выдаёт явную ошибку. Автоматическая миграция пользовательских данных не выполняется.

## Инструменты агента

Основные read-only инструменты:

- `run_sql` — свободный read-only SQL по публичной SQLite-схеме;
- `list_s2t_transformations` и `search_s2t_transformations` — просмотр S2T;
- `summarize_s2t_tables` — сводка по логическим таблицам;
- `list_files`, `resolve_file`, `list_sheets`, `list_columns` — навигация по загрузкам;
- `run_cypher` — свободный read-only Cypher;
- `trace_neo4j_lineage` — upstream/downstream-пути колонок;
- `show_plan` — явная фиксация выполненных и следующих действий.

Инструменты объявлены через `@tool(parse_docstring=True)` в `agents/tools/` и явно включаются в `agents/tools/registry.py`.

## HTTP API

| Метод | Путь | Назначение |
|---|---|---|
| `GET` | `/` | основной веб-интерфейс |
| `GET` | `/chat_app` | интерфейс чата |
| `GET` | `/analysis_progress/<upload_id>` | прогресс фонового анализа |
| `POST` | `/upload` | загрузка и первичный разбор Excel |
| `POST` | `/apply_corrections` | повторный разбор с исправлениями заголовков |
| `POST` | `/preview_headers` | предварительный просмотр выбранного заголовка |
| `GET` | `/summary/<file_id>` | summary файла |
| `GET` | `/description/<file_id>` | описание файла |
| `GET` | `/transformations/<file_id>` | строки S2T |
| `POST` | `/transformations/<file_id>/refresh` | повторное извлечение S2T |
| `DELETE` | `/transformations/<file_id>` | удаление S2T указанного файла |
| `DELETE` | `/storage` | полная очистка SQLite, Neo4j и runtime-кеша |
| `GET` | `/sheet_groups/<file_id>/classify` | классификация листов |
| `GET` | `/exports/sql/<filename>` | выгрузка подготовленного SQL-файла |
| `POST` | `/chat` | запрос к инструментальному агенту |

История чата хранится в `sessionStorage` браузера, поэтому переживает перезагрузку страницы в той же вкладке, но не записывается в SQLite.

## Langfuse

Интеграция вынесена в `agents/observability.py` и остаётся необязательной. При заданных ключах Langfuse получает трассировки поддерживаемых LLM-вызовов; без конфигурации основная обработка продолжает работать.

```ini
LANGFUSE_PUBLIC_KEY=pk-lf-...
LANGFUSE_SECRET_KEY=sk-lf-...
LANGFUSE_TRACING_ENVIRONMENT=development
```

## Разработка

```bash
pytest tests/ -q
pytest tests/ --cov=. --cov-config=.coveragerc
```

Перед изменением схемы SQLite, формата конфигураций или набора tools обновляйте соответствующие проверки в `tests/`.
