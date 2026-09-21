# FastAPI / async migration audit

Дата аудита: 2026-09-21

## Решение по техническому заданию

Основная миграция принята. FastAPI, async runtime и следующий этап DAG
реализуются в одной отдельной ветке `refactor/async-dag-runtime`.
Сначала фиксируется зелёный линейный async baseline, затем в этой же ветке
добавляется DAG-планирование с отдельной проверкой корректности и эффекта.

Уточнение к исходному плану: `threading.Lock` нельзя механически заменить на
`asyncio.Lock` для состояния, к которому обращаются progress callbacks или
sync-код, выполняемый через thread offload. Для таких mixed sync/thread
границ остаётся короткая критическая секция под `threading.Lock`; внутри неё
нет `await` и внешнего I/O.

## Текущий HTTP ingress

- `app.py` создаёт глобальный `Flask` app и вызывает `init_db()` при импорте.
- Прямой запуск использует Flask development server.
- Ограничение upload задано через `MAX_CONTENT_LENGTH = 10 MiB`.
- HTML отдаётся через `render_template`, exports — через
  `send_from_directory`.
- Маршруты: `/`, `/chat_app`, `/chat`, `/upload`,
  `/analysis_progress/{upload_id}`, `/summary/{file_id}`,
  `/description/{file_id}`, `/transformations`,
  `/transformations/{file_id}`, `/storage`,
  `/sheet_groups/{file_id}/classify` и три семейства `/exports/...`.
- HTTP-тесты используют Flask test client из `tests/conftest.py`.

## Текущая sync-цепочка `/chat`

```text
Flask /chat
  -> supervisor_chat(...)
     -> supervisor graph.invoke(...)
        -> coordinator_chat(...)
           -> coordinator graph.invoke(...)
              -> worker_chat(...) последовательно для каждого PlanStep
                 -> run_worker_graph(...)
                    -> worker graph.invoke(...)
                       -> sync model.invoke(...) / ToolNode.invoke(...)
```

Single-agent baseline идёт через `agent_chat -> run_agent_graph ->
graph.invoke`.

## Места `.invoke(...)` в runtime

- `agents/supervisor.py`: supervisor model и compiled graph.
- `agents/coordinator.py`: operation router, downstream/upstream models и
  compiled graph.
- `agents/worker.py` / `agents/chat_graph.py`: router, planner, observer,
  responder, ToolNode и compiled worker graph.
- `agents/agent.py`: header chain и single-agent graph.
- `agents/tools/routing.py`: structured tool router.
- Специализированные sync-границы: entity resolution, validation readers,
  SQL-risk readers, semantic judge, summarizer, sheet-group and extraction
  helpers.

Основной `/chat` path должен использовать `ainvoke`. Локальные sync tools и
SQLite readers будут выполняться контролируемо через LangGraph async tool
execution или `asyncio.to_thread`, а не прямо в event loop. Sync compatibility
facades могут остаться только для существующих non-ASGI callers и тестов.

## DB и внешнее I/O

- Основная БД — `sqlite3`; функции создают отдельное соединение на операцию.
- Запись Excel и очистка хранилища уже имеют явные commit/rollback boundaries;
  их атомарность должна быть сохранена.
- Run-scoped saved results используют отдельную временную SQLite БД и `RLock`.
- Neo4j driver и graph sync синхронные.
- Excel parsing, embeddings, filesystem exports и часть summary/classification
  являются sync/CPU-or-I/O-bound.
- LLM providers поддерживают LangChain `ainvoke`; это основной native async
  путь.

HTTP handlers не будут удерживать SQLite transaction во время `await`.
Upload parsing/storage/analysis и другие длительные sync операции будут
вынесены целиком в `asyncio.to_thread`, сохраняя одну логическую transaction
boundary внутри sync storage-функции.

## Locks и shared mutable state

- `app.py`: `analysis_progress` + `threading.Lock`.
- `agents/worker.py`: display-result registry + `threading.Lock`.
- `agents/run_metrics.py`: completed-run registry и mutable per-run metrics +
  `threading.Lock`.
- `agents/tools/saved_results.py`: run-scoped store + `RLock`.

Эти lock sections короткие и не выполняют LLM/network I/O. Они остаются
thread-safe, потому что upload/tool work может исполняться в worker threads.
Run-scoped stores and metrics must continue to use context-local binding so
concurrent requests do not share request state.

## Тесты и CI до миграции

- `tests/test_app.py` покрывает Flask HTTP contract, upload, progress, exports,
  storage and chat validation.
- `tests/test_supervisor.py`, `test_coordinator.py`, `test_worker.py` and
  `test_agent.py` подробно покрывают sync agent contracts.
- Live HTTP runner поднимает Werkzeug server in a thread.
- Async ASGI/concurrency/cancellation tests отсутствуют.
- `.github/workflows` отсутствует; coverage настроен через pytest/pytest-cov.

## Целевая цепочка

```text
FastAPI endpoint
  -> await supervisor_chat_async(...)
     -> await supervisor graph.ainvoke(...)
        -> await coordinator_chat_async(...)
           -> await coordinator graph.ainvoke(...)
              -> await worker_chat_async(...) sequentially per PlanStep
                 -> await worker graph.ainvoke(...)
                    -> await model.ainvoke(...) / async tool execution
```

Для single-agent режима используется симметричный async path. Линейный порядок
workers и lazy previous-results semantics не меняются.

## Порядок реализации

1. Добавить FastAPI/uvicorn/httpx/python-multipart dependencies and ASGI app.
2. Перенести все HTTP contracts; upload size проверять при чтении потока,
   сохранив лимит 10 MiB и прежние JSON error bodies где это часть UI contract.
3. Добавить native async router/agent/worker/coordinator/supervisor APIs и
   `ainvoke`; оставить минимальные sync facades для внутренних legacy callers.
4. Offload blocking SQLite/Excel/Neo4j/filesystem boundaries from handlers and
   async graph nodes without wrapping the whole agent runtime in a thread.
5. Добавить timeout/cancellation and configurable LLM/tool concurrency gates.
6. Перевести HTTP tests на `httpx.AsyncClient` + ASGI transport; добавить
   concurrent `/chat`, timeout/cancellation and rollback regression tests.
7. Обновить live server runner, README, Makefile and CI.

## Основные риски

- Незакоммиченные изменения уже затрагивают supervisor/coordinator/worker
  contracts; миграция должна быть additive and preserve them.
- LangGraph async execution rejects sync-only assumptions in node-local mocks;
  compatibility helpers and deterministic async tests are required.
- FastAPI/Pydantic default 422 responses differ from the existing 400 JSON
  contract; frontend-facing validation must be explicitly normalized.
- `UploadFile` has no Flask-style global body limit; application streaming
  guard and optional proxy/server limit are both documented.
- Cancellation of a thread-offloaded function cannot stop its Python thread;
  cancellation prevents response/next stages, while storage functions retain
  their own rollback guarantees.
- Multi-process ASGI deployment does not share in-memory progress. Initial
  support remains single-process; external progress storage is a documented
  future boundary, not part of this refactor.

## Exit criteria before DAG phase

- All ASGI endpoints and UI contracts pass.
- `/chat` uses native async LLM and LangGraph execution end-to-end.
- Linear worker order is tested explicitly.
- Existing unit suite and new async/concurrency/transaction tests pass.
- Remaining sync boundaries and single-process progress limitation are
  documented.

## Статус реализации

Baseline реализован 2026-09-21:

- Flask/Werkzeug заменены на FastAPI/Uvicorn, HTTP-контракт сохранён;
- supervisor, coordinator, worker, single-agent graph, LLM и ToolNode работают
  через native async API;
- SQLite/Excel/Neo4j/filesystem операции остались явными sync-границами и
  выполняются вне event loop;
- добавлены request/LLM/tool timeouts, отдельные concurrency limits и
  распространение cancellation;
- добавлены ASGI, параллельные `/chat`, timeout/cancellation и transaction
  rollback tests;
- live HTTP fixture переведён с Werkzeug на Uvicorn;
- добавлен GitHub Actions pipeline с locked install, compile check, tests и
  coverage.

Финальный контрольный прогон чистого опубликованного состава после DAG-этапа:
`1228 passed, 80 skipped`; суммарное покрытие всего репозитория — `88.74%`
(`12 898 / 14 535` statements). Для основного async/DAG-контура (`app`,
single-agent, supervisor/coordinator/worker, graph/router/runtime) — `90.10%`
(`2 558 / 2 839`). Критические границы FastAPI и совместимости вызовов имеют
отдельные регрессионные проверки: `app.py` — `99.73%`, `async_runtime.py` и
`native_call_adapter.py` — `100%`.
DAG реализован в той же ветке `refactor/async-dag-runtime`: `PlanStep` содержит
`id`/`depends_on`, контракт отклоняет duplicate/missing/self/cyclic зависимости,
а coordinator исполняет ready groups через fail-fast `asyncio.TaskGroup` с
лимитом `WORKER_MAX_CONCURRENCY`. Legacy-планы без DAG-полей сохраняют прежнюю
линейную семантику.
