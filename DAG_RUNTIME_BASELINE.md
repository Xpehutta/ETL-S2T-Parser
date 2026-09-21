# DAG runtime baseline

Дата: 2026-09-21<br>
Ветка: `refactor/async-dag-runtime`<br>
Baseline commit: `908ecb682fb12e803af9e579bf9cc92242c0fa5f`

## Состояние рабочей копии

Tracked-файлы не изменены. Пользовательские untracked-файлы сохранены без
изменений:

- `.review_workspace/`;
- `scripts/judge_live_manifest_results.py`;
- `scripts/run_word9_small_model_benchmark.py`;
- `tests/test_word9_small_model_benchmark.py`.

## Детерминированные проверки

Целевой прогон:

```text
tests/test_coordinator_dag.py
tests/test_async_runtime.py
39 passed in 0.17s
```

Полный non-live прогон опубликованного состава:

```text
1228 passed, 80 skipped in 26.30s
statement coverage: 88.74%
branch coverage: 85.66%
```

Локальный untracked `tests/test_word9_small_model_benchmark.py` намеренно не
включён: его нет в опубликованной ветке, и он относится к отдельной работе
пользователя.

## Покрытие критических компонентов

| Компонент | Statement | Branch |
| --- | ---: | ---: |
| `agents/coordinator.py` | 91.32% | 89.13% |
| `agents/contracts.py` | 91.62% | 87.55% |
| `agents/worker.py` | 90.20% | 88.28% |
| `agents/tools/saved_results.py` | 88.44% | 85.04% |
| `agents/async_runtime.py` | 100.00% | 98.62% |

## Текущая DAG-телеметрия

Сценарий из шести шагов фиксирует:

```text
plan_size=6
dag_depth=3
max_parallel_width=4
worker_max_concurrency=4
max_observed_concurrency=4
workers=6
```

Для запущенного worker записываются `depends_on`, layer, время постановки,
старта и окончания, dependency/semaphore wait, concurrency при старте и status.

## Подтверждённые ограничения baseline

1. Scheduler использует барьер топологического слоя: готовый child ждёт все
   независимые задачи предыдущего слоя.
2. Dataset связывается с accepted tool message через глобальный поиск
   `source_tool_call_id`; одинаковые provider call IDs конкурентных workers
   могут выбрать чужой descriptor.
3. `read_previous_result` и `query_saved_result` ограничены наличием run-scoped
   store, но не имеют per-worker allowlist прямых зависимостей.
4. LLM-ingress и внутренний legacy contract используют одну модель с defaults;
   отсутствие `id`/`depends_on` может молча превратиться в линейный план.
5. Structured `failed`/`partial` outcomes не имеют формальной dependency policy;
   текущий scheduler считает любой возвращённый outcome завершённой зависимостью.
6. В telemetry отсутствуют terminal records для не запущенных blocked/cancelled
   шагов и нет различия `cancelled_before_start`/`cancelled_running`.
7. `pytest.ini` глобально отключает порог через `--cov-fail-under=0`, а branch
   coverage не включён в канонический CI-прогон.

Baseline подтверждает: существующие DAG/async-тесты зелёные, но не проверяют
collision ownership, handoff ACL, structured dependency failure и снятие layer
barrier.
