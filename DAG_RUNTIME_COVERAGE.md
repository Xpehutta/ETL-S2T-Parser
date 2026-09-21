# DAG runtime coverage gate

Дата: 2026-09-21  
Ветка: `refactor/async-dag-runtime`

## Канонические пороги

Coverage считается с `branch = True`. Единственный gate —
`scripts/check_coverage.py`, который независимо проверяет:

- statement coverage: не ниже 88,00%;
- raw branch coverage: не ниже 76,35%.

`pytest.ini` больше не отключает порог, а CI передаёт Coverage JSON одному
gate-скрипту. Порог statement не снижен. Branch gate зафиксирован от измеренного
до включения gate baseline 76,38%; после тестов самого gate поднят результат и
оставлен небольшой запас над порогом.

## Итоговый non-live прогон

```text
1285 passed, 88 skipped in 22.65s
statement coverage: 88.94%
branch coverage: 76.83%
coverage.py combined line+branch: 85.91%
```

## Критические компоненты

| Компонент | Statement | Branch |
| --- | ---: | ---: |
| `agents/coordinator.py` | 93.16% | 83.78% |
| `agents/contracts.py` | 90.67% | 71.84% |
| `agents/worker.py` | 90.64% | 82.00% |
| `agents/tools/saved_results.py` | 89.24% | 77.33% |
| `agents/async_runtime.py` | 93.57% | 86.96% |

Значения берутся из одного Coverage JSON тем же скриптом, который применяет
пороги в CI. Исторический `DAG_RUNTIME_BASELINE.md` теперь явно называет старую
вторую метрику combined line+branch, чтобы не смешивать её с raw branch.
