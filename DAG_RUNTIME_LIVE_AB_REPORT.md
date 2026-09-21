# DAG runtime: live A/B

Дата: 2026-09-21  
Ветка: `refactor/async-dag-runtime`  
Модель агента и semantic judge: `GigaChat-2-Max`

## Итог

Вердикт: **inconclusive, candidate не продвигать**.

Зафиксированная матрица требовала 48 обменов: 8 DAG-сценариев × 2 режима ×
3 paired-повтора с контрбалансированным порядком AB/BA. Выполнен 21 обмен.
После этого GigaChat вернул `402 Payment Required`; сохранённые сценарные
артефакты представляют эту ошибку как нулевой по токенам `ResponseError`
одновременно у агента и judge. Сравнение нельзя объявлять завершённым или
улучшенным.

Default не изменён: `WORKER_MAX_CONCURRENCY=1`.

## Что успели подтвердить

| Метрика | Baseline | Candidate |
| --- | ---: | ---: |
| Выполнено запусков | 10 | 11 |
| Технически успешных | 7 | 2 |
| Semantic pass | 7 | 2 |
| Semantic failed | 1 | 7 |
| Judge error | 2 | 2 |
| Agent seconds | 1136.248 | 809.759 |
| Agent tokens | 616 495 | 491 541 |
| Максимальный наблюдаемый параллелизм | 1 | 4 |

- Все 21 проверки SHA256 до и после обмена совпали:
  `3a69de80dc68d84433447bd31ceb18feb17366f7ef75c41da5f22b0a9d4a0bc0`.
- Candidate действительно исполнил workers параллельно и достиг concurrency 4.
- В десяти завершённых парах median latency ratio составил `0.984`; четыре
  пары прошли preregistered latency gate `<= 0.95`. Из неполного набора нельзя
  делать итоговый вывод о скорости.
- В доступных trace не обнаружено смешивания сохранённых результатов между
  sibling/transitive workers. Заблокированные потомки корректно имеют пустой
  список входных результатов.
- При реальном перекрытии запросов candidate часто терял отдельные workers из-за
  provider errors. Это нарушало hard gates полноты DAG и ответа; поэтому даже
  доступная часть данных не поддерживает включение candidate по умолчанию.
- Сценарий `failed_parent` выявил проблему самого live prompt: точное чтение
  отсутствующей таблицы вернуло валидный пустой набор, а не структурированный
  `failed`, поэтому ожидаемая ветка `failed → blocked` не возникла. Gate это
  зафиксировал, результат не был ошибочно засчитан как доказательство семантики.

## Исправления после прогона

- Default worker concurrency явно возвращён к `1`; значение `4` остаётся
  экспериментальным opt-in до успешного live gate.
- Для GigaChat общий admission limit LLM-вызовов равен `1`, хотя сами DAG-workers
  продолжают работать конкурентно. Другие providers сохраняют default `8`.
- GigaChat SDK теперь делает три retry временных `429/500/502/503/504` с
  exponential backoff и jitter; `402` не повторяется.
- `failed_parent` теперь намеренно вызывает read-only `run_sql` к отсутствующей
  таблице, то есть воспроизводит настоящий tool error вместо корректного пустого
  catalog-result.
- Resume сверяет текущие transport/runtime-настройки с сохранённой
  preregistration и fail-closed отклоняет смешивание конфигураций.
- Post-fix smoke `2026-09-21 15:40 MSK` остановился до построения DAG: GigaChat
  снова вернул точный HTTP `402 Payment Required`. Ответ пришёл за 0,54 секунды,
  то есть non-retryable `402` корректно не расходовал три transient retry.

## Воспроизводимость

Полные локальные артефакты находятся в
`.test_runs/dag_concurrency_ab/20260921_140814/`: preregistration, config,
journal, transcripts, JUnit, DAG metrics, semantic verdicts, latency/tokens,
failures и итоговый report. Каталог намеренно не коммитится: transcripts могут
содержать значения из рабочей базы.

Этот журнал терминальный: после transport-исправлений продолжать его нельзя,
поскольку это смешало бы две конфигурации. После восстановления лимита GigaChat
нужен новый полный preregistered запуск:

```powershell
.\.venv\Scripts\python.exe scripts\run_dag_concurrency_ab.py `
  --db-path excel_data.db
```

До полного выполнения 48/48 и прохождения всех hard gates статус остаётся
`inconclusive`, а default — последовательным.
