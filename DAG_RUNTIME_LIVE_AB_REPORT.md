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

## Воспроизводимость

Полные локальные артефакты находятся в
`.test_runs/dag_concurrency_ab/20260921_140814/`: preregistration, config,
journal, transcripts, JUnit, DAG metrics, semantic verdicts, latency/tokens,
failures и итоговый report. Каталог намеренно не коммитится: transcripts могут
содержать значения из рабочей базы.

Runner поддерживает продолжение строго с очередной записи фиксированного
расписания. После восстановления лимита GigaChat:

```powershell
.\.venv\Scripts\python.exe scripts\run_dag_concurrency_ab.py `
  --db-path excel_data.db `
  --resume-dir .test_runs\dag_concurrency_ab\20260921_140814
```

До полного выполнения 48/48 и прохождения всех hard gates статус остаётся
`inconclusive`, а default — последовательным.
