# Добавленные возможности и статус 9 основных Word-требований

Дата: 2026-09-15

## Итог

В версию добавлены и проверены следующие возможности:

- отдельный `sql_risk_scope`: LLM извлекает режим и точный scope,
  exact readers получают данные, SQLGlot формирует нейтральную структуру, а
  второй LLM-вызов делает вывод о риске и формирует ответ;
- обязательный LLM-review полноты и ролей для `validation_protocol` с одной
  ограниченной попыткой исправления контракта;
- точные field-level S2T readers и `run_sql` доступны worker сразу;
- role-aware обработка истории, независимые lineage-чтения и более строгая
  evidence-проверка производных выводов;
- улучшенная проверка пользовательского display и физических идентификаторов;
- полный offline suite: **1036 passed, 81 skipped, 3 warnings**.

Все семь основных Word-сценариев прошли доступный итоговый gate: №1–6
подтверждены live, №7 полностью прошёл offline на старой БД после исправления
extraction boundary. Повторный post-fix live-вызов №7 выполнить не удалось:
доступные токены GigaChat закончились (`402 Payment Required`). Поэтому это
честный результат **7/7 по совокупной проверке**, но не заявление «7/7 live».

## Добавленные улучшения по результатам проверок

- `run_sql` добавлен в начальную model-selected worker palette: точный SQLite
  count больше не зависит от предварительного ошибочного reroute.
- Exact `list_s2t_source_field` и `list_s2t_target_field` доступны сразу:
  запрос одного поля не обязан анализировать широкий preview таблицы.
- Supervisor получает настоящие role-aware user/assistant messages. Текст
  assistant больше не может подменить отложенный или запрещённый user choice.
- Для lineage один endpoint планируется как один полный path-read; сравнение
  двух endpoint делает два независимых чтения. Current task идёт после lazy
  references и не заменяется результатом другого endpoint.
- Semantic judge различает физический identifier и общий технический термин
  (`PK`, `UNIQUE`, SQL-конструкции), а payload явно описывает отдельный
  scrollable display как видимую пользователю часть ответа.
- Для производной метрики или разности upstream обязан иметь evidence каждого
  операнда; отсутствие второго набора не означает пустое множество или ноль.
- Для validation contract добавлен отдельный native LLM-review полноты и ролей.
  Код не ищет filename, arrow-пару или checks в natural language.

## Статус 9 основных Word-требований

| № | Требование | Текущее доказательство | Статус |
|---:|---|---|---|
| 1 | Nullable compatibility | Последний достоверный exact-prompt live: source `not_null=0`, target `not_null=1`, корректный вывод о несовместимости. Старая БД подтверждает 0→1. | Live PASS |
| 2 | Type compatibility | Exact pair-reader подтвердил `uuid → uuid`; ответ о совместимости корректен. | Live PASS |
| 3 | Duplicate risk | Exact historical live корректно отделил возможный `LEFT JOIN` fan-out от неподтверждённых фактических дублей/уникальности. Judge-классификация `PK/UNIQUE` исправлена offline. | Live PASS для исходного prompt; judge fix offline |
| 4 | Mandatory fields without mapping | Последний exact historical replay вернул доказанные `1 / 0 / []`: обязательный `optn_id` имеет mapping. Ранее наблюдалась стохастическая ошибка, поэтому сценарий остаётся важным для следующего smoke. | Последний live PASS |
| 5 | Row-loss risk | Exact historical live прочитал сохранённое правило и корректно разобрал отсутствие потери строк в исходном Word-case. Более строгий текущий oracle с реальным predicate также проходил live. | Live PASS |
| 6 | Transformation explanation | Exact S2T evidence и полный SQL подтверждают прямой `object_id_uid → optn_id`, JOIN и filter semantics; исходный prompt прошёл live. | Live PASS |
| 7 | SQL test protocol | Последний exact live до исправления дал `partial_protocol`, потому extraction потерял filename. Сейчас обязательный LLM-review ловит schema-valid потерю file/load/check/role и запускает один LLM repair. На старой БД downstream даёт `file_id=3`, exact reads `9/10/10`, protocol `ready`, 4/4 checks `ready`, 7/7 preflight `pass`, 4/4 SQL parse. Post-fix live не повторён, потому закончились доступные токены GigaChat. | **PASS (offline)** |
| 8 | Сохранить все исходные S2T-строки, включая дубли и пустой target field | Writer regression сохраняет одинаковые строки с разными `row_num`; пустой target field остаётся `NULL`, содержательные строки не дедуплицируются. | Offline PASS |
| 9 | Отчёт по пустым target columns на каждом листе | Upload pipeline проверяет общий и sheet-level count и сохранённые `target_field=NULL`; API/UI regression также проходит. | Offline PASS |

## Критический сценарий №7

Новый validation flow:

```text
operation router
→ LLM RawTestProtocolContract extraction
→ LLM completeness/role review
→ при repair: один повтор extraction и повтор review
→ exact file/source/target resolution
→ dependency-based exact readers
→ SQLGlot normalization + declarative Phase 0–3 compiler
→ answer
```

Review имеет закрытые issue codes для потерянных или лишних
file/table/load/check, неверных mode/key, filename в table-role,
неразделённой пары и перепутанных source/target ролей.
Код только валидирует typed review и управляет двумя ограниченными попытками;
семантическую полноту исходной задачи оценивает LLM. Повторный reject, malformed
review или provider error возвращает `missing_parameter`/`unavailable` с
`silent_fallback=false`; readers и workers не запускаются.

Offline end-to-end покрывает:

- дословный исторический Word prompt;
- второй prompt с другими filename/table/field и иной формулировкой;
- exact resolution файла, source и target;
- необходимые `s2t_pair`, `s2t_target`, `target_column_catalog` readers;
- четыре `ready` checks с непустыми goal/SQL/pass criterion;
- SQLGlot parse каждого шаблона после подстановки scope placeholders.

## Проверенная старая БД и live-артефакты

- БД: `.test_runs/ultra-46/20260914_full/fixture.db`.
- SHA256: `c18e84479a1358a1f98266d0922f2da2140e634bbf791c9b9ffa22aae89c7b08`.
- `PRAGMA quick_check = ok`; `files = 3`; `s2t_transformations = 3880`.
- Exact historical harness SHA256:
  `bde81b40b53b09c6e40efa6d7f4a1e206acd9d952d10ac5ae60122486876cb2d`.
- Replay transcript:
  `.test_runs/historical-original-46-current/historical_5757_prompts_current_code.md`.
- Replay JUnit:
  `.test_runs/historical-original-46-current/historical_5757_prompts_current_code.xml`.

Полный historical replay нельзя интерпретировать как продуктовые `17/46`: три
Neo4j-сценария корректно skipped, а начиная с позиции 25 оставшиеся 22
сценария остановлены `402 Payment Required` после исчерпания доступных токенов
GigaChat. До исчерпания баланса основные Word №1–6 прошли; №7 выявил описанную
выше потерю filename на extraction boundary и затем прошёл исправленный offline
gate на той же старой БД.

## Проверки текущего дерева

- `pytest tests/ -q -o addopts= --tb=short`:
  **1036 passed, 81 skipped, 3 collection warnings**.
- Targeted validation/review/Word-7 release gate: **112 passed**.
- `git diff --check`: clean.
- `python -m compileall -q agents scripts tests`: clean.

## Публикация

Функциональные изменения и этот отчёт опубликованы в ветке `main`, начиная с
коммита `fa0f05d`. Итог по семи основным Word-сценариям: **7/7 прошли совокупный gate:
6/7 live + №7 offline после исправления**. Повторный live №7 остаётся желательной
проверкой после пополнения токенов, но его отсутствие явно зафиксировано и не
выдаётся за live-результат. Дополнительно upload-требования №8–9 прошли offline.
