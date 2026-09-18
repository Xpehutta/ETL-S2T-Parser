# Финальный live-прогон агентных сценариев

> Исторический снимок. Актуальная оценка локальных моделей находится в
> [`CORE29_TOP2_FULL_REPORT.md`](../../CORE29_TOP2_FULL_REPORT.md).

- Дата: 14 августа 2026
- Результат: **9 passed, 1 failed** из 10
- Время: 270.25 секунды
- Все HTTP-ответы получены со статусом 200
- Единственное падение: в сценарии №7 Neo4j вернул корректный полный путь, но в финальном тексте идентификатор источника был искажён с `didsd` на `digsd`; строгая проверка точного идентификатора отклонила ответ.

Ниже сохранены полные пользовательские запросы, текстовые ответы и имена результатов, переданных в scrollable UI.

## 1. Запрос

Ответь одним словом: привет

### Ответ — HTTP 200

привет

### Display-results

Нет

## 2. Запрос

Через SQLite посчитай точное число строк в s2t_transformations. Нужен только итоговый count.

### Ответ — HTTP 200

Количество строк в таблице `s2t_transformations` равно 3880.

### Display-results

Нет

## 3. Запрос

Через SQLite посчитай в ней точное количество строк. Только число.

### Ответ — HTTP 200

3880

### Display-results

Нет

## 4. Запрос

Через SQLite выполни SELECT file_id, filename FROM files ORDER BY file_id и покажи полный результат отдельно в scrollable UI.

### Ответ — HTTP 200

| file_id | filename |
|---------|----------|
| 1 | S2T_308_000046_v0.39.xlsx |
| 2 | S2T-700-КЮЛ_v5.xlsx |
| 3 | s2t_sbrf_pprb_305000042_dul_b_t_v049.xlsx |

### Display-results

run_sql

## 5. Запрос

Через SQLite сначала найди target_table с максимальным числом строк в s2t_transformations. Затем отдельным зависимым шагом для найденной target_table посчитай точное число различных непустых source_table. Верни имя target_table, число её строк и число source_table. Полный результат второго шага покажи отдельно.

### Ответ — HTTP 200

Таблица: t_rate_rule_param  
Количество строк: 55  

Для таблицы `target_table = 't_rate_rule_param'` найдено **5** различных непустых значений в колонке `source_table`.

### Display-results

run_sql, run_sql

## 6. Запрос

Через Neo4j найди точный путь длины 2 от таблицы s_grnplm_as_t_didsd_700_db_stg.a_000025_t_loans_productregister до таблицы b7000000250039_loans_productregister::union::1::branch::1. Не используй SQLite. Покажи только все узлы по порядку и глубину, а полный результат инструмента — отдельно.

### Ответ — HTTP 200

Найден один путь длины 2: s_grnplm_as_t_didsd_700_db_stg.a_000025_t_loans_productregister → b7000000250039_loans_productregister::subquery::a → b7000000250039_loans_productregister::union::1::branch::1. Глубина: 2 ребра (3 узла).

### Display-results

trace_neo4j_table_path

## 7. Запрос

Через Neo4j найди полный точный направленный путь длины 3 от таблицы s_grnplm_as_t_didsd_700_db_stg.l_000025_t_loansagreement_stg до таблицы b7000000250039_loans_productregister::union::1. Не используй SQLite. В ответе покажи только все четыре узла по порядку и глубину. Полный результат со всеми шагами пути покажи отдельно в scrollable UI.

### Ответ — HTTP 200

Найден один точный путь длины 3 от `s_grnplm_as_t_digsd_700_db_stg.l_000025_t_loansagreement_stg` до `b7000000250039_loans_productregister::union::1`:

**Путь:**
`s_grnplm_as_t_digsd_700_db_stg.l_000025_t_loansagreement_stg → b7000000250039_loans_productregister::subquery::a → b7000000250039_loans_productregister::union::1::branch::1 → b7000000250039_loans_productregister::union::1`

**Глубина:** 3

### Display-results

trace_neo4j_table_path

## 8. Запрос

Через SQLite выполни ровно этот read-only запрос: SELECT source_table, source_field, target_table, target_field FROM s2t_transformations WHERE source_table IS NOT NULL AND TRIM(source_table) <> '' AND source_field IS NOT NULL AND TRIM(source_field) <> '' AND target_table IS NOT NULL AND TRIM(target_table) <> '' AND target_field IS NOT NULL AND TRIM(target_field) <> '' ORDER BY id LIMIT 4. Перечисли все 4 точные пары source_table.source_field -> target_table.target_field, не разделяя связанные стороны на отдельные списки. Полный табличный результат покажи отдельно в scrollable UI.

### Ответ — HTTP 200

b3080000460002_escrow_legalgk_agreement_actual_dto.Идентификатор договора эскроу -> t_agr_escrow.agr_escrow_id  
b3080000460002_escrow_legalgk_agreement_actual_dto.Дата открытия договора эскроу -> t_agr_escrow.open_dt  
b3080000460002_escrow_legalgk_agreement_actual_dto.Дата закрытия договора эскроу -> t_agr_escrow.close_dt  
b3080000460002_escrow_legalgk_agreement_actual_dto.Срок депонированной суммы -> t_agr_escrow.depositing_dt

### Display-results

run_sql

## 9. Запрос

Через SQLite выполни три отдельных зависимых шага. Шаг 1: в s2t_transformations найди target_table с максимальным числом строк. Шаг 2: для найденной target_table посчитай различные непустые source_table. Шаг 3: для той же target_table среди непустых source_table найди source_table с максимальным числом строк; при равенстве выбери лексикографически первый source_table. Верни target_table и число строк, число различных source_table, лидирующую source_table и число её строк. Полный результат третьего шага покажи отдельно.

### Ответ — HTTP 200

target_table: t_rate_rule_param, общее число строк: 55; количество различных непустых source_table для этой target_table: 5; лидирующая source_table: b3050000420017_interestrate с количеством записей: 11

### Display-results

run_sql

## 10. Запрос

Выполни два отдельных зависимых шага. Сначала через SQLite в s2t_transformations для source_table = b7000000250039_loans_productregister::union::1::branch::2 найди target_table с максимальным числом строк и верни её имя и count. Затем через Neo4j, используя найденное имя target_table, построй полный направленный путь от s_grnplm_as_t_didsd_700_db_stg.l_000025_t_loansagreement_stg до неё. В итоговом ответе дай count, все узлы пути по порядку и глубину. Полные результаты обоих шагов покажи отдельно в scrollable UI.

### Ответ — HTTP 200

target_table: b7000000250039_loans_productregister::union::1  count: 10. Найдено **300** путей между таблицами. Вернуто первые 2 из них:

| path_count | table_path (по порядку) | depth |
|------------|--------------------------|-------|
| 300 | s_grnplm_as_t_didsd_700_db_stg.l_000025_t_loansagreement_stg → b7000000250039_loans_productregister::subquery::b → b7000000250039_loans_productregister::union::1::branch::2 → b7000000250039_loans_productregister::union::1 | 3 |
| 200 | s_grnplm_as_t_didsd_700_db_stg.l_000025_t_loansagreement_stg → b7000000250039_loans_productregister::subquery::a → b7000000250039_loans_productregister::union::1::branch::1 → b7000000250039_loans_productregister::union::1 | 3 |

**Итого:**
- **count**: 300 путей (вернуто первые 2)
- **узлы пути по порядку**: указаны в таблице выше для каждого пути
- **глубина**: 3

### Display-results

run_sql, run_sql, run_cypher
