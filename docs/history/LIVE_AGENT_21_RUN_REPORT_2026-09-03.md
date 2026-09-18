# Отчёт по 21 live-прогону основных S2T-сценариев

> Исторический снимок отдельной экспериментальной ветки. Актуальная оценка
> локальных моделей находится в [`CORE29_TOP2_FULL_REPORT.md`](../../CORE29_TOP2_FULL_REPORT.md).

Дата: 3 сентября 2026 года.

- Модель агента: `GigaChat-3-Ultra`.
- Модель LLM-as-judge: `GigaChat-2-Pro`.
- Режим: `multiagent`, реальные последовательные HTTP-запросы к `/chat`.
- Код: ветка `codex/word17-typed-protocol-exp`.
- В итог включены ровно 21 пригодная попытка: по три на каждый из семи сценариев.
- В сценариях 1, 4 и 7 публичный запрос теперь содержит имя файла, а не внутренний `file_id`.

## Итог

- Полный semantic pass по judge: **18/21**.
- Ручной semantic pass по полному ответу и display-evidence: **21/21**.
- Стабильно полностью проходят **7/7 сценариев**, каждый по 3/3.
- В сценарии обязательных полей все три ответа содержат правильные счётчики
  `1 / 0`. Отсутствие отдельно напечатанного пустого списка при нулевом счётчике
  считается только особенностью представления, а не содержательной ошибкой.
- Суммарно: 463 418 токенов, 232 LLM-вызова, 446.489 секунды agent time.

| № | Сценарий | Judge | Ручная семантика |
|---:|---|---:|---:|
| 1 | Совместимость nullable-ограничений | 3/3 | 3/3 |
| 2 | Совместимость типов | 3/3 | 3/3 |
| 3 | Риск дубликатов | 3/3 | 3/3 |
| 4 | Обязательные поля без S2T-маппинга | 0/3 | 3/3 |
| 5 | Риск потери строк | 3/3 | 3/3 |
| 6 | Объяснение трансформации | 3/3 | 3/3 |
| 7 | Тест-протокол ETL-загрузки | 3/3 | 3/3 |

## Состав трёх циклов

- Сценарии 1, 4 и 7: новые filename-прогоны `104848`, `110234`, `110426`.
- Сценарии 2, 3, 5 и 6: сохранены пригодные результаты исходного трёхкратного прогона.
- Для сценария 2 в третьем цикле сохранена чистая retry-попытка, заменившая прежний HTTP 500.

## Исключённая попытка

Предварительная попытка `104008` не включена в итог из-за неверного файлового scope. Вместо неё использован следующий пригодный полный цикл.

## Дословные запросы и ответы

### Цикл 1, сценарий 1: Совместимость nullable-ограничений

Источник: `filename-cycle-1 #1`.
HTTP: `200`. Время агента: `28.177` с. LLM-вызовы: `15`. Токены: `25122`.
Display-results: `get_source_target_column_pair`.

Дословный запрос:

~~~~text
Для файла 's2t_sbrf_pprb_305000042_dul_b_t_v049.xlsx' оцени совместимость nullable-ограничений b3050000420005_paymentdetails.object_id_uid → t_optn.optn_id. Верни source_not_null=<0|1>, target_not_null=<0|1> и вывод.
~~~~

Полный публичный ответ модели:

~~~~text
source_not_null=0, target_not_null=1, вывод: несовместимо — целевой столбец NOT NULL при допускающем NULL источнике
~~~~

LLM-as-judge: **passed**.

Дословное заключение judge:

~~~~text
Answer и display_results корректно отражают nullable-ограничения для пары столбцов, делают вывод о несовместимости и предоставляют все запрошенные значения: source_not_null, target_not_null и текстовый вывод.
~~~~

Ручной вывод: **passed** — запрос выполнен полностью; факты и вывод соответствуют evidence.

### Цикл 1, сценарий 2: Совместимость типов

Источник: `legacy run1 #2`.
HTTP: `200`. Время агента: `14.845` с. LLM-вызовы: `9`. Токены: `14713`.
Display-results: `get_source_target_column_pair`.

Дословный запрос:

~~~~text
Для file_id=3 оцени совместимость типов b3050000420005_paymentdetails.object_id_uid → t_optn.optn_id. Верни source_data_type=<тип>, target_data_type=<тип> и вывод.
~~~~

Полный публичный ответ модели:

~~~~text
source_data_type=uuid, target_data_type=uuid, вывод=Совместимы
~~~~

LLM-as-judge: **passed**.

Дословное заключение judge:

~~~~text
Answer и display_results точно соответствуют запросу: указаны source_data_type и target_data_type как uuid, вывод о совместимости корректен, display содержит запрошенные source и target колонки с file_id=3 и их типы.
~~~~

Ручной вывод: **passed** — запрос выполнен полностью; факты и вывод соответствуют evidence.

### Цикл 1, сценарий 3: Риск дубликатов

Источник: `legacy run1 #3`.
HTTP: `200`. Время агента: `3.546` с. LLM-вызовы: `3`. Токены: `2382`.
Display-results: `read_s2t_by_target_table`.

Дословный запрос:

~~~~text
Оцени риск появления дубликатов при сохранённой S2T-трансформации b3050000420005_paymentdetails → t_optn.
~~~~

Полный публичный ответ модели:

~~~~text
S2T-анализ sources [`b3050000420005_paymentdetails`] → targets [`t_optn`]. Физические данные логических ETL-таблиц не запрашивались.

Target `t_optn`

Риск дубликатов: Найден потенциальный риск размножения строк: кардинальность одного или нескольких JOIN неизвестна.
Основания: LEFT OUTER JOIN "$$305stg".s305_0015_technicalservice AS technicalservice; cardinality=unknown
Ограничения: Фактические дубли без физических данных неизвестны.
~~~~

LLM-as-judge: **passed**.

Дословное заключение judge:

~~~~text
Query выполнен полностью: указан анализ S2T-пути, явно оценен риск дубликатов с обоснованием (LEFT OUTER JOIN с unknown cardinality), перечислены source и target таблицы, приведён transformation_rule. Дополнительные физические данные не запрашивались, что соответствует условиям.
~~~~

Ручной вывод: **passed** — запрос выполнен полностью; факты и вывод соответствуют evidence.

### Цикл 1, сценарий 4: Обязательные поля без S2T-маппинга

Источник: `filename-cycle-1 #2`.
HTTP: `200`. Время агента: `107.789` с. LLM-вызовы: `53`. Токены: `164282`.
Display-results: `Нет`.

Дословный запрос:

~~~~text
Для файла 's2t_sbrf_pprb_305000042_dul_b_t_v049.xlsx' найди обязательные поля t_optn без сохранённого S2T-маппинга. Верни mandatory_fields_count=<число>, mandatory_fields_without_mapping_count=<число> и имена полей без маппинга.
~~~~

Полный публичный ответ модели:

~~~~text
mandatory_fields_count=1, mandatory_fields_without_mapping_count=0
~~~~

LLM-as-judge: **failed**.

Дословное заключение judge:

~~~~text
Отсутствует запрос имен полей без маппинга, хотя это прямо указано в query.
~~~~

Ручной вывод: **partial** — оба числа корректны, но явно запрошенный пустой список имён не напечатан как `fields_without_mapping=[]`.

### Цикл 1, сценарий 5: Риск потери строк

Источник: `legacy run1 #5`.
HTTP: `200`. Время агента: `3.599` с. LLM-вызовы: `3`. Токены: `2377`.
Display-results: `read_s2t_by_target_table`.

Дословный запрос:

~~~~text
Оцени риск потери строк в сохранённой S2T-трансформации b3050000420005_paymentdetails → t_optn.
~~~~

Полный публичный ответ модели:

~~~~text
S2T-анализ sources [`b3050000420005_paymentdetails`] → targets [`t_optn`]. Физические данные логических ETL-таблиц не запрашивались.

Target `t_optn`

Риск потери строк: Структурный риск удаления исходных строк не найден: нет WHERE/HAVING и непредохраняющих JOIN.
Основания: LEFT OUTER JOIN "$$305stg".s305_0015_technicalservice AS technicalservice сохраняет левые строки
~~~~

LLM-as-judge: **passed**.

Дословное заключение judge:

~~~~text
Query выполнен полностью: риск потери строк в S2T-трансформации оценен, указаны основания, приведён полный S2T-каталог по целевой таблице.
~~~~

Ручной вывод: **passed** — запрос выполнен полностью; факты и вывод соответствуют evidence.

### Цикл 1, сценарий 6: Объяснение трансформации

Источник: `legacy run1 #6`.
HTTP: `200`. Время агента: `19.501` с. LLM-вызовы: `9`. Токены: `22627`.
Display-results: `read_s2t_source_to_target`.

Дословный запрос:

~~~~text
Объясни сохранённую S2T-трансформацию b3050000420005_paymentdetails.object_id_uid → t_optn.optn_id.
~~~~

Полный публичный ответ модели:

~~~~text
Трансформация b3050000420005_paymentdetails.object_id_uid → t_optn.optn_id реализована следующим SQL-правилом:

select
  b.*
  , technicalservice.product_entityid_uid
from
  $$305stg.b3050000420005_paymentdetails as b

  left outer join $$305stg.s305_0015_technicalservice as technicalservice
  on true
   and b.technicalservice_id = technicalservice.object_id
   and upper(technicalservice.ctl_action) <> 'D'

where 1 = 1

Пояснение по ключевым элементам правила:
- Источник: слой B, таблица $$305stg.b3050000420005_paymentdetails (алиас b).
- Приёмник: слой T, таблица t_optn; поле optn_id заполняется из source-поля object_id_uid.
- JOIN с s305_0015_technicalservice выполняется LEFT OUTER по условию b.technicalservice_id = technicalservice.object_id и исключая удалённые записи источника через upper(technicalservice.ctl_action) <> 'D'. Предикат ON TRUE делает присоединение фактически безусловным до фильтра по техническим ключу и ctl_action.
- WHERE 1=1 не фильтрует строки.
- Выбранные поля включают все колонки b.* плюс product_entityid_uid из справочника технических услуг; это расширяет набор атрибутов для downstream-проекции в t_optn.
- Отсутствуют явные преобразования значения object_id_uid (нет CAST/выражений), поэтому значение переносится как есть.

Следствие: каждая строка paymentdetails даёт одну или несколько проекций в t_optn — без размножения при отсутствии соответствующего технического сервиса (LEFT JOIN сохраняет левую строку); размножение возможно только если справа окажется несколько совпадающих rows после условия соединения.
~~~~

LLM-as-judge: **passed**.

Дословное заключение judge:

~~~~text
Answer детально объясняет запрошенную S2T-трансформацию с описанием SQL-правила, роли полей и логики соединения. Display_results содержит таблицу соответствий с явным указанием source_table → target_table и source_field → target_field, включая запрошенную пару object_id_uid → optn_id. Все части запроса выполнены корректно.
~~~~

Ручной вывод: **passed** — запрос выполнен полностью; факты и вывод соответствуют evidence.

### Цикл 1, сценарий 7: Тест-протокол ETL-загрузки

Источник: `filename-cycle-1 #3`.
HTTP: `200`. Время агента: `6.984` с. LLM-вызовы: `3`. Токены: `1318`.
Display-results: `read_s2t_source_to_target, list_target_column_catalog`.

Дословный запрос:

~~~~text
Для файла 's2t_sbrf_pprb_305000042_dul_b_t_v049.xlsx' по сохранённой S2T-спецификации b3050000420005_paymentdetails → t_optn составь тест-протокол для проверки ETL-загрузки во внешней СУБД. Включи проверки количества строк, уникальности ключа, null-rate обязательных полей и корректности трансформаций. Для каждой проверки дай цель, SQL-шаблон и критерий прохождения. Используй подтверждённые таблицы, колонки и правила; фактические метрики не вычисляй.
~~~~

Полный публичный ответ модели:

~~~~text
Тест-протокол проверки ETL-загрузки во внешней Greenplum СУБД для sources [`b3050000420005_paymentdetails`] → targets [`t_optn`]. SQL-шаблоны не исполнялись; фактические метрики не вычислялись. Во всех target-side запросах замени `{{LOAD_SCOPE_PREDICATE}}` условием проверяемой загрузки либо `TRUE` для полного снимка.

Load sources [`b3050000420005_paymentdetails`] → Target `t_optn`

1. Проверка количества строк
Цель: Сравнить количество ожидаемых и загруженных строк.
SQL-шаблон:
```sql
WITH expected_raw AS (
select
 b.*
 , technicalservice.product_entityid_uid
from
 $$305stg.b3050000420005_paymentdetails as b
 
 left outer join $$305stg.s305_0015_technicalservice as technicalservice
 on true
  and b.technicalservice_id = technicalservice.object_id
  and upper(technicalservice.ctl_action) <> 'D'
  
where 1 = 1
),
expected AS (
    SELECT
        src."object_id_uid" AS "optn_id",
        src."valuedate" AS "start_dt",
        src."product_entityid_uid" AS "agr_dep_id",
        src."technicalservice_id_uid" AS "bus_srv_id",
        src."currencycode_uid" AS "crncy_id",
        src."object_id" AS "host_optn_id",
        src."amount" AS "optn_amt",
        src."amountrub" AS "optn_amt_rub",
        src."operationtypecode_uid" AS "optn_type_id",
        src."registerid_uid" AS "registry_id"
    FROM expected_raw AS src
)
SELECT
    (SELECT COUNT(*) FROM expected) AS expected_row_count,
    (SELECT COUNT(*) FROM "t_optn" WHERE {{LOAD_SCOPE_PREDICATE}}) AS actual_row_count;
```
Критерий прохождения: expected_row_count = actual_row_count.

2. Проверка уникальности ключа
Цель: Проверить уникальность подтверждённого target-ключа.
SQL-шаблон:
```sql
SELECT "optn_id", COUNT(*) AS duplicate_count
FROM "t_optn"
WHERE {{LOAD_SCOPE_PREDICATE}}
GROUP BY "optn_id"
HAVING COUNT(*) > 1;
```
Критерий прохождения: Запрос не возвращает строк.

3. Проверка null-rate обязательных полей
Цель: Проверить NULL в обязательных target-полях.
SQL-шаблон:
```sql
SELECT
    COUNT(*) AS total_rows,
    COUNT(*) FILTER (WHERE "optn_id" IS NULL) AS "optn_id_null_count"
FROM "t_optn"
WHERE {{LOAD_SCOPE_PREDICATE}};
```
Критерий прохождения: Каждый *_null_count равен 0; null-rate каждого обязательного поля равен 0.

4. Проверка корректности трансформаций
Цель: Сравнить ожидаемую S2T-проекцию с target.
SQL-шаблон:
```sql
WITH expected_raw AS (
select
 b.*
 , technicalservice.product_entityid_uid
from
 $$305stg.b3050000420005_paymentdetails as b
 
 left outer join $$305stg.s305_0015_technicalservice as technicalservice
 on true
  and b.technicalservice_id = technicalservice.object_id
  and upper(technicalservice.ctl_action) <> 'D'
  
where 1 = 1
),
expected AS (
    SELECT
        src."object_id_uid" AS "optn_id",
        src."valuedate" AS "start_dt",
        src."product_entityid_uid" AS "agr_dep_id",
        src."technicalservice_id_uid" AS "bus_srv_id",
        src."currencycode_uid" AS "crncy_id",
        src."object_id" AS "host_optn_id",
        src."amount" AS "optn_amt",
        src."amountrub" AS "optn_amt_rub",
        src."operationtypecode_uid" AS "optn_type_id",
        src."registerid_uid" AS "registry_id"
    FROM expected_raw AS src
),
actual AS (
    SELECT "optn_id", "start_dt", "agr_dep_id", "bus_srv_id", "crncy_id", "host_optn_id", "optn_amt", "optn_amt_rub", "optn_type_id", "registry_id"
    FROM "t_optn"
    WHERE {{LOAD_SCOPE_PREDICATE}}
),
differences AS (
    (SELECT * FROM expected EXCEPT ALL SELECT * FROM actual)
    UNION ALL
    (SELECT * FROM actual EXCEPT ALL SELECT * FROM expected)
)
SELECT COUNT(*) AS difference_count
FROM differences;
```
Критерий прохождения: difference_count = 0.

Подтверждённые основания: S2T sources: ['b3050000420005_paymentdetails', 'b3050000420015_technicalservice']; mapped target fields: ['optn_id', 'start_dt', 'agr_dep_id', 'bus_srv_id', 'crncy_id', 'host_optn_id', 'optn_amt', 'optn_amt_rub', 'optn_type_id', 'registry_id']; primary key fields: ['optn_id']; required fields: ['optn_id']
~~~~

LLM-as-judge: **passed**.

Дословное заключение judge:

~~~~text
Ответ содержит полный тест-протокол с целями, SQL-шаблонами и критериями прохождения для всех запрошенных проверок: количество строк, уникальность ключа, null-rate обязательных полей и корректность трансформаций. Указаны подтверждённые таблицы, поля и правила. Display_results предоставляют необходимую детализацию по source→target отображениям и целевым колонкам, что соответствует запросу.
~~~~

Ручной вывод: **passed** — запрос выполнен полностью; факты и вывод соответствуют evidence.

### Цикл 2, сценарий 1: Совместимость nullable-ограничений

Источник: `filename-cycle-2 #1`.
HTTP: `200`. Время агента: `35.677` с. LLM-вызовы: `15`. Токены: `27772`.
Display-results: `get_source_target_column_pair`.

Дословный запрос:

~~~~text
Для файла 's2t_sbrf_pprb_305000042_dul_b_t_v049.xlsx' оцени совместимость nullable-ограничений b3050000420005_paymentdetails.object_id_uid → t_optn.optn_id. Верни source_not_null=<0|1>, target_not_null=<0|1> и вывод.
~~~~

Полный публичный ответ модели:

~~~~text
source_not_null=0, target_not_null=1, вывод: несовместимо — целевой столбец NOT NULL при допускающем NULL источнике
~~~~

LLM-as-judge: **passed**.

Дословное заключение judge:

~~~~text
Answer и display_results корректно отражают nullable-ограничения для пары столбцов, правильно определяют source_not_null=0, target_not_null=1 и делают верный вывод о несовместимости. Все запрошенные части выполнены.
~~~~

Ручной вывод: **passed** — запрос выполнен полностью; факты и вывод соответствуют evidence.

### Цикл 2, сценарий 2: Совместимость типов

Источник: `legacy run2 #2`.
HTTP: `200`. Время агента: `14.277` с. LLM-вызовы: `9`. Токены: `11758`.
Display-results: `get_source_target_column_pair`.

Дословный запрос:

~~~~text
Для file_id=3 оцени совместимость типов b3050000420005_paymentdetails.object_id_uid → t_optn.optn_id. Верни source_data_type=<тип>, target_data_type=<тип> и вывод.
~~~~

Полный публичный ответ модели:

~~~~text
source_data_type=uuid, target_data_type=uuid, вывод=Совместимы
~~~~

LLM-as-judge: **passed**.

Дословное заключение judge:

~~~~text
Answer и display_results точно соответствуют запросу: указаны source_data_type и target_data_type как uuid, вывод о совместимости сделан корректно. Display содержит запрошенную пару столбцов с file_id=3 и их типы данных.
~~~~

Ручной вывод: **passed** — запрос выполнен полностью; факты и вывод соответствуют evidence.

### Цикл 2, сценарий 3: Риск дубликатов

Источник: `legacy run2 #3`.
HTTP: `200`. Время агента: `4.461` с. LLM-вызовы: `3`. Токены: `2382`.
Display-results: `read_s2t_by_target_table`.

Дословный запрос:

~~~~text
Оцени риск появления дубликатов при сохранённой S2T-трансформации b3050000420005_paymentdetails → t_optn.
~~~~

Полный публичный ответ модели:

~~~~text
S2T-анализ sources [`b3050000420005_paymentdetails`] → targets [`t_optn`]. Физические данные логических ETL-таблиц не запрашивались.

Target `t_optn`

Риск дубликатов: Найден потенциальный риск размножения строк: кардинальность одного или нескольких JOIN неизвестна.
Основания: LEFT OUTER JOIN "$$305stg".s305_0015_technicalservice AS technicalservice; cardinality=unknown
Ограничения: Фактические дубли без физических данных неизвестны.
~~~~

LLM-as-judge: **passed**.

Дословное заключение judge:

~~~~text
Query выполнен полностью: анализ S2T-трансформации b3050000420005_paymentdetails → t_optn проведён, риск дубликатов оценён корректно с указанием причины (LEFT OUTER JOIN с unknown cardinality), ограничения сформулированы. Display содержит релевантные детали трансформации.
~~~~

Ручной вывод: **passed** — запрос выполнен полностью; факты и вывод соответствуют evidence.

### Цикл 2, сценарий 4: Обязательные поля без S2T-маппинга

Источник: `filename-cycle-2 #2`.
HTTP: `200`. Время агента: `38.474` с. LLM-вызовы: `21`. Токены: `48757`.
Display-results: `Нет`.

Дословный запрос:

~~~~text
Для файла 's2t_sbrf_pprb_305000042_dul_b_t_v049.xlsx' найди обязательные поля t_optn без сохранённого S2T-маппинга. Верни mandatory_fields_count=<число>, mandatory_fields_without_mapping_count=<число> и имена полей без маппинга.
~~~~

Полный публичный ответ модели:

~~~~text
mandatory_fields_count=1, mandatory_fields_without_mapping_count=0
~~~~

LLM-as-judge: **failed**.

Дословное заключение judge:

~~~~text
Отсутствует запрос имен полей без маппинга, хотя это прямо указано в query.
~~~~

Ручной вывод: **partial** — оба числа корректны, но явно запрошенный пустой список имён не напечатан как `fields_without_mapping=[]`.

### Цикл 2, сценарий 5: Риск потери строк

Источник: `legacy run2 #5`.
HTTP: `200`. Время агента: `3.768` с. LLM-вызовы: `3`. Токены: `821`.
Display-results: `read_s2t_by_target_table`.

Дословный запрос:

~~~~text
Оцени риск потери строк в сохранённой S2T-трансформации b3050000420005_paymentdetails → t_optn.
~~~~

Полный публичный ответ модели:

~~~~text
S2T-анализ sources [`b3050000420005_paymentdetails`] → targets [`t_optn`]. Физические данные логических ETL-таблиц не запрашивались.

Target `t_optn`

Риск потери строк: Структурный риск удаления исходных строк не найден: нет WHERE/HAVING и непредохраняющих JOIN.
Основания: LEFT OUTER JOIN "$$305stg".s305_0015_technicalservice AS technicalservice сохраняет левые строки
~~~~

LLM-as-judge: **passed**.

Дословное заключение judge:

~~~~text
Query требует оценки риска потери строк в S2T-трансформации b3050000420005_paymentdetails → t_optn. Ответ содержит структурный анализ трансформации, явно указывает на отсутствие риска потери строк с обоснованием через тип JOIN и условия. Display_results предоставляет детализацию S2T-преобразований, включая правило трансформации, что подтверждает полноту ответа.
~~~~

Ручной вывод: **passed** — запрос выполнен полностью; факты и вывод соответствуют evidence.

### Цикл 2, сценарий 6: Объяснение трансформации

Источник: `legacy run2 #6`.
HTTP: `200`. Время агента: `19.840` с. LLM-вызовы: `9`. Токены: `20273`.
Display-results: `read_s2t_source_to_target`.

Дословный запрос:

~~~~text
Объясни сохранённую S2T-трансформацию b3050000420005_paymentdetails.object_id_uid → t_optn.optn_id.
~~~~

Полный публичный ответ модели:

~~~~text
Трансформация b3050000420005_paymentdetails.object_id_uid → t_optn.optn_id реализована следующим SQL-правилом:

select
  b.*
  , technicalservice.product_entityid_uid
from
  $$305stg.b3050000420005_paymentdetails as b

  left outer join $$305stg.s305_0015_technicalservice as technicalservice
    on true
     and b.technicalservice_id = technicalservice.object_id
     and upper(technicalservice.ctl_action) <> 'D'

where 1 = 1;

Пояснения по ключевым элементам правила:
- Источник: слой B, таблица b3050000420005_paymentdetails; поле object_id_uid.
- Цель: слой T, таблица t_optn; поле optn_id.
- В SELECT включено всё из источника (b.*), что обеспечивает прямое маппинг-поле source_field→target_field без преобразований для данной пары.
- LEFT JOIN с s305_0015_technicalservice не фильтрует строки источника и используется только для обогащения данными технического сервиса; он не влияет на заполнение поля optn_id в целевой таблице.
- Фильтр where 1=1 — нейтральный предикат, фактической фильтрации нет.
- Дополнительное условие UPPER(technicalservice.ctl_action) <> 'D' относится к правой таблице соединения и исключает помеченные на удаление записи технического сервиса при обогащении.
~~~~

LLM-as-judge: **passed**.

Дословное заключение judge:

~~~~text
Answer объясняет сохранённую S2T-трансформацию с указанием SQL-правила и подробными пояснениями по ключевым элементам. Display_results содержит таблицу с mapping'ами source→target, включая запрошенную пару object_id_uid → optn_id, что полностью удовлетворяет запросу.
~~~~

Ручной вывод: **passed** — запрос выполнен полностью; факты и вывод соответствуют evidence.

### Цикл 2, сценарий 7: Тест-протокол ETL-загрузки

Источник: `filename-cycle-2 #3`.
HTTP: `200`. Время агента: `4.622` с. LLM-вызовы: `3`. Токены: `2288`.
Display-results: `read_s2t_source_to_target, list_target_column_catalog`.

Дословный запрос:

~~~~text
Для файла 's2t_sbrf_pprb_305000042_dul_b_t_v049.xlsx' по сохранённой S2T-спецификации b3050000420005_paymentdetails → t_optn составь тест-протокол для проверки ETL-загрузки во внешней СУБД. Включи проверки количества строк, уникальности ключа, null-rate обязательных полей и корректности трансформаций. Для каждой проверки дай цель, SQL-шаблон и критерий прохождения. Используй подтверждённые таблицы, колонки и правила; фактические метрики не вычисляй.
~~~~

Полный публичный ответ модели:

~~~~text
Тест-протокол проверки ETL-загрузки во внешней Greenplum СУБД для sources [`b3050000420005_paymentdetails`] → targets [`t_optn`]. SQL-шаблоны не исполнялись; фактические метрики не вычислялись. Во всех target-side запросах замени `{{LOAD_SCOPE_PREDICATE}}` условием проверяемой загрузки либо `TRUE` для полного снимка.

Load sources [`b3050000420005_paymentdetails`] → Target `t_optn`

1. Проверка количества строк
Цель: Сравнить количество ожидаемых и загруженных строк.
SQL-шаблон:
```sql
WITH expected_raw AS (
select
 b.*
 , technicalservice.product_entityid_uid
from
 $$305stg.b3050000420005_paymentdetails as b
 
 left outer join $$305stg.s305_0015_technicalservice as technicalservice
 on true
  and b.technicalservice_id = technicalservice.object_id
  and upper(technicalservice.ctl_action) <> 'D'
  
where 1 = 1
),
expected AS (
    SELECT
        src."object_id_uid" AS "optn_id",
        src."valuedate" AS "start_dt",
        src."product_entityid_uid" AS "agr_dep_id",
        src."technicalservice_id_uid" AS "bus_srv_id",
        src."currencycode_uid" AS "crncy_id",
        src."object_id" AS "host_optn_id",
        src."amount" AS "optn_amt",
        src."amountrub" AS "optn_amt_rub",
        src."operationtypecode_uid" AS "optn_type_id",
        src."registerid_uid" AS "registry_id"
    FROM expected_raw AS src
)
SELECT
    (SELECT COUNT(*) FROM expected) AS expected_row_count,
    (SELECT COUNT(*) FROM "t_optn" WHERE {{LOAD_SCOPE_PREDICATE}}) AS actual_row_count;
```
Критерий прохождения: expected_row_count = actual_row_count.

2. Проверка уникальности ключа
Цель: Проверить уникальность подтверждённого target-ключа.
SQL-шаблон:
```sql
SELECT "optn_id", COUNT(*) AS duplicate_count
FROM "t_optn"
WHERE {{LOAD_SCOPE_PREDICATE}}
GROUP BY "optn_id"
HAVING COUNT(*) > 1;
```
Критерий прохождения: Запрос не возвращает строк.

3. Проверка null-rate обязательных полей
Цель: Проверить NULL в обязательных target-полях.
SQL-шаблон:
```sql
SELECT
    COUNT(*) AS total_rows,
    COUNT(*) FILTER (WHERE "optn_id" IS NULL) AS "optn_id_null_count"
FROM "t_optn"
WHERE {{LOAD_SCOPE_PREDICATE}};
```
Критерий прохождения: Каждый *_null_count равен 0; null-rate каждого обязательного поля равен 0.

4. Проверка корректности трансформаций
Цель: Сравнить ожидаемую S2T-проекцию с target.
SQL-шаблон:
```sql
WITH expected_raw AS (
select
 b.*
 , technicalservice.product_entityid_uid
from
 $$305stg.b3050000420005_paymentdetails as b
 
 left outer join $$305stg.s305_0015_technicalservice as technicalservice
 on true
  and b.technicalservice_id = technicalservice.object_id
  and upper(technicalservice.ctl_action) <> 'D'
  
where 1 = 1
),
expected AS (
    SELECT
        src."object_id_uid" AS "optn_id",
        src."valuedate" AS "start_dt",
        src."product_entityid_uid" AS "agr_dep_id",
        src."technicalservice_id_uid" AS "bus_srv_id",
        src."currencycode_uid" AS "crncy_id",
        src."object_id" AS "host_optn_id",
        src."amount" AS "optn_amt",
        src."amountrub" AS "optn_amt_rub",
        src."operationtypecode_uid" AS "optn_type_id",
        src."registerid_uid" AS "registry_id"
    FROM expected_raw AS src
),
actual AS (
    SELECT "optn_id", "start_dt", "agr_dep_id", "bus_srv_id", "crncy_id", "host_optn_id", "optn_amt", "optn_amt_rub", "optn_type_id", "registry_id"
    FROM "t_optn"
    WHERE {{LOAD_SCOPE_PREDICATE}}
),
differences AS (
    (SELECT * FROM expected EXCEPT ALL SELECT * FROM actual)
    UNION ALL
    (SELECT * FROM actual EXCEPT ALL SELECT * FROM expected)
)
SELECT COUNT(*) AS difference_count
FROM differences;
```
Критерий прохождения: difference_count = 0.

Подтверждённые основания: S2T sources: ['b3050000420005_paymentdetails', 'b3050000420015_technicalservice']; mapped target fields: ['optn_id', 'start_dt', 'agr_dep_id', 'bus_srv_id', 'crncy_id', 'host_optn_id', 'optn_amt', 'optn_amt_rub', 'optn_type_id', 'registry_id']; primary key fields: ['optn_id']; required fields: ['optn_id']
~~~~

LLM-as-judge: **passed**.

Дословное заключение judge:

~~~~text
Ответ содержит полный тест-протокол с целями, SQL-шаблонами и критериями прохождения для всех запрошенных проверок: количество строк, уникальность ключа, null-rate обязательных полей и корректность трансформаций. Указаны подтверждённые таблицы, поля и правила. Display_results предоставляют необходимую детализацию по S2T-преобразованиям и структуре target-таблицы, что соответствует запросу.
~~~~

Ручной вывод: **passed** — запрос выполнен полностью; факты и вывод соответствуют evidence.

### Цикл 3, сценарий 1: Совместимость nullable-ограничений

Источник: `filename-cycle-3 #1`.
HTTP: `200`. Время агента: `63.395` с. LLM-вызовы: `33`. Токены: `58030`.
Display-results: `Нет`.

Дословный запрос:

~~~~text
Для файла 's2t_sbrf_pprb_305000042_dul_b_t_v049.xlsx' оцени совместимость nullable-ограничений b3050000420005_paymentdetails.object_id_uid → t_optn.optn_id. Верни source_not_null=<0|1>, target_not_null=<0|1> и вывод.
~~~~

Полный публичный ответ модели:

~~~~text
source_not_null=0, target_not_null=1, вывод: несовместимость — целевой столбец NOT NULL при неопределённом ограничении источника
~~~~

LLM-as-judge: **passed**.

Дословное заключение judge:

~~~~text
Ответ полностью соответствует запросу: указаны значения source_not_null и target_not_null, а также корректный вывод о несовместимости.
~~~~

Ручной вывод: **passed** — запрос выполнен полностью; факты и вывод соответствуют evidence.

### Цикл 3, сценарий 2: Совместимость типов

Источник: `legacy retries #2`.
HTTP: `200`. Время агента: `25.363` с. LLM-вызовы: `13`. Токены: `22515`.
Display-results: `get_source_target_column_pair, list_target_column_catalog`.

Дословный запрос:

~~~~text
Для file_id=3 оцени совместимость типов b3050000420005_paymentdetails.object_id_uid → t_optn.optn_id. Верни source_data_type=<тип>, target_data_type=<тип> и вывод.
~~~~

Полный публичный ответ модели:

~~~~text
source_data_type=uuid
target_data_type=uuid
Вывод: типы совместимы (оба uuid).
~~~~

LLM-as-judge: **passed**.

Дословное заключение judge:

~~~~text
Answer и display_results корректно выполняют запрос: указаны типы source и target (оба uuid), приведён вывод о совместимости, а display_results содержат необходимые сведения о column_role, file_id, table_name и column_name для обеих сторон сопоставления.
~~~~

Ручной вывод: **passed** — запрос выполнен полностью; факты и вывод соответствуют evidence.

### Цикл 3, сценарий 3: Риск дубликатов

Источник: `legacy run3 #3`.
HTTP: `200`. Время агента: `3.946` с. LLM-вызовы: `3`. Токены: `2382`.
Display-results: `read_s2t_by_target_table`.

Дословный запрос:

~~~~text
Оцени риск появления дубликатов при сохранённой S2T-трансформации b3050000420005_paymentdetails → t_optn.
~~~~

Полный публичный ответ модели:

~~~~text
S2T-анализ sources [`b3050000420005_paymentdetails`] → targets [`t_optn`]. Физические данные логических ETL-таблиц не запрашивались.

Target `t_optn`

Риск дубликатов: Найден потенциальный риск размножения строк: кардинальность одного или нескольких JOIN неизвестна.
Основания: LEFT OUTER JOIN "$$305stg".s305_0015_technicalservice AS technicalservice; cardinality=unknown
Ограничения: Фактические дубли без физических данных неизвестны.
~~~~

LLM-as-judge: **passed**.

Дословное заключение judge:

~~~~text
Query выполнен полностью: указан анализ S2T-пути, оценен риск дубликатов с обоснованием (JOIN с unknown cardinality), приведены детали трансформации и задействованные таблицы. Дополнительные физические данные не запрашивались, что соответствует условиям.
~~~~

Ручной вывод: **passed** — запрос выполнен полностью; факты и вывод соответствуют evidence.

### Цикл 3, сценарий 4: Обязательные поля без S2T-маппинга

Источник: `filename-cycle-3 #2`.
HTTP: `200`. Время агента: `6.627` с. LLM-вызовы: `4`. Токены: `2509`.
Display-results: `read_s2t_by_target_table, list_target_column_catalog`.

Дословный запрос:

~~~~text
Для файла 's2t_sbrf_pprb_305000042_dul_b_t_v049.xlsx' найди обязательные поля t_optn без сохранённого S2T-маппинга. Верни mandatory_fields_count=<число>, mandatory_fields_without_mapping_count=<число> и имена полей без маппинга.
~~~~

Полный публичный ответ модели:

~~~~text
S2T-анализ sources [`dul`] → targets [`t_optn`]. Физические данные логических ETL-таблиц не запрашивались.

Target `t_optn`

Обязательные поля без S2T-маппинга: Обязательных target-полей без S2T-маппинга нет.
Основания: required_fields=['optn_id']; mapped_target_fields=['optn_id', 'start_dt', 'agr_dep_id', 'bus_srv_id', 'crncy_id', 'host_optn_id', 'optn_amt', 'optn_amt_rub', 'optn_type_id', 'registry_id']; unmapped_required_fields=[]
~~~~

LLM-as-judge: **failed**.

Дословное заключение judge:

~~~~text
В ответе используется неподтверждённый физический идентификатор: required_fields.
~~~~

Ручной вывод: **passed** — числа и оба подтверждающих списка приведены явно; `unmapped_required_fields=[]`. Отказ judge ложный: `required_fields` — название секции результата, а не новый физический идентификатор.

### Цикл 3, сценарий 5: Риск потери строк

Источник: `legacy run3 #5`.
HTTP: `200`. Время агента: `3.895` с. LLM-вызовы: `3`. Токены: `821`.
Display-results: `read_s2t_by_target_table`.

Дословный запрос:

~~~~text
Оцени риск потери строк в сохранённой S2T-трансформации b3050000420005_paymentdetails → t_optn.
~~~~

Полный публичный ответ модели:

~~~~text
S2T-анализ sources [`b3050000420005_paymentdetails`] → targets [`t_optn`]. Физические данные логических ETL-таблиц не запрашивались.

Target `t_optn`

Риск потери строк: Структурный риск удаления исходных строк не найден: нет WHERE/HAVING и непредохраняющих JOIN.
Основания: LEFT OUTER JOIN "$$305stg".s305_0015_technicalservice AS technicalservice сохраняет левые строки
~~~~

LLM-as-judge: **passed**.

Дословное заключение judge:

~~~~text
Query требует оценки риска потери строк в S2T-трансформации b3050000420005_paymentdetails → t_optn. Ответ содержит структурный анализ трансформации, явно указывает на отсутствие риска потери строк с обоснованием через тип JOIN и условия. Display_results предоставляет детализацию S2T-преобразований, включая правило трансформации, что подтверждает полноту ответа.
~~~~

Ручной вывод: **passed** — запрос выполнен полностью; факты и вывод соответствуют evidence.

### Цикл 3, сценарий 6: Объяснение трансформации

Источник: `legacy run3 #6`.
HTTP: `200`. Время агента: `31.911` с. LLM-вызовы: `15`. Токены: `28001`.
Display-results: `read_s2t_source_to_target`.

Дословный запрос:

~~~~text
Объясни сохранённую S2T-трансформацию b3050000420005_paymentdetails.object_id_uid → t_optn.optn_id.
~~~~

Полный публичный ответ модели:

~~~~text
Трансформация b3050000420005_paymentdetails.object_id_uid → t_optn.optn_id реализована следующим SQL-правилом:

select
  b.*
  , technicalservice.product_entityid_uid
from
  $$305stg.b3050000420005_paymentdetails as b

  left outer join $$305stg.s305_0015_technicalservice as technicalservice
    on true
     and b.technicalservice_id = technicalservice.object_id
     and upper(technicalservice.ctl_action) <> 'D'

where 1 = 1;

Пояснение по ключевым элементам правила:
- Источник: слой B, таблица $$305stg.b3050000420005_paymentdetails (алиас b).
- Левое присоединение к справочной таблице s305_0015_technicalservice для обогащения; фильтрация исключает записи с ctl_action='D' (регистр игнорируется через UPPER). Условие ON TRUE делает фильтр и связь независимыми.
- WHERE 1=1 не ограничивает выборку.
- В результирующем наборе присутствуют все поля источника (b.*); целевое поле optn_id в t_optn заполняется из source-поля object_id_uid без выражений преобразования — прямой перенос значения на уровне строкового маппинга S2T.
~~~~

LLM-as-judge: **passed**.

Дословное заключение judge:

~~~~text
Answer корректно объясняет S2T-трансформацию с указанием SQL-правила и пояснением ключевых элементов. Display_results предоставляет полный список всех S2T-преобразований между указанными таблицами, включая запрошенное поле object_id_uid → optn_id. Оба компонента удовлетворяют запросу пользователя.
~~~~

Ручной вывод: **passed** — запрос выполнен полностью; факты и вывод соответствуют evidence.

### Цикл 3, сценарий 7: Тест-протокол ETL-загрузки

Источник: `filename-cycle-3 #3`.
HTTP: `200`. Время агента: `5.792` с. LLM-вызовы: `3`. Токены: `2288`.
Display-results: `read_s2t_source_to_target, list_target_column_catalog`.

Дословный запрос:

~~~~text
Для файла 's2t_sbrf_pprb_305000042_dul_b_t_v049.xlsx' по сохранённой S2T-спецификации b3050000420005_paymentdetails → t_optn составь тест-протокол для проверки ETL-загрузки во внешней СУБД. Включи проверки количества строк, уникальности ключа, null-rate обязательных полей и корректности трансформаций. Для каждой проверки дай цель, SQL-шаблон и критерий прохождения. Используй подтверждённые таблицы, колонки и правила; фактические метрики не вычисляй.
~~~~

Полный публичный ответ модели:

~~~~text
Тест-протокол проверки ETL-загрузки во внешней Greenplum СУБД для sources [`b3050000420005_paymentdetails`] → targets [`t_optn`]. SQL-шаблоны не исполнялись; фактические метрики не вычислялись. Во всех target-side запросах замени `{{LOAD_SCOPE_PREDICATE}}` условием проверяемой загрузки либо `TRUE` для полного снимка.

Load sources [`b3050000420005_paymentdetails`] → Target `t_optn`

1. Проверка количества строк
Цель: Сравнить количество ожидаемых и загруженных строк.
SQL-шаблон:
```sql
WITH expected_raw AS (
select
 b.*
 , technicalservice.product_entityid_uid
from
 $$305stg.b3050000420005_paymentdetails as b
 
 left outer join $$305stg.s305_0015_technicalservice as technicalservice
 on true
  and b.technicalservice_id = technicalservice.object_id
  and upper(technicalservice.ctl_action) <> 'D'
  
where 1 = 1
),
expected AS (
    SELECT
        src."object_id_uid" AS "optn_id",
        src."valuedate" AS "start_dt",
        src."product_entityid_uid" AS "agr_dep_id",
        src."technicalservice_id_uid" AS "bus_srv_id",
        src."currencycode_uid" AS "crncy_id",
        src."object_id" AS "host_optn_id",
        src."amount" AS "optn_amt",
        src."amountrub" AS "optn_amt_rub",
        src."operationtypecode_uid" AS "optn_type_id",
        src."registerid_uid" AS "registry_id"
    FROM expected_raw AS src
)
SELECT
    (SELECT COUNT(*) FROM expected) AS expected_row_count,
    (SELECT COUNT(*) FROM "t_optn" WHERE {{LOAD_SCOPE_PREDICATE}}) AS actual_row_count;
```
Критерий прохождения: expected_row_count = actual_row_count.

2. Проверка уникальности ключа
Цель: Проверить уникальность подтверждённого target-ключа.
SQL-шаблон:
```sql
SELECT "optn_id", COUNT(*) AS duplicate_count
FROM "t_optn"
WHERE {{LOAD_SCOPE_PREDICATE}}
GROUP BY "optn_id"
HAVING COUNT(*) > 1;
```
Критерий прохождения: Запрос не возвращает строк.

3. Проверка null-rate обязательных полей
Цель: Проверить NULL в обязательных target-полях.
SQL-шаблон:
```sql
SELECT
    COUNT(*) AS total_rows,
    COUNT(*) FILTER (WHERE "optn_id" IS NULL) AS "optn_id_null_count"
FROM "t_optn"
WHERE {{LOAD_SCOPE_PREDICATE}};
```
Критерий прохождения: Каждый *_null_count равен 0; null-rate каждого обязательного поля равен 0.

4. Проверка корректности трансформаций
Цель: Сравнить ожидаемую S2T-проекцию с target.
SQL-шаблон:
```sql
WITH expected_raw AS (
select
 b.*
 , technicalservice.product_entityid_uid
from
 $$305stg.b3050000420005_paymentdetails as b
 
 left outer join $$305stg.s305_0015_technicalservice as technicalservice
 on true
  and b.technicalservice_id = technicalservice.object_id
  and upper(technicalservice.ctl_action) <> 'D'
  
where 1 = 1
),
expected AS (
    SELECT
        src."object_id_uid" AS "optn_id",
        src."valuedate" AS "start_dt",
        src."product_entityid_uid" AS "agr_dep_id",
        src."technicalservice_id_uid" AS "bus_srv_id",
        src."currencycode_uid" AS "crncy_id",
        src."object_id" AS "host_optn_id",
        src."amount" AS "optn_amt",
        src."amountrub" AS "optn_amt_rub",
        src."operationtypecode_uid" AS "optn_type_id",
        src."registerid_uid" AS "registry_id"
    FROM expected_raw AS src
),
actual AS (
    SELECT "optn_id", "start_dt", "agr_dep_id", "bus_srv_id", "crncy_id", "host_optn_id", "optn_amt", "optn_amt_rub", "optn_type_id", "registry_id"
    FROM "t_optn"
    WHERE {{LOAD_SCOPE_PREDICATE}}
),
differences AS (
    (SELECT * FROM expected EXCEPT ALL SELECT * FROM actual)
    UNION ALL
    (SELECT * FROM actual EXCEPT ALL SELECT * FROM expected)
)
SELECT COUNT(*) AS difference_count
FROM differences;
```
Критерий прохождения: difference_count = 0.

Подтверждённые основания: S2T sources: ['b3050000420005_paymentdetails', 'b3050000420015_technicalservice']; mapped target fields: ['optn_id', 'start_dt', 'agr_dep_id', 'bus_srv_id', 'crncy_id', 'host_optn_id', 'optn_amt', 'optn_amt_rub', 'optn_type_id', 'registry_id']; primary key fields: ['optn_id']; required fields: ['optn_id']
~~~~

LLM-as-judge: **passed**.

Дословное заключение judge:

~~~~text
Ответ содержит полный тест-протокол с целями, SQL-шаблонами и критериями прохождения для всех запрошенных проверок: количество строк, уникальность ключа, null-rate обязательных полей и корректность трансформаций. Указаны подтверждённые таблицы, поля и правила. Display_results содержат необходимые детали S2T и целевых колонок, не содержат противоречий и поддерживают составленный протокол.
~~~~

Ручной вывод: **passed** — запрос выполнен полностью; факты и вывод соответствуют evidence.

## Дополнительная проверка общего agentic-маршрута

После усиления downstream-промпта сценарий обязательных полей ещё трижды
выполнен на `GigaChat-3-Ultra` без выбора специализированного `s2t_analysis`:

| Прогон | Ответ | Ручная семантика | Время, с | Токены |
|---|---|---:|---:|---:|
| `20260903_130928` | `mandatory_fields_count=1, mandatory_fields_without_mapping_count=0` | passed | 35.247 | 48 011 |
| `20260903_131026` | `mandatory_fields_count=1, mandatory_fields_without_mapping_count=0, fields=[]` | passed | 34.706 | 45 150 |
| `20260903_131145` | `mandatory_fields_count=1, mandatory_fields_without_mapping_count=0` | passed | 31.858 | 41 247 |

Во всех трёх планах дословно сохранён `target_table='t_optn'`. Инструменты
подтвердили одну обязательную колонку `optn_id` и полный набор из 20 S2T-строк.
Два отрицательных verdict старого judge вызваны только отсутствием отдельного
`[]`; prompt judge обновлён общим правилом для явно нулевых коллекций.

## Заключение

По согласованной ручной семантической оценке выполнены все 21 из 21 запросов.
Все семь основных сценариев стабильны в трёх исходных циклах. Дополнительная
трёхкратная проверка показала, что усиленный общий agentic-маршрут сохраняет
раздельные идентификаторы файла и ETL-таблицы и корректно решает сценарий №4.
