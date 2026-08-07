# Демонстрация ETL-агента


## 1. Непосредственный upstream колонки

### Запрос

> Покажи непосредственный upstream для колонки `t_bus_srv.bus_srv_id`. Нужны
> исходные таблицы, исходные колонки и слои связей.

### Дословный результат

````text
Для колонки `t_bus_srv.bus_srv_id` найдено четыре непосредственных источника (слой B):

| Исходная таблица | Слои связи | Колонка-источник | ID трансформации |
|---|---|---|---|
| b3050000420001_agreementservice | B → T | object_id_uid | 585 |
| b3050000420010_productservice | B → T | object_id_uid | 679 |
| b3050000420015_technicalservice | B → T | object_id_uid | 696 |
| b3050000420029_planservice | B → T | object_id_uid | 785 |

Все источники используют одну и ту же исходную колонку `object_id_uid` из слоя `B`.
````

## 2. Два уровня downstream через wildcard

### Запрос

> Покажи downstream до глубины 2 для колонки
> `s_grnplm_as_t_didsd_305_db_stg.b3050000420007_product.object_id`.
> Перечисли шаги по порядку и явно обозначь переход `* → *`.

### Дословный результат

````text
Для колонки `s_grnplm_as_t_didsd_305_db_stg.b3050000420007_product.object_id` downstream-линейность до глубины 2 выглядит следующим образом:

**Шаг 1:**
*   **Исходная таблица:** `s_grnplm_as_t_didsd_305_db_stg.b3050000420007_product` (поле `object_id`)
*   **Целевая таблица:** `v_b3050000420007_product_productrate` (поле `object_id`, слой B)

**Шаг 2:**
*   **Исходная таблица:** `v_b3050000420007_product_productrate` (поле `*`)
*   **Целевая таблица:** `s305_0007_product_productrate` (поле `object_id`, слой B)

**Переход между шагами:**
Второй шаг использует правило трансформации с источником и целью, обозначенными как «*» (`source_field: * → target_field: *`). Это указывает на автоматическое наследование или отсутствие явного сопоставления конкретной колонки при переходе от виртуальной таблицы к физической.
````

## 3. Обратный двухуровневый upstream

### Запрос

> Покажи upstream до глубины 2 для колонки
> `s305_0007_product_productrate.object_id`. Верни одну полную цепочку в
> физическом направлении потока данных, от исходной колонки к запрошенной:
> upstream означает поиск предков, но стрелки не разворачивай. Явно обозначь
> переход `* → *`.

### Дословный результат

````text
```text
[["table","count"],["s_grnplm_as_t_didsd_305_db_stg.b3050000420007_product",1]]
```

**Цепочка данных (физическое направление потока):** `s_grnplm_as_t_didsd_305_db_stg.b3050000420007_product` → `v_b3050000420007_product_productrate` → `s305_0007_product_productrate`.

**Переход:** На глубине 1 имя поля в целевой таблице (`s305_...`) было `*`, а на глубине 2 оно явно указано как `object_id` во всех этапах пути, что подтверждает непрерывность потока данных для этой конкретной колонки.
````

## 4. Обе стороны промежуточной колонки

### Запрос

> Покажи непосредственный upstream и downstream для промежуточной колонки
> `v_b3050000420007_product_productrate.object_id`. Нужны обе связи, слои и
> отметка wildcard-перехода.

### Дословный результат

````text
```table
["Направление","Таблица источника (source_table)","Поле источника (source_field)","Слои (layers)","ID трансформации"]
["Upstream","s_grnplm_as_t_didsd_305_db_stg.b3050000420007_product","object_id",null,"17627 (точечная связь)"]
["Downstream","v_b3050000420007_product_productrate","*","B","17675 (wildcard-переход: поле \"*\" в источнике и цели)"]
```

**Отметка wildcard-перехода:** Для downstream связи используется правило `"*"`, что соответствует переносу одноимённой колонки через таблицу.
````

## 5. Зависимая цепочка из двух инструментов

### Запрос

> Найди одну ETL-таблицу, которая одновременно является источником и
> приёмником. Затем покажи её непосредственные входящие и исходящие табличные
> связи и количество колонковых трансформаций в каждой связи.

### Дословный результат

````text
Найдена ETL-таблица `b7000000250002_loansdictstatus::subquery::cr`, которая одновременно является источником и приёмником данных. Её непосредственные связи:

| Направление | Исходящая таблица (source_table) | Входящая таблица (target_table) | Количество трансформаций |
|-------------|----------------------------------|-------------------------------|---------------------------|
| upstream    | loansdictstatus                  | b7000000250002_loansdictstatus::subquery::cr | 5                         |
| downstream  | b7000000250002_loansdictstatus::subquery::cr | b7000000250002_loansdictstatus (слой B)      | 4
````

## 6. Объяснимый путь через additional object

### Запрос

> Объясни upstream-путь до глубины 3 для колонки
> `b7000000250002_loansdictstatus.c_id`: покажи все промежуточные узлы, правила
> преобразования, участие additional object и подтверждение Neo4j.

### Дословный результат

````text
Найдено один upstream-путь глубины 2 (запрошенная глубина 3 не достигнута). Путь начинается с таблицы `loansdictstatus` и заканчивается в таблице `b7000000250002_loansdictstatus`. Промежуточным узлом является подтаблица `b7000000250002_loansdictstatus::subquery::cr`, созданная дополнительным объектом.

**Структура пути:**
1.  **Шаг 1 (loansdictstatus → subquery):** Преобразование выражения (`expression`) с правилом `ls.c_id`. Дополнительный объект отсутствует.
    *   SQL-логика: выборка поля `C_ID` из исходной таблицы `LOANSDICTSTATUS`.
2.  **Шаг 2 (subquery → b7000000250002_loansdictstatus):** Преобразование выражения (`expression`) с правилом `cr.c_id`. Используется дополнительный объект:
    *   ID объекта: 2, имя таблицы: `b7000000250002_loansdictstatus`, SQL-запрос сортирует записи по дате валидности и выбирает уникальные записи.

**Подтверждение Neo4j:**
Связи подтверждены отношениями типа `TABLE_TRANSFORMS_TO` (между таблицами) и `TRANSFORMS_TO` (между колонками). Оба преобразования являются выражениями без фильтрации данных, просто перенося поле через промежуточную подтаблицу.

```text_diagram
Путь 1:
  [loansdictstatus.c_id] --expression--> [b7000000250002_loansdictstatus::subquery::cr.c_id]
    [b7000000250002_loansdictstatus::subquery::cr.c_id] --expression--> [b7000000250002_loansdictstatus.c_id]
```

**Дополнительный объект:**
*   **ID объекта:** 2
*   **Имя таблицы:** `b7000000250002_loansdictstatus`
*   **SQL-запрос:**
    ```sql
    select * from (select C_ID, C_code, C_name, row_number() over (partition by C_ID order by ctl_validfrom desc) as rn from LOANSDICTSTATUS ls) cr where rn=1
    ```
````

## 7. Текстовая схема сохранённого пути

### Запрос

> Покажи схемой upstream-путь до глубины 3 для колонки
> `b7000000250002_loansdictstatus.c_id`. Нужна готовая текстовая схема, без
> Mermaid-кода.

### Дословный результат

````text
```text_diagram
Путь 1:
  [loansdictstatus.c_id] --expression--> [b7000000250002_loansdictstatus::subquery::cr.c_id]
    [b7000000250002_loansdictstatus::subquery::cr.c_id] --expression--> [b7000000250002_loansdictstatus.c_id]
```
````

## 8. Сложный колонковый lineage Greenplum SQL

### Запрос

> Разбери колонковый lineage этого Greenplum SQL, но не выполняй его:
>
> ```sql
> WITH paid AS (
>   SELECT o.customer_id, SUM(o.amount) AS total_amount
>   FROM raw.orders o
>   WHERE o.status = 'paid'
>   GROUP BY o.customer_id
> )
> SELECT
>   c.id AS customer_id,
>   UPPER(c.name) AS customer_name,
>   COALESCE(p.total_amount, 0)::numeric(18,2) AS revenue,
>   CASE WHEN p.total_amount > 100000 THEN 'vip' ELSE 'regular' END AS segment
> FROM raw.customers c
> LEFT JOIN paid p ON p.customer_id = c.id;
> ```

### Дословный результат

````text
Анализ колонкового lineage для указанного Greenplum SQL:

- **Целевая таблица**: отсутствует (результат — виртуальная таблица).
- **Исходные таблицы**: `raw.customers`, `raw.orders`.
- **Линейность колонок**:
  - `customer_id` → из колонки `id` в таблице `raw.customers`.
  - `customer_name` (UPPER(c.name)) → из колонки `name` в таблице `raw.customers`.
  - `revenue` (CAST(COALESCE(p.total_amount, 0) AS DECIMAL(18,2))) → вычисляется на основе суммы колонок `amount` из таблицы `raw.orders`.
  - `segment` (CASE WHEN p.total_amount > 100000 THEN 'vip' ELSE 'regular') → зависит от значения колонки `total_amount`, которая агрегирована из `amount` в таблице `raw.orders`; прямая связь с сырой колонокой `amount` верна только транзитивно через CTE.
````

## 9. Интерактивный граф конкретного SQL

### Запрос

> Построй интерактивный граф колонкового lineage для Greenplum SQL, не выполняя
> его: `CREATE VIEW mart.order_customer AS SELECT o.id, c.name FROM raw.orders o
> JOIN raw.customers c ON c.id = o.customer_id;`

### Дословный результат

````text
Интерактивный граф наследования для запроса `CREATE VIEW mart.order_customer` построен. Он показывает связь между целевым представлением и источниками:

- **Целевая таблица**: `mart.order_customer`
  - Столбец `id` берётся из таблицы `raw.orders`.
  - Столбец `name` берётся из таблицы `raw.customers`.

Интерактивная визуализация доступна по ссылке: `/exports/sql-lineage/sql_lineage_4c12500d997341ec702b1654.html`
````

## 10. Интерактивный граф всех S2T-связей

### Запрос

> Построй интерактивный граф всех сохранённых табличных S2T-связей. Кратко
> сообщи количество узлов и связей и дай ссылку на граф.

### Дословный результат

````text
Интерактивный граф всех табличных S2T-связей готов. В нём 343 узла и 400 связей (проанализировано 3880 строк, исключено 235). Ссылка на визуализацию: `/exports/s2t-graphs/s2t_table_graph_7288068c55eb0dba929a6767.html`. JSON-данные доступны по ссылке `/exports/s2t-graphs/s2t_table_graph_7288068c55eb0dba929a6767.json`.

[Открыть данные графа в JSON](/exports/s2t-graphs/s2t_table_graph_7288068c55eb0dba929a6767.json)
````
