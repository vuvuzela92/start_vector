# 📊 Conditional Calculations

## 📌 Назначение

Джоба `conditional_calculations` предназначена для:

- расчета агрегированных показателей по условному расчету;
- сохранения результатов в PostgreSQL;
- последующей выгрузки данных в Google Sheets.

### 🔁 Основной сценарий

1. Получение агрегированных данных по аккаунтам и датам  
2. Сохранение результата в таблицу `conditions_calculation`  
3. (Опционально) Выгрузка данных в Google Sheets  

---

## 🏗️ Структура проекта
src_oop/jobs/conditional_calculations/
├── repository.py
├── tables_scheme.py
└── run.py


---

## 📦 Описание компонентов

### `repository.py`

Содержит класс `ConditionalCalculationsRepository`, отвечающий за работу с базой данных.

#### ⚙️ Параметры инициализации

| Параметр  | Тип | По умолчанию | Описание |
|----------|-----|-------------|----------|
| `days_ago` | int | 30 | Левая граница диапазона дат |
| `days_to`  | int | 1  | Правая граница диапазона дат |

---

#### 📌 Метод: `execute_conditional_calculations()`

Выполняет агрегацию данных из таблицы `orders_articles_analyze`.

**Группировка:**
- `account`
- `date`

**Фильтрация:**
- диапазон: `CURRENT_DATE - days_ago` → `CURRENT_DATE - days_to`
- исключение: `account = '0'`

**Возвращает:**
`pandas.DataFrame` со следующими метриками:

- `orders_sum`
- `sales_sum`
- `profit_by_ind_cond_orders`
- `profit_by_ind_cond_sales`
- `sales_count`
- `order_count`
- `adv_spend`
- `bonuses`
- `profit_cond_sales_minus_adv_spend`
- `cost_price_orders`
- `cost_price_sales`
- `general_profit_orders`

---

#### 📌 Метод: `get_conditional_calculations()`

Получает данные из таблицы `conditions_calculation`.

**Сортировка:**
- по `date` ↑
- по `account` ↑

---

### `tables_scheme.py`

Описывает структуру таблицы `conditions_calculation`.

#### 🧱 Схема таблицы

| Колонка | Тип |
|--------|-----|
| account | String(255) |
| orders_sum | BigInteger |
| sales_sum | BigInteger |
| profit_by_ind_cond_orders | BigInteger |
| profit_by_ind_cond_sales | BigInteger |
| sales_count | BigInteger |
| order_count | BigInteger |
| adv_spend | BigInteger |
| bonuses | BigInteger |
| profit_cond_sales_minus_adv_spend | BigInteger |
| cost_price_orders | BigInteger |
| cost_price_sales | BigInteger |
| general_profit_orders | BigInteger |
| date | Date |

#### 🔑 Уникальный ключ
["date", "account"]


Используется при `upsert` и гарантирует уникальность записи на дату и аккаунт.

---

### `run.py`

Содержит сценарии запуска джобы.

---

#### 🚀 `conditional_calculation_to_db_run(days_ago=30, days_to=1)`

Основной сценарий загрузки в PostgreSQL.

**Логика:**

1. Создается `ConditionalCalculationsRepository`
2. Выполняется расчет
3. Если DataFrame пуст → запись пропускается
4. Данные сохраняются через:
   ```python
   Database.sync_data_to_postgres()

Используются:

title
columns
unique_keys

📤 update_conditional_calculations_to_gs(...)

Выгрузка данных в Google Sheets.

Параметры:

Параметр	По умолчанию
table_name	"Условный расчет"
sheet_name	"Справочная информация"

Логика:

Чтение данных из PostgreSQL
Подключение к Google Sheets через GoogleTabs
Запись через set_with_dataframe
⚠️ Обработка ошибок
gspread.exceptions.SpreadsheetNotFound
gspread.exceptions.WorksheetNotFound
StopIteration
RuntimeError

🔄 Поток данных
orders_articles_analyze
        ↓
execute_conditional_calculations()
        ↓
pandas.DataFrame
        ↓
Database.sync_data_to_postgres()
        ↓
conditions_calculation
        ↓
get_conditional_calculations()
        ↓
Google Sheets


🧠 Бизнес-логика

Агрегация выполняется:

по каждому аккаунту
по каждой дате
📊 Рассчитываемые показатели
сумма заказов
сумма продаж
прибыль по заказам (инд. условия)
прибыль по продажам (инд. условия)
количество продаж
количество заказов
рекламные расходы
бонусы
прибыль (sales - ads)
себестоимость заказов
себестоимость продаж
общая прибыль по заказам

👉 Таблица conditions_calculation — это агрегированная витрина данных.

🧰 Используемые зависимости
sqlalchemy
pandas
gspread
gspread_dataframe
Внутренние модули:
Database
GoogleTabs

▶️ Примеры запуска
Запись в PostgreSQL
conditional_calculation_to_db_run(days_ago=30, days_to=1)

Выгрузка в Google Sheets
update_conditional_calculations_to_gs()
Выгрузка в конкретный лист
update_conditional_calculations_to_gs(
    table_name="Условный расчет",
    sheet_name="Справочная информация"
)
⚙️ Требования к окружению

Для корректной работы необходимо:

доступ к PostgreSQL
настроенные переменные окружения
реализованный класс Database:
read_sql_to_dataframe
sync_data_to_postgres
реализованный класс GoogleTabs
доступ сервисного аккаунта к Google Sheets
🧩 Ответственность компонентов
ConditionalCalculationsRepository
Чтение и агрегация данных из PostgreSQL
Database
Подключение к БД
Выполнение SQL
Работа с DataFrame
Upsert данных
GoogleTabs
Подключение к Google Sheets
Получение листов
run.py
Оркестрация процессов:
расчет → БД
БД → Google Sheets
✅ Результат работы
После conditional_calculation_to_db_run()
данные агрегируются из orders_articles_analyze
записываются в conditions_calculation
После update_conditional_calculations_to_gs()
данные выгружаются в Google Sheets
лист обновляется актуальным состоянием