# Orders Articles Analyze

## Назначение

Джоба `orders_articles_analyze` предназначена для построения аналитической витрины по артикульному анализу. Она собирает данные из нескольких источников, объединяет их в единый `DataFrame` и сохраняет результат в таблицу `orders_articles_analyze`.

Основной сценарий работы:

1. получить общую статистику по артикулам;
2. получить рекламную статистику;
3. объединить данные в единый датасет;
4. проверить уникальность ключей;
5. загрузить результат в PostgreSQL.

---

## Структура джобы

```text
src_oop/jobs/orders_articles_analyze/
├─ repository.py
├─ process.py
├─ tables_scheme.py
└─ run.py
Состав файлов
repository.py
Файл содержит класс ArticleAnalyzeRepository, который отвечает за чтение исходных данных из базы.

Класс ArticleAnalyzeRepository
Используется как repository-слой для получения данных по артикульному анализу.

Параметры инициализации
days_ago: int = 30
Левая граница диапазона дат.
days_to: int = 1
Правая граница диапазона дат.
Метод get_general_stat()
Получает основную статистику по артикулам за указанный диапазон дат.

В запросе используется таблица funnel_daily как базовый источник, а также дополнительные справочные и расчетные таблицы через LEFT JOIN.

Основные присоединяемые источники
card_data
article
orders
cost_price
accurate_net_profit_data
sales
promo_and_managers_info
inventory_turnover_by_reg
comission_wb_data
individual_conditions
prices_data
wb_stock
historical_stocks_fbs_service
Основные поля результата
дата, номер недели, номер месяца;
аккаунт;
артикул;
local_vendor_code;
предмет;
цена;
показатели заказов и продаж;
показатели прибыли;
конверсии;
менеджер и промо-информация;
остатки и региональное распределение;
закупочная цена;
комиссия;
логистика;
рейтинг;
FBO/FBS-метрики;
остатки в пути.
Метод возвращает pandas.DataFrame.

Метод get_adv_stat()
Получает рекламную статистику по артикулам за указанный диапазон дат.

Источники:

advert_stat
advert_spend
Возвращаемые метрики:

clicks
views
cpc
cpm
adv_spend
bonuses
Агрегация выполняется по:

article_id
date
Метод возвращает pandas.DataFrame.

process.py
Файл содержит класс ProcessArticleAnalyze, который отвечает за объединение и валидацию данных.

Класс ProcessArticleAnalyze
Принимает repository-объект ArticleAnalyzeRepository и строит итоговый датасет.

Метод build_dataset(days_ago: int, days_to: int) -> pd.DataFrame
Основной метод обработки.

Логика работы:

получает рекламную статистику через repo.get_adv_stat();
получает общую статистику через repo.get_general_stat();
логирует предупреждение, если один из датафреймов пустой;
проверяет оба источника на дубли по ключу ["article_id", "date"];
объединяет датафреймы через pd.merge(..., how="left");
заполняет NaN в числовых колонках нулями;
сортирует результат по:
date по убыванию;
orders_sum_rub по убыванию.
Метод _validate_unique_keys(df, name)
Проверяет, что в датафрейме нет дублей по ключу:

["article_id", "date"]
Если дубли есть, выбрасывается исключение ValueError.

Это защищает джобу от некорректного merge и от невалидной загрузки в витрину.

tables_scheme.py
Файл содержит описание схемы целевой таблицы orders_articles_analyze.

Таблица
orders_articles_analyze
Основные колонки
Схема включает поля:

календарные признаки:
date
week_num
month_num
товарные признаки:
article_id
local_vendor_code
subject_name
parent_name
procurement_status
коммерческие показатели:
price_with_disc
spp
orders_sum_rub
orders_count
sales_sum
sales_count
показатели прибыли:
profit_by_orders
profit_by_cond_orders
profit_by_cond_sales
рекламные показатели:
clicks
views
ctr
cpc
cpm
adv_spend
bonuses
карточка и конверсии:
open_card_count
add_to_cart_count
to_cart_convers
to_orders_convers
промо и менеджмент:
manager
promo_title
promo_status
остатки и логистика:
total_quantity
end_of_day_balance
in_way_to_client
in_way_from_client
logistic_from_wb_wh_to_opp
региональные показатели:
central
south
privolzhskiy
north_caucase
central_fo_orders
south_fo_orders
privolzhskiy_fo_orders
north_caucase_fo_orders
far_eastern_fo_orders
ural_fo_orders
north_west_fo_orders
syberian_fo_orders
прочее:
purchase_price
kgvp_marketplace
ind_comission_fbo
fbs_orders
fbo_orders
rating
скользящие и агрегированные показатели за 30 дней
Уникальный ключ
["date", "article_id"]
Уникальный ключ используется при upsert-загрузке и гарантирует уникальность записи по артикулу в рамках одной даты.

run.py
Файл содержит точку запуска джобы.

Функция orders_article_analyze_run(days_ago: int = 30, days_to: int = 1)
Основной orchestration-сценарий.

Логика работы:

логирует старт джобы;
создает repository ArticleAnalyzeRepository;
передает repository в ProcessArticleAnalyze;
строит итоговый датафрейм;
проверяет, что датафрейм не пустой;
получает схему таблицы из tables_scheme.py;
загружает данные в PostgreSQL через Database.sync_data_to_postgres().
Поток данных
advert_stat + advert_spend
        ->
ArticleAnalyzeRepository.get_adv_stat()
        ->

funnel_daily + справочные и расчетные таблицы
        ->
ArticleAnalyzeRepository.get_general_stat()
        ->

ProcessArticleAnalyze.build_dataset()
        ->
единый pandas.DataFrame
        ->
Database.sync_data_to_postgres()
        ->
orders_articles_analyze
Источники данных
Джоба использует следующие таблицы как источники:

funnel_daily
card_data
article
orders
cost_price
accurate_net_profit_data
sales
promo_and_managers_info
inventory_turnover_by_reg
comission_wb_data
individual_conditions
prices_data
wb_stock
historical_stocks_fbs_service
advert_stat
advert_spend
Бизнес-логика
Джоба формирует расширенную витрину по артикулу и дате, которая объединяет:

воронку;
заказы;
продажи;
прибыль;
рекламные расходы;
бонусы;
цены;
закупку;
промо-активности;
региональные метрики;
логистику;
остатки;
рейтинг.
Итоговая таблица используется как единый слой аналитики для последующих расчетов и витрин.

Валидация данных
Перед объединением датафреймов выполняется проверка на дубли по ключу:

["article_id", "date"]
Если в df_adv или df_gen появляются повторяющиеся строки по этому ключу, выполнение прерывается с ошибкой.

Это позволяет:

избегать некорректного размножения строк при merge;
обеспечивать корректную загрузку в таблицу с уникальным ключом;
быстрее обнаруживать ошибки в SQL-агрегации.
Пример запуска
orders_article_analyze_run(days_ago=30, days_to=1)
Используемые зависимости
Для работы джобы используются:

sqlalchemy
pandas
внутренний модуль Database
Требования к окружению
Для корректной работы джобы должны быть настроены:

подключение к PostgreSQL;
переменные окружения для доступа к базе;
корректная работа класса Database;
наличие всех используемых исходных таблиц в БД.
Ответственность компонентов
ArticleAnalyzeRepository
Отвечает за SQL-запросы и чтение данных из БД.

ProcessArticleAnalyze
Отвечает за:

объединение данных;
проверку уникальности ключей;
подготовку итогового датафрейма.
Database
Отвечает за:

подключение к PostgreSQL;
выполнение SQL-запросов;
чтение результата в DataFrame;
запись результата в целевую таблицу.
run.py
Отвечает за orchestration-сценарий и запуск полной загрузки.

Результат работы
После выполнения джобы:

формируется единый аналитический датасет по артикулам и датам;
данные сохраняются в таблицу orders_articles_analyze;
при совпадении по ключу date + article_id записи обновляются через upsert.