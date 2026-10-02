import asyncio
import inspect
from typing import Any, Callable, Dict

from src.modules.GOOGLE_SHEETS.credit_analyze_vector import update_credit_data_vector
from src.modules.WB.reports.tasks import orders_report_today
from src_oop.jobs.add_new_items.run import add_new_items_run, add_new_items_telegram_bot
from src_oop.jobs.advert_info.run import advert_info
from src_oop.jobs.advert.run import advert_stat
from src_oop.jobs.advert_spend.run import advert_spend
from src_oop.jobs.autopilot_daily.run import autopilot_daily_run
from src_oop.jobs.annual_procurement_plan.run import (
    transport_data_to_annual_procurement_plan,
    transport_parfume_data_to_annual_procurement_plan,
    transport_supplies_data_to_annual_procurement_plan,
    transport_unit_data_to_annual_procurement_plan,
    update_quarterly_prices_to_annual_procurement_plan,
)
from src_oop.jobs.assembly_info.run import assembly_status_model_run
from src_oop.jobs.autopilot.run import (
    autopilot_hourly_run,
    autopilot_remove_duplicates,
    update_individual_info,
)
from src_oop.jobs.bukh_docs.run import get_bukh_docs_async
from src_oop.jobs.bukh_docs.week_n_redeem_run import update_week_n_redeem
from src_oop.jobs.bitrix_chat_control.run import (
    bitrix_chat_control_create_tables,
    daily_report,
    initialize_bitrix_chat_cursors,
    sync_bitrix_chats,
    telegram_bot,
    weekly_report,
)
from src_oop.jobs.calculation_of_purchases_china.run import (
    transport_quarterly_plan_to_pivot,
    update_payments_analyze_with_ved,
)
from src_oop.jobs.calculation_of_purchases_russia.run import (
    set_orders_quantity,
    transport_orders_and_supply,
    update_penalties_in_gs_purchase_russia,
    update_supplies_1c_in_purchase_russia,
)
from src_oop.jobs.conditional_calculations.run import (
    conditional_calculation_to_db_run,
    update_conditional_calculations_to_gs,
)
from src_oop.jobs.fin_reports_analyze.run import (
    update_cash_flow_writeoffs,
    update_daily_fin_reports_deductions_agg,
    update_deductions_by_month,
    update_fin_deductions_mv,
    update_monthly_report,
    update_outcomes_detalize,
    update_stock_analyze,
    update_weekly_profit_report,
)
from src_oop.jobs.fin_reports_analyze.system_penalties_analytics import (
    run_system_penalties_analysis,
)
from src_oop.jobs.fbo_supplies.run import fbo_supplies_run
from src_oop.jobs.fbs2_test.run import fbs2_test_movements_run, fbs2_test_stocks_run
from src_oop.jobs.funnel_sales.run import funnel_sales
from src_oop.jobs.fbs_warehouses.run import (
    create_fbs_warehouse,
    delete_fbs_warehouse,
    import_created_fbs_warehouse,
    import_existing_fbs_warehouse,
    list_fbs_warehouses,
    list_wb_offices,
    sync_fbs_warehouses_from_wb,
)
from src_oop.jobs.fbs_stocks.run import (
    apply_new_fbs_stocks_from_unit,
    auto_refill_fbs_stocks_from_unit,
    update_fbs_stocks_in_unit,
)
from src_oop.jobs.health_check.run import health_check_run
from src_oop.jobs.logistic_ved.run import logistic_ved_full_run
from src_oop.jobs.orders_articles_analyze.run import orders_article_analyze_run
from src_oop.jobs.orders_feed.run import order_feed
from src_oop.jobs.orders.run import orders
from src_oop.jobs.purchase_price_update.run import purchase_price_update_run
from src_oop.jobs.returns_to_customers.run import returns_to_customers
from src_oop.jobs.sales_analyze.run import update_sales_warehouse_analytics
from src_oop.jobs.seller_balance.run import seller_balance_run
from src_oop.jobs.sales_plan.run import (
    sales_plan_run,
    sync_sales_plan_accounting_category_reference_to_db,
    sync_sales_plan_manager_reference_to_db,
    sync_sales_wild_status_daily_to_db,
)
from src_oop.jobs.unit.update_adv_participants import update_adv_participants_to_gs
from src_oop.jobs.unit.update_wild_statuses import update_wild_statuses
from src_oop.jobs.wb_api.measurements.run import (
    collect_and_store_measurements,
    set_measurements_to_google,
)
from src_oop.jobs.wms_stocks.run import historical_stocks_run
from src_oop.jobs.wms_stocks.run_stock import wms_stock_backfill_run, wms_stock_run
from src_oop.jobs.wb_stock_control.run import wb_stock_control_run
from src_oop.jobs.yandex_designer_output.run import yandex_designer_output_run


def smart_run(func: Callable):
    if inspect.iscoroutinefunction(func):
        return lambda: asyncio.run(func())
    return func


TASKS: Dict[str, Dict[str, Any]] = {
    # Маркетплейс WB: реклама и ежедневные оперативные отчеты.
    "assembly_status_model_run": {
        "func": smart_run(assembly_status_model_run),
        "desc": "WB API → PostgreSQL `assembly_task_status_model`: сохранение сборочных заданий и их статусов для контроля обработки и штрафов",
    },
    "advert_info": {
        "func": smart_run(advert_info),
        "desc": "WB Advertising API → PostgreSQL `advert_campaigns_info`: обновление справочной информации о рекламных кампаниях",
    },
    "advert_spend": {
        "func": smart_run(advert_spend),
        "desc": "WB Advertising API → PostgreSQL `advert_spend`: загрузка расходов по рекламным кампаниям с разбивкой по датам и кабинетам",
    },
    "advert_stat": {
        "func": smart_run(advert_stat),
        "desc": "WB Advertising API → PostgreSQL `advert_stat`: загрузка статистики показов, кликов и результатов рекламных кампаний",
    },
    "orders_report_today": {
        "func": smart_run(orders_report_today),
        "desc": "Запуск обновления отчета о заказах за сегодня",
    },
    "orders_run": {
        "func": smart_run(orders),
        "desc": "WB API → PostgreSQL `orders`: загрузка заказов WB с уникальностью по `date` и `srid`",
    },
    # Yandex Disk и ежедневная выработка дизайнеров.
    "yandex_designer_output": {
        "func": smart_run(yandex_designer_output_run),
        "desc": "Яндекс.Диск → Google Sheets «Дизайнеры выработка», лист «Дизайнеры»: создание папок дизайнеров и публикация ежедневной выработки",
    },
    "order_feed": {
        "func": smart_run(order_feed),
        "desc": "Почасовое обновление WB Order Feed за доступные последние 31 сутки",
    },
    "funnel_sales": {
        "func": smart_run(funnel_sales),
        "desc": "WB API → PostgreSQL `funnel_daily`: ежедневная загрузка показателей воронки продаж по артикулам",
    },
    "fbs2_test_movements_run": {
        "func": smart_run(fbs2_test_movements_run),
        "desc": "PostgreSQL/WMS → Google Sheets «Карта БД Закупщиков», лист «БД Отгрузки»: дневные количества FBS-отгрузок, поставок, движения ФБО, брака, пересорта и FBS-остаток",
    },
    "fbs2_test_stocks_run": {
        "func": smart_run(fbs2_test_stocks_run),
        "desc": "PostgreSQL/WMS → Google Sheets «Карта БД Закупщиков», лист «БД Остатки»: дневные остатки по состояниям WMS — общий остаток, FBS, приёмка, упаковка, недостача, ФБО, брак и хранение",
    },
    # Старый контур Google Sheets и управленческих витрин.
    "update_penalties_in_gs_purchase_russia": {
        "func": smart_run(update_penalties_in_gs_purchase_russia),
        "desc": "PostgreSQL/WB → Google Sheets «Расчет закупки Россия»: обновление показателей штрафов и остатков для расчёта закупки по России",
    },
    "get_bukh_docs": {
        "func": smart_run(get_bukh_docs_async),
        "desc": "Запуск получения данных по бухгалтерским документам",
    },
    "get_bukh_docs_async": {
        "func": smart_run(get_bukh_docs_async),
        "desc": "Запуск получения данных по бухгалтерским документам (CLI-алиас)",
    },
    "update_credit_data_vector": {
        "func": smart_run(update_credit_data_vector),
        "desc": "Обновление данных для кредитного анализа Вектор",
    },
    # Корпоративные чаты и управленческие Telegram-саммари.
    "bitrix_chat_control_create_tables": {
        "func": smart_run(bitrix_chat_control_create_tables),
        "desc": "Создание таблиц и bootstrap monitored_chats для Bitrix chat control",
    },
    "initialize_bitrix_chat_cursors": {
        "func": smart_run(initialize_bitrix_chat_cursors),
        "desc": "Установка курсоров Bitrix на текущие сообщения без анализа старой истории",
    },
    "sync_bitrix_chats": {
        "func": smart_run(sync_bitrix_chats),
        "desc": "Read-only синхронизация рабочих чатов Bitrix24 и обновление состояния проблем",
    },
    "daily_report": {
        "func": smart_run(daily_report),
        "desc": "Формирование и отправка ежедневного Telegram-отчёта по рабочим чатам Bitrix24",
    },
    "weekly_report": {
        "func": smart_run(weekly_report),
        "desc": "Формирование и отправка недельного Telegram-саммари по рабочим чатам Bitrix24",
    },
    "telegram_bot": {
        "func": smart_run(telegram_bot),
        "desc": "Запуск интерактивного Telegram-бота для Bitrix chat control",
    },
    "add_new_items_telegram_bot": {
        "func": smart_run(add_new_items_telegram_bot),
        "desc": "Запуск Telegram-бота для серверного запуска add_new_items_run",
    },
    # Контроль ежедневных выгрузок и сервисные health-check.
    "health_check": {
        "func": smart_run(health_check_run),
        "desc": "Проверка свежести и полноты критичных ежедневных выгрузок",
    },
    # План продаж
    "sync_sales_plan_manager_reference_to_db": {
        "func": smart_run(sync_sales_plan_manager_reference_to_db),
        "desc": "Google Sheets «Панель управления продажами Вектор», лист «Справочник Категория-Менеджер» → PostgreSQL `sales_plan_category_manager_reference`: сохранение ежедневного снимка справочника менеджеров",
    },
    "sync_sales_plan_accounting_category_reference_to_db": {
        "func": smart_run(sync_sales_plan_accounting_category_reference_to_db),
        "desc": "Google Sheets «Годовой план закупа 2026», лист «Поквартально» → PostgreSQL `sales_plan_accounting_category_reference`: обновление соответствия wild, предмета и учётной категории",
    },
    "sync_sales_wild_status_daily_to_db": {
        "func": smart_run(sync_sales_wild_status_daily_to_db),
        "desc": "Google Sheets «Годовой план закупа 2026», лист «Поквартально» → PostgreSQL `sales_wild_status_daily`: сохранение ежедневного snapshot статусов wild",
    },
    "sales_plan_run": {
        "func": smart_run(sales_plan_run),
        "desc": "Расчёт плана продаж по wild → Google Sheets «План продаж», лист «План продаж v2.0»",
    },
    # Бухгалтерские и регламентные выгрузки.
    "seller_balance_run": {
        "func": smart_run(seller_balance_run),
        "desc": "PostgreSQL финансового отчёта/WB → Google Sheets «ДДС», лист «Переменные»: обновление баланса продавцов по кабинетам",
    },
    "add_new_items_run": {
        "func": smart_run(add_new_items_run),
        "desc": "Google Sheets «Новый товар», лист «Для юнит» → Google Sheets «UNIT 2.0 (tested)», листы «Сопост», «MAIN (tested)» и «Конкуренты», а также «Панель управления продажами Вектор», лист «Автопилот»; новые карточки записываются в PostgreSQL `products`",
    },
    "update_week_n_redeem": {
        "func": smart_run(update_week_n_redeem),
        "desc": "Обновление показателей недельной реализации и выкупов в Google Sheets «ОТЧЕТ за 2026 пров v.2.0»",
    },
    # Планирование закупок: годовой и квартальный контур.
    "transport_data_to_annual_procurement_plan": {
        "func": smart_run(transport_data_to_annual_procurement_plan),
        "desc": "Перенос данных заказов в Google Sheets «Годовой план закупа 2026», лист «БД_ЗАКАЗЫ»",
    },
    "transport_parfume_data_to_annual_procurement_plan": {
        "func": smart_run(transport_parfume_data_to_annual_procurement_plan),
        "desc": "Перенос данных по парфюмерии в Google Sheets «Годовой план закупа 2026», лист «Данные_Парфюм»",
    },
    "transport_unit_data_to_annual_procurement_plan": {
        "func": smart_run(transport_unit_data_to_annual_procurement_plan),
        "desc": "Перенос данных из Google Sheets «UNIT 2.0 (tested)» в таблицу «Годовой план закупа 2026», лист «Данные_Юнитки»",
    },
    "transport_supplies_data_to_annual_procurement_plan": {
        "func": smart_run(transport_supplies_data_to_annual_procurement_plan),
        "desc": "Перенос данных о поставках в Google Sheets «Годовой план закупа 2026», лист «Данные_Поставки»",
    },
    "update_quarterly_prices_to_annual_procurement_plan": {
        "func": smart_run(update_quarterly_prices_to_annual_procurement_plan),
        "desc": "Пересчёт и обновление ценовых колонок в Google Sheets «Годовой план закупа 2026», лист «Поквартально»",
    },
    "transport_quarterly_plan_to_pivot": {
        "func": smart_run(transport_quarterly_plan_to_pivot),
        "desc": "Google Sheets «Годовой план закупа 2026», лист «Поквартально» → Google Sheets «Расчет поставки Китай_по обороту»: перенос поквартальных объёмов в свод по поставщикам",
    },
    "update_payments_analyze_with_ved": {
        "func": smart_run(update_payments_analyze_with_ved),
        "desc": "Google Sheets «Расчет поставки Китай_по обороту», лист «Заказы белые ТЕСТ» → Google Sheets «Платежный календарь» и «Форма Платеж календарь», лист «Аналитика_платежей»: объединение аналитики белых заказов и ВЭД",
    },
    # Аналитика артикулов и закупочной цены.
    "orders_article_analyze_run": {
        "func": smart_run(orders_article_analyze_run),
        "desc": "PostgreSQL `orders` и данные по артикулам → PostgreSQL: расчёт артикульной аналитики и запись результата в таблицу витрины артикульного анализа",
    },
    "purchase_price_update_run": {
        "func": smart_run(purchase_price_update_run),
        "desc": "PostgreSQL → Google Sheets «Новый товар», лист «UNIT: Изменение закупочной цены»: обновление закупочных цен с учётом Google Sheets «UNIT 2.0 (tested)», лист «Сопост»",
    },
    # Условные расчеты и их выгрузки.
    "conditional_calculation_to_db_run": {
        "func": smart_run(conditional_calculation_to_db_run),
        "desc": "Расчёт условных показателей и сохранение результата в PostgreSQL `conditions_calculation` для последующей выгрузки",
    },
    "update_conditional_calculations_to_gs": {
        "func": smart_run(update_conditional_calculations_to_gs),
        "desc": "PostgreSQL `conditions_calculation` → Google Sheets «Условный расчет», лист «Справочная информация»: публикация рассчитанных условных показателей",
    },
    # Финансовая аналитика и управленческая отчетность.
    "update_monthly_report": {
        "func": smart_run(update_monthly_report),
        "desc": "PostgreSQL → Google Sheets «Анализ_фин_отчетов_Вектор», лист «отчет_по_месяцам»: выгрузка сводных данных финансового отчёта за месяц",
    },
    "update_weekly_profit_report": {
        "func": smart_run(update_weekly_profit_report),
        "desc": "PostgreSQL → Google Sheets «Анализ_фин_отчетов_Вектор», лист «отчет_по_неделям»: выгрузка сводных данных финансового отчёта за неделю",
    },
    "update_outcomes_detalize": {
        "func": smart_run(update_outcomes_detalize),
        "desc": "PostgreSQL → Google Sheets «Анализ_фин_отчетов_Вектор», лист «детализация_расходов»: выгрузка детализации расходов",
    },
    "update_fin_deductions_mv": {
        "func": smart_run(update_fin_deductions_mv),
        "desc": "PostgreSQL → Google Sheets «Анализ_фин_отчетов_Вектор», лист «удержания_детализация»: выгрузка детализации удержаний",
    },
    "update_daily_fin_reports_deductions_agg": {
        "func": smart_run(update_daily_fin_reports_deductions_agg),
        "desc": "PostgreSQL → Google Sheets «Анализ_фин_отчетов_Вектор», лист «удержания_детализация_месяц»: выгрузка помесячной детализации удержаний",
    },
    "update_deductions_by_month": {
        "func": smart_run(update_deductions_by_month),
        "desc": "PostgreSQL `deductions_by_month` → Google Sheets «Анализ_фин_отчетов_Вектор», лист «удержания_детализация_месяц»: выгрузка удержаний по месяцам",
    },
    "update_cash_flow_writeoffs": {
        "func": smart_run(update_cash_flow_writeoffs),
        "desc": "Данные затрат из 1С/PostgreSQL → Google Sheets «Анализ_фин_отчетов_Вектор», лист «расходы_по_банку»",
    },
    "update_stock_analyze": {
        "func": smart_run(update_stock_analyze),
        "desc": "Данные артикульного анализа/PostgreSQL → Google Sheets «Анализ_фин_отчетов_Вектор», лист «анализ_остатков»",
    },
    "run_system_penalties_analysis": {
        "func": smart_run(run_system_penalties_analysis),
        "desc": "Построение системной витрины штрафов с событиями FBS и контрольной сверкой",
    },
    # Логистика, склады и операционные остатки.
    "fbo_supplies_run": {
        "func": smart_run(fbo_supplies_run),
        "desc": "PostgreSQL `orders` и `article` → Google Sheets «Отгрузка ФБО», лист «Заказы по округам»: количество заказов за последние 7 дней по округам и артикулам",
    },
    "historical_stocks_run": {
        "func": smart_run(historical_stocks_run),
        "desc": "WMS → PostgreSQL `historical_stocks_fbs_service`: первичная историческая загрузка FBS-остатков",
    },
    "wms_stock_run": {
        "func": smart_run(wms_stock_run),
        "desc": "WMS → PostgreSQL `public.wms_stock`: повторное обновление агрегированных дневных остатков за последние 7 дней",
    },
    "wms_stock_backfill_run": {
        "func": smart_run(wms_stock_backfill_run),
        "desc": "WMS → PostgreSQL `public.wms_stock`: backfill агрегированных дневных остатков начиная с 2026-07-29",
    },
    "update_sales_warehouse_analytics": {
        "func": smart_run(update_sales_warehouse_analytics),
        "desc": "PostgreSQL → Google Sheets «Новый товар», лист «Аналитика складов»: продажи по собственным складам",
    },
    "list_wb_offices": {
        "func": smart_run(list_wb_offices),
        "desc": "Получение списка офисов WB для выбора officeId при создании FBS-склада",
    },
    "list_fbs_warehouses": {
        "func": smart_run(list_fbs_warehouses),
        "desc": "Получение списка FBS-складов продавца WB",
    },
    "create_fbs_warehouse": {
        "func": smart_run(create_fbs_warehouse),
        "desc": "Создание FBS-склада продавца WB по выбранному officeId",
    },
    "delete_fbs_warehouse": {
        "func": smart_run(delete_fbs_warehouse),
        "desc": "Удаление FBS-склада продавца WB по warehouseId",
    },
    "import_created_fbs_warehouse": {
        "func": smart_run(import_created_fbs_warehouse),
        "desc": "Создание или обновление справочника warehouses_fbs из JSON ответа WB",
    },
    "import_existing_fbs_warehouse": {
        "func": smart_run(import_existing_fbs_warehouse),
        "desc": "Добавление существующего WB-склада в справочник warehouses_fbs",
    },
    "sync_fbs_warehouses_from_wb": {
        "func": smart_run(sync_fbs_warehouses_from_wb),
        "desc": "Дозаполнение справочника warehouses_fbs текущими данными складов WB",
    },
    "update_fbs_stocks_in_unit": {
        "func": smart_run(update_fbs_stocks_in_unit),
        "desc": "WB API → Google Sheets «UNIT 2.0 (tested)», лист «MAIN (tested)»: запись фактического общего FBS-остатка в колонку «ФБС общий остаток»",
    },
    "apply_new_fbs_stocks_from_unit": {
        "func": smart_run(apply_new_fbs_stocks_from_unit),
        "desc": "Google Sheets «UNIT 2.0 (tested)», лист «MAIN (tested)», колонки «Новый остаток для всех складов» и «Новый остаток Вешки» → WB API: ручное применение новых FBS-остатков",
    },
    "auto_refill_fbs_stocks_from_unit": {
        "func": smart_run(auto_refill_fbs_stocks_from_unit),
        "desc": "Google Sheets «UNIT 2.0 (tested)», листы «MAIN (tested)» и «Сопост», колонки «Минимальный остаток» и «Добавляем» → WB API: cron-автопополнение складов при снижении остатка ниже порога",
    },
    "wb_stock_control_run": {
        "func": smart_run(wb_stock_control_run),
        "desc": "Полный read-only контроль опубликованных FBS-остатков вне юнитки",
    },
    # UNIT: сервисные обновления справочников, статусов и ценовых витрин.
    "update_adv_participants_to_gs": {
        "func": smart_run(update_adv_participants_to_gs),
        "desc": "WB Advertising API → Google Sheets «UNIT 2.0 (tested)», лист «MAIN (tested)»: обновление признака участия артикула в рекламной кампании",
    },
    "update_wild_statuses": {
        "func": smart_run(update_wild_statuses),
        "desc": "Google Sheets «Годовой план закупа 2026», лист «Поквартально» → Google Sheets «UNIT 2.0 (tested)», лист «MAIN (tested)»: обновление статусов wild",
    },
    # WB API: замеры и производные выгрузки.
    "collect_and_store_measurements": {
        "func": smart_run(collect_and_store_measurements),
        "desc": "WB API → PostgreSQL `wb_measurements`: сбор и сохранение результатов замеров товаров",
    },
    "set_measurements_to_google": {
        "func": smart_run(set_measurements_to_google),
        "desc": "PostgreSQL `wb_measurements` → Google Sheets «Отгрузка ФБО», лист «БД_Замеры_ВБ»: публикация результатов замеров",
    },
    # Закупки Россия: расчетные и транспортные задачи.
    "set_orders_quantity": {
        "func": smart_run(set_orders_quantity),
        "desc": "Запись количества заказов в Google Sheets «Расчет закупки Россия»",
    },
    "transport_orders_and_supply": {
        "func": smart_run(transport_orders_and_supply),
        "desc": "Google Sheets «Расчет закупки Россия» и «Расчет поставки Китай_по обороту»: перенос данных о заказах и поступлениях товаров между расчётными листами",
    },
    "update_supplies_1c_in_purchase_russia": {
        "func": smart_run(update_supplies_1c_in_purchase_russia),
        "desc": "Обновление Google Sheets «Расчет закупки Россия», лист «Приходы_1С», данными о поставках из 1С",
    },
    # Автопилот и индивидуальные настройки.
    "update_individual_info": {
        "func": smart_run(update_individual_info),
        "desc": "Обновление индивидуальных условий в Google Sheets «Панель управления продажами Вектор», лист «Автопилот»",
    },
    "autopilot_hourly_run": {
        "func": smart_run(autopilot_hourly_run),
        "desc": "Почасовое обновление метрик Google Sheets «Панель управления продажами Вектор», лист «Автопилот»",
    },
    "autopilot_remove_duplicates": {
        "func": smart_run(autopilot_remove_duplicates),
        "desc": "Удаление повторных строк артикулов в Google Sheets «Панель управления продажами Вектор», лист «Автопилот»",
    },
    "autopilot_daily_run": {
        "func": smart_run(autopilot_daily_run),
        "desc": "Дневное обновление завершённых дней в Google Sheets «Панель управления продажами Вектор», лист «Автопилот», и связанных блоков Google Sheets «UNIT 2.0 (tested)», лист «MAIN (tested)»",
    },
    # Логистика ВЭД
    "logistic_ved_full_run": {
        "func": smart_run(logistic_ved_full_run),
        "desc": "Синхронизация Google Sheets «Логистика ВЭД 2026», лист «ОТЧЁТ_2.0», с Google Sheets «Расчет поставки Китай_по обороту», лист «Заказы белые ТЕСТ»",
    },
    "returns_to_customers": {
        "func": smart_run(returns_to_customers),
        "desc": "WB Returns API → PostgreSQL `claims` → Google Sheets «Start-Потенциал матрицы», лист «Возвраты»: выгрузка заявок на возврат покупателей",
    },
}
