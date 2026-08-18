import asyncio
import inspect
from typing import Any, Callable, Dict

from src.modules.GOOGLE_SHEETS.credit_analyze_vector import update_credit_data_vector
from src.modules.WB.reports.tasks import orders_report_today
from src_oop.jobs.add_new_items.run import add_new_items_run
from src_oop.jobs.advert_info.run import advert_info
from src_oop.jobs.advert.run import advert_stat
from src_oop.jobs.advert_spend.run import advert_spend
from src_oop.jobs.autopilot_daily.run import autopilot_daily_run
from src_oop.jobs.annual_procurement_plan.run import (
    transport_data_to_annual_procurement_plan,
    transport_parfume_data_to_annual_procurement_plan,
    transport_supplies_data_to_annual_procurement_plan,
    transport_unit_data_to_annual_procurement_plan,
)
from src_oop.jobs.autopilot.run import autopilot_hourly_run, update_individual_info
from src_oop.jobs.bukh_docs.run import get_bukh_docs_async
from src_oop.jobs.bukh_docs.week_n_redeem_run import update_week_n_redeem
from src_oop.jobs.calculation_of_purchases_china.run import (
    transport_quarterly_plan_to_pivot,
    update_orders_white_balance_analytics,
    update_payments_analyze_with_ved,
    update_test_balance_with_ved,
)
from src_oop.jobs.calculation_of_purchases_russia.run import (
    set_orders_quantity,
    transport_orders_and_supply,
    update_penalties_in_gs_purchase_russia,
)
from src_oop.jobs.conditional_calculations.run import (
    conditional_calculation_to_db_run,
    update_conditional_calculations_to_gs,
)
from src_oop.jobs.fin_reports_analyze.run import (
    update_cash_flow_writeoffs,
    update_deductions_by_month,
    update_fin_deductions_mv,
    update_monthly_report,
    update_outcomes_detalize,
    update_stock_analyze,
    update_weekly_profit_report,
)
from src_oop.jobs.fbo_supplies.run import fbo_supplies_run
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
from src_oop.jobs.logistic_ved.reverse_run import logistic_ved_reverse_run
from src_oop.jobs.logistic_ved.run import logistic_ved_full_run, logistic_ved_run
from src_oop.jobs.orders_articles_analyze.run import orders_article_analyze_run
from src_oop.jobs.orders_feed.run import order_feed
from src_oop.jobs.purchase_price_update.run import purchase_price_update_run
from src_oop.jobs.returns_to_customers.run import returns_to_customers
from src_oop.jobs.sales_analyze.run import update_sales_warehouse_analytics
from src_oop.jobs.unit.competitors import update_competitors_prices
from src_oop.jobs.unit.update_adv_participants import update_adv_participants_to_gs
from src_oop.jobs.unit.update_wild_statuses import update_wild_statuses
from src_oop.jobs.wb_api.measurements.run import (
    collect_and_store_measurements,
    set_measurements_to_google,
)
from src_oop.jobs.wms_stocks.run import wms_stocks_run


def smart_run(func: Callable):
    if inspect.iscoroutinefunction(func):
        return lambda: asyncio.run(func())
    return func


TASKS: Dict[str, Dict[str, Any]] = {
    # Маркетплейс WB: реклама и ежедневные оперативные отчеты.
    "advert_info": {
        "func": smart_run(advert_info),
        "desc": "Запуск обновления данных о рекламных кампаниях",
    },
    "advert_spend": {
        "func": smart_run(advert_spend),
        "desc": "Запуск получения данных о рекламных затратах",
    },
    "advert_stat": {
        "func": smart_run(advert_stat),
        "desc": "Запуск OOP-job статистики рекламных кампаний WB",
    },
    "orders_report_today": {
        "func": smart_run(orders_report_today),
        "desc": "Запуск обновления отчета о заказах за сегодня",
    },
    "order_feed": {
        "func": smart_run(order_feed),
        "desc": "Почасовое обновление WB Order Feed за доступные последние 31 сутки",
    },
    # Старый контур Google Sheets и управленческих витрин.
    "update_penalties_in_gs_purchase_russia": {
        "func": smart_run(update_penalties_in_gs_purchase_russia),
        "desc": "Обновление данных о штрафах и остатках в Google Sheets",
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
    # Бухгалтерские и регламентные выгрузки.
    "add_new_items_run": {
        "func": smart_run(add_new_items_run),
        "desc": "OOP transfer of new items to UNIT, autopilot, competitors and products",
    },
    "update_week_n_redeem": {
        "func": smart_run(update_week_n_redeem),
        "desc": "Обновление данных в ОТЧЕТ за 2026 пров v.2.0",
    },
    # Планирование закупок: годовой и квартальный контур.
    "transport_data_to_annual_procurement_plan": {
        "func": smart_run(transport_data_to_annual_procurement_plan),
        "desc": "Обновление годового плана закупа 2026 во вкладке БД_ЗАКАЗЫ",
    },
    "transport_parfume_data_to_annual_procurement_plan": {
        "func": smart_run(transport_parfume_data_to_annual_procurement_plan),
        "desc": "Обновление данных парфюма в годовом плане закупа 2026",
    },
    "transport_unit_data_to_annual_procurement_plan": {
        "func": smart_run(transport_unit_data_to_annual_procurement_plan),
        "desc": "Обновление данных юнитки в годовом плане закупа 2026",
    },
    "transport_supplies_data_to_annual_procurement_plan": {
        "func": smart_run(transport_supplies_data_to_annual_procurement_plan),
        "desc": "Обновление данных поставок в годовом плане закупа 2026",
    },
    "transport_quarterly_plan_to_pivot": {
        "func": smart_run(transport_quarterly_plan_to_pivot),
        "desc": "Перенос поквартального плана в свод по поставщикам",
    },
    "update_orders_white_balance_analytics": {
        "func": smart_run(update_orders_white_balance_analytics),
        "desc": "Выгрузка аналитики платежей по белым заказам",
    },
    "update_test_balance_with_ved": {
        "func": smart_run(update_test_balance_with_ved),
        "desc": "Тестовая выгрузка объединенного balance_df и ved_balance_df",
    },
    "update_payments_analyze_with_ved": {
        "func": smart_run(update_payments_analyze_with_ved),
        "desc": "Production-выгрузка объединенного balance_df и ved_balance_df в Платежный календарь во вкладку Аналитика_платежей",
    },
    # Аналитика артикулов и закупочной цены.
    "orders_article_analyze_run": {
        "func": smart_run(orders_article_analyze_run),
        "desc": "Запуск артикульного анализа заказов",
    },
    "purchase_price_update_run": {
        "func": smart_run(purchase_price_update_run),
        "desc": "Запуск обновления закупочных цен в UNIT по актуальным данным БД",
    },
    # Условные расчеты и их выгрузки.
    "conditional_calculation_to_db_run": {
        "func": smart_run(conditional_calculation_to_db_run),
        "desc": "Запуск условного расчета и загрузки в БД",
    },
    "update_conditional_calculations_to_gs": {
        "func": smart_run(update_conditional_calculations_to_gs),
        "desc": "Выгрузка условного расчета в Google Sheets",
    },
    # Финансовая аналитика и управленческая отчетность.
    "update_monthly_report": {
        "func": smart_run(update_monthly_report),
        "desc": "Выгрузка сводных данных фин отчета за месяц",
    },
    "update_weekly_profit_report": {
        "func": smart_run(update_weekly_profit_report),
        "desc": "Выгрузка сводных данных фин отчета за неделю",
    },
    "update_outcomes_detalize": {
        "func": smart_run(update_outcomes_detalize),
        "desc": "Выгрузка детализации расходов фин отчета",
    },
    "update_fin_deductions_mv": {
        "func": smart_run(update_fin_deductions_mv),
        "desc": "Выгрузка детализации удержаний фин отчета",
    },
    "update_deductions_by_month": {
        "func": smart_run(update_deductions_by_month),
        "desc": "Выгрузка удержаний по месяцам",
    },
    "update_cash_flow_writeoffs": {
        "func": smart_run(update_cash_flow_writeoffs),
        "desc": "Выгрузка данных по затратам из 1С",
    },
    "update_stock_analyze": {
        "func": smart_run(update_stock_analyze),
        "desc": "Выгрузка данных об остатках из арт анализа",
    },
    # Логистика, склады и операционные остатки.
    "fbo_supplies_run": {
        "func": smart_run(fbo_supplies_run),
        "desc": "Выгрузка заказов по округам из PostgreSQL в Google Sheets Отгрузка ФБО",
    },
    "wms_stocks_run": {
        "func": smart_run(wms_stocks_run),
        "desc": "Выгрузка данных об остатках из WMS",
    },
    "update_sales_warehouse_analytics": {
        "func": smart_run(update_sales_warehouse_analytics),
        "desc": "Выгрузка аналитики продаж по нашим складам в Google Sheets",
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
        "desc": "Обновление текущих FBS-остатков WB в UNIT 2.0 (tested)",
    },
    "apply_new_fbs_stocks_from_unit": {
        "func": smart_run(apply_new_fbs_stocks_from_unit),
        "desc": "Отправка новых FBS-остатков из UNIT в WB",
    },
    "auto_refill_fbs_stocks_from_unit": {
        "func": smart_run(auto_refill_fbs_stocks_from_unit),
        "desc": "Cron-автопополнение FBS-остатков по минимальному остатку UNIT",
    },
    # UNIT: сервисные обновления справочников, статусов и ценовых витрин.
    "update_adv_participants_to_gs": {
        "func": smart_run(update_adv_participants_to_gs),
        "desc": "Выгрузка участия артикулов в рекламных кампаниях",
    },
    "update_wild_statuses": {
        "func": smart_run(update_wild_statuses),
        "desc": "Обновление статусов вилдов в юнитке",
    },
    "update_competitors_prices": {
        "func": smart_run(update_competitors_prices),
        "desc": "Обновление колонок конкурентов и цен в UNIT-таблице",
    },
    # WB API: замеры и производные выгрузки.
    "collect_and_store_measurements": {
        "func": smart_run(collect_and_store_measurements),
        "desc": "Сбор и сохранение данных о замерах в БД",
    },
    "set_measurements_to_google": {
        "func": smart_run(set_measurements_to_google),
        "desc": "Запись данных о замерах в гугл-таблицу Отгрузка ФБО",
    },
    # Закупки Россия: расчетные и транспортные задачи.
    "set_orders_quantity": {
        "func": smart_run(set_orders_quantity),
        "desc": "Запись данных о количестве заказов в гугл-таблицу Расчет Закупки Россия",
    },
    "transport_orders_and_supply": {
        "func": smart_run(transport_orders_and_supply),
        "desc": "Запись данных о заказах и поступлениях товаров",
    },
    # Автопилот и индивидуальные настройки.
    "update_individual_info": {
        "func": smart_run(update_individual_info),
        "desc": "Обновление данных об индивидуальных условиях",
    },
    "autopilot_hourly_run": {
        "func": smart_run(autopilot_hourly_run),
        "desc": "Почасовое обновление метрик панели управления автопилотом",
    },
    "autopilot_daily_run": {
        "func": smart_run(autopilot_daily_run),
        "desc": "Дневное обновление завершенных дней панели управления автопилотом и связанных UNIT-блоков",
    },
    # Логистика ВЭД
    "logistic_ved_run": {
        "func": smart_run(logistic_ved_run),
        "desc": "Актуализация таблицы Логистика ВЭД 2026 из расчета поставки Китай по ключу ORDER_LINE_ID",
    },
    "logistic_ved_full_run": {
        "func": smart_run(logistic_ved_full_run),
        "desc": "Полный цикл Логистика ВЭД: сначала обратная отправка данных в Заказы белые ТЕСТ, затем актуализация ОТЧЁТ_2.0",
    },
    "logistic_ved_reverse_run": {
        "func": smart_run(logistic_ved_reverse_run),
        "desc": "Обратная отправка логистических полей из ОТЧЁТ_2.0 в Заказы белые ТЕСТ по ключу ORDER_LINE_ID",
    },
    "returns_to_customers": {
        "func": smart_run(returns_to_customers),
        "desc": "Выгрузка заявок по возвратам покупателей WB в Google Sheets",
    },
}
