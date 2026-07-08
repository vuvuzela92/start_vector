import asyncio
import inspect
from typing import Any, Callable, Dict

from src.modules.GOOGLE_SHEETS.credit_analyze_vector import update_credit_data_vector
from src.modules.WB.advert.tasks import advert_info, advert_spend
from src.modules.WB.reports.tasks import orders_report_today
from src_oop.jobs.advert.run import advert_stat
from src_oop.jobs.annual_procurement_plan.run import (
    transport_data_to_annual_procurement_plan,
    transport_parfume_data_to_annual_procurement_plan,
    transport_supplies_data_to_annual_procurement_plan,
    transport_unit_data_to_annual_procurement_plan,
)
from src_oop.jobs.autopilot.run import update_individual_info
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
from src_oop.jobs.logistic_ved.run import logistic_ved_run
from src_oop.jobs.orders_articles_analyze.run import orders_article_analyze_run
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
    "update_penalties_in_gs_purchase_russia": {
        "func": smart_run(update_penalties_in_gs_purchase_russia),
        "desc": "Обновление данных о штрафах и остатках в Google Sheets",
    },
    # === Бухгалтерия ===
    "get_bukh_docs": {
        "func": smart_run(get_bukh_docs_async),
        "desc": "Запуск получения данных по бухгалтерским документам",
    },
    "update_credit_data_vector": {
        "func": smart_run(update_credit_data_vector),
        "desc": "Обновление данных для кредитного анализа Вектор",
    },
    "update_week_n_redeem": {
        "func": smart_run(update_week_n_redeem),
        "desc": "Обновление данных в ОТЧЕТ за 2026 пров v.2.0",
    },
    # === Гугл-таблица годовой план закупа ===
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
        "desc": "Production-выгрузка объединенного balance_df и ved_balance_df",
    },
    # === Артикульный анализ === 
    "orders_article_analyze_run": {
        "func": smart_run(orders_article_analyze_run),
        "desc": "Запуск артикульного анализа заказов",
    },
    # === Условный расчет ===
    "conditional_calculation_to_db_run": {
        "func": smart_run(conditional_calculation_to_db_run),
        "desc": "Запуск условного расчета и загрузки в БД",
    },
    "update_conditional_calculations_to_gs": {
        "func": smart_run(update_conditional_calculations_to_gs),
        "desc": "Выгрузка условного расчета в Google Sheets",
    },
    # === Финаносвый отчет ===
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
    # === Данные об остатках ===
    "update_stock_analyze": {
        "func": smart_run(update_stock_analyze),
        "desc": "Выгрузка данных об остатках из арт анализа",
    },
    # === Выгрузка в таблицу отгрузка ФБО ===
    "logistic_ved_run": {
        "func": smart_run(logistic_ved_run),
        "desc": "Выгрузка заказов по округам из PostgreSQL в Google Sheets Отгрузка ФБО",
    },
    "wms_stocks_run": {
        "func": smart_run(wms_stocks_run),
        "desc": "Выгрузка данных об остатках из WMS",
    },
    # === Таблица Юнит-Экономики ===
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
    # Данные о замерах габаритов товаров на ВБ ===
    "collect_and_store_measurements": {
        "func": smart_run(collect_and_store_measurements),
        "desc": "Сбор и сохранение данных о замерах в БД",
    },
    "set_measurements_to_google": {
        "func": smart_run(set_measurements_to_google),
        "desc": "Запись данных о замерах в гугл-таблицу Отгрузка ФБО",
    },
    # === Гугл-таблица Расчет Закупки Россия ===
    "set_orders_quantity": {
        "func": smart_run(set_orders_quantity),
        "desc": "Запись данных о количестве заказов в гугл-таблицу Расчет закупки Россия",
    },
    "transport_orders_and_supply": {
        "func": smart_run(transport_orders_and_supply),
        "desc": "Запись данных о заказах и поступлениях товаров",
    },
    "update_individual_info": {
        "func": smart_run(update_individual_info),
        "desc": "Обновление данных об индивидуальных условиях",
    },
}
