import asyncio
import inspect
from typing import Callable, Dict, Any

# --- ИМПОРТЫ ЗАДАЧ ---
from src.modules.WB.advert.tasks import advert_info, advert_spend
from src.modules.WB.reports.tasks import orders_report_today
from src.modules.WB.docs.tasks import get_bukh_docs
from src.modules.GOOGLE_SHEETS.calculation_of_purchases_russia import update_penalties_in_gs_purchase_russia
from src.modules.GOOGLE_SHEETS.credit_analyze_vector import update_credit_data_vector
from src.modules.GOOGLE_SHEETS.week_n_redeem import update_week_n_redeem
from src_oop.services.googles_sheets_job.annual_procurement_plan import transport_data_to_annual_procurement_plan
from src_oop.jobs.orders_articles_analyze.run import orders_article_analyze_run
from src_oop.jobs.conditional_calculations.run import conditional_calculation_to_db_run, update_conditional_calculations_to_gs
from src_oop.jobs.wms_stocks.run import wms_stocks_run
from src_oop.jobs.fin_reports_analyze.run import update_monthly_profit_report, update_weekly_profit_report, update_outcomes_detalize, update_fin_deductions_mv, update_deductions_by_month, update_cash_flow_writeoffs

def smart_run(func: Callable):
    """
    Автоматически определяет, как запускать функцию:
    как обычную или через asyncio.run()
    """
    if inspect.iscoroutinefunction(func):
        return lambda: asyncio.run(func())
    return func

# --- РЕЕСТР ЗАДАЧ ---
# Формат: "команда_в_консоли": (функция, "текст_описания")
TASKS: Dict[str, Dict[str, Any]] = {
    # Раздел: Реклама WB
    "advert_info": {
        "func": smart_run(advert_info),
        "desc": "🚀 Запуск обновления данных о рекламных кампаниях"
    },
    "advert_spend": {
        "func": smart_run(advert_spend),
        "desc": "💵 Запуск получения данных о рекламных затратах"
    },

    # Раздел: Отчеты и Документы WB
    "orders_report_today": {
        "func": smart_run(orders_report_today),
        "desc": "🛒 Запуск обновления отчета о заказах за сегодня"
    },
    "get_bukh_docs": {
        "func": smart_run(get_bukh_docs),
        "desc": "📑 Запуск получения данных по документам бухгалтерии"
    },

    # Раздел: Google Sheets (Закупки и Аналитика)
    "update_penalties_in_gs_purchase_russia": {
        "func": smart_run(update_penalties_in_gs_purchase_russia),
        "desc": "📊 Запуск обновления данных о штрафах и остатках в Google Sheets"
    },
    "update_credit_data_vector": {
        "func": smart_run(update_credit_data_vector),
        "desc": "📊 Запуск обновления данных для Кредитного анализа Вектор"
    },
    "update_week_n_redeem": {
        "func": smart_run(update_week_n_redeem),
        "desc": "🔁 Запуск обновления данных в ОТЧЕТ за 2023-2024"
    },
    "transport_data_to_annual_procurement_plan": {
        "func": smart_run(transport_data_to_annual_procurement_plan),
        "desc": "🔁 Запуск обновления в Годовой план закупа 2026"
    },

    # Артикульный анализ
    "orders_article_analyze_run": {
        "func": smart_run(orders_article_analyze_run),
        "desc": "📉 Запуск артикульного анализа заказов"
    },

    # Условный расчет
    "conditional_calculation_to_db_run": {
        "func": smart_run(conditional_calculation_to_db_run),
        "desc": "🧪 Запуск условного расчета и загрузки в БД"
    },
    "update_conditional_calculations_to_gs": {
        "func": smart_run(update_conditional_calculations_to_gs),
        "desc": "📤 Выгрузка условного расчета в Google Sheets"
    },
    # Таблица в гугл Анализ_фин_отчетов_Вектор
    "update_monthly_profit_report": {
        "func": smart_run(update_monthly_profit_report),
        "desc": "💵 Выгрузка сводных данных фин отчета за месяц"
    },  
    "update_weekly_profit_report": {
        "func": smart_run(update_weekly_profit_report),
        "desc": "💵 Выгрузка сводных данных фин отчета за неделю"
    },  
    "update_outcomes_detalize": {
        "func": smart_run(update_outcomes_detalize),
        "desc": "💵 Выгрузка данных фин отчета о еженедельных удержаниях"
    }, 
    "update_fin_deductions_mv": {
        "func": smart_run(update_fin_deductions_mv),
        "desc": "💵 Выгрузка детализированных данных фин отчета об удержаниях"
    }, 
    "update_deductions_by_month": {
        "func": smart_run(update_deductions_by_month),
        "desc": "💵 Выгрузка детализированных данных фин отчета об удержаниях, сгруппированных по типам"
    }, 
    "update_cash_flow_writeoffs": {
        "func": smart_run(update_cash_flow_writeoffs),
        "desc": "💵 Выгрузка детализированных данных по затратам из 1С"
    }, 
    # Раздел: Склад
    "wms_stocks_run": {
        "func": smart_run(wms_stocks_run),
        "desc": "📦 Выгрузка данных об остатках из сервиса WMS"
    },
}