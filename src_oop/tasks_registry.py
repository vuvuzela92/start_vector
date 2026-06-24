import asyncio
import inspect
from typing import Callable, Dict, Any

# --- ИМПОРТЫ ЗАДАЧ ---
from src.modules.WB.advert.tasks import advert_info, advert_spend
from src.modules.WB.reports.tasks import orders_report_today
from src_oop.jobs.bukh_docs.run import get_bukh_docs_async
from src.modules.GOOGLE_SHEETS.credit_analyze_vector import update_credit_data_vector
from src.modules.GOOGLE_SHEETS.week_n_redeem import update_week_n_redeem
from src_oop.jobs.advert.run import advert_stat
from src_oop.jobs.unit.update_adv_participants import update_adv_participants_to_gs
# Годовой план закупа 2026
from src_oop.jobs.annual_procurement_plan.run import transport_data_to_annual_procurement_plan, transport_parfume_data_to_annual_procurement_plan, transport_unit_data_to_annual_procurement_plan, transport_supplies_data_to_annual_procurement_plan
from src_oop.jobs.calculation_of_purchases_china.run import (
    transport_quarterly_plan_to_pivot,
    update_payments_analyze_with_ved,
    update_orders_white_balance_analytics,
    update_test_balance_with_ved,
)
# Артикульный анализ
from src_oop.jobs.orders_articles_analyze.run import orders_article_analyze_run
# Условный расчет
from src_oop.jobs.conditional_calculations.run import conditional_calculation_to_db_run, update_conditional_calculations_to_gs
from src_oop.jobs.unit.update_wild_statuses import update_wild_statuses
# Замеры товара
from src_oop.jobs.wb_api.measurements.run import collect_and_store_measurements, set_measurements_to_google
from src_oop.jobs.wms_stocks.run import wms_stocks_run
# Финансовые отчеты
from src_oop.jobs.fin_reports_analyze.run import update_monthly_report, update_weekly_profit_report, update_outcomes_detalize, update_fin_deductions_mv, update_deductions_by_month, update_cash_flow_writeoffs, update_stock_analyze
# Таблица Расчет закупки Россия
from src_oop.jobs.calculation_of_purchases_russia.run import (
    set_orders_quantity,
    transport_orders_and_supply,
    update_penalties_in_gs_purchase_russia,
)
# Таблица Панель Управления
from src_oop.jobs.autopilot.run import update_individual_info

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
    # ===
    # Раздел: Реклама WB
    # ===
    "advert_info": {
        "func": smart_run(advert_info),
        "desc": "🚀 Запуск обновления данных о рекламных кампаниях"
    },
    "advert_spend": {
        "func": smart_run(advert_spend),
        "desc": "💵 Запуск получения данных о рекламных затратах"
    },

    "advert_stat": {
        "func": smart_run(advert_stat),
        "desc": "📊 Запуск OOP-job получения и записи статистики рекламных кампаний WB"
    },

    # ===
    # Раздел: Отчеты и Документы WB
    # ===
    "orders_report_today": {
        "func": smart_run(orders_report_today),
        "desc": "🛒 Запуск обновления отчета о заказах за сегодня"
    },
    "get_bukh_docs": {
        "func": smart_run(get_bukh_docs_async),
        "desc": "📑 Запуск получения данных по документам бухгалтерии"
    },

    # ===   
    # Для бухгалтерии
    # ===
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
        "desc": "🔁 Запуск обновления данных в ОТЧЕТ за 2026 пров v.2.0"
    },
    
    # ===
    # Годовой план закупа 2026
    # ===
    "transport_data_to_annual_procurement_plan": {
        "func": smart_run(transport_data_to_annual_procurement_plan),
        "desc": "🔁 Запуск обновления в Годовой план закупа 2026"
    },
    "transport_parfume_data_to_annual_procurement_plan": {
        "func": smart_run(transport_parfume_data_to_annual_procurement_plan),
        "desc": "🔁 Запуск обновления данных парфюма в Годовой план закупа 2026"
    },
    "transport_unit_data_to_annual_procurement_plan": {
        "func": smart_run(transport_unit_data_to_annual_procurement_plan),
        "desc": "🔁 Запуск обновления данных юнитки в Годовой план закупа 2026"
    },
    "transport_supplies_data_to_annual_procurement_plan": {
        "func": smart_run(transport_supplies_data_to_annual_procurement_plan),
        "desc": "🔁 Запуск обновления данных поставок в Годовой план закупа 2026"
    },

    # ===
    # Расчет закупки по обороту Китай
    # ===
    "transport_quarterly_plan_to_pivot": {
        "func": smart_run(transport_quarterly_plan_to_pivot),
        "desc": "Перенос поквартального плана в свод по поставщикам"
    },
    "update_orders_white_balance_analytics": {
        "func": smart_run(update_orders_white_balance_analytics),
        "desc": "Выгрузка аналитики платежей по белым заказам на лист "
    },
    "update_test_balance_with_ved": {
        "func": smart_run(update_test_balance_with_ved),
        "desc": "Тестовая выгрузка объединенного balance_df и ved_balance_df в test_sheet"
    },
    "update_payments_analyze_with_ved": {
        "func": smart_run(update_payments_analyze_with_ved),
        "desc": "Production-выгрузка объединенного balance_df и ved_balance_df в payments_analyze_sheet"
    },
    # ===

    # Артикульный анализ
    # ===
    "orders_article_analyze_run": {
        "func": smart_run(orders_article_analyze_run),
        "desc": "📉 Запуск артикульного анализа заказов"
    },
    # ===
    # Условный расчет
    # ===
    "conditional_calculation_to_db_run": {
        "func": smart_run(conditional_calculation_to_db_run),
        "desc": "🧪 Запуск условного расчета и загрузки в БД"
    },
    "update_conditional_calculations_to_gs": {
        "func": smart_run(update_conditional_calculations_to_gs),
        "desc": "📤 Выгрузка условного расчета в Google Sheets"
    },
    # ===
    # Таблица в гугл Анализ_фин_отчетов_Вектор
    # ===
    "update_monthly_report": {
        "func": smart_run(update_monthly_report),
        "desc": "💵 Выгрузка сводных данных фин отчета за месяц в отчет_по_месяцам"
    },  
    "update_weekly_profit_report": {
        "func": smart_run(update_weekly_profit_report),
        "desc": "💵 Выгрузка сводных данных фин отчета за неделю"
    },  
    "update_outcomes_detalize": {
        "func": smart_run(update_outcomes_detalize),
        "desc": "💵 Выгрузка данных фин отчета о еженедельных удержаниях в детализация_расходов"
    }, 
    "update_fin_deductions_mv": {
        "func": smart_run(update_fin_deductions_mv),
        "desc": "💵 Выгрузка детализированных данных фин отчета об удержаниях в удержания_детализация"
    }, 
    "update_deductions_by_month": {
        "func": smart_run(update_deductions_by_month),
        "desc": "💵 Выгрузка детализированных данных фин отчета об удержаниях, сгруппированных по видам удержания_детализация_месяц"
    }, 
    "update_cash_flow_writeoffs": {
        "func": smart_run(update_cash_flow_writeoffs),
        "desc": "💵 Выгрузка детализированных данных по затратам из 1С"
    }, 
    "update_stock_analyze": {
        "func": smart_run(update_stock_analyze),
        "desc": "📦 Выгрузка данных об остатках из арт анализа"
    }, 
    # ===
    # Раздел: Склад
    # ===
    "wms_stocks_run": {
        "func": smart_run(wms_stocks_run),
        "desc": "📦 Выгрузка данных об остатках из сервиса WMS"
    },
    # ===
    # Таблица юнит-экономики
    # ===
    "update_adv_participants_to_gs": {
        "func": smart_run(update_adv_participants_to_gs),
        "desc": "🚗 Выгрузка данных об участии артикулов в рекламных кампаниях в таблице Юнит-экономики"
    },
    "update_wild_statuses": {
        "func": smart_run(update_wild_statuses),
        "desc": "🚩 Обновление статусов вилдов в Юнитке"
    },
    # === Получение данных с WB===
    # python main.py collect_and_store_measurements > output.txt 2>&1 - для запуска из консоли и сохранения логов в файл
    # ===
    "collect_and_store_measurements": {
        "func": smart_run(collect_and_store_measurements),
        "desc": "📥 Сбор и сохранение данных о замерах в БД"
    },
    "set_measurements_to_google": {
        "func": smart_run(set_measurements_to_google),
        "desc": "📤 Запись данных о замерах в таблице Дизайн по новинкам"
    },
    # ===
    # Расчет закупки Россия
    # ===
    "set_orders_quantity": {
        "func": smart_run(set_orders_quantity),
        "desc": "📤 Запись данных о количестве заказов в таблицу Расчет закупки Россия"
    },
    "transport_orders_and_supply": {
        "func": smart_run(transport_orders_and_supply),
        "desc": "📤 Запись данных о заказах и фактических поступлениях товаров"
    },
    
    # ===
    # Панель Управления
    # ===
    "update_individual_info": {
        "func": smart_run(update_individual_info),
        "desc": "🚩 Обновление данных об индивидуальных условиях в ПУ на листе ИУ_ИНФО"
    },
}
