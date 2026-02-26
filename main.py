# main.py
import argparse
import asyncio
# import sys
from src.modules.WB.advert.tasks import advert_info, advert_spend
from src.modules.WB.reports.tasks import orders_report_today
from src.modules.WB.docs.tasks import get_bukh_docs
# === Импорты для гугл-таблиц ===
from src.modules.GOOGLE_SHEETS.calculation_of_purchases_russia import update_penalties_in_gs_purchase_russia
# Импорт данных для обновления таблицы таблица Расчет закупки Россия
from src.modules.GOOGLE_SHEETS.credit_analyze_vector import update_credit_data_vector
# Импорт данных для обновления таблицы ОТЧЕТ за 2023-2024 пров v.2.0
from src.modules.GOOGLE_SHEETS.week_n_redeem import update_week_n_redeem


def main():
    parser = argparse.ArgumentParser(description="Регулировщик запуска задач, просматривает все что напечатали в консоли после слова python main.py")
    
    # Добавляем аргумент 'task'
    parser.add_argument(
        # первое слово после имени скрипта будет записано в переменную task
        "task",
        # Заполняем список запускаемых задач 
        choices=["advert_info", "orders_report_today", "advert_spend", "update_penalties_in_gs_purchase_russia", "update_credit_data_vector", "get_bukh_docs", "update_week_n_redeem"], 
        help="Укажите задачу для запуска из списка choices"
    )
    # Считывает те команды, что попадают в терминал
    args = parser.parse_args()

    #-------------------------------------------------------------------------
     
    # === Запуск программ для раздела реклама ===
    if args.task == "advert_info":
        print("🚀 Запуск обновления рекламы...")
        advert_info()
    elif args.task == "advert_spend":
        print("💵 Запуск получения данных о рекламных затратах")
        advert_spend()
    # === Запуск программ для раздела отчеты ===
    elif args.task == "orders_report_today":
        print("🛒 Запуск обновления отчета о заказах за сегодня")
        orders_report_today()
    # === Запуск программ для раздела документы ===
    # Получение данных по еженедельным отчетам о реализации и Уведомлении о выкупе
    elif args.task == "get_bukh_docs":
        print("📑 Запуск получения данных по документам бухгалтерии")
        asyncio.run(get_bukh_docs())  
    # === Запуск программ для раздела GOOGLE_SHEETS ===
    # таблица Расчет закупки Россия
    elif args.task == "update_penalties_in_gs_purchase_russia":
        print("📊 Запуск обновления данных о штрафах и виртуальных остатках в Google Sheets")
        update_penalties_in_gs_purchase_russia()
    # таблица Кредитный анализ Вектор
    elif args.task == "update_credit_data_vector":
        print("📊 Запуск обновления данных в Google Sheets для Кредитного анализа Вектор")
        update_credit_data_vector()
    # Таблица ОТЧЕТ за 2023-2024 пров v.2.0
    elif args.task == "update_week_n_redeem":
        print("🔁 Запуск обновления данных в гугл-таблице ОТЧЕТ за 2023-2024 пров v.2.0")
        update_week_n_redeem() 
        

if __name__ == "__main__":
    main()