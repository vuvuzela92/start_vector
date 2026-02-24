# main.py
import argparse
import asyncio
# import sys
from src.modules.WB.advert.tasks import advert_info, advert_spend
from src.modules.WB.reports.tasks import orders_report_today
from src.modules.WB.docs.tasks import get_bukh_docs
from src.modules.GOOGLE_SHEETS.calculation_of_purchases_russia import update_penalties_in_gs_purchase_russia
from src.modules.GOOGLE_SHEETS.credit_analyze_vector import update_credit_data_vector


def main():
    parser = argparse.ArgumentParser(description="Регулировщик запуска задач, просматривает все что напечатали в консоли после слова python main.py")
    
    # Добавляем аргумент 'task'
    parser.add_argument(
        # первое слово после имени скрипта будет записано в переменную task
        "task",
        # Заполняем список запускаемых задач 
        choices=["advert_info", "orders_report_today", "advert_spend", "update_penalties_in_gs_purchase_russia", "update_credit_data_vector", "get_bukh_docs"], 
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
    # === Запуск программ для раздела GOOGLE_SHEETS ===
    # таблица Расчет закупки Россия
    elif args.task == "update_penalties_in_gs_purchase_russia":
        print("📊 Запуск обновления данных о штрафах и виртуальных остатках в Google Sheets")
        update_penalties_in_gs_purchase_russia()
    # таблица Кредитный анализ Вектор
    elif args.task == "update_credit_data_vector":
        print("📊 Запуск обновления данных в Google Sheets для Кредитного анализа Вектор")
        update_credit_data_vector()
    elif args.task == "get_bukh_docs":
        print("📑 Запуск получения данных по документам бухгалтерии")
        asyncio.run(get_bukh_docs())   
        
    # elif args.task == "all":
    #     print("🔄 Запуск полной синхронизации...")
    #     advert_info()
        # warehouse_info()
        # cards_info()

if __name__ == "__main__":
    main()