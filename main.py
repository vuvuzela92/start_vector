# main.py
import argparse
import sys
from src.modules.WB.advert.tasks import advert_info, advert_spend
from src.modules.WB.reports.tasks import orders_report_today
from src.modules.GOOGLE_SHEETS.calculation_of_purchases_russia import update_penalties_in_gs_purchase_russia


def main():
    parser = argparse.ArgumentParser(description="Регулировщик запуска задач, просматривает все что напечатали в консоли после слова python main.py")
    
    # Добавляем аргумент 'task'
    parser.add_argument(
        # первое слово после имени скрипта будет записано в переменную task
        "task",
        # Заполняем список запускаемых задач 
        choices=["advert_info", "orders_report_today", "advert_spend", "update_penalties_in_gs_purchase_russia"], 
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
    elif args.task == "update_penalties_in_gs_purchase_russia":
        print("📊 Запуск обновления данных о штрафах и виртуальных остатках в Google Sheets")
        update_penalties_in_gs_purchase_russia()
        
    # elif args.task == "all":
    #     print("🔄 Запуск полной синхронизации...")
    #     advert_info()
        # warehouse_info()
        # cards_info()

if __name__ == "__main__":
    main()