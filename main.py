# main.py
import argparse
import sys
from src.modules.WB.advert.tasks import advert_info


def main():
    parser = argparse.ArgumentParser(description="Регулировщик запуска задач, просматривает все что напечатали в консоли после слова python main.py")
    
    # Добавляем аргумент 'task'
    parser.add_argument(
        # первое слово после имени скрипта будет записано в переменную task
        "task",
        # Заполняем список запускаемых задач 
        choices=["advert_info"], 
        help="Укажите задачу для запуска из списка choices"
    )
    # Считывает те команды, что попадают в терминал
    args = parser.parse_args()

    if args.task == "advert_info":
        print("🚀 Запуск обновления рекламы...")
        advert_info()
    
    # elif args.task == "warehouse":
    #     # warehouse_info()
    #     print("📦 Запуск обновления склада (в разработке)...")
        
    # elif args.task == "all":
    #     print("🔄 Запуск полной синхронизации...")
    #     advert_info()
        # warehouse_info()
        # cards_info()

if __name__ == "__main__":
    main()