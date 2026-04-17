import argparse
import sys
from src_oop.tasks_registry import TASKS
import traceback

def main():
    parser = argparse.ArgumentParser(description="Регулировщик запуска задач")
    
    # Список choices берется автоматически из ключей словаря TASKS
    parser.add_argument(
        "task",
        choices=list(TASKS.keys()), 
        help="Укажите задачу для запуска"
    )
    
    # Если аргументов нет, выводим help
    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(1)

    args = parser.parse_args()
    
    # Получаем данные задачи
    task_data = TASKS[args.task]
    
    # Вывод описания и запуск
    print(f"\n{'='*50}")
    print(task_data["desc"])
    print(f"{'='*50}\n")
    
    try:
        # Запуск функции
        task_data["func"]()
        print(f"\n✅ Задача '{args.task}' успешно завершена.")
    except Exception as e:
        # Выводим заголовок ошибки
        print(f"\n❌ Ошибка при выполнении задачи '{args.task}':")
        
        # Печатаем подробный путь ошибки (traceback)
        # file=sys.stdout гарантирует, что текст попадет в стандартный вывод
        traceback.print_exc(file=sys.stdout)
        
        sys.exit(1)

if __name__ == "__main__":
    main()