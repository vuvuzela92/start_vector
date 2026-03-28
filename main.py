import argparse
import sys
from src_oop.tasks_registry import TASKS

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
        # Запуск функции (smart_run уже подготовил её)
        task_data["func"]()
        print(f"\n✅ Задача '{args.task}' успешно завершена.")
    except Exception as e:
        print(f"\n❌ Ошибка при выполнении задачи '{args.task}': {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()