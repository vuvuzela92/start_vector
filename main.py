import argparse
import logging
import sys
import traceback

from src_oop.core.logger import setup_logger
from src_oop.tasks_registry import TASKS


logger = logging.getLogger(__name__)


def main() -> None:
    """Запускает задачу из реестра по имени, переданному через CLI."""
    setup_logger()

    parser = argparse.ArgumentParser(description="Запуск задач проекта")
    parser.add_argument(
        "task",
        choices=list(TASKS.keys()),
        help="Укажите имя задачи для запуска",
    )

    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(1)

    args = parser.parse_args()
    logger.info(
        "Получен запрос на запуск задачи из CLI | task=%s | argv=%s",
        args.task,
        sys.argv[1:],
    )

    task_data = TASKS[args.task]
    logger.info(
        "Задача найдена в реестре | task=%s | callable=%s",
        args.task,
        getattr(task_data["func"], "__name__", repr(task_data["func"])),
    )

    print(f"\n{'=' * 50}")
    print(task_data["desc"])
    print(f"{'=' * 50}\n")

    try:
        logger.info("Начато выполнение задачи | task=%s", args.task)
        task_data["func"]()
        logger.info("Задача завершилась успешно | task=%s", args.task)
        print(f"\n✅ Задача '{args.task}' успешно завершена.")
    except Exception as error:
        logger.exception(
            "Ошибка при выполнении задачи | task=%s | error_type=%s | error=%s",
            args.task,
            type(error).__name__,
            error,
        )
        print(f"\n❌ Ошибка при выполнении задачи '{args.task}':")
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)


if __name__ == "__main__":
    main()
