import argparse
import logging
import sys
import traceback

from src_oop.core.logger import setup_logger
from src_oop.tasks_registry import TASKS


logger = logging.getLogger(__name__)


def main() -> None:
    """Запускает задачу из реестра и передаёт ей параметры периода.

    Для `orders_article_analyze_run` поддерживаются сокращённый параметр
    `--days` и явные границы `--days-ago`/`--days-to`. Остальные задачи
    запускаются без параметров периода.
    """
    setup_logger()

    parser = argparse.ArgumentParser(description="Запуск задач проекта")
    parser.add_argument(
        "task",
        choices=list(TASKS.keys()),
        help="Укажите имя задачи для запуска",
    )
    parser.add_argument(
        "--days",
        type=int,
        help="Количество последних завершённых дней для orders_article_analyze_run",
    )
    parser.add_argument(
        "--days-ago",
        type=int,
        help="Начальная граница периода в днях назад для orders_article_analyze_run",
    )
    parser.add_argument(
        "--days-to",
        type=int,
        help="Конечная граница периода в днях назад для orders_article_analyze_run",
    )

    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(1)

    args = parser.parse_args()
    period_arguments = (args.days, args.days_ago, args.days_to)
    if any(value is not None for value in period_arguments):
        if args.task != "orders_article_analyze_run":
            parser.error("Параметры периода доступны только для orders_article_analyze_run")
        if args.days is not None and (args.days_ago is not None or args.days_to is not None):
            parser.error("Нельзя одновременно использовать --days и --days-ago/--days-to")
        if args.days is None and (args.days_ago is None or args.days_to is None):
            parser.error("Для диапазона нужно одновременно указать --days-ago и --days-to")

        if args.days is not None:
            task_kwargs = {"days_ago_total": args.days, "days_to": 1}
        else:
            task_kwargs = {
                "days_ago_total": args.days_ago,
                "days_to": args.days_to,
            }
    else:
        task_kwargs = {}
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
        task_data["func"](**task_kwargs)
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
