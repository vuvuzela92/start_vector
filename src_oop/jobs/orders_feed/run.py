"""Точки запуска регулярной и ручной загрузки WB Order Feed."""

from __future__ import annotations

import argparse
import asyncio
import logging
from datetime import datetime

from src_oop.jobs.orders_feed.repository import OrderFeedRepository
from src_oop.jobs.orders_feed.service import OrderFeedService


def _parse_datetime(value: str | datetime | None) -> datetime | None:
    """Разбирает ISO datetime ручного запуска, сохраняя переданный часовой пояс."""
    if value is None or isinstance(value, datetime):
        return value
    try:
        return datetime.fromisoformat(value)
    except ValueError as error:
        raise ValueError("Дата должна быть в ISO-формате, например 2026-07-23T00:00:00+03:00.") from error


async def order_feed_async(
    date_from: str | datetime | None = None,
    date_to: str | datetime | None = None,
    account: str | None = None,
) -> None:
    """Загружает Order Feed и возвращает ошибку планировщику при сбое любого кабинета."""
    summary = await OrderFeedService().run(
        date_from=_parse_datetime(date_from),
        date_to=_parse_datetime(date_to),
        account=account,
    )
    if summary.failed_accounts:
        raise RuntimeError(
            "Загрузка Order Feed завершилась с ошибками кабинетов: "
            f"{', '.join(summary.failed_accounts)}. Уже полученные страницы сохранены."
        )


def order_feed(
    date_from: str | datetime | None = None,
    date_to: str | datetime | None = None,
    account: str | None = None,
) -> None:
    """Запускает полный синхронный сценарий Order Feed для cron и реестра задач."""
    asyncio.run(order_feed_async(date_from=date_from, date_to=date_to, account=account))


def create_order_feed_table() -> None:
    """Создаёт таблицу, enum-типы и индексы Order Feed без выполнения запросов к WB."""
    OrderFeedRepository().create_table()


def main() -> None:
    """Запускает ручную CLI-загрузку с необязательными периодом и кабинетом."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )
    parser = argparse.ArgumentParser(description="Загрузка WB Order Feed в PostgreSQL")
    parser.add_argument("--date-from")
    parser.add_argument("--date-to")
    parser.add_argument("--account")
    parser.add_argument(
        "--create-table-only",
        action="store_true",
        help="Создать таблицу и PostgreSQL enum-типы без обращения к WB API",
    )
    args = parser.parse_args()
    if args.create_table_only:
        create_order_feed_table()
        return
    order_feed(args.date_from, args.date_to, args.account)


if __name__ == "__main__":
    main()
