"""Точки запуска регулярной и ручной загрузки WB Order Feed."""

from __future__ import annotations

import argparse
import asyncio
import logging
from datetime import datetime
from zoneinfo import ZoneInfo

from src_oop.jobs.orders_feed.repository import OrderFeedRepository
from src_oop.jobs.orders_feed.service import OrderFeedService

MOSCOW_TZ = ZoneInfo("Europe/Moscow")
logger = logging.getLogger(__name__)


def _parse_datetime(value: str | datetime | None) -> datetime | None:
    """Разбирает удобные форматы даты ручного запуска."""
    if value is None or isinstance(value, datetime):
        return value
    normalized = value.strip()
    if not normalized:
        return None
    try:
        # Поддерживает YYYY-MM-DD, пробел или T, минуты/секунды и timezone.
        parsed = datetime.fromisoformat(normalized)
        return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=MOSCOW_TZ)
    except ValueError:
        pass

    for date_format in (
        "%d.%m.%Y %H:%M:%S",
        "%d.%m.%Y %H:%M",
        "%d.%m.%Y",
    ):
        try:
            return datetime.strptime(normalized, date_format).replace(tzinfo=MOSCOW_TZ)
        except ValueError:
            continue

    raise ValueError(
        "Не удалось распознать дату. Допустимые примеры: "
        "2026-07-23, '2026-07-23 12:00', "
        "2026-07-23T12:00:00+03:00 или '23.07.2026 12:00'."
    )


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
    parser.add_argument(
        "--date-from",
        help="Начало: YYYY-MM-DD, 'YYYY-MM-DD HH:MM' или ISO datetime",
    )
    parser.add_argument(
        "--date-to",
        help="Конец: YYYY-MM-DD, 'YYYY-MM-DD HH:MM' или ISO datetime",
    )
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
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Программа останолвенна вручную")

