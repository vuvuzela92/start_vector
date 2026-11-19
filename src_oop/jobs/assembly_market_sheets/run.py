"""Точки входа задач старых витрин сборочных заданий."""

from __future__ import annotations

import asyncio

from src_oop.jobs.assembly_market_sheets.service import AssemblyMarketSheetsService


async def market_orders_run() -> None:
    """Загружает заказы WB всех кабинетов за последние 11 дней в тестовый лист."""
    await AssemblyMarketSheetsService().update_orders()


def market_status_from_db_run() -> None:
    """Связывает статусы PostgreSQL с заданиями листа и публикует результат."""
    AssemblyMarketSheetsService().update_statuses()


def migration_data_to_hang_run() -> None:
    """Переносит сборочные задания из рабочей таблицы в таблицу висящих заказов."""
    AssemblyMarketSheetsService().migrate_assembly_data()


def main() -> None:
    """Запускает асинхронный сценарий загрузки заказов WB в тестовый лист."""
    asyncio.run(market_orders_run())


if __name__ == "__main__":
    main()
