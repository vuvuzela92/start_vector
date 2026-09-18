"""Entrypoint контроля остатков WB."""

from src_oop.jobs.wb_stock_control.service import WBStockControlService


async def wb_stock_control_run() -> None:
    """Запускает полный read-only контроль остатков WB и уведомляет коммерческий отдел."""
    await WBStockControlService().run()
