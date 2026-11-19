"""Entrypoint контроля остатков WB."""

from src_oop.jobs.wb_stock_control.service import WBStockControlService


async def wb_stock_control_run() -> None:
    """Запускает контроль, обнуление неподконтрольных остатков и отчёт в Bitrix24."""
    await WBStockControlService().run()
