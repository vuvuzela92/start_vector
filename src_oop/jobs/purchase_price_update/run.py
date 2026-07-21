from __future__ import annotations

import logging

from src_oop.core.logger import setup_logger
from src_oop.core.my_gspread import GoogleTabs
from src_oop.jobs.purchase_price_update.comparison import (
    ComparisonSummary,
    compare_with_legacy,
)
from src_oop.jobs.purchase_price_update.config import UNIT_SHEET_CONFIG
from src_oop.jobs.purchase_price_update.processor import (
    build_unit_sheet_dataframe,
    prepare_unit_state,
)
from src_oop.jobs.purchase_price_update.repository import PurchasePriceUpdateRepository
from src_oop.jobs.purchase_price_update.service import (
    PurchasePriceUpdateService,
    PurchasePriceUpdateSummary,
)

logger = logging.getLogger(__name__)


def purchase_price_update_run(round_price: bool = True) -> PurchasePriceUpdateSummary:
    """
    Боевая точка входа для обновления закупочных цен.

    По умолчанию используется legacy-совместимое округление,
    чтобы поведение новой реализации не отличалось от исторического скрипта.
    """

    setup_logger()
    logger.info("Инициализация боевого запуска purchase_price_update.")
    service = PurchasePriceUpdateService(
        repository=PurchasePriceUpdateRepository(),
    )
    return service.run(round_price=round_price)


def purchase_price_update_compare(round_price: bool = True) -> ComparisonSummary:
    """
    Диагностический режим сравнения без записи в Google Sheets.

    Используется для безопасной проверки того, что новый путь подготовки данных
    дает тот же результат, что и legacy-совместимый сценарий.
    """

    setup_logger()
    logger.info("Инициализация compare-режима purchase_price_update.")
    repository = PurchasePriceUpdateRepository()
    db_dataframe = repository.fetch_latest_purchase_prices(days_count=2)
    unit_connector = GoogleTabs(
        table_title=UNIT_SHEET_CONFIG.table_title,
        sheet_title=UNIT_SHEET_CONFIG.sheet_title,
    )
    unit_values = unit_connector.sheet_title.get_all_values()
    unit_dataframe = build_unit_sheet_dataframe(
        values=unit_values,
        header_row_index=UNIT_SHEET_CONFIG.header_row_index,
        data_row_index=UNIT_SHEET_CONFIG.data_row_index,
    )
    unit_state = prepare_unit_state(unit_dataframe)
    summary, _ = compare_with_legacy(
        db_dataframe=db_dataframe,
        unit_state=unit_state,
        round_price=round_price,
    )
    return summary


if __name__ == "__main__":
    purchase_price_update_run()
