"""Регрессионные проверки ложных тревог по SKU из рабочей UNIT."""

import asyncio
from datetime import UTC, datetime
from unittest.mock import Mock, patch

import pytest

from src_oop.jobs.wb_stock_control.config import StockControlSettings
from src_oop.jobs.wb_stock_control.google_sheets_client import (
    parse_unit_article_ids,
    read_unit_article_ids,
)
from src_oop.jobs.wb_stock_control.models import StockObservation
from src_oop.jobs.wb_stock_control.repository import WBStockControlRepository
from src_oop.jobs.wb_stock_control.service import WBStockControlService


def test_reads_working_unit_not_stock_management_copy() -> None:
    """Защищает от повторного выбора копии UNIT при построении исключений."""
    with patch(
        "src_oop.jobs.wb_stock_control.google_sheets_client.GoogleTabs"
    ) as factory:
        factory.return_value.get_all_values_with_retry.return_value = [
            ["Артикул", "ЛК"], ["979947734", ""], ["979947680", "старт5020"]
        ]
        assert read_unit_article_ids() == {979947734, 979947680}
        factory.assert_called_once_with("UNIT 2.0 (tested)", "MAIN (tested)")


def test_article_format_and_empty_account_do_not_drop_sku() -> None:
    """Числовое оформление и пустой ЛК не превращают SKU в товар вне UNIT."""
    assert parse_unit_article_ids([
        ["ЛК", " Артикул "],
        ["", "979\u00a0947\u202f734"],
        ["", "979947680.0"],
        ["", "979947680"],
        [],
    ]) == {979947734, 979947680}


@pytest.mark.parametrize("values", [
    [], [["SKU"], ["979947734"]], [["Артикул"]],
    [["Артикул", "Артикул"], ["979947734", "979947680"]],
    [["Артикул"], ["ошибка формулы"]], [["Артикул"], ["123.5"]],
])
def test_incomplete_reference_stops_control(values: list[list[str]]) -> None:
    """Неполный список исключений не должен порождать массовые ложные тревоги."""
    with pytest.raises(ValueError, match="Контроль остатков остановлен"):
        parse_unit_article_ids(values)


def test_known_unit_skus_are_excluded_from_notification(capsys) -> None:
    """SKU пользователя исключаются, а действительно внешний SKU остаётся в выводе."""
    unit_ids = parse_unit_article_ids([
        ["Артикул"], ["979947734"], ["979947680"], ["979946251"],
    ])
    rows = [
        dict(article_id=sku, chrt_id=index + 1, account="старт5020",
             local_vendor_code=wild, warehouse_id=2, wb_warehouse_id=100)
        for index, (sku, wild) in enumerate([
            (979947734, "wild1571"), (979947680, "wild345"),
            (979946251, "wild1789"), (979948841, "wild160"),
        ])
    ]
    repository = WBStockControlRepository(clickhouse=Mock())
    with patch(
        "src_oop.jobs.wb_stock_control.repository.Database.read_sql_to_dict",
        return_value=rows,
    ):
        targets = repository.fetch_targets(unit_ids)
    service = WBStockControlService(
        repository=repository, settings=StockControlSettings(bitrix_dialog_id=""),
    )
    observations = [
        StockObservation(target, 100, "ok", None, datetime.now(UTC))
        for target in targets
    ]
    with patch(
        "src_oop.jobs.wb_stock_control.service.ReadonlyBitrixRESTClient"
    ) as bitrix:
        asyncio.run(service._notify(observations, {}))
        bitrix.assert_not_called()
    output = capsys.readouterr().out
    assert "979948841" in output
    for sku in unit_ids:
        assert str(sku) not in output
