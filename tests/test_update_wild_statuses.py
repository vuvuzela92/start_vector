"""Проверяет точечное чтение и безопасное обновление статусов товаров UNIT."""

from unittest.mock import Mock

import pytest
import requests
from gspread.utils import ValueRenderOption

from src_oop.core import my_gspread
from src_oop.jobs.unit import update_wild_statuses as job


def _connector(headers: list[str], values: list[list[list[str]]]) -> my_gspread.GoogleTabs:
    """Создаёт локальный лист, чтобы проверить чтение статусов без сетевого вызова."""
    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setattr(my_gspread.GoogleTabs, "_safe_connect", lambda self: None)
        connector = my_gspread.GoogleTabs("UNIT", "MAIN")
    worksheet = Mock()
    worksheet.title = "MAIN"
    worksheet.row_values.return_value = headers
    worksheet.batch_get.return_value = values
    connector.sheet_title = worksheet
    return connector


def test_reads_only_required_columns_and_keeps_blank_rows() -> None:
    """Узкий FORMULA-запрос сохраняет позиции строк для сопоставления по wild."""
    connector = _connector(
        ["Описание", "wild", "Статус товара", "Лишняя колонка"],
        [[["wild1"], [], ["wild2"]], [["готов"], ["старое"], ["новое"]]],
    )

    headers, keys, statuses = job._read_columns_with_formula_support(
        connector,
        key_column=job.WILD_COLUMN,
        value_column=job.STATUS_TARGET_COLUMN,
        operation_name="проверка",
    )

    assert headers[1:3] == ["wild", "Статус товара"]
    assert keys == [["wild1"], [], ["wild2"]]
    assert statuses == [["готов"], ["старое"], ["новое"]]
    connector.sheet_title.batch_get.assert_called_once_with(
        ["B2:B", "C2:C"], value_render_option=ValueRenderOption.formula
    )


def test_formula_key_is_read_as_calculated_value() -> None:
    """Формульный wild вычисляется, чтобы соединение ключей не получило текст формулы."""
    connector = _connector(
        ["wild", "статус вилд"],
        [[['=CONCAT("wild";1)']], [["готов"]]],
    )
    connector.sheet_title.batch_get.side_effect = [
        [[['=CONCAT("wild";1)']], [["готов"]]],
        [[["wild1"]]],
    ]

    _, keys, statuses = job._read_columns_with_formula_support(
        connector,
        key_column=job.WILD_COLUMN,
        value_column=job.STATUS_SOURCE_COLUMN,
        operation_name="проверка формул",
    )

    assert keys == [["wild1"]]
    assert statuses == [["готов"]]
    connector.sheet_title.batch_get.assert_called_with(
        ["A2:A"], value_render_option=ValueRenderOption.unformatted
    )


def test_network_disconnect_is_retried() -> None:
    """Обрыв соединения при чтении заголовков повторяется до успешного ответа."""
    connector = _connector(["wild", "статус вилд"], [[["wild1"]], [["готов"]]])
    connector.sheet_title.row_values.side_effect = [
        requests.exceptions.ConnectionError("remote disconnected"),
        ["wild", "статус вилд"],
    ]

    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setattr(my_gspread.time, "sleep", lambda _: None)
        headers, keys, statuses = job._read_columns_with_formula_support(
            connector,
            key_column=job.WILD_COLUMN,
            value_column=job.STATUS_SOURCE_COLUMN,
            operation_name="проверка повтора",
        )

    assert headers == ["wild", "статус вилд"]
    assert keys == [["wild1"]]
    assert statuses == [["готов"]]
    assert connector.sheet_title.row_values.call_count == 2


def test_update_statuses_writes_only_target_column(monkeypatch: pytest.MonkeyPatch) -> None:
    """Сверка по wild записывает статусы в прежние строки и только в их колонку."""
    source = _connector(
        ["wild", "статус вилд"],
        [[["wild1"], ["wild2"]], [["новый"], ["готов"]]],
    )
    unit = _connector(
        ["Артикул", "wild", "Статус товара", "Примечание"],
        [[["wild2"], ["wild1"], []], [["старый"], ["старый"], ["старый"]]],
    )
    calc = Mock(google_connect_statuses=source)
    economics = Mock(google_connect=unit)
    monkeypatch.setattr(job, "Calculation_of_purchases_russia", lambda: calc)
    monkeypatch.setattr(job, "UnitEconomics", lambda: economics)
    monkeypatch.setattr(my_gspread.time, "sleep", lambda _: None)

    job.update_wild_statuses()

    unit.sheet_title.update.assert_called_once_with(
        range_name="C2:C4",
        values=[["готов"], ["новый"], [""]],
        value_input_option="RAW",
    )
