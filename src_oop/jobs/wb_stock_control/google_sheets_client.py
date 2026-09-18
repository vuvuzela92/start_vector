"""Чтение рабочего списка UNIT для исключения контролируемых SKU из тревог."""

from __future__ import annotations

import logging
from decimal import Decimal, InvalidOperation

from src_oop.core.my_gspread import GoogleTabs

logger = logging.getLogger(__name__)


def read_unit_article_ids() -> set[int]:
    """Читает все SKU рабочей UNIT независимо от заполненности ЛК и wild.

    Для контроля используется именно `UNIT 2.0 (tested)`, а не копия для
    управления остатками. Чтение с повторами выполняется общим GoogleTabs.
    Отсутствующая/дублирующаяся колонка, нечисловой SKU или пустой список
    прерывают проверку: неполный справочник нельзя считать списком исключений.
    """
    connector = GoogleTabs("UNIT 2.0 (tested)", "MAIN (tested)")
    values = connector.get_all_values_with_retry()
    article_ids = parse_unit_article_ids(values)
    logger.info(
        "Прочитан рабочий список UNIT для исключения SKU из предупреждений "
        "| table=%s | sheet=%s | articles=%s",
        connector.table_title,
        connector.sheet_title.title,
        len(article_ids),
    )
    return article_ids


def parse_unit_article_ids(values: list[list[str]]) -> set[int]:
    """Извлекает SKU строго из колонки «Артикул» без фильтра по соседним полям.

    Пустые ячейки пропускаются; пробелы-разделители и числовое представление
    целого SKU нормализуются. Некорректный непустой артикул блокирует отчёт,
    чтобы контролируемый товар не оказался ошибочно вне UNIT.
    """
    headers = [str(value).strip() for value in values[0]] if values else []
    if headers.count("Артикул") != 1:
        raise ValueError(
            "Контроль остатков остановлен: в MAIN (tested) рабочей UNIT "
            "должна быть ровно одна колонка «Артикул»."
        )
    column_index = headers.index("Артикул")
    article_ids: set[int] = set()
    for row_number, row in enumerate(values[1:], start=2):
        raw_value = row[column_index] if column_index < len(row) else ""
        prepared = "".join(str(raw_value).split()).replace(",", ".")
        if not prepared:
            continue
        try:
            value = Decimal(prepared)
            valid = value.is_finite() and 0 < value < 2**63 and value == value.to_integral_value()
        except InvalidOperation:
            valid = False
        if not valid:
            raise ValueError(
                "Контроль остатков остановлен: некорректный артикул "
                f"в MAIN (tested) рабочей UNIT | row={row_number}"
            )
        article_ids.add(int(value))
    if not article_ids:
        raise ValueError(
            "Контроль остатков остановлен: колонка «Артикул» рабочей UNIT пуста."
        )
    return article_ids
