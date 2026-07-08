from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class GoogleSheetConfig:
    title: str
    sheet: str


unit_gs = GoogleSheetConfig(
    title="UNIT 2.0 (tested)",
    sheet="MAIN (tested)",
)

unit_gs_test = GoogleSheetConfig(
    title="Копия UNIT 2.0 (tested) 17.04.2026",
    sheet="MAIN (tested)",
)

UNIT_ARTICLE_COLUMN = "Артикул"
UNIT_WILD_COLUMN = "wild"

OUR_ARTICLE_COLUMN = "Наш артикул"
COMPETITOR_ARTICLE_COLUMN = "Артикул конкурента"
COMPETITOR_PRICE_COLUMN = "цена конкурента"
COMPETITOR_NAME_COLUMN = "Конкурент"
COMPETITOR_POSITION_COLUMN = "Позиция в полках конкурента"

COMPETITOR_TARGET_COLUMNS = (
    "Конкурент 1",
    "Конкурент 2",
    "Конкурент 3",
)
COMPETITOR_PRICE_TARGET_COLUMNS = (
    "Цена 1 конкурента",
    "Цена 2 конкурента",
    "Цена 3 конкурента",
)
OUR_PRICE_TARGET_COLUMN = "Наша цена после СПП"

FIXED_COMPETITOR_NAMES = (
    "Первый конкурент",
    "Второй конкурент",
    "Третий конкурент",
)

NO_COMPETITOR_VALUE = "нет конкурента"
GOOGLE_HEADER_ROW_INDEX = 1
GOOGLE_DATA_START_ROW = 2
