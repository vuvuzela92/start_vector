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
GOOGLE_HEADER_ROW_INDEX = 1
GOOGLE_DATA_START_ROW = 2
