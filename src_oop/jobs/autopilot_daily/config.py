from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

from src_oop.jobs.autopilot.config import autopilot_gs, unit_gs

load_dotenv()


@dataclass(frozen=True, slots=True)
class DailySheetConfig:
    """Настройки листа ПУ для дневной выгрузки.

    Бизнес-логика:
    хранит координаты строк и фиксированных колонок, в которые дневной сценарий
    раскладывает последние 6 завершенных дней и исторические средние метрики без поиска по
    заголовкам. Это защищает ПУ от случайного смещения при изменении шапки.
    """

    values_first_row: int = 4
    source_header_row: int = 3
    # Daily обновляет только завершенные дни; последняя колонка блока остается hourly-сценарию текущего дня.
    current_days_width: int = 6
    avg_position_current_width: int = 6
    articles_column_index: int = 1
    status_cell: str = "A2"
    avg_position_current_range_start: str = "IQ"
    avg_position_current_range_end: str = "IV"
    avg_position_history_column: str = "IP"


@dataclass(frozen=True, slots=True)
class UnitSheetConfig:
    """Настройки UNIT-листов, которые обслуживает дневной сценарий.

    Бизнес-логика:
    дневной автопилот после обновления ПУ синхронизирует статус рекламы в UNIT и
    обновляет заказы в листе сопоставлений, поэтому названия таблицы и листов
    держатся рядом с основным сценарием.
    """

    main_sheet: str = unit_gs["unit_sheet"]
    sopost_sheet: str = "Сопост"
    adv_status_column: str = "Реклама"
    deleted_status: str = "ТОВАР \nУДАЛЕН "
    active_adv_status: str = "реклама"


BASE_DIR = Path(__file__).resolve().parents[3]
CREDS_PATH = BASE_DIR / os.getenv("CREDS_DIR", "creds") / os.getenv(
    "CREDS_FILE",
    "creds.json",
)

DAILY_SHEET = DailySheetConfig()
UNIT_SHEET = UnitSheetConfig()

AUTOPILOT_TABLE_TITLE = os.getenv("AUTOPILOT_TABLE_NAME") or autopilot_gs["title"]
AUTOPILOT_SHEET_TITLE = os.getenv("AUTOPILOT_SHEET_NAME") or autopilot_gs["hourly_sheet"]
UNIT_TABLE_TITLE = os.getenv("UNIT_TABLE") or unit_gs["title"]
UNIT_MAIN_SHEET_TITLE = os.getenv("UNIT_MAIN_SHEET") or unit_gs["unit_sheet"]

# Онлайн-парсинг публичной карточки WB нестабилен; для дневного переноса он
# отключен до отдельного подтверждения пользователя.
ENABLE_WB_DAILY_PUBLIC_CARD_PARSING = False

# Метрики пишутся в фиксированные блоки ПУ. Для текущих daily-метрик ширина равна
# 6 завершенным дням; колонка текущего дня остается за autopilot_hourly_run.
CURRENT_METRIC_TO_BASE_COLUMN: dict[str, str] = {
    "orders_sum_rub": "AX",
    "orders_count": "BI",
    "adv_spend": "BQ",
    "price_with_disc": "CD",
    "spp": "CW",
    "total_quantity": "DN",
    "profit_by_cond_orders": "DW",
    "views": "EW",
    "clicks": "FF",
    "ctr": "FN",
    "to_cart_convers": "FV",
    "to_orders_convers": "GD",
    "add_to_cart_count": "GL",
    "open_card_count": "GT",
    "cpo": "HB",
    "cpc": "HJ",
    "rating": "HR",
    "promo_status": "DF",
    "net_profit_after_ad": "EE",
    "advertising_cost_share": "EN",
    "cpm": "HZ",
    "organic": "II",
    "unit_free_stock": "DU",
}

HISTORY_METRIC_TO_COLUMN: dict[str, str] = {
    "avg_orders_sum_rub": "AW",
    "avg_orders_count": "BH",
    "avg_adv_spend": "BP",
    "avg_price_with_disc": "CC",
    "avg_spp": "CV",
    "avg_total_quantity": "DM",
    "avg_profit_by_cond_orders": "DV",
    "avg_views": "EV",
    "avg_clicks": "FE",
    "avg_ctr": "FM",
    "avg_to_cart_convers": "FU",
    "avg_to_orders_convers": "GC",
    "avg_add_to_cart_count": "GK",
    "avg_open_card_count": "GS",
    "avg_cpo": "HA",
    "avg_cpc": "HI",
    "avg_rating": "HQ",
    "month_median_price_with_disc": "CA",
    "month_avg_price_with_disc": "CB",
    "avg_net_profit_after_ad": "ED",
    "avg_advertising_cost_share": "EM",
    "avg_cpm": "HY",
    "avg_organic": "IH",
}

# В legacy этот блок писал свободный остаток в диапазон DU:EA. Колонка EA уже
# используется пользователем для текущей прибыли, поэтому метрика отключена до
# ручного подтверждения корректного назначения.
DISABLED_PU_METRICS = {"unit_free_stock"}
