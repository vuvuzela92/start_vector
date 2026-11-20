"""Фиксация разового среза свободных остатков для сравнения ноябрьского плана."""

from __future__ import annotations

import json
import logging
from datetime import datetime
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from pathlib import Path

from sqlalchemy import text

from src_oop.core.database import Database
from src_oop.core.my_gspread import GoogleTabs
from src_oop.jobs.sales_plan.config import (
    SALES_PLAN_V2_SNAPSHOT_MONTH,
    SALES_PLAN_V2_SNAPSHOT_TABLE,
    SALES_PLAN_V2_SOURCE,
    SALES_PLAN_V2_SOURCE_STOCK_HEADER,
    SALES_PLAN_V2_SOURCE_WILD_HEADER,
)

logger = logging.getLogger(__name__)


def _parse_quantity(value: object, wild: str) -> Decimal | None:
    """Читает свободный остаток товара; пустота означает неизвестный остаток, а не ноль."""

    if value is None:
        return None
    raw = str(value).replace("\u00a0", "").replace(" ", "").replace(",", ".").strip()
    if not raw:
        return None
    try:
        quantity = Decimal(raw)
    except InvalidOperation as error:
        raise ValueError(f"Некорректный свободный остаток для {wild}") from error
    if not quantity.is_finite():
        raise ValueError(f"Некорректный свободный остаток для {wild}")
    return max(quantity, Decimal(0))


def _read_stock_snapshot() -> dict[str, Decimal | None]:
    """Читает фиксированный снимок 01.11; сверяет ключи и сохраняет пропуски как NULL.

    Локальный JSON нужен для однократной загрузки: сервисный аккаунт проекта
    не имеет доступа к архивной копии UNIT, доступной подключённому Drive.
    """

    path = Path(__file__).with_name("november_2026_stock_snapshot.json")
    if path.exists():
        payload = json.loads(path.read_text(encoding="utf-8"))
        if payload.get("source_spreadsheet_id") != SALES_PLAN_V2_SOURCE["spreadsheet_id"] or payload.get("snapshot_date") != SALES_PLAN_V2_SNAPSHOT_MONTH.isoformat():
            raise ValueError("Локальный снимок остатков не соответствует источнику и месяцу")
        return {wild: _parse_quantity(stock, wild) for wild, stock in payload["stock_by_wild"].items()}

    connector = GoogleTabs(
        table_title=SALES_PLAN_V2_SOURCE["title"],
        sheet_title=SALES_PLAN_V2_SOURCE["sheet_title"],
        spreadsheet_id=SALES_PLAN_V2_SOURCE["spreadsheet_id"],
    )
    sheet = connector.sheet_title
    headers = sheet.row_values(1)
    if headers.count(SALES_PLAN_V2_SOURCE_WILD_HEADER) != 1 or headers.count(SALES_PLAN_V2_SOURCE_STOCK_HEADER) != 1:
        raise ValueError("В снимке UNIT не найдены однозначные колонки wild и свободного остатка")

    wild_index = headers.index(SALES_PLAN_V2_SOURCE_WILD_HEADER) + 1
    stock_index = headers.index(SALES_PLAN_V2_SOURCE_STOCK_HEADER) + 1
    from gspread.utils import rowcol_to_a1

    wild_col = rowcol_to_a1(1, wild_index).rstrip("1")
    stock_col = rowcol_to_a1(1, stock_index).rstrip("1")
    ranges = [f"{wild_col}2:{wild_col}{sheet.row_count}", f"{stock_col}2:{stock_col}{sheet.row_count}"]
    wild_rows, stock_rows = sheet.batch_get(ranges)
    stock_by_wild: dict[str, Decimal | None] = {}
    for position, row in enumerate(wild_rows):
        wild = str(row[0]).strip() if row else ""
        if not wild:
            continue
        stock_row = stock_rows[position] if position < len(stock_rows) else []
        stock = _parse_quantity(stock_row[0] if stock_row else "", wild)
        if wild in stock_by_wild and stock_by_wild[wild] is not None and stock is not None and stock_by_wild[wild] != stock:
            raise ValueError(f"В снимке UNIT разные свободные остатки для {wild}")
        if wild not in stock_by_wild or stock_by_wild[wild] is None:
            stock_by_wild[wild] = stock

    return stock_by_wild


def freeze_november_v2_snapshot() -> int:
    """Один раз фиксирует ноябрьский остаток, план в штуках, цену и план v2.0 по wild.

    Повторный запуск не меняет уже сохранённый срез. Если свободный остаток
    меньше 50 единиц, план v2.0 обнуляется. Противоречивые остатки для одного
    wild прерывают загрузку; пустой остаток сохраняется как NULL.
    """

    engine = Database.get_engine()
    with engine.begin() as connection:
        connection.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {SALES_PLAN_V2_SNAPSHOT_TABLE} (
                month_start date NOT NULL,
                wild varchar(255) NOT NULL,
                available_quantity numeric(14, 2),
                quantity_plan numeric(14, 2),
                plan_price numeric(14, 2),
                quantity_plan_v2 numeric(14, 2),
                plan_orders_rub_v2 numeric(18, 2),
                created_at timestamp NOT NULL,
                PRIMARY KEY (month_start, wild)
            )
        """))
        existing = connection.execute(
            text(f"SELECT COUNT(*) FROM {SALES_PLAN_V2_SNAPSHOT_TABLE} WHERE month_start = :month"),
            {"month": SALES_PLAN_V2_SNAPSHOT_MONTH},
        ).scalar_one()
        if existing:
            logger.info("Ноябрьский срез плана v2.0 уже сохранён | rows=%s", existing)
            return existing

    stock_by_wild = _read_stock_snapshot()
    with engine.connect() as connection:
        plan_rows = connection.execute(text("""
            SELECT wild, ROUND(quarter_4_units_2026) AS quantity_plan, plan_price
            FROM sales_plan_accounting_category_reference
            ORDER BY wild
        """)).mappings().all()
    if not plan_rows or not stock_by_wild:
        raise ValueError("Нельзя зафиксировать план v2.0: отсутствует план или снимок остатков")

    created_at = datetime.now()
    records = []
    for row in plan_rows:
        wild = row["wild"]
        stock = stock_by_wild.get(wild)
        quantity = Decimal(str(row["quantity_plan"])) if row["quantity_plan"] is not None else None
        price = Decimal(str(row["plan_price"])) if row["plan_price"] is not None else None
        quantity_v2 = (
            Decimal(0) if stock is not None and stock < 50
            else min(stock, quantity) if stock is not None and quantity is not None
            else None
        )
        amount = (quantity_v2 * price).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP) if quantity_v2 is not None and price is not None else None
        records.append({
            "month_start": SALES_PLAN_V2_SNAPSHOT_MONTH,
            "wild": wild,
            "available_quantity": stock,
            "quantity_plan": quantity,
            "plan_price": price,
            "quantity_plan_v2": quantity_v2,
            "plan_orders_rub_v2": amount,
            "created_at": created_at,
        })

    with engine.begin() as connection:
        existing = connection.execute(
            text(f"SELECT COUNT(*) FROM {SALES_PLAN_V2_SNAPSHOT_TABLE} WHERE month_start = :month"),
            {"month": SALES_PLAN_V2_SNAPSHOT_MONTH},
        ).scalar_one()
        if existing:
            return existing
        connection.execute(text(f"""
            INSERT INTO {SALES_PLAN_V2_SNAPSHOT_TABLE} (
                month_start, wild, available_quantity, quantity_plan, plan_price,
                quantity_plan_v2, plan_orders_rub_v2, created_at
            ) VALUES (
                :month_start, :wild, :available_quantity, :quantity_plan, :plan_price,
                :quantity_plan_v2, :plan_orders_rub_v2, :created_at
            ) ON CONFLICT (month_start, wild) DO NOTHING
        """), records)
    logger.info("Ноябрьский срез плана v2.0 сохранён | rows=%s", len(records))
    return len(records)
