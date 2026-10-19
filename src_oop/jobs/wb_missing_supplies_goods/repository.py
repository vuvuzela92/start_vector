"""Репозиторий PostgreSQL для поставок и товаров поставок WB."""

from __future__ import annotations

import logging
from typing import Any

from sqlalchemy import MetaData, Table, select
from sqlalchemy.dialects.postgresql import insert

from src_oop.core.database import Database
from src_oop.jobs.wb_missing_supplies_goods.config import (
    GOODS_COLUMN_MAPPING,
    SUPPLY_COLUMN_MAPPING,
)

logger = logging.getLogger(__name__)


class WBSuppliesRepository:
    """Читает уже загруженные товары и пакетно добавляет недостающие строки."""

    def __init__(self) -> None:
        """Подготавливает подключение к существующей таблице проекта."""
        self.engine = Database.get_engine()
        self.metadata = MetaData()
        self.goods_table = Table(
            "wb_supplies_goods",
            self.metadata,
            autoload_with=self.engine,
        )
        self.supplies_table = Table(
            "wb_supplies",
            self.metadata,
            autoload_with=self.engine,
        )

    def insert_supplies(self, records: list[dict[str, Any]]) -> int:
        """Сохраняет сведения о новых поставках в ``wb_supplies`` идемпотентно."""
        required_columns = (
            "id",
            "updated_date",
            "ready_for_sale_quantity",
            "accepted_quantity",
            "unloading_quantity",
        )
        normalized = []
        for record in records:
            row = {
                SUPPLY_COLUMN_MAPPING[key]: value
                for key, value in record.items()
                if key in SUPPLY_COLUMN_MAPPING
            }
            if all(row.get(column) is not None for column in required_columns):
                normalized.append(row)
            else:
                logger.warning(
                    "Поставка пропущена перед записью: в detail-ответе WB отсутствует обязательное поле | supply_id=%s",
                    row.get("id"),
                )
        if not normalized:
            return 0

        statement = insert(self.supplies_table).values(normalized).on_conflict_do_nothing()
        with self.engine.begin() as connection:
            connection.execute(statement)
        return len(normalized)

    def fetch_existing_goods(self) -> dict[int, set[str]]:
        """Возвращает vendor_code по каждому supply ID для проверки пропусков."""
        query = select(
            self.goods_table.c.id,
            self.goods_table.c.vendor_code,
        )
        with self.engine.connect() as connection:
            rows = connection.execute(query).all()
        grouped: dict[int, set[str]] = {}
        for supply_id, vendor_code in rows:
            if supply_id is not None and vendor_code is not None:
                grouped.setdefault(int(supply_id), set()).add(str(vendor_code))
        return grouped

    def insert_missing_goods(self, records: list[dict[str, Any]]) -> int:
        """Добавляет валидные товары пакетно и возвращает число переданных строк.

        Бизнес-правило: первичная загрузка новой поставки и повторная
        дозагрузка используют один идемпотентный INSERT, поэтому повтор job не
        создаёт дубликаты в ``wb_supplies_goods``.
        """
        normalized = []
        for record in records:
            row = {
                GOODS_COLUMN_MAPPING[key]: value
                for key, value in record.items()
                if key in GOODS_COLUMN_MAPPING
            }
            if row.get("id") is not None and row.get("vendor_code") is not None:
                normalized.append(row)
        if not normalized:
            return 0

        statement = insert(self.goods_table).values(normalized).on_conflict_do_nothing()
        with self.engine.begin() as connection:
            connection.execute(statement)
        return len(normalized)
