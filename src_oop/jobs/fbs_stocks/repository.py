from __future__ import annotations

import logging
from collections.abc import Sequence
from dataclasses import dataclass

from src_oop.core.database import Database

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class FBSWarehouseInfo:
    """Описание активного FBS-склада для диагностики операций с остатками.

    Бизнес-сценарий: при ошибках WB по конкретному складу пользователю нужно видеть не только
    технический `wb_warehouse_id`, но и наше название склада с `wb_office_id` из `warehouses_fbs`.
    """

    account: str
    warehouse_id: int
    warehouse_name: str
    wb_warehouse_id: int
    wb_office_id: int | None


class FBSStocksRepository:
    """Читает из PostgreSQL данные, нужные для сопоставления UNIT и WB FBS-остатков."""

    def __init__(self, database_cls: type[Database] = Database) -> None:
        """Подключает общий Database-слой проекта для чтения справочников."""
        self.database_cls = database_cls

    def fetch_chrt_ids_by_articles(self, article_ids: Sequence[int]) -> dict[int, int]:
        """Возвращает `article_id -> chrt_id`, чтобы запросить FBS-остатки WB по строкам UNIT."""
        prepared_ids = sorted({int(article_id) for article_id in article_ids if article_id})
        if not prepared_ids:
            return {}

        rows = self.database_cls.read_sql_to_dict(
            """
            SELECT article_id, MAX(chrt_id) AS chrt_id
            FROM card_data
            WHERE article_id = ANY(:article_ids)
              AND chrt_id IS NOT NULL
            GROUP BY article_id
            """,
            params={"article_ids": prepared_ids},
        )
        result = {
            int(row["article_id"]): int(row["chrt_id"])
            for row in rows
            if row.get("article_id") is not None and row.get("chrt_id") is not None
        }
        logger.info(
            "chrt_id для FBS-остатков загружены из card_data | requested_articles=%s | found=%s",
            len(prepared_ids),
            len(result),
        )
        return result

    def fetch_fbs_warehouses(self) -> dict[tuple[str, int], int]:
        """Возвращает маппинг `(account, our_warehouse_id) -> wb_warehouse_id` для запросов остатков WB."""
        rows = self.database_cls.read_sql_to_dict(
            """
            SELECT account, warehouse_id, wb_warehouse_id
            FROM warehouses_fbs
            WHERE status = 'active'
              AND wb_warehouse_id IS NOT NULL
            """
        )
        result = {
            (self.normalize_account(str(row["account"])), int(row["warehouse_id"])): int(
                row["wb_warehouse_id"]
            )
            for row in rows
            if row.get("account") and row.get("warehouse_id") is not None
        }
        logger.info("Справочник FBS-складов загружен для остатков | rows=%s", len(result))
        return result

    def fetch_fbs_warehouse_details(self) -> dict[tuple[str, int], FBSWarehouseInfo]:
        """Возвращает детали активных FBS-складов для понятных логов по ошибкам WB.

        Бизнес-правило: остатками управляем по WB warehouse ID, но оператору удобнее разбирать
        ограничения хранения по названию нашего склада и `wb_office_id`, сохраненным в БД.
        """
        rows = self.database_cls.read_sql_to_dict(
            """
            SELECT account, warehouse_id, warehouse_name, wb_warehouse_id, wb_office_id
            FROM warehouses_fbs
            WHERE status = 'active'
              AND wb_warehouse_id IS NOT NULL
            """
        )
        result: dict[tuple[str, int], FBSWarehouseInfo] = {}
        for row in rows:
            if not row.get("account") or row.get("wb_warehouse_id") is None:
                continue
            normalized_account = self.normalize_account(str(row["account"]))
            wb_warehouse_id = int(row["wb_warehouse_id"])
            result[(normalized_account, wb_warehouse_id)] = FBSWarehouseInfo(
                account=normalized_account,
                warehouse_id=int(row["warehouse_id"]),
                warehouse_name=str(row.get("warehouse_name") or ""),
                wb_warehouse_id=wb_warehouse_id,
                wb_office_id=int(row["wb_office_id"])
                if row.get("wb_office_id") is not None
                else None,
            )
        logger.info("Детали FBS-складов загружены для диагностики остатков | rows=%s", len(result))
        return result

    @staticmethod
    def normalize_account(account: str) -> str:
        """Нормализует название ЛК, чтобы `Старт5020` из UNIT совпал с `СТАРТ5020` из токенов."""
        return account.strip().casefold()
