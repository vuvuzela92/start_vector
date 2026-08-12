from __future__ import annotations

import logging
from collections.abc import Sequence

from src_oop.core.database import Database

logger = logging.getLogger(__name__)


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

    @staticmethod
    def normalize_account(account: str) -> str:
        """Нормализует название ЛК, чтобы `Старт5020` из UNIT совпал с `СТАРТ5020` из токенов."""
        return account.strip().casefold()
