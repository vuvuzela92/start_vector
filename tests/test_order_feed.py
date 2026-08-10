"""Проверки ключевых бизнес-правил загрузки WB Order Feed."""

from __future__ import annotations

import unittest
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from src_oop.jobs.orders_feed.client import WBOrderFeedClient
from src_oop.jobs.orders_feed.models import (
    DataSource,
    OrderFeedPage,
    OrderFeedPeriod,
    OrderFeedSaveResult,
    OrderStatus,
    SaleType,
    WarehouseType,
    WBOrderFeedRecord,
)
from src_oop.jobs.orders_feed.normalizer import OrderFeedNormalizer
from src_oop.jobs.orders_feed.service import OrderFeedService


def _order(srid: str, updated_at: str = "2026-07-26T19:19:38+03:00") -> dict:
    """Создаёт минимальный реалистичный заказ для проверки API-маппинга."""
    return {
        "nmId": 47254354,
        "chrtId": 91663228,
        "srid": srid,
        "createdAt": "2026-07-24T12:57:26+03:00",
        "updatedAt": updated_at,
        "status": "cancel",
        "cancelType": "app",
        "warehouseName": "Склад продавца Ростов-на-Дону",
        "warehouseRegion": "Южный и Северо-Кавказский",
        "isMp": True,
        "destinationCity": "Санкт-Петербург",
        "destinationDistrict": "Северо-Западный",
        "sellerPrice": 4328,
        "isB2b": False,
    }


class OrderFeedNormalizerTest(unittest.TestCase):
    """Проверяет контракт между camelCase WB и snake_case PostgreSQL."""

    def test_normalize_adds_business_metadata_and_converts_columns(self) -> None:
        """Гарантирует сохранение кабинета, валюты, источника и временных полей."""
        page = OrderFeedPage(
            "vector",
            "2026-07-26T20:00:00Z",
            "RUB",
            [_order("order-1")],
            0,
            1000,
        )

        result = OrderFeedNormalizer().normalize(page)

        row = result.iloc[0]
        self.assertEqual(row["account"], "vector")
        self.assertEqual(row["nm_id"], 47254354)
        self.assertEqual(row["data_source"], "order_feed")
        self.assertEqual(row["status"], OrderStatus.CANCEL.value)
        self.assertEqual(row["warehouse_type"], WarehouseType.SELLER.value)
        self.assertEqual(row["sale_type"], SaleType.B2C.value)
        self.assertEqual(row["currency"], "RUB")
        self.assertEqual(str(row["created_at"].tz), "UTC")
        self.assertEqual(str(row["snapshot_time"].tz), "UTC")
        self.assertIsNotNone(row["loaded_at"])

    def test_table_model_uses_semantic_enum_columns(self) -> None:
        """Подтверждает понятную схему таблицы без неоднозначных is_mp и is_b2b."""
        columns = WBOrderFeedRecord.__table__.columns

        self.assertIn("warehouse_type", columns)
        self.assertIn("sale_type", columns)
        self.assertNotIn("is_mp", columns)
        self.assertNotIn("is_b2b", columns)
        self.assertEqual(columns["status"].type.enums, [item.value for item in OrderStatus])
        self.assertEqual(
            columns["data_source"].type.enums,
            [item.value for item in DataSource],
        )


class OrderFeedClientTest(unittest.TestCase):
    """Проверяет стабильность тела запроса при offset-пагинации."""

    def test_snapshot_is_sent_only_after_first_page(self) -> None:
        """Фиксирует snapshotTime начиная со второй страницы одного отчёта."""
        tz = ZoneInfo("Europe/Moscow")
        period = OrderFeedPeriod(
            datetime(2026, 7, 1, tzinfo=tz),
            datetime(2026, 7, 2, tzinfo=tz),
        )
        client = WBOrderFeedClient(page_limit=1000)

        first = client._build_request_body(period, 0, None)
        second = client._build_request_body(period, 1000, "2026-07-02T10:00:00Z")

        self.assertNotIn("snapshotTime", first["pagination"])
        self.assertEqual(second["pagination"]["snapshotTime"], "2026-07-02T10:00:00Z")
        self.assertEqual(second["pagination"]["offset"], 1000)


class _FakeClient:
    """Имитирует две страницы WB без сетевых запросов и минутного ожидания."""

    def __init__(self) -> None:
        """Готовит журнал параметров для проверки snapshot-пагинации."""
        self.calls: list[tuple[int, str | None]] = []

    async def fetch_page(self, **kwargs) -> OrderFeedPage:
        """Возвращает полную первую и неполную вторую страницу тестового снимка."""
        offset = kwargs["offset"]
        snapshot = kwargs["snapshot_time"]
        self.calls.append((offset, snapshot))
        orders = [_order("order-1"), _order("order-2")] if offset == 0 else [_order("order-3")]
        return OrderFeedPage(
            "vector", "2026-07-26T20:00:00Z", "RUB", orders, offset, 2
        )


class _FakeRepository:
    """Запоминает батчи, подтверждая сохранение каждой страницы отдельно."""

    def __init__(self) -> None:
        """Создаёт пустой журнал размеров сохранённых страниц."""
        self.batch_sizes: list[int] = []

    def save(self, dataframe) -> OrderFeedSaveResult:
        """Имитирует успешный upsert страницы без подключения к PostgreSQL."""
        size = len(dataframe.index)
        self.batch_sizes.append(size)
        return OrderFeedSaveResult(input_rows=size, written_rows=size)


class OrderFeedServiceTest(unittest.IsolatedAsyncioTestCase):
    """Проверяет оркестрацию пагинации и немедленного batch-upsert."""

    async def test_each_page_is_saved_with_fixed_snapshot(self) -> None:
        """Гарантирует, что падение поздней страницы не потеряет ранний сохранённый батч."""
        client = _FakeClient()
        repository = _FakeRepository()
        service = OrderFeedService(
            client=client,
            repository=repository,
            tokens_loader=lambda: {"vector": "secret"},
            request_interval_seconds=0,
        )
        now = datetime.now(tz=ZoneInfo("Europe/Moscow"))

        summary = await service.run(now - timedelta(days=1), now)

        self.assertEqual(client.calls, [(0, None), (2, "2026-07-26T20:00:00Z")])
        self.assertEqual(repository.batch_sizes, [2, 1])
        self.assertEqual(summary.pages_received, 2)
        self.assertEqual(summary.written_rows, 3)


if __name__ == "__main__":
    unittest.main()
