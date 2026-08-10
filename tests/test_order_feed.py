"""Проверки ключевых бизнес-правил загрузки WB Order Feed."""

from __future__ import annotations

import unittest
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from pydantic import ValidationError

from src_oop.jobs.orders_feed.client import WBOrderFeedClient
from src_oop.jobs.orders_feed.models import (
    OrderFeedPage,
    OrderFeedPeriod,
    OrderFeedSaveResult,
    WBOrderFeedRecord,
)
from src_oop.jobs.orders_feed.normalizer import OrderFeedNormalizer
from src_oop.jobs.orders_feed.repository import OrderFeedRepository
from src_oop.jobs.orders_feed.schemas.api import (
    OrderFeedOrderResponse,
    OrderFeedResponse,
)
from src_oop.jobs.orders_feed.schemas.enums import (
    DataSource,
    OrderStatus,
    SaleType,
    WarehouseType,
)
from src_oop.jobs.orders_feed.service import OrderFeedService


def _order(
    srid: str,
    updated_at: str = "2026-07-26T19:19:38+03:00",
) -> dict[str, object]:
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


def _validated_order(srid: str) -> OrderFeedOrderResponse:
    """Создаёт проверенную Pydantic-строку API для тестов normalizer и service."""
    return OrderFeedOrderResponse.model_validate(_order(srid))


class OrderFeedNormalizerTest(unittest.TestCase):
    """Проверяет контракт между camelCase WB и snake_case PostgreSQL."""

    def test_normalize_adds_business_metadata_and_converts_columns(self) -> None:
        """Гарантирует сохранение кабинета, валюты, источника и временных полей."""
        page = OrderFeedPage(
            "vector",
            "2026-07-26T20:00:00Z",
            "RUB",
            [_validated_order("order-1")],
            0,
            1000,
        )

        result = OrderFeedNormalizer().normalize(page)

        row = result[0]
        self.assertEqual(row.account, "vector")
        self.assertEqual(row.nm_id, 47254354)
        self.assertEqual(row.data_source, "order_feed")
        self.assertEqual(row.status, OrderStatus.CANCEL)
        self.assertEqual(row.warehouse_type, WarehouseType.SELLER)
        self.assertEqual(row.sale_type, SaleType.B2C)
        self.assertEqual(row.currency, "RUB")
        self.assertEqual(str(row.created_at.tzinfo), "UTC")
        self.assertEqual(str(row.snapshot_time.tzinfo), "UTC")
        self.assertIsNotNone(row.loaded_at)

    def test_missing_cancel_type_is_null_for_non_cancelled_order(self) -> None:
        """Сохраняет Pydantic None для действующего заказа без cancelType."""
        cancelled = _validated_order("order-cancelled")
        created = _order("order-created")
        created["status"] = "created"
        created.pop("cancelType")
        validated_created = OrderFeedOrderResponse.model_validate(created)
        page = OrderFeedPage(
            "vector",
            "2026-07-26T20:00:00Z",
            "RUB",
            [cancelled, validated_created],
            0,
            1000,
        )

        result = OrderFeedNormalizer().normalize(page)

        created_row = next(row for row in result if row.srid == "order-created")
        self.assertIsNone(created_row.cancel_type)

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

    def test_nm_id_references_unique_article(self) -> None:
        """Фиксирует связь многих заказов с одной уникальной карточкой article."""
        foreign_keys = list(WBOrderFeedRecord.__table__.c.nm_id.foreign_keys)

        self.assertEqual(len(foreign_keys), 1)
        self.assertEqual(foreign_keys[0].target_fullname, "article.nm_id")
        self.assertEqual(foreign_keys[0].ondelete, "RESTRICT")
        self.assertEqual(foreign_keys[0].onupdate, "CASCADE")


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

    def test_pydantic_rejects_unknown_order_status(self) -> None:
        """Останавливает страницу до БД, если WB прислал неизвестный статус заказа."""
        invalid_order = _order("order-1")
        invalid_order["status"] = "unknown"
        payload = {
            "data": {
                "snapshotTime": "2026-07-26T20:00:00Z",
                "currency": "RUB",
                "orders": [invalid_order],
            }
        }

        with self.assertRaises(ValidationError):
            OrderFeedResponse.model_validate(payload)

    def test_client_returns_page_only_after_pydantic_validation(self) -> None:
        """Проверяет интеграцию Pydantic-модели с внутренней страницей API-клиента."""
        order = _order("order-1")
        payload = {
            "data": {
                "snapshotTime": "2026-07-26T20:00:00Z",
                "currency": "RUB",
                "orders": [order],
            }
        }

        page = WBOrderFeedClient()._parse_page("vector", 0, payload, 0)

        self.assertEqual(page.snapshot_time, "2026-07-26T20:00:00Z")
        self.assertEqual(page.orders[0].status, OrderStatus.CANCEL)
        self.assertEqual(page.orders[0].cancel_type, "app")


class OrderFeedRepositoryTest(unittest.TestCase):
    """Проверяет подготовку типизированного батча без подключения к PostgreSQL."""

    def test_deduplication_keeps_latest_pydantic_row(self) -> None:
        """Оставляет последний статус заказа по updated_at внутри одного API-батча."""
        older = OrderFeedOrderResponse.model_validate(
            _order("order-1", "2026-07-26T18:00:00+03:00")
        )
        newer = OrderFeedOrderResponse.model_validate(
            _order("order-1", "2026-07-26T19:00:00+03:00")
        )
        page = OrderFeedPage(
            "vector",
            "2026-07-26T20:00:00Z",
            "RUB",
            [older, newer],
            0,
            1000,
        )
        rows = OrderFeedNormalizer().normalize(page)

        deduplicated, collapsed = OrderFeedRepository()._deduplicate_by_keys(rows)

        self.assertEqual(collapsed, 1)
        self.assertEqual(len(deduplicated), 1)
        self.assertEqual(deduplicated[0].updated_at.hour, 16)


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
        orders = (
            [_validated_order("order-1"), _validated_order("order-2")]
            if offset == 0
            else [_validated_order("order-3")]
        )
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
        size = len(dataframe)
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
