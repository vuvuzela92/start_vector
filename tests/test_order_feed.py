"""Проверки ключевых бизнес-правил загрузки WB Order Feed."""

from __future__ import annotations

import math
import unittest
from datetime import date, datetime, timedelta
from decimal import Decimal
from unittest.mock import Mock, patch
from zoneinfo import ZoneInfo

from pydantic import ValidationError
from sqlalchemy import String
from sqlalchemy.exc import IntegrityError, OperationalError

from src_oop.jobs.orders_feed.backfill import BackfillSource, OrderFeedBackfill
from src_oop.jobs.orders_feed.client import WBOrderFeedClient
from src_oop.jobs.orders_feed.config import UPSERT_UPDATE_COLUMNS
from src_oop.jobs.orders_feed.exceptions import (
    OrderFeedAuthenticationError,
    OrderFeedBadRequestError,
    OrderFeedRateLimitError,
)
from src_oop.jobs.orders_feed.models import (
    OrderFeedSaveResult,
    WBOrderFeedRecord,
)
from src_oop.jobs.orders_feed.normalizer import OrderFeedNormalizer
from src_oop.jobs.orders_feed.repository import OrderFeedRepository
from src_oop.jobs.orders_feed.run import _parse_datetime
from src_oop.jobs.orders_feed.schemas.api import (
    OrderFeedOrderResponse,
    OrderFeedResponse,
)
from src_oop.jobs.orders_feed.schemas.backfill import LegacyOrderFeedRow
from src_oop.jobs.orders_feed.schemas.enums import (
    DataSource,
    OrderStatus,
    SaleType,
    WarehouseType,
)
from src_oop.jobs.orders_feed.schemas.internal import OrderFeedPage, OrderFeedPeriod
from src_oop.jobs.orders_feed.service import OrderFeedService


class OrderFeedRunTest(unittest.TestCase):
    """Проверяет удобные форматы периода ручного CLI-запуска."""

    def test_parse_manual_datetime_formats(self) -> None:
        """Принимает короткий, ISO и привычный российский форматы."""
        moscow_tz = ZoneInfo("Europe/Moscow")
        cases = {
            "2026-07-23": datetime(2026, 7, 23, tzinfo=moscow_tz),
            "2026-07-23 12:00": datetime(2026, 7, 23, 12, tzinfo=moscow_tz),
            "2026-07-23T12:30:45": datetime(
                2026, 7, 23, 12, 30, 45, tzinfo=moscow_tz
            ),
            "23.07.2026 12:00": datetime(2026, 7, 23, 12, tzinfo=moscow_tz),
        }

        for value, expected in cases.items():
            with self.subTest(value=value):
                self.assertEqual(_parse_datetime(value), expected)

    def test_parse_manual_datetime_keeps_timezone(self) -> None:
        """Не теряет явно заданный offset, включая суффикс Z."""
        self.assertEqual(
            _parse_datetime("2026-07-23T12:00:00+03:00").utcoffset(),
            timedelta(hours=3),
        )
        self.assertEqual(
            _parse_datetime("2026-07-23T09:00:00Z").utcoffset(),
            timedelta(0),
        )


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
            account="vector",
            snapshot_time="2026-07-26T20:00:00Z",
            currency="RUB",
            orders=[_validated_order("order-1")],
            offset=0,
            limit=1000,
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
            account="vector",
            snapshot_time="2026-07-26T20:00:00Z",
            currency="RUB",
            orders=[cancelled, validated_created],
            offset=0,
            limit=1000,
        )

        result = OrderFeedNormalizer().normalize(page)

        created_row = next(row for row in result if row.srid == "order-created")
        self.assertIsNone(created_row.cancel_type)

    def test_empty_location_fields_are_replaced_without_dropping_order(self) -> None:
        """Сохраняет заказ, если WB прислал пустые или null-поля местоположения."""
        order_payload = _order("order-empty-location")
        order_payload["warehouseName"] = ""
        order_payload["warehouseRegion"] = "   "
        order_payload["destinationCity"] = None
        order_payload["destinationDistrict"] = ""
        order = OrderFeedOrderResponse.model_validate(order_payload)
        page = OrderFeedPage(
            account="vector",
            snapshot_time=None,
            currency="RUB",
            orders=[order],
            offset=0,
            limit=1000,
        )

        row = OrderFeedNormalizer().normalize(page)[0]

        self.assertEqual(row.warehouse_name, "Не указано")
        self.assertEqual(row.warehouse_region, "Не указано")
        self.assertEqual(row.destination_city, "Не указано")
        self.assertEqual(row.destination_district, "Не указано")

    def test_table_model_uses_semantic_enum_columns(self) -> None:
        """Оставляет enum только для справочников, которыми управляет приложение."""
        columns = WBOrderFeedRecord.__table__.columns

        self.assertIn("warehouse_type", columns)
        self.assertIn("sale_type", columns)
        self.assertNotIn("is_mp", columns)
        self.assertNotIn("is_b2b", columns)
        self.assertIsInstance(columns["status"].type, String)
        self.assertIsInstance(columns["cancel_type"].type, String)
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
            start=datetime(2026, 7, 1, tzinfo=tz),
            end=datetime(2026, 7, 2, tzinfo=tz),
        )
        client = WBOrderFeedClient(page_limit=1000)

        first = client._build_request_body(period, 0, None)
        second = client._build_request_body(period, 1000, "2026-07-02T10:00:00Z")

        self.assertNotIn("snapshotTime", first["pagination"])
        self.assertEqual(second["pagination"]["snapshotTime"], "2026-07-02T10:00:00Z")
        self.assertEqual(second["pagination"]["offset"], 1000)

    def test_internal_period_rejects_invalid_boundaries(self) -> None:
        """Переносит базовые инварианты периода из orchestration в Pydantic-модель."""
        tz = ZoneInfo("Europe/Moscow")

        with self.assertRaises(ValidationError):
            OrderFeedPeriod(
                start=datetime(2026, 7, 2, tzinfo=tz),
                end=datetime(2026, 7, 1, tzinfo=tz),
            )

    def test_internal_page_rejects_invalid_pagination(self) -> None:
        """Не выпускает из API-клиента страницу с отрицательным offset или limit."""
        with self.assertRaises(ValidationError):
            OrderFeedPage(
                account="vector",
                snapshot_time=None,
                currency="RUB",
                orders=[],
                offset=-1,
                limit=0,
            )

    def test_api_errors_are_typed_and_keep_wb_diagnostics(self) -> None:
        """Сохраняет статус, requestId и detail для диагностики ответа WB."""
        client = WBOrderFeedClient()
        cases = (
            (400, OrderFeedBadRequestError),
            (401, OrderFeedAuthenticationError),
            (429, OrderFeedRateLimitError),
        )
        payload = {
            "title": "too many requests",
            "detail": "limited by test-limit",
            "requestId": "request-123",
            "origin": "s2s-api-auth-catalog",
        }

        for status, expected_type in cases:
            with self.subTest(status=status):
                error = client._api_error(status, "vector", payload)

                self.assertIsInstance(error, expected_type)
                self.assertEqual(error.status, status)
                self.assertEqual(error.request_id, "request-123")
                self.assertEqual(error.detail, "limited by test-limit")

    def test_pydantic_preserves_unknown_external_enum_values(self) -> None:
        """Не теряет страницу при появлении новых status и cancelType в WB API."""
        invalid_order = _order("order-1")
        invalid_order["status"] = "awaitingPayment"
        invalid_order["cancelType"] = "sellerRequest"
        payload = {
            "data": {
                "snapshotTime": "2026-07-26T20:00:00Z",
                "currency": "RUB",
                "orders": [invalid_order],
            }
        }

        response = OrderFeedResponse.model_validate(payload)
        order = response.data.orders[0]

        self.assertEqual(order.status, "awaitingPayment")
        self.assertEqual(order.cancel_type, "sellerRequest")
        self.assertIsNone(order.known_status)
        self.assertIsNone(order.known_cancel_type)

    def test_pydantic_rejects_non_finite_seller_price(self) -> None:
        """Не допускает NaN и infinity, которые PostgreSQL Numeric не должен получать."""
        for invalid_price in (math.nan, math.inf, -math.inf):
            invalid_order = _order("order-1")
            invalid_order["sellerPrice"] = invalid_price
            payload = {
                "data": {
                    "snapshotTime": "2026-07-26T20:00:00Z",
                    "currency": "RUB",
                    "orders": [invalid_order],
                }
            }

            with self.subTest(invalid_price=invalid_price), self.assertRaises(
                ValidationError
            ):
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

    def test_empty_snapshot_is_allowed_for_last_page(self) -> None:
        """Сохраняет неполную страницу, когда WB не сформировал snapshotTime."""
        payload = {
            "data": {
                "snapshotTime": "",
                "currency": "RUB",
                "orders": [_order("order-1")],
            }
        }

        page = WBOrderFeedClient(page_limit=1000)._parse_page(
            "vector", 0, payload, 0
        )

        self.assertFalse(page.has_next_page)
        self.assertIsNone(page.snapshot_time)

    def test_empty_snapshot_stops_pagination_even_for_full_page(self) -> None:
        """Считает отсутствие snapshotTime явным признаком последней страницы WB."""
        payload = {
            "data": {
                "snapshotTime": "",
                "currency": "RUB",
                "orders": [_order("order-1")],
            }
        }

        page = WBOrderFeedClient(page_limit=1)._parse_page("vector", 0, payload, 0)

        self.assertFalse(page.has_next_page)
        self.assertIsNone(page.snapshot_time)


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
            account="vector",
            snapshot_time="2026-07-26T20:00:00Z",
            currency="RUB",
            orders=[older, newer],
            offset=0,
            limit=1000,
        )
        rows = OrderFeedNormalizer().normalize(page)

        deduplicated, collapsed = OrderFeedRepository()._deduplicate_by_keys(rows)

        self.assertEqual(collapsed, 1)
        self.assertEqual(len(deduplicated), 1)
        self.assertEqual(deduplicated[0].updated_at.hour, 16)

    def test_upsert_whitelist_does_not_change_order_identity(self) -> None:
        """Фиксирует явный whitelist изменяемых полей и защищает идентичность заказа."""
        immutable_columns = {
            "account",
            "srid",
            "nm_id",
            "chrt_id",
            "created_at",
            "data_source",
        }

        self.assertTrue(immutable_columns.isdisjoint(UPSERT_UPDATE_COLUMNS))
        self.assertIn("status", UPSERT_UPDATE_COLUMNS)
        self.assertIn("updated_at", UPSERT_UPDATE_COLUMNS)

    def test_legacy_identity_columns_are_nullable_and_use_partial_indexes(self) -> None:
        """Позволяет legacy-строкам жить без account/chrt_id и сохраняет идемпотентность."""
        table = WBOrderFeedRecord.__table__
        unique_indexes = {index.name for index in table.indexes if index.unique}

        self.assertTrue(table.c.account.nullable)
        self.assertTrue(table.c.chrt_id.nullable)
        self.assertFalse(table.c.srid.nullable)
        self.assertIn("uq_wb_order_feed_account_srid", unique_indexes)
        self.assertIn("uq_wb_order_feed_legacy_source_srid", unique_indexes)

    def test_page_is_split_into_database_chunks(self) -> None:
        """Не отправляет большую API-страницу одной тяжёлой транзакцией PostgreSQL."""
        rows = OrderFeedNormalizer().normalize(
            OrderFeedPage(
                account="vector",
                snapshot_time=None,
                currency="RUB",
                orders=[_validated_order(f"order-{index}") for index in range(5)],
                offset=0,
                limit=10,
            )
        )
        repository = OrderFeedRepository(chunk_size=2, max_retries=1)

        with patch.object(repository, "_upsert_chunk") as upsert_chunk:
            result = repository.save(rows, account="vector", offset=9000)

        self.assertEqual([len(call.args[0]) for call in upsert_chunk.call_args_list], [2, 2, 1])
        self.assertEqual(result.written_rows, 5)

    def test_transient_upsert_error_is_retried(self) -> None:
        """Повторяет оборванное соединение и не помечает кабинет сразу проваленным."""
        rows = OrderFeedNormalizer().normalize(
            OrderFeedPage(
                account="vector",
                snapshot_time=None,
                currency="RUB",
                orders=[_validated_order("order-1")],
                offset=0,
                limit=10,
            )
        )
        repository = OrderFeedRepository(chunk_size=1, max_retries=2)
        transient_error = OperationalError("INSERT", {}, Exception("connection lost"))
        engine = Mock()

        with (
            patch.object(
                repository,
                "_upsert_chunk",
                side_effect=[transient_error, None],
            ) as upsert_chunk,
            patch("src_oop.jobs.orders_feed.repository.time.sleep"),
            patch(
                "src_oop.jobs.orders_feed.repository.Database.get_engine",
                return_value=engine,
            ),
        ):
            result = repository.save(rows, account="vector", offset=36000)

        self.assertEqual(upsert_chunk.call_count, 2)
        engine.dispose.assert_called_once()
        self.assertEqual(result.written_rows, 1)

    def test_non_transient_upsert_error_is_not_retried(self) -> None:
        """Не повторяет нарушения ограничений БД, которые retry исправить не сможет."""
        repository = OrderFeedRepository(max_retries=4)
        error = IntegrityError("INSERT", {}, Exception("foreign key violation"))

        self.assertFalse(repository._is_transient_database_error(error))


class OrderFeedBackfillTest(unittest.TestCase):
    """Проверяет безопасные SQL-шаблоны разового переноса больших таблиц."""

    def test_source_selects_independent_keyset_query(self) -> None:
        """Использует id для sales и пару date/srid для orders без дорогого OFFSET."""
        backfill = OrderFeedBackfill()
        sales_query, sales_params = backfill._build_select_query(
            BackfillSource.SALES, 100, 500
        )
        orders_query, orders_params = backfill._build_select_query(
            BackfillSource.ORDERS, (date(2026, 1, 1), "srid-1"), 500
        )

        self.assertIn("FROM sales", sales_query)
        self.assertIn("source.id > :cursor_id", sales_query)
        self.assertIn("FROM orders", orders_query)
        self.assertIn("(source.date, source.srid) >", orders_query)
        self.assertNotIn(" OFFSET ", sales_query.upper())
        self.assertNotIn(" OFFSET ", orders_query.upper())
        self.assertEqual(sales_params["batch_size"], 500)
        self.assertEqual(orders_params["batch_size"], 500)

    def test_period_is_applied_before_batch_limit(self) -> None:
        """Ограничивает обе legacy-таблицы включительными календарными датами."""
        backfill = OrderFeedBackfill()
        timezone = ZoneInfo("Europe/Moscow")
        start, end_exclusive = backfill._resolve_period(
            datetime(2026, 1, 10, tzinfo=timezone),
            datetime(2026, 1, 12, tzinfo=timezone),
        )
        query, parameters = backfill._build_select_query(
            BackfillSource.SALES, 0, 500, start, end_exclusive
        )

        self.assertIn("source.date_from >= :date_from", query)
        self.assertIn("source.date_from < :date_to", query)
        self.assertEqual(start.date(), date(2026, 1, 10))
        self.assertEqual(end_exclusive.date(), date(2026, 1, 13))
        self.assertEqual(parameters["date_from"], start)
        self.assertEqual(parameters["date_to"], end_exclusive)

    def test_backfill_rejects_reversed_period(self) -> None:
        """Не запускает чтение таблиц при перепутанных границах периода."""
        timezone = ZoneInfo("Europe/Moscow")
        with self.assertRaisesRegex(ValueError, "не может быть позже"):
            OrderFeedBackfill()._resolve_period(
                datetime(2026, 2, 1, tzinfo=timezone),
                datetime(2026, 1, 1, tzinfo=timezone),
            )

    def test_python_schema_applies_required_legacy_business_rules(self) -> None:
        """Фиксирует статусы, источник, цену и тип покупателя в Pydantic-схеме."""
        source_row = {
            "srid": "legacy-1",
            "article_id": 123,
            "date_from": datetime(2026, 1, 1, tzinfo=ZoneInfo("Europe/Moscow")),
            "last_change_date": datetime(
                2026, 1, 2, tzinfo=ZoneInfo("Europe/Moscow")
            ),
            "warehouse_name": " Склад ",
            "warehouse_type": "Склад продавца",
            "country_name": "Казахстан",
            "oblast_okrug_name": "",
            "region_name": "",
            "total_price": 1000,
            "discount_percent": 10,
            "order_type": "Клиентский",
            "sale_id": "R123",
        }

        row = LegacyOrderFeedRow.from_source(source_row, DataSource.SALES)

        self.assertEqual(row.status, "return")
        self.assertEqual(row.seller_price, Decimal(900))
        self.assertEqual(row.sale_type, SaleType.B2C)
        self.assertEqual(row.warehouse_type, WarehouseType.SELLER)
        self.assertEqual(row.destination_district, "Казахстан")
        self.assertEqual(row.warehouse_region, "Не указано")
        self.assertEqual(row.data_source, DataSource.SALES)

    def test_seller_price_is_rounded_to_whole_rubles_half_up(self) -> None:
        """Округляет денежный результат до рубля по правилу 50 копеек вверх."""
        cases = {
            "311.49": Decimal(311),
            "311.50": Decimal(312),
            "311.85": Decimal(312),
        }

        for total_price, expected in cases.items():
            with self.subTest(total_price=total_price):
                result = LegacyOrderFeedRow._seller_price(
                    {"total_price": total_price, "discount_percent": 0}
                )
                self.assertEqual(result, expected)


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
            account="vector",
            snapshot_time="2026-07-26T20:00:00Z",
            currency="RUB",
            orders=orders,
            offset=offset,
            limit=2,
        )


class _FakeRepository:
    """Запоминает батчи, подтверждая сохранение каждой страницы отдельно."""

    def __init__(self) -> None:
        """Создаёт пустой журнал размеров сохранённых страниц."""
        self.batch_sizes: list[int] = []

    def save(self, dataframe, **_context) -> OrderFeedSaveResult:
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

    def test_account_parameter_never_accepts_credentials(self) -> None:
        """Не допускает JWT в параметре account и не возвращает секрет в тексте ошибки."""
        token = "header.payload.signature" * 10
        service = OrderFeedService(tokens_loader=lambda: {"vector": "secret"})

        with self.assertRaisesRegex(ValueError, "credentials") as context:
            service._resolve_tokens(f"vector:{token}")

        self.assertNotIn(token, str(context.exception))

if __name__ == "__main__":
    unittest.main()
