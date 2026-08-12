from __future__ import annotations

import asyncio
import logging
import os
from collections.abc import Callable, Mapping
from dataclasses import dataclass

import aiohttp

from src_oop.core.utils_general import load_api_tokens
from src_oop.jobs.fbs_stocks.client import WBFBSStocksClient
from src_oop.jobs.fbs_stocks.config import (
    APPLY_STOCKS_ENV,
    CREATE_MISSING_COLUMNS_ENV,
    TARGET_WAREHOUSES,
)
from src_oop.jobs.fbs_stocks.google_sheets_client import (
    FBSStocksGoogleSheetsClient,
    UnitNewStockRow,
    UnitStocksRow,
)
from src_oop.jobs.fbs_stocks.repository import FBSStocksRepository

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class FBSStocksUpdateSummary:
    """Сводка обновления FBS-остатков в UNIT."""

    unit_rows: int = 0
    articles_with_chrt_id: int = 0
    wb_requests: int = 0
    updated_columns: int = 0


@dataclass(slots=True)
class FBSStocksApplySummary:
    """Сводка подготовки или отправки новых FBS-остатков в WB."""

    requested_rows: int = 0
    prepared_rows: int = 0
    skipped_rows: int = 0
    wb_requests: int = 0
    applied: bool = False


class FBSStocksService:
    """Оркестрирует чтение FBS-остатков WB и запись в складовые колонки UNIT."""

    def __init__(
        self,
        sheets_client: FBSStocksGoogleSheetsClient | None = None,
        repository: FBSStocksRepository | None = None,
        wb_client: WBFBSStocksClient | None = None,
        tokens_loader: Callable[[], Mapping[str, str]] | None = None,
    ) -> None:
        """Собирает зависимости сценария управления остатками без глобального состояния."""
        self.sheets_client = sheets_client or FBSStocksGoogleSheetsClient()
        self.repository = repository or FBSStocksRepository()
        self.wb_client = wb_client or WBFBSStocksClient()
        self.tokens_loader = tokens_loader or load_api_tokens

    async def update_current_fbs_stocks(self) -> FBSStocksUpdateSummary:
        """Обновляет текущие FBS-остатки в `ФБС Вешки` и `ФБС Казань`.

        Бизнес-сценарий: UNIT остается рабочей поверхностью для планирования
        новых остатков, а текущие значения FBS подтягиваются из WB по нашим
        логическим складам из `warehouses_fbs`.
        """
        if self._should_create_missing_columns():
            self.sheets_client.ensure_stock_management_columns()
        unit_rows, headers = self.sheets_client.read_unit_rows()
        summary = FBSStocksUpdateSummary(unit_rows=len(unit_rows))
        if not unit_rows:
            logger.warning("Обновление FBS-остатков пропущено: в UNIT нет строк с артикулами.")
            return summary

        chrt_ids_by_article = self.repository.fetch_chrt_ids_by_articles(
            [row.article_id for row in unit_rows]
        )
        summary.articles_with_chrt_id = len(chrt_ids_by_article)
        warehouses = self.repository.fetch_fbs_warehouses()
        tokens_by_account = self._resolve_tokens()

        stocks_by_account_warehouse = await self._fetch_all_stocks(
            unit_rows=unit_rows,
            chrt_ids_by_article=chrt_ids_by_article,
            warehouses=warehouses,
            tokens_by_account=tokens_by_account,
        )
        summary.wb_requests = len(stocks_by_account_warehouse)

        values_by_column = self._build_column_values(
            unit_rows=unit_rows,
            chrt_ids_by_article=chrt_ids_by_article,
            stocks_by_account_warehouse=stocks_by_account_warehouse,
        )
        self.sheets_client.write_stock_columns(headers=headers, values_by_column=values_by_column)
        summary.updated_columns = len(values_by_column)
        logger.info(
            "Обновление FBS-остатков UNIT завершено | unit_rows=%s | articles_with_chrt_id=%s | wb_requests=%s | updated_columns=%s",
            summary.unit_rows,
            summary.articles_with_chrt_id,
            summary.wb_requests,
            summary.updated_columns,
        )
        return summary

    async def apply_new_fbs_stocks(self, apply: bool | None = None) -> FBSStocksApplySummary:
        """Отправляет в WB новые остатки из `Новый остаток Вешки/Казань`.

        Бизнес-сценарий: пользователь планирует новые остатки в UNIT, а job
        отправляет только явно заполненные ячейки в соответствующий склад WB.
        По умолчанию выполняется dry-run, чтобы случайно не перезаписать остатки.
        """
        if self._should_create_missing_columns():
            self.sheets_client.ensure_stock_management_columns()

        new_stock_rows = self.sheets_client.read_new_stock_rows()
        summary = FBSStocksApplySummary(
            requested_rows=len(new_stock_rows),
            applied=self._should_apply_stocks() if apply is None else apply,
        )
        if not new_stock_rows:
            logger.info("Отправка FBS-остатков пропущена: в UNIT нет новых остатков.")
            return summary

        chrt_ids_by_article = self.repository.fetch_chrt_ids_by_articles(
            [row.article_id for row in new_stock_rows]
        )
        warehouses = self.repository.fetch_fbs_warehouses()
        tokens_by_account = self._resolve_tokens()
        update_plan = self._build_update_plan(
            new_stock_rows=new_stock_rows,
            chrt_ids_by_article=chrt_ids_by_article,
            warehouses=warehouses,
        )
        summary.prepared_rows = sum(len(stocks) for stocks in update_plan.values())
        summary.skipped_rows = summary.requested_rows - summary.prepared_rows

        if not summary.applied:
            logger.info(
                "Dry-run отправки FBS-остатков WB | requested_rows=%s | prepared_rows=%s | skipped_rows=%s | groups=%s",
                summary.requested_rows,
                summary.prepared_rows,
                summary.skipped_rows,
                len(update_plan),
            )
            return summary

        async with aiohttp.ClientSession() as session:
            tasks = []
            for (normalized_account, wb_warehouse_id), stocks_by_chrt_id in update_plan.items():
                token = tokens_by_account.get(normalized_account)
                if token is None:
                    logger.warning(
                        "Пропуск отправки FBS-остатков: для аккаунта UNIT нет токена WB | account=%s",
                        normalized_account,
                    )
                    continue
                tasks.append(
                    self.wb_client.update_stocks(
                        session=session,
                        account=normalized_account,
                        token=token,
                        wb_warehouse_id=wb_warehouse_id,
                        stocks_by_chrt_id=stocks_by_chrt_id,
                    )
                )
            update_results = await asyncio.gather(*tasks, return_exceptions=True)

        for result in update_results:
            if isinstance(result, Exception):
                logger.error(
                    "Отправка FBS-остатков WB завершилась ошибкой для одной группы, остальные группы обработаны | error_type=%s | error=%s",
                    type(result).__name__,
                    result,
                )
                continue
            summary.wb_requests += 1

        logger.info(
            "Отправка FBS-остатков WB завершена | requested_rows=%s | prepared_rows=%s | skipped_rows=%s | wb_requests=%s",
            summary.requested_rows,
            summary.prepared_rows,
            summary.skipped_rows,
            summary.wb_requests,
        )
        return summary

    async def _fetch_all_stocks(
        self,
        unit_rows: list[UnitStocksRow],
        chrt_ids_by_article: dict[int, int],
        warehouses: dict[tuple[str, int], int],
        tokens_by_account: dict[str, str],
    ) -> dict[tuple[str, int], dict[int, int]]:
        """Загружает остатки WB по всем парам аккаунт-склад, которые нужны строкам UNIT."""
        chrt_ids_by_account: dict[str, set[int]] = {}
        for row in unit_rows:
            chrt_id = chrt_ids_by_article.get(row.article_id)
            if chrt_id is None:
                continue
            normalized_account = self.repository.normalize_account(row.account)
            chrt_ids_by_account.setdefault(normalized_account, set()).add(chrt_id)

        async with aiohttp.ClientSession() as session:
            tasks = []
            for normalized_account, chrt_ids in chrt_ids_by_account.items():
                token = tokens_by_account.get(normalized_account)
                if token is None:
                    logger.warning(
                        "Пропуск FBS-остатков: для аккаунта UNIT нет токена WB | account=%s",
                        normalized_account,
                    )
                    continue
                for target_warehouse in TARGET_WAREHOUSES:
                    wb_warehouse_id = warehouses.get(
                        (normalized_account, target_warehouse.warehouse_id)
                    )
                    if wb_warehouse_id is None:
                        logger.warning(
                            "Пропуск FBS-остатков: склад не найден в warehouses_fbs | account=%s | warehouse_id=%s",
                            normalized_account,
                            target_warehouse.warehouse_id,
                        )
                        continue
                    tasks.append(
                        self.wb_client.fetch_stocks(
                            session=session,
                            account=normalized_account,
                            token=token,
                            wb_warehouse_id=wb_warehouse_id,
                            chrt_ids=sorted(chrt_ids),
                        )
                    )

            fetch_results = await asyncio.gather(*tasks, return_exceptions=True)

        stocks: dict[tuple[str, int], dict[int, int]] = {}
        for result in fetch_results:
            if isinstance(result, Exception):
                logger.error(
                    "Запрос FBS-остатков WB завершился ошибкой, сценарий продолжает остальные склады | error_type=%s | error=%s",
                    type(result).__name__,
                    result,
                )
                continue
            stocks[(result.account, result.wb_warehouse_id)] = result.stocks_by_chrt_id
        return stocks

    def _build_column_values(
        self,
        unit_rows: list[UnitStocksRow],
        chrt_ids_by_article: dict[int, int],
        stocks_by_account_warehouse: dict[tuple[str, int], dict[int, int]],
    ) -> dict[str, list[list[int | str]]]:
        """Раскладывает FBS-остатки WB в матрицы колонок UNIT по складам Вешки и Казань."""
        warehouses = self.repository.fetch_fbs_warehouses()
        values_by_column: dict[str, list[list[int | str]]] = {}

        for target_warehouse in TARGET_WAREHOUSES:
            column_values: list[list[int | str]] = []
            for row in unit_rows:
                normalized_account = self.repository.normalize_account(row.account)
                wb_warehouse_id = warehouses.get(
                    (normalized_account, target_warehouse.warehouse_id)
                )
                chrt_id = chrt_ids_by_article.get(row.article_id)
                if wb_warehouse_id is None or chrt_id is None:
                    column_values.append([""])
                    continue
                stock_value = stocks_by_account_warehouse.get(
                    (normalized_account, wb_warehouse_id),
                    {},
                ).get(chrt_id, 0)
                column_values.append([stock_value])
            values_by_column[target_warehouse.target_column] = column_values

        return values_by_column

    def _build_update_plan(
        self,
        new_stock_rows: list[UnitNewStockRow],
        chrt_ids_by_article: dict[int, int],
        warehouses: dict[tuple[str, int], int],
    ) -> dict[tuple[str, int], dict[int, int]]:
        """Группирует новые остатки UNIT в payload-ы WB по аккаунту и складу."""
        update_plan: dict[tuple[str, int], dict[int, int]] = {}
        for row in new_stock_rows:
            normalized_account = self.repository.normalize_account(row.account)
            chrt_id = chrt_ids_by_article.get(row.article_id)
            wb_warehouse_id = warehouses.get((normalized_account, row.warehouse_id))
            if chrt_id is None:
                logger.warning(
                    "Новый FBS-остаток пропущен: не найден chrt_id | row=%s | article_id=%s | account=%s | warehouse=%s",
                    row.row_number,
                    row.article_id,
                    row.account,
                    row.warehouse_alias,
                )
                continue
            if wb_warehouse_id is None:
                logger.warning(
                    "Новый FBS-остаток пропущен: склад не найден в warehouses_fbs | row=%s | account=%s | warehouse_id=%s | warehouse=%s",
                    row.row_number,
                    row.account,
                    row.warehouse_id,
                    row.warehouse_alias,
                )
                continue
            update_plan.setdefault((normalized_account, wb_warehouse_id), {})[chrt_id] = row.amount
        return update_plan

    def _resolve_tokens(self) -> dict[str, str]:
        """Загружает WB-токены и нормализует имена аккаунтов под колонку `ЛК` в UNIT."""
        loaded_tokens = self.tokens_loader()
        if not isinstance(loaded_tokens, Mapping):
            raise TypeError("load_api_tokens() должен возвращать Mapping account -> token.")
        return {
            self.repository.normalize_account(account): token.strip()
            for account, token in loaded_tokens.items()
            if isinstance(account, str)
            and account.strip()
            and isinstance(token, str)
            and token.strip()
        }

    def _should_create_missing_columns(self) -> bool:
        """Проверяет явное разрешение на создание колонок, чтобы не менять структуру UNIT случайно."""
        return os.getenv(CREATE_MISSING_COLUMNS_ENV, "").strip().lower() in {
            "1",
            "true",
            "yes",
            "y",
            "да",
        }

    def _should_apply_stocks(self) -> bool:
        """Проверяет явное подтверждение отправки новых остатков в WB."""
        return os.getenv(APPLY_STOCKS_ENV, "").strip().lower() in {
            "1",
            "true",
            "yes",
            "y",
            "да",
        }
