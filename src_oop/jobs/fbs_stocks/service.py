from __future__ import annotations

import asyncio
import logging
import os
from collections.abc import Callable, Mapping
from dataclasses import dataclass

import aiohttp

from src_oop.core.utils_general import load_api_tokens
from src_oop.jobs.fbs_stocks.client import WBFBSStocksClient
from src_oop.jobs.fbs_warehouses.config import ACCOUNT_ENV
from src_oop.jobs.fbs_stocks.config import (
    AUTO_REFILL_APPLY_ENV,
    APPLY_STOCKS_ENV,
    CREATE_MISSING_COLUMNS_ENV,
    REFRESH_VERIFY_ATTEMPTS,
    REFRESH_VERIFY_SLEEP_SECONDS,
    TARGET_WAREHOUSES,
    TOTAL_STOCK_COLUMN,
)
from src_oop.jobs.fbs_stocks.google_sheets_client import (
    FBSStocksGoogleSheetsClient,
    UnitAutoRefillRow,
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
    unchanged_rows: int = 0
    wb_requests: int = 0
    cleared_cells: int = 0
    refreshed_columns: int = 0
    applied: bool = False


@dataclass(slots=True)
class FBSStocksAutoRefillSummary:
    """Сводка автопополнения FBS-остатков для cron-сценария."""

    checked_rows: int = 0
    triggered_rows: int = 0
    prepared_rows: int = 0
    skipped_rows: int = 0
    wb_requests: int = 0
    refreshed_columns: int = 0
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
        """Обновляет текущий общий FBS-остаток в UNIT.

        Бизнес-сценарий: `ФБС общий остаток` показывает сумму остатков по всем активным внутренним
        FBS-складам аккаунта. Список складов задается нашей конфигурацией, а фактическая активная
        привязка к WB берется из `warehouses_fbs`.
        """
        if self._should_create_missing_columns():
            self.sheets_client.ensure_stock_management_columns()
        unit_rows, headers = self.sheets_client.read_unit_rows()
        unit_rows = self._filter_rows_by_account(unit_rows)
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
        """Отправляет в WB новые остатки из управляющих колонок UNIT.

        Бизнес-сценарии: `Новый остаток для всех складов` ставит указанное значение на каждый
        активный внутренний склад. `Новый остаток Вешки` ставит значение на склад Вешки, а остальные
        активные внутренние склады обнуляет. По умолчанию выполняется dry-run; после реальной
        успешной отправки исходные управляющие ячейки очищаются, а `ФБС общий остаток` перечитывается
        из WB. Если новых команд уже нет, но включена реальная отправка, сценарий работает как
        актуализация текущего общего FBS-остатка.
        """
        if self._should_create_missing_columns():
            self.sheets_client.ensure_stock_management_columns()

        new_stock_rows = self.sheets_client.read_new_stock_rows()
        new_stock_rows = self._filter_new_stock_rows_by_account(new_stock_rows)
        summary = FBSStocksApplySummary(
            requested_rows=len(new_stock_rows),
            applied=self._should_apply_stocks() if apply is None else apply,
        )
        if not new_stock_rows:
            logger.info("Отправка FBS-остатков пропущена: в UNIT нет новых остатков.")
            if summary.applied:
                refresh_summary = await self.update_current_fbs_stocks()
                summary.refreshed_columns = refresh_summary.updated_columns
            return summary

        chrt_ids_by_article = self.repository.fetch_chrt_ids_by_articles(
            [row.article_id for row in new_stock_rows]
        )
        warehouses = self.repository.fetch_fbs_warehouses()
        warehouse_details = self.repository.fetch_fbs_warehouse_details()
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
            (
                update_plan,
                summary.unchanged_rows,
                successful_groups,
            ) = await self._filter_unchanged_stock_updates(
                session=session,
                update_plan=update_plan,
                tokens_by_account=tokens_by_account,
            )
            sent_successful_groups: set[tuple[str, int]] = set()
            for (normalized_account, wb_warehouse_id), stocks_by_chrt_id in update_plan.items():
                token = tokens_by_account.get(normalized_account)
                if token is None:
                    logger.warning(
                        "Пропуск отправки FBS-остатков: для аккаунта UNIT нет токена WB | account=%s",
                        normalized_account,
                    )
                    continue
                warehouse_info = warehouse_details.get((normalized_account, wb_warehouse_id))
                try:
                    result = await self.wb_client.update_stocks(
                        session=session,
                        account=normalized_account,
                        token=token,
                        wb_warehouse_id=wb_warehouse_id,
                        stocks_by_chrt_id=stocks_by_chrt_id,
                        warehouse_name=warehouse_info.warehouse_name
                        if warehouse_info is not None
                        else None,
                        wb_office_id=warehouse_info.wb_office_id
                        if warehouse_info is not None
                        else None,
                    )
                except Exception as error:
                    logger.error(
                        "Отправка FBS-остатков WB завершилась ошибкой для одной группы, остальные группы обработаны | error_type=%s",
                        type(error).__name__,
                    )
                    continue
                summary.wb_requests += 1
                successful_groups.add((normalized_account, wb_warehouse_id))
                if result.sent_rows > 0:
                    sent_successful_groups.add((normalized_account, wb_warehouse_id))

        if successful_groups:
            successfully_applied_rows = self._filter_successfully_applied_rows(
                new_stock_rows=new_stock_rows,
                warehouses=warehouses,
                successful_groups=successful_groups,
            )
            summary.cleared_cells = self.sheets_client.clear_new_stock_cells(
                successfully_applied_rows
            )
            await self._wait_until_sent_stocks_are_visible(
                update_plan=update_plan,
                successful_groups=sent_successful_groups,
                tokens_by_account=tokens_by_account,
            )
            refresh_summary = await self.update_current_fbs_stocks()
            summary.refreshed_columns = refresh_summary.updated_columns

        logger.info(
            "Отправка FBS-остатков WB завершена | requested_rows=%s | prepared_rows=%s | skipped_rows=%s | unchanged_rows=%s | wb_requests=%s | cleared_cells=%s | refreshed_columns=%s",
            summary.requested_rows,
            summary.prepared_rows,
            summary.skipped_rows,
            summary.unchanged_rows,
            summary.wb_requests,
            summary.cleared_cells,
            summary.refreshed_columns,
        )
        return summary

    async def auto_refill_fbs_stocks(self, apply: bool | None = None) -> FBSStocksAutoRefillSummary:
        """Автоматически пополняет FBS-остатки, если средний остаток склада ниже минимума.

        Бизнес-сценарий: cron проверяет строки UNIT. Если текущая сумма FBS по активным внутренним
        складам, деленная на количество этих складов, меньше значения `Минимальный остаток`, задача
        берет `Добавляем` из `Сопост` по `wild` и устанавливает это значение на каждом активном
        складе. После реальной отправки задача перечитывает WB и обновляет
        `ФБС общий остаток`, чтобы подтвердить результат выгрузки.
        """
        rows = self._filter_auto_refill_rows_by_account(self.sheets_client.read_auto_refill_rows())
        summary = FBSStocksAutoRefillSummary(
            checked_rows=len(rows),
            applied=self._should_apply_auto_refill() if apply is None else apply,
        )
        if not rows:
            logger.info("Автопополнение FBS-остатков пропущено: нет строк для проверки.")
            return summary

        add_amounts_by_wild = self.sheets_client.read_sopost_add_amounts_by_wild()
        chrt_ids_by_article = self.repository.fetch_chrt_ids_by_articles(
            [row.article_id for row in rows]
        )
        warehouses = self.repository.fetch_fbs_warehouses()
        warehouse_details = self.repository.fetch_fbs_warehouse_details()
        tokens_by_account = self._resolve_tokens()
        stock_rows = [
            UnitStocksRow(
                row_number=row.row_number,
                article_id=row.article_id,
                account=row.account,
            )
            for row in rows
        ]
        stocks_by_account_warehouse = await self._fetch_all_stocks(
            unit_rows=stock_rows,
            chrt_ids_by_article=chrt_ids_by_article,
            warehouses=warehouses,
            tokens_by_account=tokens_by_account,
        )
        update_plan = self._build_auto_refill_update_plan(
            rows=rows,
            add_amounts_by_wild=add_amounts_by_wild,
            chrt_ids_by_article=chrt_ids_by_article,
            warehouses=warehouses,
            stocks_by_account_warehouse=stocks_by_account_warehouse,
            summary=summary,
        )
        summary.prepared_rows = sum(len(stocks) for stocks in update_plan.values())
        if not update_plan:
            logger.info(
                "Автопополнение FBS-остатков не требуется | checked_rows=%s | skipped_rows=%s",
                summary.checked_rows,
                summary.skipped_rows,
            )
            return summary

        if not summary.applied:
            logger.info(
                "Dry-run автопополнения FBS-остатков WB | checked_rows=%s | triggered_rows=%s | prepared_rows=%s | groups=%s",
                summary.checked_rows,
                summary.triggered_rows,
                summary.prepared_rows,
                len(update_plan),
            )
            return summary

        sent_successful_groups: set[tuple[str, int]] = set()
        async with aiohttp.ClientSession() as session:
            for (normalized_account, wb_warehouse_id), stocks_by_chrt_id in update_plan.items():
                token = tokens_by_account.get(normalized_account)
                if token is None:
                    logger.warning(
                        "Пропуск автопополнения FBS-остатков: для аккаунта UNIT нет токена WB | account=%s",
                        normalized_account,
                    )
                    continue
                warehouse_info = warehouse_details.get((normalized_account, wb_warehouse_id))
                try:
                    result = await self.wb_client.update_stocks(
                        session=session,
                        account=normalized_account,
                        token=token,
                        wb_warehouse_id=wb_warehouse_id,
                        stocks_by_chrt_id=stocks_by_chrt_id,
                        warehouse_name=warehouse_info.warehouse_name
                        if warehouse_info is not None
                        else None,
                        wb_office_id=warehouse_info.wb_office_id
                        if warehouse_info is not None
                        else None,
                    )
                except Exception as error:
                    logger.error(
                        "Автопополнение FBS-остатков WB завершилось ошибкой для одной группы, остальные группы продолжают работу | error_type=%s",
                        type(error).__name__,
                    )
                    continue
                summary.wb_requests += 1
                if result.sent_rows > 0:
                    sent_successful_groups.add((normalized_account, wb_warehouse_id))

        if sent_successful_groups:
            await self._wait_until_sent_stocks_are_visible(
                update_plan=update_plan,
                successful_groups=sent_successful_groups,
                tokens_by_account=tokens_by_account,
            )
            refresh_summary = await self.update_current_fbs_stocks()
            summary.refreshed_columns = refresh_summary.updated_columns

        logger.info(
            "Автопополнение FBS-остатков завершено | checked_rows=%s | triggered_rows=%s | prepared_rows=%s | skipped_rows=%s | wb_requests=%s | refreshed_columns=%s | applied=%s",
            summary.checked_rows,
            summary.triggered_rows,
            summary.prepared_rows,
            summary.skipped_rows,
            summary.wb_requests,
            summary.refreshed_columns,
            summary.applied,
        )
        return summary

    def _build_auto_refill_update_plan(
        self,
        rows: list[UnitAutoRefillRow],
        add_amounts_by_wild: dict[str, int],
        chrt_ids_by_article: dict[int, int],
        warehouses: dict[tuple[str, int], int],
        stocks_by_account_warehouse: dict[tuple[str, int], dict[int, int]],
        summary: FBSStocksAutoRefillSummary,
    ) -> dict[tuple[str, int], dict[int, int]]:
        """Формирует payload автопополнения, устанавливая `Добавляем` на каждом складе.

        Бизнес-правило: `Минимальный остаток` задан на один внутренний склад, поэтому сравнение идет
        по среднему остатку на активный склад. Если пополнение нужно, значение из `Сопост` в колонке
        `Добавляем` становится целевым остатком для каждого активного склада аккаунта.
        """
        update_plan: dict[tuple[str, int], dict[int, int]] = {}
        for row in rows:
            normalized_account = self.repository.normalize_account(row.account)
            chrt_id = chrt_ids_by_article.get(row.article_id)
            add_amount = add_amounts_by_wild.get(row.wild.casefold())
            active_wb_warehouse_ids = [
                wb_warehouse_id
                for target_warehouse in TARGET_WAREHOUSES
                if (
                    wb_warehouse_id := warehouses.get(
                        (normalized_account, target_warehouse.warehouse_id)
                    )
                )
                is not None
            ]
            if chrt_id is None or add_amount is None or not active_wb_warehouse_ids:
                summary.skipped_rows += 1
                logger.warning(
                    "Автопополнение FBS-остатков пропущено для строки UNIT: не хватает chrt_id, значения Добавляем или активных складов | row=%s | account=%s | article_id=%s | wild=%s",
                    row.row_number,
                    normalized_account,
                    row.article_id,
                    row.wild,
                )
                continue

            current_amounts_by_warehouse = {
                wb_warehouse_id: stocks_by_account_warehouse.get(
                    (normalized_account, wb_warehouse_id),
                    {},
                ).get(chrt_id, 0)
                for wb_warehouse_id in active_wb_warehouse_ids
            }
            current_total = sum(current_amounts_by_warehouse.values())
            average_stock = current_total / len(active_wb_warehouse_ids)
            if average_stock >= row.minimum_stock:
                continue

            summary.triggered_rows += 1
            logger.info(
                "Подготовлено автопополнение FBS-остатков | row=%s | account=%s | article_id=%s | wild=%s | current_total=%s | sheet_total=%s | warehouses=%s | average_stock=%.2f | minimum_stock=%s | target_amount=%s",
                row.row_number,
                normalized_account,
                row.article_id,
                row.wild,
                current_total,
                row.sheet_total_stock,
                len(active_wb_warehouse_ids),
                average_stock,
                row.minimum_stock,
                add_amount,
            )
            for wb_warehouse_id in current_amounts_by_warehouse:
                update_plan.setdefault((normalized_account, wb_warehouse_id), {})[
                    chrt_id
                ] = add_amount

        return update_plan

    async def _wait_until_sent_stocks_are_visible(
        self,
        update_plan: dict[tuple[str, int], dict[int, int]],
        successful_groups: set[tuple[str, int]],
        tokens_by_account: dict[str, str],
    ) -> None:
        """Ждет, пока read-API WB начнет видеть только что отправленные остатки.

        Бизнес-сценарий: после успешного PUT WB может ещё несколько секунд отдавать старые остатки
        в GET/POST-запросе чтения. Без ожидания `ФБС общий остаток` в UNIT может обновиться старой
        суммой сразу после успешной отправки нового значения.
        """
        expected_plan = {
            group: stocks_by_chrt_id
            for group, stocks_by_chrt_id in update_plan.items()
            if group in successful_groups and stocks_by_chrt_id
        }
        if not expected_plan:
            return

        for attempt in range(1, REFRESH_VERIFY_ATTEMPTS + 1):
            mismatches = await self._fetch_stock_visibility_mismatches(
                expected_plan=expected_plan,
                tokens_by_account=tokens_by_account,
            )
            if not mismatches:
                logger.info(
                    "WB read-API подтвердил новые FBS-остатки перед обновлением UNIT | groups=%s | attempt=%s/%s",
                    len(expected_plan),
                    attempt,
                    REFRESH_VERIFY_ATTEMPTS,
                )
                return

            sleep_seconds = REFRESH_VERIFY_SLEEP_SECONDS[
                min(attempt - 1, len(REFRESH_VERIFY_SLEEP_SECONDS) - 1)
            ]
            logger.info(
                "WB read-API пока возвращает старые FBS-остатки, ждем перед обновлением UNIT | mismatches=%s | attempt=%s/%s | sleep_seconds=%s",
                len(mismatches),
                attempt,
                REFRESH_VERIFY_ATTEMPTS,
                sleep_seconds,
            )
            await asyncio.sleep(sleep_seconds)

        logger.warning(
            "WB read-API не подтвердил часть новых FBS-остатков перед обновлением UNIT, обновляем таблицу последним доступным чтением | mismatches=%s",
            len(mismatches),
        )

    async def _fetch_stock_visibility_mismatches(
        self,
        expected_plan: dict[tuple[str, int], dict[int, int]],
        tokens_by_account: dict[str, str],
    ) -> list[tuple[str, int, int, int, int]]:
        """Сравнивает ожидаемые остатки с текущим ответом WB перед refresh таблицы.

        Бизнес-правило: значение `ФБС общий остаток` должно обновляться после того, как WB уже
        показывает в read-API отправленные остатки по складам. Нулевой остаток считается нулем,
        даже если WB не вернул строку по `chrtId`.
        """
        mismatches: list[tuple[str, int, int, int, int]] = []
        async with aiohttp.ClientSession() as session:
            for (normalized_account, wb_warehouse_id), stocks_by_chrt_id in expected_plan.items():
                token = tokens_by_account.get(normalized_account)
                if token is None:
                    continue
                current_result = await self.wb_client.fetch_stocks(
                    session=session,
                    account=normalized_account,
                    token=token,
                    wb_warehouse_id=wb_warehouse_id,
                    chrt_ids=sorted(stocks_by_chrt_id),
                )
                for chrt_id, expected_amount in stocks_by_chrt_id.items():
                    actual_amount = current_result.stocks_by_chrt_id.get(chrt_id, 0)
                    if actual_amount == expected_amount:
                        continue
                    mismatches.append(
                        (
                            normalized_account,
                            wb_warehouse_id,
                            chrt_id,
                            expected_amount,
                            actual_amount,
                        )
                    )
        return mismatches

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
                    "Запрос FBS-остатков WB завершился ошибкой, сценарий продолжает остальные склады | error_type=%s",
                    type(result).__name__,
                )
                continue
            stocks[(result.account, result.wb_warehouse_id)] = result.stocks_by_chrt_id
        return stocks

    async def _filter_unchanged_stock_updates(
        self,
        session: aiohttp.ClientSession,
        update_plan: dict[tuple[str, int], dict[int, int]],
        tokens_by_account: dict[str, str],
    ) -> tuple[dict[tuple[str, int], dict[int, int]], int, set[tuple[str, int]]]:
        """Убирает из отправки остатки, которые уже имеют нужное значение в WB.

        Бизнес-правило: повторный прогон после частичного успеха не должен заново отправлять
        уже примененные складские остатки, иначе WB может дольше держать конфликт 409 по тем же
        артикулам. Если весь складской payload уже совпадает с WB, группа считается успешной для
        очистки управляющей ячейки после обработки остальных складов.
        """
        filtered_plan: dict[tuple[str, int], dict[int, int]] = {}
        already_successful_groups: set[tuple[str, int]] = set()
        unchanged_rows = 0

        for (normalized_account, wb_warehouse_id), stocks_by_chrt_id in update_plan.items():
            token = tokens_by_account.get(normalized_account)
            if token is None:
                filtered_plan[(normalized_account, wb_warehouse_id)] = stocks_by_chrt_id
                continue

            try:
                current_result = await self.wb_client.fetch_stocks(
                    session=session,
                    account=normalized_account,
                    token=token,
                    wb_warehouse_id=wb_warehouse_id,
                    chrt_ids=sorted(stocks_by_chrt_id),
                )
            except Exception as error:
                logger.warning(
                    "Сверка текущих FBS-остатков перед отправкой не выполнена, группа будет отправлена как есть | account=%s | wb_warehouse_id=%s | error_type=%s",
                    normalized_account,
                    wb_warehouse_id,
                    type(error).__name__,
                )
                filtered_plan[(normalized_account, wb_warehouse_id)] = stocks_by_chrt_id
                continue

            changed_stocks = {
                chrt_id: amount
                for chrt_id, amount in stocks_by_chrt_id.items()
                if current_result.stocks_by_chrt_id.get(chrt_id, 0) != amount
            }
            unchanged_rows += len(stocks_by_chrt_id) - len(changed_stocks)
            if changed_stocks:
                filtered_plan[(normalized_account, wb_warehouse_id)] = changed_stocks
            else:
                already_successful_groups.add((normalized_account, wb_warehouse_id))

        if unchanged_rows:
            logger.info(
                "Часть новых FBS-остатков уже была применена в WB и пропущена при повторной отправке | rows=%s | groups_before=%s | groups_after=%s",
                unchanged_rows,
                len(update_plan),
                len(filtered_plan),
            )
        return filtered_plan, unchanged_rows, already_successful_groups

    def _build_column_values(
        self,
        unit_rows: list[UnitStocksRow],
        chrt_ids_by_article: dict[int, int],
        stocks_by_account_warehouse: dict[tuple[str, int], dict[int, int]],
    ) -> dict[str, list[list[int | str]]]:
        """Считает сумму FBS-остатков по всем активным внутренним складам для записи в UNIT."""
        warehouses = self.repository.fetch_fbs_warehouses()
        total_values: list[list[int | str]] = []

        for row in unit_rows:
            chrt_id = chrt_ids_by_article.get(row.article_id)
            if chrt_id is None:
                total_values.append([""])
                continue

            normalized_account = self.repository.normalize_account(row.account)
            total_stock = 0
            has_available_warehouse = False
            for target_warehouse in TARGET_WAREHOUSES:
                wb_warehouse_id = warehouses.get(
                    (normalized_account, target_warehouse.warehouse_id)
                )
                if wb_warehouse_id is None:
                    continue
                has_available_warehouse = True
                total_stock += stocks_by_account_warehouse.get(
                    (normalized_account, wb_warehouse_id),
                    {},
                ).get(chrt_id, 0)

            total_values.append([total_stock if has_available_warehouse else ""])

        return {TOTAL_STOCK_COLUMN: total_values}

    def _build_update_plan(
        self,
        new_stock_rows: list[UnitNewStockRow],
        chrt_ids_by_article: dict[int, int],
        warehouses: dict[tuple[str, int], int],
    ) -> dict[tuple[str, int], dict[int, int]]:
        """Группирует новые остатки UNIT в payload-ы WB по аккаунту и складу."""
        update_plan: dict[tuple[str, int], dict[int, int]] = {}
        missing_chrt_rows = 0
        missing_warehouse_rows: dict[tuple[str, int, str], int] = {}
        for row in new_stock_rows:
            normalized_account = self.repository.normalize_account(row.account)
            chrt_id = chrt_ids_by_article.get(row.article_id)
            wb_warehouse_id = warehouses.get((normalized_account, row.warehouse_id))
            if chrt_id is None:
                missing_chrt_rows += 1
                continue
            if wb_warehouse_id is None:
                missing_warehouse_key = (
                    normalized_account,
                    row.warehouse_id,
                    row.warehouse_alias,
                )
                missing_warehouse_rows[missing_warehouse_key] = (
                    missing_warehouse_rows.get(missing_warehouse_key, 0) + 1
                )
                continue
            update_plan.setdefault((normalized_account, wb_warehouse_id), {})[chrt_id] = row.amount

        if missing_chrt_rows:
            logger.warning(
                "Новые FBS-остатки частично пропущены: для части строк не найден chrt_id | rows=%s",
                missing_chrt_rows,
            )
        for (account, warehouse_id, warehouse_alias), rows_count in missing_warehouse_rows.items():
            logger.warning(
                "Новые FBS-остатки частично пропущены: склад не найден среди активных строк warehouses_fbs | account=%s | warehouse_id=%s | warehouse=%s | rows=%s",
                account,
                warehouse_id,
                warehouse_alias,
                rows_count,
            )
        return update_plan

    def _filter_successfully_applied_rows(
        self,
        new_stock_rows: list[UnitNewStockRow],
        warehouses: dict[tuple[str, int], int],
        successful_groups: set[tuple[str, int]],
    ) -> list[UnitNewStockRow]:
        """Выбирает управляющие ячейки UNIT, которые можно очистить после отправки в WB.

        Бизнес-правило: одна ячейка `Новый остаток ...` может создавать команды на несколько
        складов. Такая ячейка очищается только когда WB принял все активные складские команды,
        связанные с ней. При частичной ошибке исходное значение остается для повторной проверки.
        """
        rows_by_source_cell: dict[tuple[int, str], list[UnitNewStockRow]] = {}
        for row in new_stock_rows:
            rows_by_source_cell.setdefault((row.row_number, row.source_column), []).append(row)

        successfully_applied_rows: list[UnitNewStockRow] = []
        for source_rows in rows_by_source_cell.values():
            expected_groups: set[tuple[str, int]] = set()
            for row in source_rows:
                normalized_account = self.repository.normalize_account(row.account)
                wb_warehouse_id = warehouses.get((normalized_account, row.warehouse_id))
                if wb_warehouse_id is not None:
                    expected_groups.add((normalized_account, wb_warehouse_id))

            if not expected_groups:
                continue
            if expected_groups.issubset(successful_groups):
                successfully_applied_rows.extend(source_rows)
                continue

            first_row = source_rows[0]
            normalized_account = self.repository.normalize_account(first_row.account)
            logger.warning(
                "Управляющая ячейка нового FBS-остатка не очищена: не все складские команды успешно применены | row=%s | column=%s | account=%s | expected_groups=%s | successful_groups=%s",
                first_row.row_number,
                first_row.source_column,
                normalized_account,
                len(expected_groups),
                len(expected_groups.intersection(successful_groups)),
            )
        return successfully_applied_rows

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

    def _should_apply_auto_refill(self) -> bool:
        """Проверяет явное подтверждение автопополнения FBS-остатков в WB.

        Бизнес-правило: cron по умолчанию должен работать в режиме dry-run, пока пользователь явно
        не включит реальную отправку через отдельный флаг автопополнения.
        """
        return os.getenv(AUTO_REFILL_APPLY_ENV, "").strip().lower() in {
            "1",
            "true",
            "yes",
            "y",
            "да",
        }

    def _filter_rows_by_account(self, rows: list[UnitStocksRow]) -> list[UnitStocksRow]:
        """Оставляет строки одного ЛК, если пользователь ограничил сценарий через `WB_FBS_ACCOUNT`."""
        account_filter = os.getenv(ACCOUNT_ENV)
        if not account_filter:
            return rows
        normalized_filter = self.repository.normalize_account(account_filter)
        filtered_rows = [
            row
            for row in rows
            if self.repository.normalize_account(row.account) == normalized_filter
        ]
        logger.info(
            "Строки UNIT отфильтрованы по ЛК для FBS-остатков | account=%s | before=%s | after=%s",
            account_filter,
            len(rows),
            len(filtered_rows),
        )
        return filtered_rows

    def _filter_new_stock_rows_by_account(
        self,
        rows: list[UnitNewStockRow],
    ) -> list[UnitNewStockRow]:
        """Оставляет новые остатки одного ЛК, если пользователь ограничил отправку через `WB_FBS_ACCOUNT`."""
        account_filter = os.getenv(ACCOUNT_ENV)
        if not account_filter:
            return rows
        normalized_filter = self.repository.normalize_account(account_filter)
        filtered_rows = [
            row
            for row in rows
            if self.repository.normalize_account(row.account) == normalized_filter
        ]
        logger.info(
            "Новые FBS-остатки отфильтрованы по ЛК | account=%s | before=%s | after=%s",
            account_filter,
            len(rows),
            len(filtered_rows),
        )
        return filtered_rows

    def _filter_auto_refill_rows_by_account(
        self,
        rows: list[UnitAutoRefillRow],
    ) -> list[UnitAutoRefillRow]:
        """Оставляет строки автопополнения одного ЛК, если задан `WB_FBS_ACCOUNT`.

        Бизнес-сценарий: cron можно запускать как по всем аккаунтам сразу, так и точечно по одному
        личному кабинету для тестирования или временной изоляции.
        """
        account_filter = os.getenv(ACCOUNT_ENV)
        if not account_filter:
            return rows
        normalized_filter = self.repository.normalize_account(account_filter)
        filtered_rows = [
            row
            for row in rows
            if self.repository.normalize_account(row.account) == normalized_filter
        ]
        logger.info(
            "Строки UNIT для автопополнения FBS-остатков отфильтрованы по ЛК | account=%s | before=%s | after=%s",
            account_filter,
            len(rows),
            len(filtered_rows),
        )
        return filtered_rows
