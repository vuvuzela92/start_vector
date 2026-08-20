from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from collections.abc import Sequence

import aiohttp

from src_oop.jobs.fbs_stocks.config import (
    CHRT_IDS_CHUNK_SIZE,
    MAX_RETRIES,
    REQUEST_TIMEOUT_SECONDS,
    RETRY_BASE_SLEEP_SECONDS,
    RETRY_MAX_SLEEP_SECONDS,
    STOCKS_URL_TEMPLATE,
)

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class FBSStockFetchResult:
    """Результат чтения FBS-остатков WB по одному складу аккаунта."""

    account: str
    wb_warehouse_id: int
    stocks_by_chrt_id: dict[int, int]
    retries_used: int = 0


@dataclass(slots=True)
class FBSStockUpdateResult:
    """Результат отправки новых FBS-остатков WB по одному складу аккаунта."""

    account: str
    wb_warehouse_id: int
    sent_rows: int
    skipped_restricted_rows: int = 0
    skipped_not_found_rows: int = 0
    skipped_restricted_chrt_ids: tuple[int, ...] = ()
    skipped_not_found_chrt_ids: tuple[int, ...] = ()
    retries_used: int = 0


class WBFBSStocksClient:
    """Клиент WB Marketplace API для получения текущих остатков FBS-складов."""

    def __init__(
        self,
        request_timeout_seconds: int = REQUEST_TIMEOUT_SECONDS,
        max_retries: int = MAX_RETRIES,
        retry_base_sleep_seconds: int = RETRY_BASE_SLEEP_SECONDS,
        retry_max_sleep_seconds: int = RETRY_MAX_SLEEP_SECONDS,
        chrt_ids_chunk_size: int = CHRT_IDS_CHUNK_SIZE,
    ) -> None:
        """Настраивает HTTP-клиент для чтения остатков WB с retry и chunk-запросами."""
        self.request_timeout_seconds = request_timeout_seconds
        self.max_retries = max_retries
        self.retry_base_sleep_seconds = retry_base_sleep_seconds
        self.retry_max_sleep_seconds = retry_max_sleep_seconds
        self.chrt_ids_chunk_size = chrt_ids_chunk_size

    async def fetch_stocks(
        self,
        session: aiohttp.ClientSession,
        account: str,
        token: str,
        wb_warehouse_id: int,
        chrt_ids: Sequence[int],
    ) -> FBSStockFetchResult:
        """Получает остатки WB по chrt_id для склада, чтобы обновить FBS-колонку UNIT."""
        prepared_chrt_ids = sorted({int(chrt_id) for chrt_id in chrt_ids if chrt_id})
        stocks_by_chrt_id: dict[int, int] = {}
        retries_used = 0

        for chunk_start in range(0, len(prepared_chrt_ids), self.chrt_ids_chunk_size):
            chunk = prepared_chrt_ids[chunk_start : chunk_start + self.chrt_ids_chunk_size]
            result_payload, chunk_retries = await self._request_stocks_chunk(
                session=session,
                account=account,
                token=token,
                wb_warehouse_id=wb_warehouse_id,
                chrt_ids=chunk,
            )
            retries_used += chunk_retries
            stocks_by_chrt_id.update(self._parse_stocks_payload(result_payload))

        logger.info(
            "FBS-остатки WB получены | account=%s | wb_warehouse_id=%s | requested_chrt_ids=%s | returned_rows=%s | retries_used=%s",
            account,
            wb_warehouse_id,
            len(prepared_chrt_ids),
            len(stocks_by_chrt_id),
            retries_used,
        )
        return FBSStockFetchResult(
            account=account,
            wb_warehouse_id=wb_warehouse_id,
            stocks_by_chrt_id=stocks_by_chrt_id,
            retries_used=retries_used,
        )

    async def update_stocks(
        self,
        session: aiohttp.ClientSession,
        account: str,
        token: str,
        wb_warehouse_id: int,
        stocks_by_chrt_id: dict[int, int],
        warehouse_name: str | None = None,
        wb_office_id: int | None = None,
    ) -> FBSStockUpdateResult:
        """Отправляет новые FBS-остатки WB по chrt_id для выбранного склада.

        Бизнес-правило: в WB отправляются только строки, где пользователь явно
        указал новый остаток в UNIT. Пустые значения не превращаются в ноль. Название склада и
        `wb_office_id` используются только для понятной диагностики ограничений хранения WB.
        """
        prepared_stocks = [
            {"chrtId": int(chrt_id), "amount": int(amount)}
            for chrt_id, amount in sorted(stocks_by_chrt_id.items())
        ]
        retries_used = 0
        skipped_restricted_rows = 0
        skipped_not_found_rows = 0
        skipped_restricted_chrt_ids: set[int] = set()
        skipped_not_found_chrt_ids: set[int] = set()

        for chunk_start in range(0, len(prepared_stocks), self.chrt_ids_chunk_size):
            chunk = prepared_stocks[chunk_start : chunk_start + self.chrt_ids_chunk_size]
            (
                chunk_retries,
                chunk_skipped_restricted,
                chunk_skipped_not_found,
                chunk_restricted_chrt_ids,
                chunk_not_found_chrt_ids,
            ) = await self._request_update_stocks_chunk(
                session=session,
                account=account,
                token=token,
                wb_warehouse_id=wb_warehouse_id,
                stocks=chunk,
                warehouse_name=warehouse_name,
                wb_office_id=wb_office_id,
            )
            retries_used += chunk_retries
            skipped_restricted_rows += chunk_skipped_restricted
            skipped_not_found_rows += chunk_skipped_not_found
            skipped_restricted_chrt_ids.update(chunk_restricted_chrt_ids)
            skipped_not_found_chrt_ids.update(chunk_not_found_chrt_ids)

        logger.info(
            "Новые FBS-остатки отправлены в WB | account=%s | wb_warehouse_id=%s | rows=%s | skipped_restricted_rows=%s | skipped_not_found_rows=%s | retries_used=%s",
            account,
            wb_warehouse_id,
            len(prepared_stocks) - skipped_restricted_rows - skipped_not_found_rows,
            skipped_restricted_rows,
            skipped_not_found_rows,
            retries_used,
        )
        return FBSStockUpdateResult(
            account=account,
            wb_warehouse_id=wb_warehouse_id,
            sent_rows=len(prepared_stocks) - skipped_restricted_rows - skipped_not_found_rows,
            skipped_restricted_rows=skipped_restricted_rows,
            skipped_not_found_rows=skipped_not_found_rows,
            skipped_restricted_chrt_ids=tuple(sorted(skipped_restricted_chrt_ids)),
            skipped_not_found_chrt_ids=tuple(sorted(skipped_not_found_chrt_ids)),
            retries_used=retries_used,
        )

    async def _request_update_stocks_chunk(
        self,
        session: aiohttp.ClientSession,
        account: str,
        token: str,
        wb_warehouse_id: int,
        stocks: Sequence[dict[str, int]],
        warehouse_name: str | None = None,
        wb_office_id: int | None = None,
    ) -> tuple[int, int, int, set[int], set[int]]:
        """Выполняет один chunk PUT-запрос обновления остатков WB с защитой от временных сбоев.

        Бизнес-правило: ограничение `CargoWarehouseRestrictionMGT` означает, что конкретный товар
        нельзя грузить на выбранный склад. Такие chrtId пропускаются без retry, чтобы один складовой
        запрет WB не блокировал обновление остальных складов и товаров. Код `NotFound` означает,
        что товар WB больше недоступен для этого FBS-сценария, например удален или отправлен в
        корзину через сайт, поэтому такие chrtId тоже исключаются без retry.
        """
        headers = {"Authorization": token}
        timeout = aiohttp.ClientTimeout(total=self.request_timeout_seconds)
        url = STOCKS_URL_TEMPLATE.format(warehouse_id=wb_warehouse_id)
        prepared_stocks = list(stocks)
        skipped_restricted_rows = 0
        skipped_not_found_rows = 0
        skipped_restricted_chrt_ids: set[int] = set()
        skipped_not_found_chrt_ids: set[int] = set()
        last_status: int | None = None
        last_payload: dict | list | None = None

        for attempt in range(1, self.max_retries + 1):
            json_payload = {"stocks": prepared_stocks}
            try:
                async with session.put(
                    url,
                    headers=headers,
                    json=json_payload,
                    timeout=timeout,
                ) as response:
                    payload = await self._read_payload(response)
                    if response.status in {409, 429, 500, 502, 503, 504}:
                        last_status = response.status
                        last_payload = payload
                        restricted_chrt_ids = self._extract_cargo_restricted_chrt_ids(payload)
                        if response.status == 409 and restricted_chrt_ids:
                            before_count = len(prepared_stocks)
                            prepared_stocks = [
                                stock
                                for stock in prepared_stocks
                                if int(stock["chrtId"]) not in restricted_chrt_ids
                            ]
                            skipped_restricted_chrt_ids.update(restricted_chrt_ids)
                            skipped_restricted_rows += before_count - len(prepared_stocks)
                            logger.warning(
                                "FBS-остатки пропущены для склада WB: товар не подходит под тип склада | account=%s | warehouse_name=%s | wb_warehouse_id=%s | wb_office_id=%s | code=CargoWarehouseRestrictionMGT | chrt_ids=%s | skipped_rows=%s",
                                account,
                                warehouse_name,
                                wb_warehouse_id,
                                wb_office_id,
                                sorted(restricted_chrt_ids),
                                before_count - len(prepared_stocks),
                            )
                            if not prepared_stocks:
                                return (
                                    attempt - 1,
                                    skipped_restricted_rows,
                                    skipped_not_found_rows,
                                    skipped_restricted_chrt_ids,
                                    skipped_not_found_chrt_ids,
                                )
                            continue
                        not_found_chrt_ids = self._extract_not_found_chrt_ids(payload)
                        if response.status == 409 and not_found_chrt_ids:
                            before_count = len(prepared_stocks)
                            prepared_stocks = [
                                stock
                                for stock in prepared_stocks
                                if int(stock["chrtId"]) not in not_found_chrt_ids
                            ]
                            skipped_rows = before_count - len(prepared_stocks)
                            skipped_not_found_rows += skipped_rows
                            skipped_not_found_chrt_ids.update(not_found_chrt_ids)
                            logger.warning(
                                "FBS-остатки исключены для склада WB: товар не найден и, вероятно, удален или находится в корзине | account=%s | warehouse_name=%s | wb_warehouse_id=%s | wb_office_id=%s | code=NotFound | chrt_ids=%s | skipped_rows=%s",
                                account,
                                warehouse_name,
                                wb_warehouse_id,
                                wb_office_id,
                                sorted(not_found_chrt_ids),
                                skipped_rows,
                            )
                            if not prepared_stocks:
                                return (
                                    attempt - 1,
                                    skipped_restricted_rows,
                                    skipped_not_found_rows,
                                    skipped_restricted_chrt_ids,
                                    skipped_not_found_chrt_ids,
                                )
                            continue
                        await self._sleep_for_retry(
                            account=account,
                            wb_warehouse_id=wb_warehouse_id,
                            attempt=attempt,
                            status=response.status,
                            payload=payload,
                        )
                        continue
                    if response.status == 401:
                        raise PermissionError(
                            f"WB отклонил токен при обновлении FBS-остатков: account={account}"
                        )
                    self._raise_for_status_safely(
                        response=response,
                        payload=payload,
                        account=account,
                        wb_warehouse_id=wb_warehouse_id,
                        action="обновлении FBS-остатков",
                    )
                    return (
                        attempt - 1,
                        skipped_restricted_rows,
                        skipped_not_found_rows,
                        skipped_restricted_chrt_ids,
                        skipped_not_found_chrt_ids,
                    )
            except PermissionError:
                raise
            except (
                asyncio.TimeoutError,
                TimeoutError,
                aiohttp.ClientConnectionError,
                aiohttp.ClientError,
                OSError,
            ) as error:
                if attempt >= self.max_retries:
                    raise RuntimeError(
                        "Обновление FBS-остатков WB завершилось ошибкой после всех повторов: "
                        f"account={account} wb_warehouse_id={wb_warehouse_id} "
                        f"error_type={type(error).__name__}"
                    ) from error
                await self._sleep_for_retry(
                    account=account,
                    wb_warehouse_id=wb_warehouse_id,
                    attempt=attempt,
                    error_type=type(error).__name__,
                )

        raise RuntimeError(
            "Обновление FBS-остатков WB завершилось ошибкой после всех повторов: "
            f"account={account} wb_warehouse_id={wb_warehouse_id} "
            f"status={last_status} payload={self._payload_for_log(last_payload)}"
        )

    async def _request_stocks_chunk(
        self,
        session: aiohttp.ClientSession,
        account: str,
        token: str,
        wb_warehouse_id: int,
        chrt_ids: Sequence[int],
    ) -> tuple[dict | list | None, int]:
        """Выполняет один chunk-запрос остатков WB, защищая сценарий от 429 и временных сбоев."""
        headers = {"Authorization": token}
        timeout = aiohttp.ClientTimeout(total=self.request_timeout_seconds)
        url = STOCKS_URL_TEMPLATE.format(warehouse_id=wb_warehouse_id)
        json_payload = {"chrtIds": list(chrt_ids)}
        last_status: int | None = None
        last_payload: dict | list | None = None

        for attempt in range(1, self.max_retries + 1):
            try:
                async with session.post(
                    url,
                    headers=headers,
                    json=json_payload,
                    timeout=timeout,
                ) as response:
                    payload = await self._read_payload(response)
                    if response.status == 429 or response.status in {500, 502, 503, 504}:
                        last_status = response.status
                        last_payload = payload
                        await self._sleep_for_retry(
                            account=account,
                            wb_warehouse_id=wb_warehouse_id,
                            attempt=attempt,
                            status=response.status,
                            payload=payload,
                        )
                        continue
                    if response.status == 401:
                        raise PermissionError(
                            f"WB отклонил токен при чтении FBS-остатков: account={account}"
                        )
                    self._raise_for_status_safely(
                        response=response,
                        payload=payload,
                        account=account,
                        wb_warehouse_id=wb_warehouse_id,
                        action="чтении FBS-остатков",
                    )
                    return payload, attempt - 1
            except PermissionError:
                raise
            except (
                asyncio.TimeoutError,
                TimeoutError,
                aiohttp.ClientConnectionError,
                aiohttp.ClientError,
                OSError,
            ) as error:
                if attempt >= self.max_retries:
                    raise RuntimeError(
                        "Запрос FBS-остатков WB завершился ошибкой после всех повторов: "
                        f"account={account} wb_warehouse_id={wb_warehouse_id} "
                        f"error_type={type(error).__name__}"
                    ) from error
                await self._sleep_for_retry(
                    account=account,
                    wb_warehouse_id=wb_warehouse_id,
                    attempt=attempt,
                    error_type=type(error).__name__,
                )

        raise RuntimeError(
            "Запрос FBS-остатков WB завершился ошибкой после всех повторов: "
            f"account={account} wb_warehouse_id={wb_warehouse_id} "
            f"status={last_status} payload={self._payload_for_log(last_payload)}"
        )

    async def _read_payload(self, response: aiohttp.ClientResponse) -> dict | list | None:
        """Читает ответ WB по остаткам и сохраняет текст ошибки для диагностики."""
        content_type = response.headers.get("Content-Type", "")
        if "application/json" in content_type.lower():
            return await response.json()
        text_payload = await response.text()
        return {"text": text_payload} if text_payload else None

    def _parse_stocks_payload(self, payload: dict | list | None) -> dict[int, int]:
        """Нормализует разные формы ответа WB в маппинг `chrt_id -> quantity`."""
        if payload is None:
            return {}
        if isinstance(payload, list):
            items = payload
        elif isinstance(payload, dict):
            raw_items = payload.get("stocks") or payload.get("data") or payload.get("items") or []
            items = raw_items if isinstance(raw_items, list) else []
        else:
            return {}

        stocks_by_chrt_id: dict[int, int] = {}
        for item in items:
            if not isinstance(item, dict):
                continue
            chrt_id = self._extract_int(item, ("chrtId", "chrtID", "chrt_id"))
            quantity = self._extract_int(item, ("amount", "quantity", "qty", "stock", "stocks"))
            if chrt_id is None:
                continue
            stocks_by_chrt_id[chrt_id] = quantity or 0
        return stocks_by_chrt_id

    async def _sleep_for_retry(
        self,
        account: str,
        wb_warehouse_id: int,
        attempt: int,
        status: int | None = None,
        error_type: str | None = None,
        payload: dict | list | None = None,
    ) -> None:
        """Выдерживает паузу между retry, чтобы не сорвать чтение остатков лимитами WB."""
        sleep_seconds = self._calculate_retry_sleep_seconds(attempt=attempt, status=status)
        logger.warning(
            "Повторяем запрос FBS-остатков WB | account=%s | wb_warehouse_id=%s | attempt=%s/%s | status=%s | error_type=%s | payload=%s | sleep_seconds=%s",
            account,
            wb_warehouse_id,
            attempt,
            self.max_retries,
            status,
            error_type,
            self._payload_for_log(payload),
            sleep_seconds,
        )
        await asyncio.sleep(sleep_seconds)

    def _raise_for_status_safely(
        self,
        response: aiohttp.ClientResponse,
        payload: dict | list | None,
        account: str,
        wb_warehouse_id: int,
        action: str,
    ) -> None:
        """Поднимает HTTP-ошибку WB без вывода токенов и служебных headers.

        Бизнес-правило: диагностика по остаткам должна показывать статус и тело ответа WB,
        но не должна раскрывать токены, cookies и другие секреты из HTTP-запроса.
        """
        if response.status < 400:
            return
        raise RuntimeError(
            f"WB вернул ошибку при {action}: "
            f"account={account} wb_warehouse_id={wb_warehouse_id} "
            f"status={response.status} payload={self._payload_for_log(payload)}"
        )

    def _payload_for_log(self, payload: dict | list | None) -> object:
        """Готовит тело ответа WB для безопасного лога без секретных полей.

        Бизнес-сценарий: при ошибках WB нужно видеть текст ответа, но любые поля с токенами,
        cookies, паролями и похожими секретами должны быть скрыты до записи в лог или терминал.
        """
        if payload is None:
            return None
        if isinstance(payload, list):
            return [self._payload_for_log(item) for item in payload[:5]]
        if not isinstance(payload, dict):
            return payload

        hidden_keys = ("authorization", "cookie", "token", "secret", "password", "key")
        safe_payload: dict[str, object] = {}
        for key, value in payload.items():
            key_text = str(key)
            if any(hidden_key in key_text.lower() for hidden_key in hidden_keys):
                safe_payload[key_text] = "***"
            elif isinstance(value, dict | list):
                safe_payload[key_text] = self._payload_for_log(value)
            else:
                safe_payload[key_text] = value
        return safe_payload

    def _extract_cargo_restricted_chrt_ids(self, payload: dict | list | None) -> set[int]:
        """Извлекает chrtId, которые WB запретил грузить на склад из-за типа товара.

        Бизнес-правило: код `CargoWarehouseRestrictionMGT` не является временной ошибкой, поэтому
        по таким товарам нельзя делать retry на том же складе. Их нужно исключить из текущей
        отправки и продолжить остальные складские обновления.
        """
        payload_items = payload if isinstance(payload, list) else [payload]
        restricted_chrt_ids: set[int] = set()
        for item in payload_items:
            if not isinstance(item, dict):
                continue
            if item.get("code") != "CargoWarehouseRestrictionMGT":
                continue
            data_items = item.get("data")
            if not isinstance(data_items, list):
                continue
            for data_item in data_items:
                if not isinstance(data_item, dict):
                    continue
                chrt_id = self._extract_int(data_item, ("chrtId", "chrtID", "chrt_id"))
                if chrt_id is not None:
                    restricted_chrt_ids.add(chrt_id)
        return restricted_chrt_ids

    def _extract_not_found_chrt_ids(self, payload: dict | list | None) -> set[int]:
        """Извлекает chrtId, которые WB больше не видит в FBS-контуре товара.

        Бизнес-правило: код `NotFound` означает, что артикул уже не должен участвовать в FBS-
        сценариях, например товар удален или убран в корзину через сайт WB. Повторять такие
        запросы на том же складе бессмысленно, поэтому chrtId исключаются сразу.
        """
        payload_items = payload if isinstance(payload, list) else [payload]
        not_found_chrt_ids: set[int] = set()
        for item in payload_items:
            if not isinstance(item, dict):
                continue
            if item.get("code") != "NotFound":
                continue
            data_items = item.get("data")
            if not isinstance(data_items, list):
                continue
            for data_item in data_items:
                if not isinstance(data_item, dict):
                    continue
                chrt_id = self._extract_int(data_item, ("chrtId", "chrtID", "chrt_id"))
                if chrt_id is not None:
                    not_found_chrt_ids.add(chrt_id)
        return not_found_chrt_ids

    def _calculate_retry_sleep_seconds(self, attempt: int, status: int | None) -> int:
        """Рассчитывает backoff для защиты чтения остатков от 429 и временных ошибок WB."""
        backoff_steps = (5, 15, 30, 60, 120)
        index = min(max(attempt - 1, 0), len(backoff_steps) - 1)
        sleep_seconds = min(backoff_steps[index], self.retry_max_sleep_seconds)
        if status == 429:
            sleep_seconds = max(sleep_seconds, 60)
        if status == 409:
            sleep_seconds = max(sleep_seconds, 30)
        return max(sleep_seconds, self.retry_base_sleep_seconds)

    def _extract_int(
        self,
        payload: dict[str, object],
        field_names: tuple[str, ...],
    ) -> int | None:
        """Достает числовое поле WB по нескольким именам, чтобы пережить различия casing в API."""
        for field_name in field_names:
            value = payload.get(field_name)
            if isinstance(value, int):
                return value
            if isinstance(value, str) and value.strip().isdigit():
                return int(value)
        return None
