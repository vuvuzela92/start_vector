from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import aiohttp

from src_oop.core.scraper import HTTPClient

logger = logging.getLogger(__name__)


class WMSStockService:
    """Работает с WMS API для старой и новой выгрузки складских остатков.

    Бизнес-сценарий:
    модуль обслуживает два независимых контура. Старый контур исторических
    остатков должен продолжать работать без изменения поведения, а новый
    контур дневных остатков использует отдельный метод API и отдельные правила
    retry для витрины `public.wms_stock`.
    """

    def __init__(self):
        self

    async def get_historical_stocks(
    self,
    session: aiohttp.ClientSession,
    date_from: str = None,
    date_to: str = None,
    warehouse_id: int = 1,
    page_size: int = 5000,
    api_key: Optional[str] = None,  # Если API требуется ключ
    ) -> List[Dict]:
        """Получает старые исторические остатки с пагинацией без изменения legacy-поведения.

        Бизнес-сценарий:
        этот метод обслуживает прежнюю выгрузку в старую таблицу, поэтому его
        контракт и логика выхода должны оставаться совместимыми с текущим
        прод-сценарием.
        """
        
        # 1. Обработка дат
        if date_from is None:
            date_from = (datetime.now() - timedelta(days=28)).strftime("%Y-%m-%d")
        if date_to is None:
            date_to = datetime.now().strftime("%Y-%m-%d")

        # 2. URL
        url = "https://api-routing.star-vector.ru/api/warehouse_and_balances/get_historical_stocks"
        
        # 3. Создаем клиент
        client = HTTPClient(
            session=session,
            api_key=api_key,
            timeout=30.0 
        )
        
        all_res = []
        page_num = 1
        
        while True:
            payload = {
                "date_from": date_from,
                "date_to": date_to,
                "warehouse_id": warehouse_id,
                "page_size": page_size,
                "page_num": page_num
            }
            
            # 4. Асинхронный POST-запрос
            data = await client.post(url, json=payload, delay=1.0, retries=3)
            
            logger.debug(f"📄 Страница {page_num}: отправлен запрос")
            
            if data is None:
                logger.error(f"❌ Страница {page_num}: не получены данные")
                break
            
            # 5. Обработка ответа
            if isinstance(data, list):
                all_res.extend(data)
                logger.info(f"✅ Страница {page_num}: получено {len(data)} записей")
                print(f"✅ Страница {page_num}: получено {len(data)} записей")
                
                # 🔴 УСЛОВИЕ ВЫХОДА: если данных меньше размера страницы
                if len(data) == 0:
                    break
            else:
                logger.warning(f"⚠️ Неожиданный формат ответа: {type(data)}")
                break
            
            # 6. Следующая страница
            page_num += 1
            
            # 7. Защита от бесконечного цикла
            if page_num > 100:
                logger.warning("⚠️ Достигнут лимит страниц (100)")
                break
        
        logger.info(f"🏁 Всего собрано {len(all_res)} записей")
        return all_res

    async def fetch_external_stocks(self, date_from: str=None, date_to: str=None):
        """Запускает legacy-сценарий получения исторических WMS-остатков.

        Бизнес-сценарий:
        метод нужен как тонкая обертка для существующей джобы, чтобы старая
        выгрузка продолжала работать через тот же публичный интерфейс класса.
        """
        async with aiohttp.ClientSession() as session:
            return await self.get_historical_stocks(
                session=session,
                date_from=date_from,
                date_to=date_to
        )

    async def fetch_daily_balances(
        self,
        date_from: str,
        date_to: str,
        limit: int = 500,
        location_id: int | None = None,
        include_subtree: bool = False,
    ) -> list[dict[str, Any]]:
        """Получает дневные остатки WMS по новому методу `daily-balances`.

        Бизнес-сценарий:
        новая витрина `public.wms_stock` несколько раз в день перечитывает окно
        дат или делает исторический backfill. Пагинация в этом endpoint идет по
        товарам, поэтому метод последовательно читает страницы через `limit` и
        `offset`, а для каждого товара получает весь диапазон дней сразу. При
        необходимости метод поддерживает отдельный срез по `location_id`, чтобы
        сохранить в БД как общий остаток, так и FBS-остаток по конкретной WMS-локации.
        """
        url = "https://api-wms.star-vector.ru/api/inventory-history/daily-balances"
        timeout = aiohttp.ClientTimeout(total=60)
        all_items: list[dict[str, Any]] = []
        offset = 0

        self._validate_daily_balances_period(date_from=date_from, date_to=date_to)

        async with aiohttp.ClientSession(timeout=timeout) as session:
            while True:
                page_items, total_products = await self._request_daily_balances_page(
                    session=session,
                    url=url,
                    date_from=date_from,
                    date_to=date_to,
                    offset=offset,
                    limit=limit,
                    location_id=location_id,
                    include_subtree=include_subtree,
                )
                if not page_items:
                    break

                all_items.extend(page_items)
                logger.info(
                    "Получена страница дневной истории WMS-остатков | offset=%s | limit=%s | page_products=%s | total_products=%s | date_from=%s | date_to=%s | location_id=%s",
                    offset,
                    limit,
                    len(page_items),
                    total_products,
                    date_from,
                    date_to,
                    location_id,
                )

                offset += len(page_items)
                if len(page_items) < limit:
                    break
                if total_products is not None and offset >= total_products:
                    break

        logger.info(
            "Сбор дневной истории WMS-остатков завершен | products=%s | date_from=%s | date_to=%s",
            len(all_items),
            date_from,
            date_to,
        )
        return all_items

    async def _request_daily_balances_page(
        self,
        session: aiohttp.ClientSession,
        url: str,
        date_from: str,
        date_to: str,
        offset: int,
        limit: int,
        location_id: int | None,
        include_subtree: bool,
        retries: int = 4,
    ) -> tuple[list[dict[str, Any]], int | None]:
        """Запрашивает одну страницу `daily-balances` с retry и пагинацией по товарам.

        Бизнес-сценарий:
        endpoint пагинирует не по дням, а по товарам. Для каждого товара он
        возвращает весь диапазон `days[]`, поэтому корректный сбор истории
        требует последовательного обхода страниц через `offset`.
        """
        params = {
            "date_from": date_from,
            "date_to": date_to,
            "limit": limit,
            "offset": offset,
        }
        if location_id is not None:
            params["location_id"] = location_id
            params["include_subtree"] = "true" if include_subtree else "false"
        retry_delays = (2, 5, 10, 20)

        for attempt in range(1, retries + 1):
            try:
                async with session.get(url, params=params) as response:
                    if response.status == 429:
                        wait_seconds = retry_delays[min(attempt - 1, len(retry_delays) - 1)]
                        logger.warning(
                            "WMS API временно ограничил запрос дневной истории остатков, повторяем попытку | offset=%s | limit=%s | attempt=%s/%s | wait_seconds=%s",
                            offset,
                            limit,
                            attempt,
                            retries,
                            wait_seconds,
                        )
                        await asyncio.sleep(wait_seconds)
                        continue

                    if response.status >= 500:
                        wait_seconds = retry_delays[min(attempt - 1, len(retry_delays) - 1)]
                        logger.warning(
                            "WMS API вернул серверную ошибку при запросе дневной истории остатков, повторяем попытку | offset=%s | limit=%s | status=%s | attempt=%s/%s | wait_seconds=%s",
                            offset,
                            limit,
                            response.status,
                            attempt,
                            retries,
                            wait_seconds,
                        )
                        await asyncio.sleep(wait_seconds)
                        continue

                    if response.status >= 400:
                        response_text = await response.text()
                        logger.error(
                            "WMS API отклонил запрос дневной истории остатков | offset=%s | limit=%s | location_id=%s | status=%s | response_preview=%s",
                            offset,
                            limit,
                            location_id,
                            response.status,
                            response_text[:500],
                        )
                        return [], None

                    payload_json = await response.json()
                    return self._extract_daily_balance_items(payload_json)
            except aiohttp.ClientError as error:
                wait_seconds = retry_delays[min(attempt - 1, len(retry_delays) - 1)]
                logger.warning(
                    "Сетевая ошибка при запросе дневной истории остатков, повторяем попытку | offset=%s | limit=%s | attempt=%s/%s | wait_seconds=%s | error_type=%s",
                    offset,
                    limit,
                    attempt,
                    retries,
                    wait_seconds,
                    type(error).__name__,
                )
                if attempt == retries:
                    return [], None
                await asyncio.sleep(wait_seconds)
            except asyncio.TimeoutError:
                wait_seconds = retry_delays[min(attempt - 1, len(retry_delays) - 1)]
                logger.warning(
                    "Таймаут при запросе дневной истории остатков, повторяем попытку | offset=%s | limit=%s | attempt=%s/%s | wait_seconds=%s",
                    offset,
                    limit,
                    attempt,
                    retries,
                    wait_seconds,
                )
                if attempt == retries:
                    return [], None
                await asyncio.sleep(wait_seconds)

        return [], None

    def _extract_daily_balance_items(
        self,
        payload: Any,
    ) -> tuple[list[dict[str, Any]], int | None]:
        """Извлекает список товаров и `total_products` из ответа `daily-balances`.

        Бизнес-сценарий:
        endpoint возвращает полный диапазон дней внутри `items[].days[]` для
        каждого товара, а пагинация строится по товарам. Поэтому для корректной
        выгрузки нужно достать именно список `items` и при возможности общее
        число товаров `total_products`.
        """
        if not isinstance(payload, dict):
            logger.warning(
                "Получен неожиданный формат ответа дневной истории остатков | payload_type=%s",
                type(payload).__name__,
            )
            return [], None

        items = payload.get("items")
        if isinstance(items, list):
            normalized_items = [row for row in items if isinstance(row, dict)]
            total_products = payload.get("total_products")
            if total_products is not None:
                try:
                    total_products = int(total_products)
                except (TypeError, ValueError):
                    total_products = None
            return normalized_items, total_products

        logger.warning(
            "В ответе дневной истории остатков не найден массив items | available_keys=%s",
            sorted(payload.keys()),
        )
        return [], None

    def _validate_daily_balances_period(self, date_from: str, date_to: str) -> None:
        """Проверяет период выгрузки по правилам документации `daily-balances`.

        Бизнес-сценарий:
        endpoint принимает даты включительно в Europe/Moscow и ограничивает
        период 366 календарными днями. Локальная проверка дает понятную ошибку
        раньше HTTP-запроса и защищает регулярный run и backfill от заведомо
        невалидного диапазона.
        """
        start_date = datetime.strptime(date_from, "%Y-%m-%d").date()
        end_date = datetime.strptime(date_to, "%Y-%m-%d").date()
        if start_date > end_date:
            raise ValueError("Дата начала выгрузки дневных остатков не может быть позже даты конца.")
        if (end_date - start_date).days > 365:
            raise ValueError(
                "Период выгрузки дневных остатков не может превышать 366 календарных дней."
            )
