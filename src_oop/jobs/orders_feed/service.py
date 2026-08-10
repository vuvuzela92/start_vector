"""Оркестрация полной и ручной загрузки WB Order Feed."""

from __future__ import annotations

import asyncio
import json
import logging
import os
from collections.abc import Callable, Mapping
from datetime import UTC, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import aiohttp
from dotenv import load_dotenv

from src_oop.jobs.orders_feed.client import WBOrderFeedClient
from src_oop.jobs.orders_feed.config import (
    HISTORY_BOUNDARY_SAFETY_MINUTES,
    MAX_CONCURRENT_ACCOUNTS,
    MAX_PERIOD_DAYS,
    REQUEST_INTERVAL_SECONDS,
)
from src_oop.jobs.orders_feed.models import OrderFeedPeriod, OrderFeedRunSummary
from src_oop.jobs.orders_feed.normalizer import OrderFeedNormalizer
from src_oop.jobs.orders_feed.repository import OrderFeedRepository

logger = logging.getLogger(__name__)
MOSCOW_TZ = ZoneInfo("Europe/Moscow")


def _load_api_tokens() -> Mapping[str, str]:
    """Загружает WB-токены внутри Order Feed, не завися от legacy-настроек других jobs."""
    load_dotenv()
    creds_dir = os.getenv("CREDS_DIR")
    tokens_file = os.getenv("CREDS_FILE") or os.getenv("TOKENS_FILE")
    if not creds_dir or not tokens_file:
        raise ValueError(
            "Для Order Feed должны быть заданы CREDS_DIR и CREDS_FILE "
            "(или TOKENS_FILE)."
        )

    project_root = Path(__file__).resolve().parents[3]
    tokens_path = project_root / creds_dir / tokens_file
    if not tokens_path.is_file():
        raise FileNotFoundError(
            f"Файл токенов WB для Order Feed не найден: {tokens_path}"
        )

    with tokens_path.open(encoding="utf-8") as file:
        payload = json.load(file)
    if not isinstance(payload, dict):
        raise TypeError("Файл токенов WB должен содержать объект account -> token.")
    return payload


class OrderFeedService:
    """Загружает все страницы доступного периода по каждому кабинету и сразу сохраняет их."""

    def __init__(
        self,
        client: WBOrderFeedClient | None = None,
        normalizer: OrderFeedNormalizer | None = None,
        repository: OrderFeedRepository | None = None,
        tokens_loader: Callable[[], Mapping[str, str]] | None = None,
        request_interval_seconds: float = REQUEST_INTERVAL_SECONDS,
    ) -> None:
        """Собирает заменяемые зависимости для рабочего запуска и изолированных тестов."""
        self.client = client or WBOrderFeedClient()
        self.normalizer = normalizer or OrderFeedNormalizer()
        self.repository = repository or OrderFeedRepository()
        self.tokens_loader = tokens_loader or _load_api_tokens
        self.request_interval_seconds = request_interval_seconds

    async def run(
        self,
        date_from: datetime | None = None,
        date_to: datetime | None = None,
        account: str | None = None,
    ) -> OrderFeedRunSummary:
        """Запускает обновление всех кабинетов за последние 31 сутки или ручной период.

        Каждый кабинет обрабатывается независимо, каждая страница сохраняется
        немедленно, а snapshotTime первой страницы используется до конца пагинации.
        """
        period = self._resolve_period(date_from, date_to)
        tokens = self._resolve_tokens(account)
        summary = OrderFeedRunSummary(accounts_total=len(tokens))
        semaphore = asyncio.Semaphore(MAX_CONCURRENT_ACCOUNTS)
        logger.info(
            "Старт загрузки Order Feed | start=%s | end=%s | accounts=%s",
            period.start.isoformat(),
            period.end.isoformat(),
            len(tokens),
        )
        async with aiohttp.ClientSession() as session:
            tasks = [
                self._process_account(semaphore, session, name, token, period, summary)
                for name, token in tokens.items()
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)
        for account_name, result in zip(tokens, results, strict=True):
            if isinstance(result, Exception):
                summary.failed_accounts.append(account_name)
                logger.error(
                    "Загрузка Order Feed кабинета прервана, остальные кабинеты продолжают работу | account=%s | error=%s",
                    account_name,
                    result,
                )
        summary.finished_at = datetime.now(tz=UTC)
        logger.info(
            "Загрузка Order Feed завершена | pages=%s | raw_rows=%s | written_rows=%s | failed_accounts=%s",
            summary.pages_received,
            summary.raw_rows,
            summary.written_rows,
            summary.failed_accounts,
        )
        return summary

    async def _process_account(
        self,
        semaphore: asyncio.Semaphore,
        session: aiohttp.ClientSession,
        account: str,
        token: str,
        period: OrderFeedPeriod,
        summary: OrderFeedRunSummary,
    ) -> None:
        """Проходит offset-пагинацию кабинета и сохраняет каждую страницу отдельным батчем."""
        async with semaphore:
            offset = 0
            snapshot_time: str | None = None
            while True:
                page = await self.client.fetch_page(
                    session=session,
                    account=account,
                    token=token,
                    period=period,
                    offset=offset,
                    snapshot_time=snapshot_time,
                )
                summary.pages_received += 1
                summary.total_retry_count += page.retries_used
                summary.raw_rows += len(page.orders)
                snapshot_time = page.snapshot_time
                normalized = self.normalizer.normalize(page)
                summary.normalized_rows += len(normalized)
                saved = self.repository.save(normalized)
                summary.written_rows += saved.written_rows
                summary.dropped_missing_key_rows += saved.dropped_missing_key_rows
                summary.collapsed_duplicate_rows += saved.collapsed_duplicate_rows
                if not page.has_next_page:
                    break
                offset += page.limit
                logger.info(
                    "Ожидание лимита WB перед следующей страницей | account=%s | next_offset=%s | seconds=%s",
                    account,
                    offset,
                    self.request_interval_seconds,
                )
                await asyncio.sleep(self.request_interval_seconds)

    def _resolve_period(
        self,
        date_from: datetime | None,
        date_to: datetime | None,
    ) -> OrderFeedPeriod:
        """Ограничивает ручной период доступными WB последними 31 сутками."""
        now = datetime.now(tz=MOSCOW_TZ)
        end = self._ensure_timezone(date_to or now)
        earliest = now - timedelta(days=MAX_PERIOD_DAYS) + timedelta(
            minutes=HISTORY_BOUNDARY_SAFETY_MINUTES
        )
        start = self._ensure_timezone(date_from or earliest)
        if start < earliest:
            raise ValueError(
                "Начало Order Feed выходит за доступные последние "
                f"{MAX_PERIOD_DAYS} суток с техническим запасом "
                f"{HISTORY_BOUNDARY_SAFETY_MINUTES} минут."
            )
        if end > now + timedelta(minutes=1):
            raise ValueError("Конец периода Order Feed не может быть в будущем.")
        if start > end:
            raise ValueError("Начало периода Order Feed не может быть позже конца.")
        if end - start > timedelta(days=MAX_PERIOD_DAYS):
            raise ValueError(f"Период Order Feed не может превышать {MAX_PERIOD_DAYS} сутки.")
        return OrderFeedPeriod(start=start, end=end)

    def _ensure_timezone(self, value: datetime) -> datetime:
        """Назначает московскую зону наивным ручным датам для однозначного запроса WB."""
        if value.tzinfo is None:
            return value.replace(tzinfo=MOSCOW_TZ)
        return value.astimezone(MOSCOW_TZ)

    def _resolve_tokens(self, account: str | None) -> dict[str, str]:
        """Загружает валидные токены и применяет необязательный фильтр кабинета."""
        loaded = self.tokens_loader()
        if not isinstance(loaded, Mapping):
            raise TypeError("Загрузчик токенов должен вернуть Mapping account -> token.")
        tokens = {
            str(name).strip(): str(token).strip()
            for name, token in loaded.items()
            if str(name).strip() and str(token).strip()
        }
        if not tokens:
            raise ValueError("Не найдены токены кабинетов WB для Order Feed.")
        if account is None:
            return tokens
        selected = account.strip().strip('"').strip("'")
        if ":" in selected or (len(selected) > 100 and selected.count(".") >= 2):
            raise ValueError(
                "В account переданы credentials. Укажите только название кабинета "
                "без кавычек и токена."
            )
        if selected not in tokens:
            raise ValueError(f"Аккаунт '{selected}' не найден в токенах WB.")
        return {selected: tokens[selected]}
