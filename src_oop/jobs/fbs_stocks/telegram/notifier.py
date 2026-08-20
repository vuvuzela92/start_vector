from __future__ import annotations

import logging
import os
from collections import defaultdict
from datetime import datetime

from src_oop.jobs.fbs_stocks.telegram.dedup_cache import TelegramNotificationDedupCache
from src_oop.jobs.fbs_stocks.telegram.formatters import (
    format_article_failure_message,
    format_dry_run_message,
    format_full_failure_message,
    format_partial_failure_message,
    format_wild_summary_message,
)
from src_oop.jobs.fbs_stocks.telegram.models import FBSJobFailureContext, FBSNotificationEvent
from src_oop.jobs.fbs_stocks.telegram.telegram_client import FBSStocksTelegramClient
from src_oop.jobs.fbs_stocks.telegram.tg_config import (
    TELEGRAM_BOT_TOKEN_ENV,
    TELEGRAM_CHAT_IDS,
    TELEGRAM_DEDUP_CACHE_PATH,
    TELEGRAM_DEDUP_MINUTES,
    TELEGRAM_MAX_ARTICLE_EXAMPLES,
    TELEGRAM_NOTIFICATIONS_ENABLED,
    TELEGRAM_REQUEST_TIMEOUT_SECONDS,
    TELEGRAM_WILD_SUMMARY_THRESHOLD,
)

logger = logging.getLogger(__name__)


class FBSStocksTelegramNotifier:
    """Отправляет агрегированные Telegram-уведомления по FBS-сценариям.

    Бизнес-сценарий: уведомления нужны для dry-run, частичных сбоев и полных падений, но не должны
    превращаться в поток дублирующихся сообщений от cron. Нотификатор объединяет события, применяет
    дедупликацию и безопасно изолирует Telegram от основной логики управления остатками.
    """

    def __init__(
        self,
        telegram_client: FBSStocksTelegramClient | None = None,
        dedup_cache: TelegramNotificationDedupCache | None = None,
    ) -> None:
        """Собирает зависимости Telegram-уведомлений FBS-сценариев."""
        self.telegram_client = telegram_client or FBSStocksTelegramClient(
            request_timeout_seconds=TELEGRAM_REQUEST_TIMEOUT_SECONDS
        )
        self.dedup_cache = dedup_cache or TelegramNotificationDedupCache(
            cache_path=TELEGRAM_DEDUP_CACHE_PATH,
            dedup_minutes=TELEGRAM_DEDUP_MINUTES,
        )

    async def notify_dry_run(
        self,
        job_name: str,
        account_scope: str,
        checked_rows: int,
        prepared_rows: int,
        skipped_rows: int,
    ) -> None:
        """Отправляет Telegram-уведомление о dry-run запуске FBS-сценария.

        Бизнес-правило: если продакт-режим отключен, оператор должен узнать об этом сразу, иначе
        можно ошибочно ожидать изменения остатков на WB после формально успешного запуска.
        """
        now = datetime.now()
        text = format_dry_run_message(
            job_name=job_name,
            account_scope=account_scope,
            checked_rows=checked_rows,
            prepared_rows=prepared_rows,
            skipped_rows=skipped_rows,
            happened_at=now,
        )
        await self._send_if_allowed(
            dedup_key=f"dry_run:{job_name}:{account_scope}",
            text=text,
            now=now,
        )

    async def notify_full_failure(self, context: FBSJobFailureContext) -> None:
        """Отправляет Telegram-уведомление о полном падении FBS-сценария."""
        now = context.happened_at or datetime.now()
        text = format_full_failure_message(context)
        await self._send_if_allowed(
            dedup_key=f"full_failure:{context.job_name}:{context.account_scope}:{context.error_type}:{context.reason}",
            text=text,
            now=now,
        )

    async def notify_events(
        self,
        job_name: str,
        account_scope: str,
        events: list[FBSNotificationEvent],
    ) -> None:
        """Отправляет Telegram-уведомления по собранным событиям FBS-сценария.

        Бизнес-сценарий: сначала уходит общая сводка частичных проблем, затем более детальные
        сообщения по `wild` и отдельным артикулам, которые не вошли в агрегированные группы.
        """
        if not events:
            return
        now = datetime.now()
        summary_text = format_partial_failure_message(
            job_name=job_name,
            account_scope=account_scope,
            events=events,
            happened_at=now,
        )
        await self._send_if_allowed(
            dedup_key=f"partial_failure:{job_name}:{account_scope}:{self._build_reason_fingerprint(events)}",
            text=summary_text,
            now=now,
        )

        grouped_by_wild: dict[tuple[str, str], list[FBSNotificationEvent]] = defaultdict(list)
        article_events: list[FBSNotificationEvent] = []
        for event in events:
            if event.account and event.wild:
                grouped_by_wild[(event.account, event.wild)].append(event)
            else:
                article_events.append(event)

        swallowed_article_keys: set[tuple[str | None, int | None, str]] = set()
        for (account, wild), wild_events in grouped_by_wild.items():
            if len(wild_events) < TELEGRAM_WILD_SUMMARY_THRESHOLD:
                article_events.extend(wild_events)
                continue
            text = format_wild_summary_message(
                job_name=job_name,
                account=account,
                wild=wild,
                events=wild_events,
                happened_at=now,
                max_article_examples=TELEGRAM_MAX_ARTICLE_EXAMPLES,
            )
            await self._send_if_allowed(
                dedup_key=f"wild:{job_name}:{account}:{wild}:{self._build_reason_fingerprint(wild_events)}",
                text=text,
                now=now,
            )
            swallowed_article_keys.update(
                (event.account, event.article_id, event.reason_code)
                for event in wild_events
            )

        for event in article_events:
            article_key = (event.account, event.article_id, event.reason_code)
            if article_key in swallowed_article_keys:
                continue
            text = format_article_failure_message(event=event, happened_at=now)
            await self._send_if_allowed(
                dedup_key=(
                    f"article:{event.job_name}:{event.account}:{event.article_id}:"
                    f"{event.wb_warehouse_id}:{event.reason_code}"
                ),
                text=text,
                now=now,
            )

    async def _send_if_allowed(self, dedup_key: str, text: str, now: datetime) -> None:
        """Проверяет конфиг и дедупликацию перед реальной отправкой в Telegram.

        Бизнес-правило: отсутствие chat id или токена не должно ломать FBS-задачу. В таком случае
        уведомление просто логируется как пропущенное.
        """
        if not TELEGRAM_NOTIFICATIONS_ENABLED:
            return
        bot_token = os.getenv(TELEGRAM_BOT_TOKEN_ENV, "").strip()
        if not bot_token:
            logger.warning(
                "Telegram-уведомление FBS пропущено: не задан bot token | dedup_key=%s",
                dedup_key,
            )
            return
        if not TELEGRAM_CHAT_IDS:
            logger.warning(
                "Telegram-уведомление FBS пропущено: в tg_config.py не заполнены chat id | dedup_key=%s",
                dedup_key,
            )
            return
        if not self.dedup_cache.should_send(dedup_key, now):
            logger.info(
                "Telegram-уведомление FBS подавлено дедупликацией | dedup_key=%s",
                dedup_key,
            )
            return

        for chat_id in self._iter_chat_ids():
            try:
                await self.telegram_client.send_message(
                    bot_token=bot_token,
                    chat_id=chat_id,
                    text=text,
                )
            except Exception as error:
                logger.error(
                    "Не удалось отправить Telegram-уведомление FBS | error_type=%s | chat_id=%s",
                    type(error).__name__,
                    chat_id,
                )

    @staticmethod
    def _build_reason_fingerprint(events: list[FBSNotificationEvent]) -> str:
        """Собирает краткий отпечаток причин, чтобы дедупликация отличала разные сбои."""
        reasons = sorted(
            {
                f"{event.reason_code}:{event.reason}"
                for event in events
            }
        )
        return "|".join(reasons)

    @staticmethod
    def _iter_chat_ids() -> tuple[str, ...]:
        """Нормализует chat id из конфига, даже если пользователь задал один id строкой.

        Бизнес-сценарий: оператор может случайно записать `TELEGRAM_CHAT_IDS=("123")` без запятой,
        и Python превратит это в обычную строку. Нотификатор должен воспринять такой случай как
        один chat id, а не итерироваться по отдельным цифрам.
        """
        if isinstance(TELEGRAM_CHAT_IDS, str):
            prepared_chat_id = TELEGRAM_CHAT_IDS.strip()
            return (prepared_chat_id,) if prepared_chat_id else ()
        return tuple(
            str(chat_id).strip()
            for chat_id in TELEGRAM_CHAT_IDS
            if str(chat_id).strip()
        )
