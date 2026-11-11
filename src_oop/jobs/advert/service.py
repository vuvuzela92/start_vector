from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Callable, Mapping
from dataclasses import asdict, dataclass, field
from datetime import date, datetime

import aiohttp
import pandas as pd

from src_oop.core.utils_general import load_api_tokens
from src_oop.jobs.advert.client import (
    CampaignIdsFetchResult,
    FailedAdvertBatch,
    WBAdvertStatsClient,
    chunk_date_range,
)
from src_oop.jobs.advert.config import (
    MAX_CONCURRENT_ACCOUNTS,
    MAX_CONCURRENT_DB_WRITES,
    MAX_FULLSTATS_RANGE_DAYS,
    PLATFORM_COLUMN_MAPPING,
)
from src_oop.jobs.advert.normalizer import AdvertStatsNormalizer
from src_oop.jobs.advert.repository import AdvertStatsRepository, AdvertStatsSaveResult

logger = logging.getLogger(__name__)

DAY_LEVEL_METRIC_KEYS: tuple[str, ...] = (
    "views",
    "clicks",
    "orders",
    "atbs",
    "canceled",
    "shks",
    "sum",
    "sum_price",
    "cpc",
    "cr",
    "ctr",
)


@dataclass(slots=True)
class AdvertStatsRunSummary:
    accounts_total: int = 0
    accounts_processed: int = 0
    succeeded_accounts: list[str] = field(default_factory=list)
    partial_accounts: list[str] = field(default_factory=list)
    failed_accounts: list[str] = field(default_factory=list)
    failed_batches: list[dict[str, object]] = field(default_factory=list)
    account_timings: dict[str, dict[str, float]] = field(default_factory=dict)
    campaign_count_total: int = 0
    raw_rows: int = 0
    normalized_rows: int = 0
    written_rows: int = 0
    planned_batches: int = 0
    http_attempts: int = 0
    responses_429: int = 0
    api_errors: int = 0
    database_errors: int = 0
    normalization_errors: int = 0
    dropped_missing_key_rows: int = 0
    collapsed_duplicate_rows: int = 0
    total_retry_count: int = 0
    started_at: datetime = field(default_factory=datetime.now)
    finished_at: datetime | None = None
    warnings: list[str] = field(default_factory=list)
    duration_seconds: float = 0.0


@dataclass(slots=True)
class AccountProcessingResult:
    account: str
    status: str
    campaign_count: int = 0
    raw_rows: int = 0
    normalized_rows: int = 0
    written_rows: int = 0
    dropped_missing_key_rows: int = 0
    collapsed_duplicate_rows: int = 0
    retry_count: int = 0
    failed_batches: list[FailedAdvertBatch] = field(default_factory=list)
    error_message: str | None = None
    planned_batches: int = 0
    http_attempts: int = 0
    responses_429: int = 0
    api_errors: int = 0
    database_errors: int = 0
    normalization_errors: int = 0
    campaign_http_seconds: float = 0.0
    queue_wait_seconds: float = 0.0
    http_seconds: float = 0.0
    normalization_seconds: float = 0.0
    database_seconds: float = 0.0
    duration_seconds: float = 0.0


class AdvertStatsService:
    """Оркестрирует получение, нормализацию и запись рекламной статистики WB."""

    def __init__(
        self,
        client: WBAdvertStatsClient | None = None,
        normalizer: AdvertStatsNormalizer | None = None,
        repository: AdvertStatsRepository | None = None,
        tokens_loader: Callable[[], Mapping[str, str]] | None = None,
    ) -> None:
        """Подготавливает зависимости сценария загрузки рекламы и его конкурентных ограничителей."""
        self.client = client or WBAdvertStatsClient()
        self.normalizer = normalizer or AdvertStatsNormalizer()
        self.repository = repository or AdvertStatsRepository()
        self.tokens_loader = tokens_loader or load_api_tokens
        self._database_semaphore = asyncio.Semaphore(MAX_CONCURRENT_DB_WRITES)

    async def run(
        self,
        date_from: date,
        date_to: date,
        account: str | None = None,
    ) -> AdvertStatsRunSummary:
        """Загружает рекламную статистику по аккаунтам, записывая каждый date chunk отдельно."""
        run_started = time.monotonic()
        tokens_by_account = self._resolve_tokens(account=account)
        summary = AdvertStatsRunSummary(accounts_total=len(tokens_by_account))
        semaphore = asyncio.Semaphore(MAX_CONCURRENT_ACCOUNTS)

        logger.info(
            "Старт AdvertStatsService.run | date_from=%s | date_to=%s | accounts_total=%s | account_filter=%s",
            date_from.isoformat(),
            date_to.isoformat(),
            summary.accounts_total,
            account,
        )

        async with aiohttp.ClientSession() as session:
            tasks = [
                self._process_account(
                    semaphore=semaphore,
                    session=session,
                    account_name=account_name,
                    token=token,
                    date_from=date_from,
                    date_to=date_to,
                )
                for account_name, token in tokens_by_account.items()
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)

        for (account_name, _), result in zip(tokens_by_account.items(), results, strict=True):
            if isinstance(result, Exception):
                logger.error(
                    "Ошибка обработки аккаунта advert_stat, остальные аккаунты обработаны | account=%s | тип=%s",
                    account_name,
                    type(result).__name__,
                )
                summary.failed_accounts.append(account_name)
                summary.accounts_processed += 1
                summary.warnings.append(f"Ошибка обработки аккаунта {account_name} ({type(result).__name__})")
                continue

            summary.accounts_processed += 1
            summary.campaign_count_total += result.campaign_count
            summary.raw_rows += result.raw_rows
            summary.normalized_rows += result.normalized_rows
            summary.written_rows += result.written_rows
            summary.dropped_missing_key_rows += result.dropped_missing_key_rows
            summary.collapsed_duplicate_rows += result.collapsed_duplicate_rows
            summary.total_retry_count += result.retry_count
            summary.planned_batches += result.planned_batches
            summary.http_attempts += result.http_attempts
            summary.responses_429 += result.responses_429
            summary.api_errors += result.api_errors
            summary.database_errors += result.database_errors
            summary.normalization_errors += result.normalization_errors
            summary.account_timings[result.account] = {
                "queue_wait_seconds": result.queue_wait_seconds,
                "http_seconds": result.http_seconds + result.campaign_http_seconds,
                "normalization_seconds": result.normalization_seconds,
                "database_seconds": result.database_seconds,
                "duration_seconds": result.duration_seconds,
            }
            summary.failed_batches.extend(
                [asdict(failed_batch) for failed_batch in result.failed_batches]
            )

            if result.status == "success":
                summary.succeeded_accounts.append(result.account)
            elif result.status == "partial":
                summary.partial_accounts.append(result.account)
                summary.failed_accounts.append(result.account)
            else:
                summary.failed_accounts.append(result.account)
                if result.error_message:
                    summary.warnings.append(result.error_message)

        summary.finished_at = datetime.now()
        summary.duration_seconds = time.monotonic() - run_started
        logger.info(
            "Завершён AdvertStatsService.run | accounts_processed=%s | succeeded_accounts=%s | partial_accounts=%s | failed_accounts=%s | raw_rows=%s | normalized_rows=%s | written_rows=%s | dropped_missing_key_rows=%s | collapsed_duplicate_rows=%s | planned_batches=%s | http_attempts=%s | responses_429=%s | api_errors=%s | normalization_errors=%s | database_errors=%s | total_retry_count=%s | queue_wait_seconds=%.2f | http_seconds=%.2f | normalization_seconds=%.2f | database_seconds=%.2f | duration_seconds=%.2f | started_at=%s | finished_at=%s",
            summary.accounts_processed,
            summary.succeeded_accounts,
            summary.partial_accounts,
            summary.failed_accounts,
            summary.raw_rows,
            summary.normalized_rows,
            summary.written_rows,
            summary.dropped_missing_key_rows,
            summary.collapsed_duplicate_rows,
            len(summary.failed_batches),
            summary.planned_batches,
            summary.http_attempts,
            summary.responses_429,
            summary.api_errors,
            summary.normalization_errors,
            summary.database_errors,
            summary.total_retry_count,
            sum(item["queue_wait_seconds"] for item in summary.account_timings.values()),
            sum(item["http_seconds"] for item in summary.account_timings.values()),
            sum(item["normalization_seconds"] for item in summary.account_timings.values()),
            sum(item["database_seconds"] for item in summary.account_timings.values()),
            summary.duration_seconds,
            summary.started_at.isoformat(timespec="seconds"),
            summary.finished_at.isoformat(timespec="seconds")
            if summary.finished_at is not None
            else None,
        )
        return summary

    async def _process_account(
        self,
        semaphore: asyncio.Semaphore,
        session: aiohttp.ClientSession,
        account_name: str,
        token: str,
        date_from: date,
        date_to: date,
    ) -> AccountProcessingResult:
        """Обрабатывает аккаунт целиком, сохраняя успешные chunks даже при частичных ошибках."""
        account_started = time.monotonic()
        queue_started = time.monotonic()
        async with semaphore:
            queue_wait_seconds = time.monotonic() - queue_started
            logger.info(
                "Старт обработки advert_stat аккаунта | account=%s | date_from=%s | date_to=%s",
                account_name,
                date_from.isoformat(),
                date_to.isoformat(),
            )
            try:
                campaign_http_started = time.monotonic()
                campaign_result = await self.client.fetch_campaign_ids(
                    session=session,
                    account=account_name,
                    token=token,
                )
                campaign_http_seconds = time.monotonic() - campaign_http_started
            except Exception as error:
                logger.error(
                    "Не удалось получить список кампаний WB; аккаунт пропущен | account=%s | тип=%s",
                    account_name,
                    type(error).__name__,
                )
                return AccountProcessingResult(
                    account=account_name,
                    status="failed",
                    error_message=f"Ошибка списка кампаний аккаунта {account_name} ({type(error).__name__})",
                    http_attempts=1,
                    api_errors=1,
                    queue_wait_seconds=queue_wait_seconds,
                    duration_seconds=time.monotonic() - account_started,
                )

            if not isinstance(campaign_result, CampaignIdsFetchResult):
                raise TypeError("campaign_result должен быть экземпляром CampaignIdsFetchResult.")

            account_result = AccountProcessingResult(
                account=account_name,
                status="success",
                campaign_count=len(campaign_result.campaign_ids),
                retry_count=campaign_result.retries_used,
                http_attempts=campaign_result.attempts_made,
                responses_429=campaign_result.responses_429,
                queue_wait_seconds=queue_wait_seconds,
                campaign_http_seconds=campaign_http_seconds,
            )

            date_chunks = chunk_date_range(
                date_from=date_from,
                date_to=date_to,
                max_days=MAX_FULLSTATS_RANGE_DAYS,
            )
            for chunk_date_from, chunk_date_to in date_chunks:
                http_started = time.monotonic()
                fetch_result = await self.client.fetch_fullstats_chunk(
                    session=session,
                    account=account_name,
                    token=token,
                    campaign_ids=campaign_result.campaign_ids,
                    date_from=chunk_date_from,
                    date_to=chunk_date_to,
                )
                account_result.http_seconds += time.monotonic() - http_started
                account_result.planned_batches += fetch_result.requests_total
                account_result.http_attempts += fetch_result.attempts_total
                account_result.responses_429 += fetch_result.responses_429
                account_result.api_errors += fetch_result.api_errors
                account_result.retry_count += fetch_result.retries_used
                account_result.failed_batches.extend(fetch_result.failed_batches)

                if fetch_result.failed_batches:
                    account_result.status = "partial"

                if not fetch_result.payload:
                    logger.warning(
                        "Для date chunk нет данных или все batch завершились ошибкой | account=%s | date_from=%s | date_to=%s | failed_batches=%s",
                        account_name,
                        chunk_date_from.isoformat(),
                        chunk_date_to.isoformat(),
                        len(fetch_result.failed_batches),
                    )
                    continue

                stage = "normalization"
                try:
                    normalize_started = time.monotonic()
                    dataframe = self._build_dataframe_from_payload(fetch_result.payload)
                    self._log_dataframe_date_diagnostics(
                        dataframe=dataframe,
                        account=account_name,
                        date_from=chunk_date_from,
                        date_to=chunk_date_to,
                    )
                    dataframe = self._validate_and_filter_dates_before_save(
                        dataframe=dataframe,
                        account=account_name,
                        date_from=chunk_date_from,
                        date_to=chunk_date_to,
                    )
                    normalized_df = self.normalizer.normalize(dataframe)
                    account_result.normalization_seconds += time.monotonic() - normalize_started
                    db_started = time.monotonic()
                    async with self._database_semaphore:
                        stage = "database"
                        save_result = await asyncio.to_thread(self.repository.save, normalized_df)
                    account_result.database_seconds += time.monotonic() - db_started
                except Exception as error:
                    logger.error(
                        "Ошибка преобразования или записи date chunk advert_stat; предыдущие chunks сохранены | account=%s | date_from=%s | date_to=%s | тип=%s",
                        account_name,
                        chunk_date_from.isoformat(),
                        chunk_date_to.isoformat(),
                        type(error).__name__,
                    )
                    account_result.status = "partial"
                    if stage == "database":
                        account_result.database_errors += 1
                    else:
                        account_result.normalization_errors += 1
                    account_result.error_message = (
                        f"Ошибка обработки/записи chunk аккаунта {account_name} ({type(error).__name__})"
                    )
                    continue

                if not isinstance(save_result, AdvertStatsSaveResult):
                    raise TypeError("save_result должен быть экземпляром AdvertStatsSaveResult.")

                account_result.raw_rows += len(dataframe.index)
                account_result.normalized_rows += len(normalized_df.index)
                account_result.written_rows += save_result.written_rows
                account_result.dropped_missing_key_rows += save_result.dropped_missing_key_rows
                account_result.collapsed_duplicate_rows += save_result.collapsed_duplicate_rows

                logger.info(
                    "Date chunk advert_stat записан | account=%s | date_from=%s | date_to=%s | raw_rows=%s | normalized_rows=%s | written_rows=%s",
                    account_name,
                    chunk_date_from.isoformat(),
                    chunk_date_to.isoformat(),
                    len(dataframe.index),
                    len(normalized_df.index),
                    save_result.written_rows,
                )

            if account_result.written_rows == 0 and account_result.failed_batches:
                account_result.status = "failed"
                if account_result.error_message is None:
                    account_result.error_message = (
                        f"account={account_name} не удалось получить ни одного успешного batch fullstats"
                    )

            logger.info(
                "Аккаунт advert_stat обработан | account=%s | status=%s | campaign_count=%s | raw_rows=%s | normalized_rows=%s | written_rows=%s | failed_batches=%s | retry_count=%s | queue_wait_seconds=%.2f | http_seconds=%.2f | normalization_seconds=%.2f | database_seconds=%.2f | duration_seconds=%.2f",
                account_name,
                account_result.status,
                account_result.campaign_count,
                account_result.raw_rows,
                account_result.normalized_rows,
                account_result.written_rows,
                len(account_result.failed_batches),
                account_result.retry_count,
                account_result.queue_wait_seconds,
                account_result.campaign_http_seconds + account_result.http_seconds,
                account_result.normalization_seconds,
                account_result.database_seconds,
                time.monotonic() - account_started,
            )
            account_result.duration_seconds = time.monotonic() - account_started
            return account_result

    def _build_dataframe_from_payload(self, payload: list[dict]) -> pd.DataFrame:
        if not payload:
            return pd.DataFrame()

        processed_rows: list[dict] = []
        for item in payload:
            processed_rows.extend(self._expand_campaign_days(item))

        dataframe = pd.DataFrame(processed_rows)
        if "sum" in dataframe.columns and "views" in dataframe.columns:
            dataframe["cpm"] = (dataframe["sum"] / dataframe["views"] * 1000).round(4)
        if "advertId" in dataframe.columns:
            dataframe = dataframe.rename(columns={"advertId": "campaign_id"})

        dataframe = dataframe.drop_duplicates().copy()
        logger.info(
            "Сформирован DataFrame advert_stat из payload | rows=%s | columns=%s",
            len(dataframe.index),
            list(dataframe.columns),
        )
        return dataframe

    def _log_dataframe_date_diagnostics(
        self,
        dataframe: pd.DataFrame,
        account: str,
        date_from: date,
        date_to: date,
    ) -> None:
        if dataframe.empty:
            logger.info(
                "Диагностика advert_stat после flatten | account=%s | period=%s..%s | rows=0 | unique_dates=[]",
                account,
                date_from.isoformat(),
                date_to.isoformat(),
            )
            return

        if "date" not in dataframe.columns:
            raise ValueError(
                "После flatten advert_stat отсутствует колонка date. Нельзя продолжать запись в БД."
            )

        date_series = pd.to_datetime(dataframe["date"], errors="coerce")
        valid_date_series = date_series.dropna()
        unique_dates = sorted(valid_date_series.dt.date.astype(str).unique().tolist())
        rows_by_date = (
            valid_date_series.dt.date.astype(str).value_counts().sort_index().to_dict()
            if not valid_date_series.empty
            else {}
        )
        campaigns_count = (
            int(dataframe["campaign_id"].nunique(dropna=True))
            if "campaign_id" in dataframe.columns
            else 0
        )
        articles_count = (
            int(dataframe["article_id"].nunique(dropna=True))
            if "article_id" in dataframe.columns
            else 0
        )

        logger.info(
            "Диагностика advert_stat после flatten | account=%s | period=%s..%s | rows=%s | unique_dates=%s | date_min=%s | date_max=%s | campaigns=%s | articles=%s | rows_by_date=%s",
            account,
            date_from.isoformat(),
            date_to.isoformat(),
            len(dataframe.index),
            unique_dates,
            valid_date_series.min().date().isoformat() if not valid_date_series.empty else None,
            valid_date_series.max().date().isoformat() if not valid_date_series.empty else None,
            campaigns_count,
            articles_count,
            rows_by_date,
        )

        expected_period_days = (date_to - date_from).days + 1
        if expected_period_days > 1 and len(unique_dates) == 1:
            logger.warning(
                "Запрошен период %s дней, но в DataFrame только 1 уникальная дата. Нужно проверить парсинг days[].date | account=%s | period=%s..%s | unique_dates=%s",
                expected_period_days,
                account,
                date_from.isoformat(),
                date_to.isoformat(),
                unique_dates,
            )

        if unique_dates and len(unique_dates) == 1:
            only_date = unique_dates[0]
            if only_date in {date_from.isoformat(), date_to.isoformat()} and expected_period_days > 1:
                logger.warning(
                    "Все строки периода получили одну и ту же граничную дату | account=%s | period=%s..%s | unique_date=%s",
                    account,
                    date_from.isoformat(),
                    date_to.isoformat(),
                    only_date,
                )

    def _validate_and_filter_dates_before_save(
        self,
        dataframe: pd.DataFrame,
        account: str,
        date_from: date,
        date_to: date,
    ) -> pd.DataFrame:
        if dataframe.empty:
            return dataframe

        if "date" not in dataframe.columns:
            raise ValueError(
                "В DataFrame advert_stat отсутствует колонка date перед записью в PostgreSQL."
            )

        result_df = dataframe.copy()
        date_series = pd.to_datetime(result_df["date"], errors="coerce")
        missing_date_mask = date_series.isna()
        missing_date_rows = int(missing_date_mask.sum())

        if missing_date_rows:
            sample_columns = [
                column
                for column in ("campaign_id", "article_id", "date", "account")
                if column in result_df.columns
            ]
            sample_rows = (
                result_df.loc[missing_date_mask, sample_columns]
                .head(10)
                .to_dict(orient="records")
            )
            logger.error(
                "Найдены строки advert_stat с пустой или некорректной датой, они не будут записаны | account=%s | period=%s..%s | rows=%s | sample_rows=%s",
                account,
                date_from.isoformat(),
                date_to.isoformat(),
                missing_date_rows,
                sample_rows,
            )
            result_df = result_df.loc[~missing_date_mask].copy()

        if result_df.empty:
            logger.warning(
                "После фильтрации пустых дат в advert_stat не осталось строк | account=%s | period=%s..%s",
                account,
                date_from.isoformat(),
                date_to.isoformat(),
            )
            return result_df

        filtered_date_series = pd.to_datetime(result_df["date"], errors="coerce").dropna()
        expected_period_days = (date_to - date_from).days + 1
        actual_unique_dates = int(filtered_date_series.dt.date.nunique())
        if expected_period_days > 1 and actual_unique_dates == 1:
            logger.warning(
                "Перед записью advert_stat за период больше 1 дня осталась только одна уникальная дата | account=%s | period=%s..%s | unique_date=%s",
                account,
                date_from.isoformat(),
                date_to.isoformat(),
                filtered_date_series.iloc[0].date().isoformat() if not filtered_date_series.empty else None,
            )

        return result_df

    def build_dataframe_for_dev_check(self, payload: list[dict]) -> pd.DataFrame:
        """Возвращает DataFrame из mock или real payload без записи в БД."""
        return self._build_dataframe_from_payload(payload)

    def build_mock_fullstats_payload_for_dev_check(self) -> list[dict]:
        """Возвращает минимальный mock-ответ WB для проверки days[].date."""
        return [
            {
                "advertId": 123,
                "account": "MOCK_ACCOUNT",
                "days": [
                    {
                        "date": "2026-06-17",
                        "views": 10,
                        "clicks": 1,
                        "orders": 0,
                        "apps": [
                            {
                                "appType": 1,
                                "views": 10,
                                "clicks": 1,
                                "orders": 0,
                                "atbs": 0,
                                "canceled": 0,
                                "shks": 0,
                                "sum_price": 0,
                                "cr": 0,
                                "ctr": 10,
                                "nms": [{"nmId": 456}],
                            }
                        ],
                    },
                    {
                        "date": "2026-06-18",
                        "views": 20,
                        "clicks": 2,
                        "orders": 1,
                        "apps": [
                            {
                                "appType": 1,
                                "views": 20,
                                "clicks": 2,
                                "orders": 1,
                                "atbs": 0,
                                "canceled": 0,
                                "shks": 0,
                                "sum_price": 0,
                                "cr": 0,
                                "ctr": 10,
                                "nms": [{"nmId": 456}],
                            }
                        ],
                    },
                    {
                        "date": "2026-06-19",
                        "views": 30,
                        "clicks": 3,
                        "orders": 1,
                        "apps": [
                            {
                                "appType": 1,
                                "views": 30,
                                "clicks": 3,
                                "orders": 1,
                                "atbs": 0,
                                "canceled": 0,
                                "shks": 0,
                                "sum_price": 0,
                                "cr": 0,
                                "ctr": 10,
                                "nms": [{"nmId": 456}],
                            }
                        ],
                    },
                ],
                "boosterStats": [
                    {"date": "2026-06-17", "avg_position": 1.1},
                    {"date": "2026-06-18", "avg_position": 1.2},
                    {"date": "2026-06-19", "avg_position": 1.3},
                ],
            }
        ]

    def run_mock_flatten_dev_check(self) -> pd.DataFrame:
        """Локальная проверка, что days[].date корректно попадает в DataFrame."""
        dataframe = self.build_dataframe_for_dev_check(
            self.build_mock_fullstats_payload_for_dev_check()
        )
        if dataframe["date"].nunique() != 3:
            raise AssertionError("Ожидалось 3 уникальные даты в mock DataFrame.")
        views_set = set(
            pd.to_numeric(dataframe["views"], errors="coerce").dropna().astype(int).tolist()
        )
        if views_set != {10, 20, 30}:
            raise AssertionError("Ожидались значения views {10, 20, 30} в mock DataFrame.")
        logger.info(
            "Mock-проверка advert_stat пройдена | unique_dates=%s | views=%s",
            sorted(dataframe["date"].astype(str).unique().tolist()),
            sorted(views_set),
        )
        return dataframe

    def _resolve_tokens(self, account: str | None) -> dict[str, str]:
        loaded_tokens = self.tokens_loader()
        if not isinstance(loaded_tokens, Mapping):
            raise TypeError("load_api_tokens() должен возвращать Mapping account -> token.")

        tokens_by_account = {
            account_name.strip(): token.strip()
            for account_name, token in loaded_tokens.items()
            if isinstance(account_name, str)
            and account_name.strip()
            and isinstance(token, str)
            and token.strip()
        }
        if account is None:
            return tokens_by_account

        normalized_account = account.strip()
        if normalized_account in tokens_by_account:
            return {normalized_account: tokens_by_account[normalized_account]}

        raise ValueError(f"Аккаунт '{account}' не найден в токенах WB.")

    def _expand_campaign_days(self, item: dict) -> list[dict]:
        base_row = dict(item)
        booster_stats = base_row.pop("boosterStats", None) or []
        days = base_row.pop("days", None) or []

        if not days:
            base_row["avg_position"] = self._extract_avg_position_for_date(
                booster_stats=booster_stats,
                target_date=None,
            )
            return [base_row]

        result_rows: list[dict] = []
        for day_entry in days:
            if not isinstance(day_entry, dict):
                continue

            row = dict(base_row)
            row["date"] = day_entry.get("date", row.get("date"))
            for metric_key in DAY_LEVEL_METRIC_KEYS:
                if metric_key in day_entry:
                    row[metric_key] = day_entry.get(metric_key)
            row["avg_position"] = self._extract_avg_position_for_date(
                booster_stats=booster_stats,
                target_date=row.get("date"),
            )

            apps = day_entry.get("apps") or []
            for platform in apps:
                if not isinstance(platform, dict):
                    continue
                platform_mapping = PLATFORM_COLUMN_MAPPING.get(platform.get("appType"))
                if not platform_mapping:
                    continue
                for source_key, target_key in platform_mapping.items():
                    row[target_key] = platform.get(source_key)
                platform_nms = platform.get("nms") or []
                if platform_nms and isinstance(platform_nms[0], dict):
                    row["article_id"] = platform_nms[0].get("nmId")

            result_rows.append(row)

        return result_rows or [base_row]

    def _extract_avg_position_for_date(
        self,
        booster_stats: list[dict],
        target_date: str | None,
    ) -> object:
        if not booster_stats:
            return None

        if target_date is not None:
            for stat in booster_stats:
                if not isinstance(stat, dict):
                    continue
                if stat.get("date") == target_date:
                    return stat.get("avg_position")

        first_stat = booster_stats[0]
        if isinstance(first_stat, dict):
            return first_stat.get("avg_position")
        return None
