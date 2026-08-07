from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from datetime import date, datetime

import aiohttp
import pandas as pd

from src_oop.core.my_gspread import GoogleTabs
from src_oop.core.utils_general import load_api_tokens
from src_oop.jobs.autopilot.calculator import AutopilotCalculator
from src_oop.jobs.autopilot.cometa_adv_spend import CometaAdvSpendClient
from src_oop.jobs.autopilot.config import (
    ENABLE_WB_ONLINE_PRICE_PARSING,
    MAX_CONCURRENT_ACCOUNTS,
    UNIT_ARTICLE_COLUMN_INDEX,
    UNIT_EXPECTED_REMAINS_HEADER,
    UNIT_MARGIN_COLUMN_NAME,
    UNIT_REMAINS_COLUMN_INDEX,
    autopilot_gs,
    unit_gs,
)
from src_oop.jobs.autopilot.funnel_client import WBFunnelClient
from src_oop.jobs.autopilot.models import AutopilotHourlySummary, MetricValues
from src_oop.jobs.autopilot.product_clients import WBPriceClient, WBPublicCardClient
from src_oop.jobs.autopilot.repository import AutopilotRepository
from src_oop.jobs.autopilot.sheets_writer import (
    AutopilotSheetsWriter,
    execute_google_write_pause,
)

logger = logging.getLogger(__name__)


class AutopilotHourlyService:
    def __init__(
        self,
        repository: AutopilotRepository | None = None,
        funnel_client: WBFunnelClient | None = None,
        cometa_client: CometaAdvSpendClient | None = None,
        price_client: WBPriceClient | None = None,
        card_client: WBPublicCardClient | None = None,
    ) -> None:
        """
        Собирает зависимости hourly job автопилота.

        Бизнес-логика:
        позволяет подменять клиенты и repository в тестах, не меняя сценарий
        обновления ПУ: источники данных остаются явными и контролируемыми.
        """
        self.repository = repository or AutopilotRepository()
        self.funnel_client = funnel_client or WBFunnelClient()
        self.cometa_client = cometa_client or CometaAdvSpendClient()
        self.price_client = price_client or WBPriceClient()
        self.card_client = card_client or WBPublicCardClient()
        self.calculator = AutopilotCalculator()

    async def run(self, report_date: date | None = None) -> AutopilotHourlySummary:
        """
        Выполняет полный почасовой цикл обновления ПУ.

        Бизнес-логика:
        сначала читает артикула из ПУ, собирает Воронку продаж WB, записывает ее метрики
        в Google Sheets и сразу обновляет A2 временем актуализации. После этого сценарий
        дописывает расходы Cometa, рекламную активность, остатки UNIT и расчетные показатели
        порциями по каждой метрике. Онлайн-парсинг цен/СПП/рейтинга WB временно защищен флагом.
        """
        report_date = report_date or date.today()
        summary = AutopilotHourlySummary()

        autopilot_connector = GoogleTabs(
            table_title=autopilot_gs["title"],
            sheet_title=autopilot_gs["hourly_sheet"],
        )
        writer = AutopilotSheetsWriter(autopilot_connector)
        articles = writer.read_articles()
        summary.articles_total = len(articles)

        article_accounts = self.repository.fetch_article_accounts(articles)
        tokens_by_account = load_api_tokens()

        funnel_df = await self._collect_funnel_data(
            report_date=report_date,
            articles=articles,
            article_accounts=article_accounts,
            tokens_by_account=tokens_by_account,
        )
        summary.funnel_rows = len(funnel_df.index)
        funnel_metrics = self.calculator.dataframe_to_metric_dicts(funnel_df)
        self._write_metrics(
            writer=writer,
            metrics=funnel_metrics,
            articles=articles,
            summary=summary,
        )
        writer.update_status(datetime.now())

        adv_spend = await self.cometa_client.fetch_today_spend(articles=articles)
        summary.cometa_rows = len(adv_spend)

        activity_by_article = self.repository.fetch_today_advert_activity(report_date)
        advert_metrics = self.calculator.calculate_advert_metrics(
            activity_by_article=activity_by_article,
            adv_spend=adv_spend,
        )

        unit_connector = GoogleTabs(
            table_title=unit_gs["title"],
            sheet_title=unit_gs["unit_sheet"],
        )
        margin_by_article = self._load_unit_margins(unit_connector)
        unit_remains = self._load_unit_remains(unit_connector)

        calculation_metrics = self.calculator.calculate_margin_metrics(
            funnel_metrics=funnel_metrics,
            adv_spend=adv_spend,
            margin_by_article=margin_by_article,
        )
        organic = self.calculator.calculate_organic(
            funnel_metrics=funnel_metrics,
            advert_metrics=advert_metrics,
        )

        remaining_metrics: dict[str, MetricValues] = {}
        remaining_metrics["adv_spend"] = adv_spend
        logger.info(
            "Метрика свободного остатка UNIT собрана, но запись в ПУ пропущена до подтверждения колонки: "
            "rows=%s",
            len(unit_remains),
        )
        remaining_metrics.update(calculation_metrics)
        remaining_metrics.update(advert_metrics)
        remaining_metrics["organic"] = organic

        if ENABLE_WB_ONLINE_PRICE_PARSING:
            full_prices = await self._collect_full_prices(
                articles=articles,
                article_accounts=article_accounts,
                tokens_by_account=tokens_by_account,
            )
            card_snapshots = await self.card_client.fetch_cards(
                articles=articles,
                full_prices=full_prices,
            )
            summary.wb_card_rows = len(card_snapshots)
            summary.spp_history_rows = self.repository.insert_spp_history_changes(card_snapshots)
            remaining_metrics.update(self.calculator.card_snapshots_to_metrics(card_snapshots))
        else:
            logger.info("Онлайн-парсинг цен и карточек WB отключен для почасового сценария ПУ.")

        self._write_metrics(
            writer=writer,
            metrics=remaining_metrics,
            articles=articles,
            summary=summary,
        )
        summary.finished_at = datetime.now()
        return summary

    async def _collect_funnel_data(
        self,
        report_date: date,
        articles: list[int],
        article_accounts: dict[int, str],
        tokens_by_account: dict[str, str],
    ) -> pd.DataFrame:
        """
        Собирает воронку WB по всем кабинетам, к которым относятся артикула ПУ.

        Бизнес-логика:
        группирует артикула по кабинету, чтобы каждый WB token запрашивал только свои товары.
        Неуспешный кабинет не блокирует расчет остальных кабинетов.
        """
        articles_by_account = self._group_articles_by_account(articles, article_accounts)
        semaphore = asyncio.Semaphore(MAX_CONCURRENT_ACCOUNTS)
        async with aiohttp.ClientSession() as session:
            tasks = [
                self._fetch_account_funnel_safe(
                    semaphore=semaphore,
                    session=session,
                    account=account,
                    token=token,
                    nm_ids=articles_by_account.get(account.casefold(), []),
                    report_date=report_date,
                )
                for account, token in tokens_by_account.items()
                if articles_by_account.get(account.casefold())
            ]
            frames = await asyncio.gather(*tasks)

        clean_frames = [frame for frame in frames if not frame.empty]
        if not clean_frames:
            return pd.DataFrame()
        return pd.concat(clean_frames, ignore_index=True)

    async def _fetch_account_funnel_safe(
        self,
        semaphore: asyncio.Semaphore,
        session: aiohttp.ClientSession,
        account: str,
        token: str,
        nm_ids: list[int],
        report_date: date,
    ) -> pd.DataFrame:
        """
        Безопасно получает воронку одного кабинета WB.

        Бизнес-логика:
        если один кабинет WB вернул ошибку, job логирует проблему и продолжает обновление
        по остальным кабинетам, оставляя пропуски для недоступных данных.
        """
        async with semaphore:
            try:
                return await self.funnel_client.fetch_account_funnel(
                    session=session,
                    account=account,
                    token=token,
                    nm_ids=nm_ids,
                    report_date=report_date,
            )
            except Exception:
                logger.exception(
                    "Не удалось загрузить Воронку WB по кабинету, сценарий продолжает остальные кабинеты: "
                    "account=%s",
                    account,
                )
                return pd.DataFrame()

    async def _collect_full_prices(
        self,
        articles: list[int],
        article_accounts: dict[int, str],
        tokens_by_account: dict[str, str],
    ) -> dict[int, float]:
        """
        Собирает полные цены WB по всем кабинетам, когда онлайн-парсинг явно включен.

        Бизнес-логика:
        полные цены нужны для колонок цены и расчета СПП. Сейчас вызов этой функции защищен
        флагом ENABLE_WB_ONLINE_PRICE_PARSING, чтобы временно не дергать нестабильный публичный
        контур WB и не мешать записи остальных метрик ПУ.
        """
        articles_by_account = self._group_articles_by_account(articles, article_accounts)
        async with aiohttp.ClientSession() as session:
            tasks = [
                self.price_client.fetch_full_prices_by_account(
                    session=session,
                    account=account,
                    token=token,
                    articles=set(articles_by_account.get(account.casefold(), [])),
                )
                for account, token in tokens_by_account.items()
                if articles_by_account.get(account.casefold())
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)

        prices: dict[int, float] = {}
        for result in results:
            if isinstance(result, Exception):
                logger.warning("Кабинет WB с ценами пропущен после ошибки, сценарий продолжается: %s", result)
                continue
            prices.update(result)
        return prices

    @staticmethod
    def _group_articles_by_account(
        articles: list[int],
        article_accounts: dict[int, str],
    ) -> dict[str, list[int]]:
        """
        Группирует артикула ПУ по личным кабинетам WB.

        Бизнес-логика:
        обеспечивает корректное использование WB tokens: каждый запрос содержит только
        товары своего кабинета. Сравнение аккаунтов выполняется без учета регистра.
        """
        result: dict[str, list[int]] = defaultdict(list)
        normalized_articles = {
            article_id: account.casefold()
            for article_id, account in article_accounts.items()
        }
        for article_id in articles:
            account = normalized_articles.get(article_id)
            if account:
                result[account].append(article_id)
        return dict(result)

    @staticmethod
    def _load_unit_remains(connector: GoogleTabs) -> MetricValues:
        """
        Читает свободные остатки из UNIT.

        Бизнес-логика:
        эти данные заполняют метрику `unit_free_stock` в ПУ. Если остаток отсутствует
        или не является числом, по артикулу остается пропуск.
        """
        worksheet = connector.sheet_title
        skus = worksheet.col_values(UNIT_ARTICLE_COLUMN_INDEX)
        remains = worksheet.col_values(UNIT_REMAINS_COLUMN_INDEX)
        if remains and remains[0] != UNIT_EXPECTED_REMAINS_HEADER:
            logger.warning(
                "В UNIT найден неожиданный заголовок колонки остатков: expected=%s actual=%s",
                UNIT_EXPECTED_REMAINS_HEADER,
                remains[0],
            )

        result: MetricValues = {}
        for index, sku in enumerate(skus[1:], start=1):
            sku_text = str(sku).strip()
            if not sku_text.isdigit() or index >= len(remains):
                continue
            value = str(remains[index]).strip()
            if value == "":
                continue
            try:
                result[int(sku_text)] = int(value)
            except ValueError:
                continue
        return result

    @staticmethod
    def _load_unit_margins(connector: GoogleTabs) -> dict[int, float]:
        """
        Читает маржу по артикулам из UNIT.

        Бизнес-логика:
        маржа участвует в расчете прибыли с заказов по ИУ. Если маржа не найдена,
        расчет использует legacy fallback `1.0`, как в старом скрипте.
        """
        values = connector.sheet_title.get_all_values()
        if not values:
            return {}

        headers = values[0]
        if UNIT_MARGIN_COLUMN_NAME not in headers:
            logger.warning("В UNIT не найдена колонка маржи, расчет использует fallback: column=%s", UNIT_MARGIN_COLUMN_NAME)
            return {}

        margin_index = headers.index(UNIT_MARGIN_COLUMN_NAME)
        result: dict[int, float] = {}
        for row in values[1:]:
            if not row:
                continue
            article_text = str(row[0]).strip()
            if not article_text.isdigit() or margin_index >= len(row):
                continue
            margin_text = str(row[margin_index]).strip().replace("%", "").replace(",", ".")
            if margin_text == "":
                continue
            try:
                result[int(article_text)] = float(margin_text) / 100
            except ValueError:
                continue
        return result

    @staticmethod
    def _write_metrics(
        writer: AutopilotSheetsWriter,
        metrics: dict[str, MetricValues],
        articles: list[int],
        summary: AutopilotHourlySummary,
    ) -> None:
        """
        Последовательно записывает все подготовленные метрики в ПУ.

        Бизнес-логика:
        одна метрика равна одной порции записи. Ошибка конкретной метрики фиксируется
        в summary, но не останавливает запись следующих метрик.
        """
        for metric_name, values_by_article in metrics.items():
            summary.metrics_attempted += 1
            result = writer.write_metric(
                metric_name=metric_name,
                values_by_article=values_by_article,
                articles=articles,
            )
            if result.written:
                summary.metrics_written += 1
            else:
                summary.metrics_failed.append(metric_name)
            execute_google_write_pause()
