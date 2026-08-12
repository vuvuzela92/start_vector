from __future__ import annotations

import importlib
import logging
from datetime import datetime

import gspread
import pandas as pd

from src_oop.core.my_gspread import GoogleTabs
from src_oop.jobs.autopilot_daily.config import (
    AUTOPILOT_SHEET_TITLE,
    AUTOPILOT_TABLE_TITLE,
    CREDS_PATH,
    CURRENT_METRIC_TO_BASE_COLUMN,
    ENABLE_WB_DAILY_PUBLIC_CARD_PARSING,
    HISTORY_METRIC_TO_COLUMN,
    UNIT_MAIN_SHEET_TITLE,
    UNIT_TABLE_TITLE,
)
from src_oop.jobs.autopilot_daily.models import AutopilotDailySummary
from src_oop.jobs.autopilot_daily.repository import AutopilotDailyRepository
from src_oop.jobs.autopilot_daily.sheets_writer import AutopilotDailySheetsWriter
from src_oop.jobs.autopilot_daily.unit_updates import AutopilotDailyUnitUpdater

logger = logging.getLogger(__name__)


class AutopilotDailyService:
    """Оркестратор дневного обновления автопилота.

    Бизнес-логика:
    запускает полный daily-сценарий legacy `autopilot_daily.py` в OOP-контуре:
    основные и исторические метрики ПУ, средние позиции, справочные поля A:D,
    статус рекламы UNIT, Сопост и опциональный блок региональных заказов.
    """

    def __init__(self, repository: AutopilotDailyRepository | None = None) -> None:
        """Собирает зависимости дневного сценария.

        Бизнес-логика:
        repository можно подменить в тестах, а Google Sheets открываются внутри
        run, чтобы один запуск использовал актуальные листы и credentials.
        """
        self.repository = repository or AutopilotDailyRepository()

    def run(self) -> AutopilotDailySummary:
        """Выполняет полный дневной цикл обновления ПУ и связанных листов.

        Бизнес-логика:
        сначала обновляется ПУ, включая основные метрики и статус A2, затем
        вспомогательные блоки. Частичные ошибки записи метрик не останавливают
        сценарий, но ошибки инфраструктурных блоков логируются отдельно.
        """
        summary = AutopilotDailySummary()
        logger.info("Запущен дневной сценарий обновления автопилота.")

        current_metrics = self.repository.fetch_current_metrics()
        history_metrics = self.repository.fetch_history_metrics()
        summary.current_rows = len(current_metrics.index)
        summary.history_rows = len(history_metrics.index)

        autopilot_connector = GoogleTabs(
            table_title=AUTOPILOT_TABLE_TITLE,
            sheet_title=AUTOPILOT_SHEET_TITLE,
        )
        writer = AutopilotDailySheetsWriter(autopilot_connector.sheet_title)
        articles = writer.read_articles()
        summary.articles_total = len(articles)

        self._write_current_metrics(
            writer,
            current_metrics,
            articles,
            summary,
        )
        self._write_history_metrics(writer, history_metrics, articles, summary)
        writer.update_status(datetime.now())

        current_positions = self.repository.fetch_current_avg_positions(articles)
        history_positions = self.repository.fetch_history_avg_positions(articles)
        writer.write_avg_positions(current_positions, history_positions)
        summary.avg_position_current_rows = len(current_positions.index)
        summary.avg_position_history_rows = len(history_positions)

        info_by_article = self.repository.fetch_vendor_codes_info(articles)
        summary.goods_info_rows = writer.update_goods_info(info_by_article)

        unit_updater = self._build_unit_updater()
        summary.unit_adv_rows = self._update_unit_adv_status(unit_updater, current_metrics)
        summary.sopost_rows = self._update_sopost(unit_updater)
        self._update_orders_by_regions()
        self._update_feedbacks_if_enabled()

        summary.finished_at = datetime.now()
        logger.info(
            "Дневной сценарий обновления автопилота завершен: articles=%s metrics_written=%s metrics_failed=%s",
            summary.articles_total,
            summary.metrics_written,
            summary.metrics_failed,
        )
        return summary

    def _write_current_metrics(
        self,
        writer: AutopilotDailySheetsWriter,
        current_metrics: pd.DataFrame,
        articles: list[int],
        summary: AutopilotDailySummary,
    ) -> None:
        """Пишет текущие метрики дневного ПУ порционно.

        Бизнес-логика:
        каждая текущая метрика обновляет свой недельный блок. Ошибка одной
        метрики фиксируется в summary, а остальные метрики продолжают запись.
        Метрика `unit_free_stock` пропускается настройкой writer, потому что
        свободный остаток относится к текущему снимку UNIT и обновляется
        почасовым сценарием, а не дневным блоком завершенных дней.
        """
        for metric_name in CURRENT_METRIC_TO_BASE_COLUMN:
            summary.metrics_attempted += 1
            result = writer.write_current_metric(current_metrics, metric_name, articles)
            if result.written:
                summary.metrics_written += 1
            else:
                summary.metrics_failed.append(metric_name)

    def _write_history_metrics(
        self,
        writer: AutopilotDailySheetsWriter,
        history_metrics: pd.DataFrame,
        articles: list[int],
        summary: AutopilotDailySummary,
    ) -> None:
        """Пишет исторические метрики дневного ПУ порционно.

        Бизнес-логика:
        исторические средние нужны для сравнения текущей недели с базой. Как и
        текущие метрики, они пишутся независимо друг от друга.
        """
        for metric_name in HISTORY_METRIC_TO_COLUMN:
            summary.metrics_attempted += 1
            result = writer.write_history_metric(history_metrics, metric_name, articles)
            if result.written:
                summary.metrics_written += 1
            else:
                summary.metrics_failed.append(metric_name)

    def _build_unit_updater(self) -> AutopilotDailyUnitUpdater:
        """Открывает UNIT-таблицу для связанных дневных обновлений.

        Бизнес-логика:
        UNIT-блоки используют тот же сервисный аккаунт, что и остальные Google
        Sheets операции, но открываются отдельно от ПУ, потому что это другая
        управленческая таблица.
        """
        client = gspread.service_account(filename=str(CREDS_PATH))
        table = client.open(UNIT_TABLE_TITLE)
        return AutopilotDailyUnitUpdater(table)

    def _update_unit_adv_status(
        self,
        updater: AutopilotDailyUnitUpdater,
        current_metrics: pd.DataFrame,
    ) -> int:
        """Безопасно обновляет рекламный статус в UNIT.

        Бизнес-логика:
        ошибка UNIT-статуса не должна откатывать уже записанный дневной ПУ, но
        должна попасть в лог как отдельная проблема качества данных или доступа.
        """
        try:
            return updater.update_adv_status(current_metrics, UNIT_MAIN_SHEET_TITLE)
        except Exception:
            logger.exception("Не удалось обновить статус рекламы в UNIT, дневной сценарий продолжает работу.")
            return 0

    def _update_sopost(self, updater: AutopilotDailyUnitUpdater) -> int:
        """Безопасно обновляет заказы на листе Сопост.

        Бизнес-логика:
        Сопост является вспомогательным UNIT-блоком; его ошибка не должна
        блокировать основную дневную актуализацию ПУ.
        """
        try:
            orders = self.repository.fetch_sopost_orders()
            return updater.update_sopost_orders(orders)
        except Exception:
            logger.exception("Не удалось обновить заказы на листе Сопост, дневной сценарий продолжает работу.")
            return 0

    def _update_orders_by_regions(self) -> None:
        """Пытается запустить legacy-обновление заказов по регионам.

        Бизнес-логика:
        старый daily-сценарий обновлял таблицу отгрузок ФБО через внешний модуль.
        В текущем проекте этот модуль может отсутствовать, поэтому блок
        выполняется только при доступном импорте и иначе пропускается.
        """
        try:
            module = importlib.import_module("db_data_to_purch_gs")
            update_orders_by_regions = getattr(module, "update_orders_by_regions")
        except (ImportError, AttributeError):
            logger.info(
                "Обновление заказов по регионам пропущено: legacy-модуль db_data_to_purch_gs недоступен в текущем проекте."
            )
            return

        try:
            from src.core.utils_gspread import init_client

            client = init_client()
            update_orders_by_regions(client, logger=logger)
            logger.info("Заказы по регионам успешно обновлены через legacy-модуль.")
        except Exception:
            logger.exception("Не удалось обновить заказы по регионам, дневной сценарий продолжает работу.")

    def _update_feedbacks_if_enabled(self) -> None:
        """Контролирует отключенный онлайн-парсинг отзывов WB.

        Бизнес-логика:
        legacy daily-сценарий ходил в публичную карточку WB за отзывами. Этот
        контур временно выключен тем же принципом, что и онлайн-парсинг цен,
        чтобы нестабильный внешний ответ не мешал дневному обновлению ПУ.
        """
        if not ENABLE_WB_DAILY_PUBLIC_CARD_PARSING:
            logger.info("Онлайн-парсинг отзывов WB отключен для дневного сценария автопилота.")
            return
        logger.warning("Онлайн-парсинг отзывов WB включен флагом, но OOP-реализация пока не подключена.")
