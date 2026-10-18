from __future__ import annotations

import logging
from datetime import datetime
from zoneinfo import ZoneInfo

from src_oop.core.my_gspread import GoogleTabs
from src_oop.jobs.autopilot.config import (
    MONITORING_SHEET_TITLE,
    MONITORING_TABLE_ID,
    MONITORING_TABLE_TITLE,
)

logger = logging.getLogger(__name__)


class MonitoringSnapshotWriter:
    """Записывает почасовые итоги текущего дня в технический лист мониторинга.

    Класс получает уже рассчитанные суммы из hourly-сценария автопилота и не
    перечитывает лист «Автопилот». Поэтому технический лист использует тот же
    числовой payload, который был отправлен в текущий дневной блок ПУ.
    """

    _MOSCOW_TIMEZONE = ZoneInfo("Europe/Moscow")

    def __init__(self) -> None:
        """Открывает технический лист, обслуживающий почасовой мониторинг.

        Подключение выполняется по постоянному spreadsheet ID, а не только по
        названию. Это защищает hourly-запуск от ручного переименования таблицы.
        В `self.worksheet` сохраняется объект конкретной вкладки, чтобы дальше
        читать и изменять только «Технический лист».
        """
        self.connector = GoogleTabs(
            table_title=MONITORING_TABLE_TITLE,
            sheet_title=MONITORING_SHEET_TITLE,
            spreadsheet_id=MONITORING_TABLE_ID,
        )
        self.worksheet = self.connector.sheet_title

    def upsert_snapshot(
        self,
        orders_sum: float,
        adv_spend: float,
        profit_by_cond_orders: float,
        snapshot_at: datetime | None = None,
    ) -> None:
        """Создаёт или обновляет снэпшот трёх итогов за текущий час.

        Бизнес-правило: повторный запуск hourly-задачи в одном часе не должен
        добавлять дубли, поэтому ключом строки служит пара «дата + час».
        Значения берутся из тех же агрегатов, которые hourly-сценарий пишет
        в текущий дневной блок листа «Автопилот».

        Сначала читаются только строки технического листа и ищется существующая
        пара «дата + час». Если такая строка найдена, она обновляется. Если нет,
        новая строка добавляется в конец таблицы. Запись выполняется через общий
        retry-механизм Google Sheets, поэтому временный 429 или 5xx не создаёт
        сразу ложный пропуск снэпшота.
        """
        snapshot_at = snapshot_at or datetime.now(self._MOSCOW_TIMEZONE)
        # В таблицу передаются отдельные дата и час, чтобы по ним можно было
        # безопасно найти строку при повторном запуске того же часа.
        snapshot_date = snapshot_at.strftime("%Y-%m-%d")
        snapshot_time = snapshot_at.strftime("%H:00")
        # Порядок значений соответствует заголовкам A:E технического листа:
        # дата, время, сумма заказов, сумма затрат, прибыль с заказов по ИУ.
        row_values = [
            snapshot_date,
            snapshot_time,
            orders_sum,
            adv_spend,
            profit_by_cond_orders,
        ]

        # Читаем только целевую таблицу мониторинга. Лист «Автопилот» повторно
        # не запрашивается: это снижает нагрузку и сохраняет единый payload.
        existing_rows = self.connector.get_all_values_with_retry()
        target_row: int | None = None
        # Первая строка содержит заголовки, поэтому поиск начинается со строки 2.
        for row_number, row in enumerate(existing_rows[1:], start=2):
            if len(row) < 2:
                continue
            if (
                self._normalize_date(row[0]) == snapshot_date
                and self._normalize_time(row[1]) == snapshot_time
            ):
                target_row = row_number
                break

        if target_row is None:
            # Так как ключ «дата + час» ещё не найден, добавляем новый снэпшот
            # после существующих строк, сохраняя историю предыдущих запусков.
            self.connector._execute_google_write_with_retry(
                "добавление почасового снэпшота мониторинга",
                self.worksheet.append_row,
                row_values,
                value_input_option="USER_ENTERED",
                insert_data_option="INSERT_ROWS",
            )
            logger.info(
                "Почасовой снэпшот добавлен в технический лист мониторинга: date=%s time=%s",
                snapshot_date,
                snapshot_time,
            )
            return

        # Повторный запуск в том же часу не должен создавать вторую строку:
        # обновляем найденную запись актуальными суммами.
        self.connector._execute_google_write_with_retry(
            "обновление почасового снэпшота мониторинга",
            self.worksheet.update,
            f"A{target_row}:E{target_row}",
            [row_values],
            value_input_option="USER_ENTERED",
        )
        logger.info(
            "Почасовой снэпшот обновлён в техническом листе мониторинга: date=%s time=%s row=%s",
            snapshot_date,
            snapshot_time,
            target_row,
        )

    @staticmethod
    def _normalize_date(value: str) -> str:
        """Приводит дату из Google Sheets к ISO-формату для поиска строки.

        Google Sheets может вернуть дату как `02.10.2026`, а добавляемая
        программой строка использует ISO-представление `2026-10-02`. Обе формы
        приводятся к одному ключу, чтобы повторный запуск находил существующую
        строку независимо от отображаемого формата ячейки.
        """
        for date_format in ("%Y-%m-%d", "%d.%m.%Y"):
            try:
                return datetime.strptime(value.strip(), date_format).strftime("%Y-%m-%d")
            except ValueError:
                continue
        return value.strip()

    @staticmethod
    def _normalize_time(value: str) -> str:
        """Приводит время листа к ключу часа `HH:00`.

        В техническом листе время хранится с точностью до часа. Нормализация
        нужна для сравнения значения из Google Sheets с ключом текущего запуска
        и предотвращает создание дубля при повторной попытке записи.
        """
        try:
            return datetime.strptime(value.strip(), "%H:%M").strftime("%H:00")
        except ValueError:
            return value.strip()
