"""Публикация статистики дизайнеров в Google Sheets."""

from __future__ import annotations

import logging

from gspread.utils import rowcol_to_a1

from src_oop.core.my_gspread import GoogleTabs
from src_oop.jobs.yandex_designer_output.config import YandexDesignerOutputConfig
from src_oop.jobs.yandex_designer_output.models import DesignerOutputRow

logger = logging.getLogger(__name__)


class DesignerOutputRepository:
    """Добавляет ежедневный срез дизайнерской выработки в Google Sheets."""

    def __init__(self, config: YandexDesignerOutputConfig) -> None:
        """Подключает репозиторий к таблице и вкладке учёта типов файлов."""

        self.google_tabs = GoogleTabs(config.spreadsheet_title, config.worksheet_title)

    def save(self, rows: list[DesignerOutputRow]) -> int:
        """Создаёт структуру листа либо обновляет результаты без дублей.

        Бизнес-сценарий: каждая строка отражает итоги одного дизайнера за
        один день. При повторном пересчёте последних десяти дней запись
        обновляется по ключу `дата + дизайнер`, сохраняя строки вне периода.
        """

        worksheet = self.google_tabs.sheet_title
        headers = ["дата", "дизайнер", "слайды", "видео", "PDF-инструкции", "прочие файлы"]
        existing_values = self.google_tabs.get_all_values_with_retry()
        if not existing_values:
            self.google_tabs._execute_google_write_with_retry(
                operation_name="создание заголовков выработки дизайнеров",
                func=worksheet.update,
                range_name="A1:F1",
                values=[headers],
                value_input_option="USER_ENTERED",
            )
            existing_values = [headers]
        if not rows:
            logger.warning("Статистика дизайнеров не получена, заголовки листа сохранены без строк")
            return 0
        written_rows = self._upsert_rows(worksheet, existing_values, rows)
        logger.info("Выработка дизайнеров обновлена в Google Sheets | rows=%s", written_rows)
        return written_rows

    def _upsert_rows(
        self,
        worksheet,
        existing_values: list[list[str]],
        rows: list[DesignerOutputRow],
    ) -> int:
        """Обновляет существующие и добавляет новые дневные строки.

        Бизнес-правило: один дизайнер должен иметь не более одной актуальной
        строки за конкретную дату при повторном запуске job.
        """

        headers = existing_values[0]
        required_headers = ["дата", "дизайнер", "слайды", "видео", "PDF-инструкции", "прочие файлы"]
        missing_headers = [header for header in required_headers if header not in headers]
        if missing_headers:
            raise ValueError(
                "В листе дизайнерской выработки отсутствуют колонки: "
                + ", ".join(missing_headers)
            )

        column_indexes = {header: headers.index(header) for header in required_headers}
        existing_keys: dict[tuple[str, str], int] = {}
        for index, values in enumerate(existing_values[1:], start=2):
            key = (
                values[column_indexes["дата"]] if len(values) > column_indexes["дата"] else "",
                values[column_indexes["дизайнер"]]
                if len(values) > column_indexes["дизайнер"]
                else "",
            )
            existing_keys.setdefault(key, index)
        new_rows: list[list[object]] = []
        updates: list[dict[str, object]] = []

        for row in rows:
            values = row.as_dict()
            key = (str(values["дата"]), str(values["дизайнер"]))
            row_number = existing_keys.get(key)
            if row_number is None:
                new_rows.append([values.get(header, "") for header in headers])
                continue
            for header in required_headers[2:]:
                cell = rowcol_to_a1(row_number, column_indexes[header] + 1)
                updates.append({"range": cell, "values": [[values[header]]]})

        if updates:
            self.google_tabs._execute_google_write_with_retry(
                operation_name="пакетное обновление выработки дизайнеров",
                func=worksheet.batch_update,
                data=updates,
                value_input_option="USER_ENTERED",
            )
        if new_rows:
            self.google_tabs._execute_google_write_with_retry(
                operation_name="добавление новых строк выработки дизайнеров",
                func=worksheet.append_rows,
                values=new_rows,
                value_input_option="USER_ENTERED",
            )

        logger.info(
            "Повторный пересчет выработки выполнен | updated_rows=%s | added_rows=%s",
            len(updates) // (len(required_headers) - 2),
            len(new_rows),
        )
        return len(updates) // (len(required_headers) - 2) + len(new_rows)
