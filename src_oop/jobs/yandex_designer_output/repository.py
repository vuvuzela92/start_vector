"""Публикация статистики дизайнеров в Google Sheets."""

from __future__ import annotations

import logging

import pandas as pd
from gspread.utils import rowcol_to_a1

from src_oop.core.my_gspread import GoogleTabs
from src_oop.jobs.yandex_designer_output.config import YandexDesignerOutputConfig
from src_oop.jobs.yandex_designer_output.models import DesignerOutputRow

logger = logging.getLogger(__name__)


class DesignerOutputRepository:
    """Добавляет ежедневный срез дизайнерской выработки в Google Sheets."""

    def __init__(self, config: YandexDesignerOutputConfig) -> None:
        """Подключает репозиторий к заданной таблице и вкладке."""

        self.google_tabs = GoogleTabs(config.spreadsheet_title, config.worksheet_title)

    def save(self, rows: list[DesignerOutputRow]) -> int:
        """Добавляет строки статистики и возвращает их количество.

        Бизнес-сценарий: Google Sheets используется как журнал ежедневной
        выработки. Повторный запуск обновляет строки с тем же ключом `дата +
        дизайнер`, а новые сочетания добавляет в конец листа.
        """

        if not rows:
            logger.warning("Статистика дизайнеров не получена, запись в Google Sheets пропущена")
            return 0
        worksheet = self.google_tabs.sheet_title
        existing_values = self.google_tabs.get_all_values_with_retry()
        if not existing_values:
            dataframe = pd.DataFrame([row.as_dict() for row in rows])
            self.google_tabs._send_df_to_google(dataframe, worksheet)
            written_rows = len(rows)
        else:
            written_rows = self._upsert_rows(worksheet, existing_values, rows)
        logger.info("Статистика дизайнерской выработки записана в Google Sheets | rows=%s", len(rows))
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
        required_headers = ["дата", "дизайнер", "количество"]
        missing_headers = [header for header in required_headers if header not in headers]
        if missing_headers:
            raise ValueError(
                "В листе дизайнерской выработки отсутствуют колонки: "
                + ", ".join(missing_headers)
            )

        column_indexes = {header: headers.index(header) for header in required_headers}
        existing_keys = {
            (
                values[column_indexes["дата"]] if len(values) > column_indexes["дата"] else "",
                values[column_indexes["дизайнер"]]
                if len(values) > column_indexes["дизайнер"]
                else "",
            ): index
            for index, values in enumerate(existing_values[1:], start=2)
        }
        new_rows: list[list[object]] = []
        updated_count = 0

        for row in rows:
            values = row.as_dict()
            key = (str(values["дата"]), str(values["дизайнер"]))
            row_number = existing_keys.get(key)
            if row_number is None:
                # Дополнительные пользовательские колонки листа, например
                # "Неделя", не входят в модель job и для новых строк остаются пустыми.
                new_rows.append([values.get(header, "") for header in headers])
                continue

            column_number = column_indexes["количество"] + 1
            cell = rowcol_to_a1(row_number, column_number)
            self.google_tabs._execute_google_write_with_retry(
                operation_name=f"update designer output {cell}",
                func=worksheet.update,
                range_name=cell,
                values=[[values["количество"]]],
            )
            updated_count += 1

        if new_rows:
            self.google_tabs._execute_google_write_with_retry(
                operation_name="append designer output rows",
                func=worksheet.append_rows,
                values=new_rows,
                value_input_option="USER_ENTERED",
            )

        logger.info(
            "Идемпотентная выгрузка выработки выполнена | updated_rows=%s | added_rows=%s",
            updated_count,
            len(new_rows),
        )
        return updated_count + len(new_rows)
