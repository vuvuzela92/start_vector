from __future__ import annotations

import logging
from decimal import Decimal

import gspread
import pandas as pd
from gspread.utils import rowcol_to_a1

from src_oop.jobs.autopilot_daily.config import UNIT_SHEET

logger = logging.getLogger(__name__)


class AutopilotDailyUnitUpdater:
    """Обновляет связанные UNIT-листы после дневного ПУ.

    Бизнес-логика:
    legacy daily-сценарий после записи ПУ поддерживал статус рекламы в UNIT и
    лист Сопост. Этот класс переносит такие операции отдельно от записи
    метрик, чтобы частичная ошибка UNIT не ломала основную актуализацию ПУ.
    """

    def __init__(self, table: gspread.Spreadsheet) -> None:
        """Создает updater для UNIT-таблицы.

        Бизнес-логика:
        один открытый spreadsheet используется для MAIN и Сопост, чтобы не
        делать лишние подключения к Google Sheets в рамках дневного запуска.
        """
        self.table = table

    def update_adv_status(
        self,
        current_metrics: pd.DataFrame,
        unit_sheet_name: str,
    ) -> int:
        """Обновляет колонку статуса рекламы в UNIT.

        Бизнес-логика:
        если вчерашний расход по артикулу больше нуля, в UNIT ставится
        `реклама`. Статус удаленного товара сохраняется из UNIT и имеет
        приоритет; если удаленный товар одновременно активен в рекламе,
        сценарий поднимает ошибку, как legacy-проверка качества данных.
        """
        worksheet = self.table.worksheet(unit_sheet_name)
        unit_skus = self._read_unit_skus(worksheet)
        autopilot_status = self._build_adv_status(current_metrics)
        merged_status = self._merge_deleted_status(worksheet, autopilot_status, unit_skus)
        rows = [[merged_status.get(sku, "")] for sku in unit_skus]
        if not rows:
            return 0

        target_range = self._column_range_by_header(
            worksheet=worksheet,
            header=UNIT_SHEET.adv_status_column,
            first_row=2,
            rows_count=len(rows),
        )
        worksheet.update(
            target_range,
            rows,
            value_input_option="USER_ENTERED",
        )
        logger.info("Статус рекламы в UNIT обновлен: range=%s rows=%s", target_range, len(rows))
        return len(rows)

    def update_sopost_orders(
        self,
        orders: pd.DataFrame,
        sheet_name: str = UNIT_SHEET.sopost_sheet,
    ) -> int:
        """Обновляет блок заказов на листе Сопост.

        Бизнес-логика:
        строки Сопоста упорядочены вручную по wild-кодам в колонке E. Сценарий
        раскладывает заказы из БД в этот порядок и пишет диапазон `S:AV`, как
        делал legacy daily-скрипт.
        """
        worksheet = self.table.worksheet(sheet_name)
        wilds_ordered = worksheet.col_values(5)[1:]
        if orders.empty:
            logger.info("Обновление Сопоста пропущено: в БД нет заказов.")
            return 0

        order_map = orders.set_index("local_vendor_code").T.to_dict("list")
        empty_line = [0] * (len(next(iter(order_map.values()))) if order_map else 0)
        output_rows = [
            [self._sheet_value(value) for value in order_map.get(wild, empty_line)]
            for wild in wilds_ordered
        ]
        if not output_rows:
            return 0

        target_range = f"S2:AV{worksheet.row_count}"
        worksheet.update(
            values=output_rows,
            range_name=target_range,
            value_input_option="USER_ENTERED",
        )
        logger.info("Заказы на листе Сопост обновлены: range=%s rows=%s", target_range, len(output_rows))
        return len(output_rows)

    def _read_unit_skus(self, worksheet: gspread.Worksheet) -> list[int]:
        """Читает артикула UNIT в порядке листа.

        Бизнес-логика:
        порядок UNIT определяет порядок записи статуса рекламы; нечисловые
        значения пропускаются, чтобы служебные строки не попадали в обновление.
        """
        result: list[int] = []
        for value in worksheet.col_values(1)[1:]:
            value_text = str(value).strip()
            if value_text.isdigit():
                result.append(int(value_text))
        return result

    def _build_adv_status(self, current_metrics: pd.DataFrame) -> dict[int, str]:
        """Строит статус рекламы по последней дате текущих метрик.

        Бизнес-логика:
        дневной UNIT-статус отражает факт расходов за последний загруженный
        завершенный день. Артикулы без расхода получают пустой статус.
        """
        if current_metrics.empty or "adv_spend" not in current_metrics.columns:
            return {}

        max_date = current_metrics["date"].max()
        latest = current_metrics[current_metrics["date"] == max_date]
        status_by_article: dict[int, str] = {}
        for row in latest[["article_id", "adv_spend"]].to_dict(orient="records"):
            try:
                article = int(row["article_id"])
                spend = float(row.get("adv_spend") or 0)
            except (TypeError, ValueError):
                continue
            status_by_article[article] = UNIT_SHEET.active_adv_status if spend > 0 else ""
        return status_by_article

    def _merge_deleted_status(
        self,
        worksheet: gspread.Worksheet,
        autopilot_status: dict[int, str],
        unit_skus: list[int],
    ) -> dict[int, str]:
        """Объединяет рекламный статус с признаком удаленного товара.

        Бизнес-логика:
        удаленный товар не должен одновременно иметь активный рекламный статус.
        Если в UNIT уже стоит `ТОВАР УДАЛЕН`, этот статус сохраняется в итоговой
        колонке и не затирается пустым значением из автопилота.
        """
        status_column = self._find_column_by_header(worksheet, UNIT_SHEET.adv_status_column)
        previous_values = worksheet.col_values(status_column)[1:]
        deleted_status = {
            unit_skus[index]: previous_values[index]
            for index in range(min(len(unit_skus), len(previous_values)))
            if previous_values[index] == UNIT_SHEET.deleted_status
        }
        active_deleted = {
            sku
            for sku, status in autopilot_status.items()
            if status == UNIT_SHEET.active_adv_status and sku in deleted_status
        }
        if active_deleted:
            raise ValueError(
                f"В UNIT есть удаленные товары с активным рекламным статусом: {sorted(active_deleted)}"
            )

        result = {sku: autopilot_status.get(sku, "") for sku in unit_skus}
        result.update(deleted_status)
        return result

    def _column_range_by_header(
        self,
        worksheet: gspread.Worksheet,
        header: str,
        first_row: int,
        rows_count: int,
    ) -> str:
        """Строит вертикальный диапазон UNIT по названию колонки.

        Бизнес-логика:
        статус рекламы в UNIT ищется по заголовку, а не по номеру, потому что
        UNIT-лист чаще меняется вручную, чем фиксированные блоки ПУ.
        """
        column_index = self._find_column_by_header(worksheet, header)
        column_letter = rowcol_to_a1(1, column_index).rstrip("1")
        return f"{column_letter}{first_row}:{column_letter}{first_row + rows_count - 1}"

    @staticmethod
    def _find_column_by_header(worksheet: gspread.Worksheet, header: str) -> int:
        """Находит номер колонки по заголовку первой строки.

        Бизнес-логика:
        если обязательная колонка UNIT отсутствует, лучше явно остановить
        конкретный UNIT-блок, чем записать статус рекламы в неверную колонку.
        """
        headers = worksheet.row_values(1)
        if header not in headers:
            raise ValueError(f"В UNIT не найдена обязательная колонка: {header}")
        return headers.index(header) + 1

    @staticmethod
    def _sheet_value(value: object) -> object:
        """Готовит значение UNIT/Сопост для безопасной записи в Google Sheets.

        Бизнес-логика:
        заказы Сопоста приходят из PostgreSQL и могут иметь тип Decimal после
        агрегирования. Google Sheets API принимает обычные числа и строки, но
        не сериализует Decimal, поэтому numeric-значения переводятся в int/float
        до отправки, чтобы вспомогательный UNIT-блок не падал после успешной
        записи основных метрик ПУ.
        """
        if value is None or pd.isna(value):
            return ""
        if isinstance(value, Decimal):
            if value == value.to_integral_value():
                return int(value)
            return float(value)
        return value
