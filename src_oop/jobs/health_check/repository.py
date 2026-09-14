from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Any

from sqlalchemy import text

from src_oop.core.database import Database
from src_oop.core.my_gspread import GoogleTabs
from src_oop.jobs.health_check.models import HealthCheckSnapshot, HealthCheckTarget

logger = logging.getLogger(__name__)


class PostgreSQLHealthCheckRepository:
    """Читает агрегированное состояние выгрузок из PostgreSQL без тяжёлых запросов."""

    def fetch_snapshot(self, target: HealthCheckTarget) -> HealthCheckSnapshot:
        """Возвращает свежесть и объём строк PostgreSQL-витрины для health-check.

        Бизнес-правило: контроль ежедневных выгрузок не должен читать полные
        таблицы. Поэтому выполняется только агрегатный запрос `COUNT` и
        `MAX(date_column)` по заранее заданной таблице и колонке.
        """
        if not target.table_name or not target.date_column:
            raise ValueError("Для PostgreSQL-проверки должны быть заданы table_name и date_column.")

        self._validate_identifier(target.table_name, "имя таблицы")
        self._validate_identifier(target.date_column, "имя колонки даты")
        query = text(
            f"""
            SELECT
                COUNT(*) AS rows_count,
                MAX({target.date_column}) AS latest_value
            FROM {target.table_name}
            """
        )
        logger.info(
            "Проверяем свежесть PostgreSQL-выгрузки | task=%s | table=%s | date_column=%s",
            target.task_name,
            target.table_name,
            target.date_column,
        )
        with Database.get_engine().connect() as connection:
            row = connection.execute(query).mappings().one()
        return HealthCheckSnapshot(
            rows_count=int(row["rows_count"] or 0),
            latest_value=self._coerce_temporal_value(row["latest_value"]),
        )

    @staticmethod
    def _validate_identifier(value: str, label: str) -> None:
        """Защищает агрегатный SQL health-check от произвольных имён объектов."""
        parts = value.split(".")
        if not parts or not all(part.replace("_", "").isalnum() and part[0].isalpha() for part in parts):
            raise ValueError(f"Некорректное {label}: {value}")

    @staticmethod
    def _coerce_temporal_value(value: Any) -> date | datetime | None:
        """Приводит значение свежести из драйвера PostgreSQL к ожидаемому типу."""
        if value is None or isinstance(value, date | datetime):
            return value
        if isinstance(value, str):
            try:
                return datetime.fromisoformat(value)
            except ValueError:
                try:
                    return date.fromisoformat(value)
                except ValueError:
                    return None
        return None


class GoogleSheetsHealthCheckRepository:
    """Читает минимальный снимок состояния выгрузки из Google Sheets."""

    def fetch_snapshot(self, target: HealthCheckTarget) -> HealthCheckSnapshot:
        """Возвращает свежесть и количество строк листа Google Sheets.

        Бизнес-правило: для листов проекта проверяется факт непустой публикации
        и служебная колонка `updated_at`, которую общий клиент добавляет при
        записи DataFrame в Google Sheets.
        """
        if not target.spreadsheet_title or not target.worksheet_title:
            raise ValueError(
                "Для Google Sheets-проверки должны быть заданы spreadsheet_title и worksheet_title."
            )

        logger.info(
            "Проверяем свежесть Google Sheets-выгрузки | task=%s | table=%s | sheet=%s",
            target.task_name,
            target.spreadsheet_title,
            target.worksheet_title,
        )
        google_tabs = GoogleTabs(
            table_title=target.spreadsheet_title,
            sheet_title=target.worksheet_title,
        )
        values = google_tabs.get_all_values_with_retry()
        if not values:
            return HealthCheckSnapshot(rows_count=0, latest_value=None)

        headers = [str(value).strip() for value in values[0]]
        data_rows = [row for row in values[1:] if any(str(cell).strip() for cell in row)]
        latest_value = self._find_latest_updated_at(
            headers=headers,
            rows=data_rows,
            column_name=target.updated_at_column,
        )
        return HealthCheckSnapshot(rows_count=len(data_rows), latest_value=latest_value)

    def _find_latest_updated_at(
        self,
        headers: list[str],
        rows: list[list[Any]],
        column_name: str,
    ) -> datetime | None:
        """Ищет максимальное время публикации листа среди строк данных.

        В Google Sheets дата может прийти строкой из общего клиента, поэтому
        helper поддерживает основные ISO-форматы и формат `YYYY-MM-DD HH:MM:SS`.
        """
        if column_name not in headers:
            return None
        column_index = headers.index(column_name)
        latest_value: datetime | None = None
        for row in rows:
            if column_index >= len(row):
                continue
            parsed_value = self._parse_datetime(row[column_index])
            if parsed_value is None:
                continue
            if latest_value is None or parsed_value > latest_value:
                latest_value = parsed_value
        return latest_value

    @staticmethod
    def _parse_datetime(value: Any) -> datetime | None:
        """Преобразует значение Google Sheets в дату обновления для контроля свежести."""
        if isinstance(value, datetime):
            return value
        if isinstance(value, date):
            return datetime.combine(value, datetime.min.time())
        prepared = str(value).strip()
        if not prepared:
            return None
        for candidate in (prepared, prepared.replace(" ", "T", 1)):
            try:
                return datetime.fromisoformat(candidate)
            except ValueError:
                continue
        return None
