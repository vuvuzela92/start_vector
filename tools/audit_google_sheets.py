"""Формирует перечень Google-таблиц, доступных service account."""

from __future__ import annotations

import csv
import logging
from dataclasses import dataclass
from pathlib import Path

import gspread
from openpyxl import Workbook


LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class SpreadsheetRecord:
    """Описывает одну таблицу, доступную credentials для аудита."""

    name: str
    spreadsheet_id: str
    url: str
    created_at: str
    modified_at: str


def collect_spreadsheets(credentials_path: Path) -> list[SpreadsheetRecord]:
    """Получает полный список Google-таблиц, видимых credentials.

    Бизнес-сценарий: аудит должен учитывать все таблицы, к которым service
    account имеет доступ, но не должен читать содержимое ячеек или раскрывать
    секретные данные credentials.
    """

    client = gspread.service_account(filename=str(credentials_path))
    files = client.list_spreadsheet_files()
    records = [
        SpreadsheetRecord(
            name=str(item.get("name", "")),
            spreadsheet_id=str(item.get("id", "")),
            url=f"https://docs.google.com/spreadsheets/d/{item.get('id', '')}/edit",
            created_at=str(item.get("createdTime", "")),
            modified_at=str(item.get("modifiedTime", "")),
        )
        for item in files
        if item.get("id")
    ]
    return sorted(records, key=lambda record: record.name.casefold())


def write_csv(records: list[SpreadsheetRecord], output_path: Path) -> None:
    """Сохраняет перечень таблиц в CSV для дальнейшей обработки.

    Бизнес-сценарий: CSV нужен для фильтрации и загрузки аудиторского списка
    в другие системы без обращения к Google Drive.
    """

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8-sig", newline="") as file:
        writer = csv.writer(file, delimiter=";")
        writer.writerow(["Название", "ID таблицы", "Ссылка", "Создана", "Изменена"])
        writer.writerows(
            [
                record.name,
                record.spreadsheet_id,
                record.url,
                record.created_at,
                record.modified_at,
            ]
            for record in records
        )


def write_xlsx(records: list[SpreadsheetRecord], output_path: Path) -> None:
    """Сохраняет перечень таблиц в XLSX с автофильтром и закреплённой шапкой.

    Бизнес-сценарий: XLSX является основной табличной формой аудита, удобной
    для ручной проверки названий, ссылок и актуальности таблиц.
    """

    output_path.parent.mkdir(parents=True, exist_ok=True)
    workbook = Workbook()
    worksheet = workbook.active
    worksheet.title = "Доступные таблицы"
    worksheet.append(["Название", "ID таблицы", "Ссылка", "Создана", "Изменена"])
    for record in records:
        worksheet.append(
            [
                record.name,
                record.spreadsheet_id,
                record.url,
                record.created_at,
                record.modified_at,
            ]
        )

    worksheet.freeze_panes = "A2"
    worksheet.auto_filter.ref = worksheet.dimensions
    worksheet.column_dimensions["A"].width = 48
    worksheet.column_dimensions["B"].width = 48
    worksheet.column_dimensions["C"].width = 72
    worksheet.column_dimensions["D"].width = 25
    worksheet.column_dimensions["E"].width = 25
    workbook.save(output_path)


def main() -> None:
    """Запускает полный сценарий аудита и создаёт CSV и XLSX отчёты."""

    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    project_root = Path(__file__).resolve().parents[1]
    credentials_path = project_root / "creds" / "creds.json"
    output_dir = project_root / "audit_output"
    records = collect_spreadsheets(credentials_path)
    write_csv(records, output_dir / "google_sheets_audit.csv")
    write_xlsx(records, output_dir / "google_sheets_audit.xlsx")
    LOGGER.info("Аудит завершён: найдено таблиц — %s", len(records))


if __name__ == "__main__":
    main()
