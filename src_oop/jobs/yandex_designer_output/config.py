"""Конфигурация учета папок дизайнеров в Yandex Disk."""

from __future__ import annotations

import os
from dataclasses import dataclass

from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True, slots=True)
class YandexDesignerOutputConfig:
    """Хранит настройки API и Google Sheets для job.

    Бизнес-сценарий: ежедневная job работает с одной корневой папкой
    дизайнеров и публикует статистику в заранее известную вкладку.
    """

    api_token: str
    root_path: str
    spreadsheet_title: str
    worksheet_title: str
    request_timeout_seconds: float
    max_retries: int
    max_concurrent_requests: int


def get_config() -> YandexDesignerOutputConfig:
    """Возвращает конфигурацию из переменных окружения.

    Бизнес-сценарий: секрет Yandex Disk и параметры запуска не зашиваются
    в исходный код и не переносятся из старого проекта.
    """

    return YandexDesignerOutputConfig(
        api_token=os.getenv("YANDEX_DISK_API_TOKEN", "").strip(),
        root_path=os.getenv("YANDEX_DESIGNER_ROOT_PATH", "Работы_дизайнеров").strip(),
        spreadsheet_title=os.getenv(
            "YANDEX_DESIGNER_SPREADSHEET_TITLE", "Дизайнеры выработка"
        ).strip(),
        worksheet_title=os.getenv("YANDEX_DESIGNER_WORKSHEET_TITLE", "Дизайнеры").strip(),
        request_timeout_seconds=float(os.getenv("YANDEX_DISK_REQUEST_TIMEOUT_SECONDS", "30")),
        max_retries=int(os.getenv("YANDEX_DISK_MAX_RETRIES", "4")),
        max_concurrent_requests=int(os.getenv("YANDEX_DISK_MAX_CONCURRENT_REQUESTS", "5")),
    )
