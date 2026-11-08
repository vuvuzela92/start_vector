"""Бизнес-сервис учета выработки дизайнеров."""

from __future__ import annotations

import asyncio
import logging
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

from src_oop.jobs.yandex_designer_output.api_client import YandexDiskClient
from src_oop.jobs.yandex_designer_output.config import YandexDesignerOutputConfig
from src_oop.jobs.yandex_designer_output.models import DesignerOutputRow
from src_oop.jobs.yandex_designer_output.repository import DesignerOutputRepository

logger = logging.getLogger(__name__)
MOSCOW_TIMEZONE = ZoneInfo("Europe/Moscow")
DISK_PAGE_SIZE = 100
SLIDE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".webp"}
VIDEO_EXTENSIONS = {".mp4", ".mov", ".avi", ".mkv", ".webm", ".m4v"}


class DesignerOutputService:
    """Оркестрирует создание папок и публикацию дневной статистики."""

    def __init__(
        self,
        config: YandexDesignerOutputConfig,
        repository: DesignerOutputRepository | None = None,
    ) -> None:
        """Создает сервис с конфигурацией и опциональным репозиторием."""

        self.config = config
        self.repository = repository or DesignerOutputRepository(config)

    async def run(self, days: int = 10) -> int:
        """Пересчитывает заданное число последних дней и публикует выработку.

        Бизнес-сценарий: список дизайнеров читается из корневой папки,
        обеспечивается папка на завтра, затем содержимое дневных папок за
        сегодня и заданное количество предыдущих дней классифицируется и
        обновляется в Google Sheets по московскому времени.
        """

        if days < 1:
            raise ValueError("Период пересчета должен содержать хотя бы один день")
        today = datetime.now(MOSCOW_TIMEZONE).date()
        tomorrow = today + timedelta(days=1)
        async with YandexDiskClient(self.config) as client:
            root = await client.get_resource(self.config.root_path)
            designers = self._extract_designers(root)
            await asyncio.gather(
                *(client.create_folder(self._folder_path(designer, tomorrow)) for designer in designers)
            )
            rows = await asyncio.gather(
                *(
                    self._get_designer_row(client, designer, today - timedelta(days=days_ago))
                    for designer in designers
                    for days_ago in range(days)
                )
            )

        prepared_rows = [row for row in rows if row is not None]
        written_rows = self.repository.save(prepared_rows)
        logger.info(
            "Пересчет выработки дизайнеров завершён | days=%s | designers=%s | written_rows=%s | date=%s",
            days,
            len(designers),
            written_rows,
            today.strftime("%d.%m.%Y"),
        )
        return written_rows

    @staticmethod
    def _extract_designers(resource: dict[str, object]) -> list[str]:
        """Извлекает непустые имена дизайнерских папок из ответа API."""

        embedded = resource.get("_embedded", {})
        items = embedded.get("items", []) if isinstance(embedded, dict) else []
        return [
            str(item["name"])
            for item in items
            if isinstance(item, dict) and item.get("name")
        ]

    async def _get_designer_row(
        self,
        client: YandexDiskClient,
        designer: str,
        work_date: date,
    ) -> DesignerOutputRow | None:
        """Классифицирует все файлы дизайнера за указанную дату.

        Бизнес-правило: изображения учитываются как слайды, видеофайлы — как
        видео, PDF — как инструкции; каждый прочий файл отражается отдельно.
        Вложенные папки обходятся рекурсивно, а недоступная страница прерывает
        сбор этой строки, чтобы частичные данные не попали в отчет.
        """

        root_path = self._folder_path(designer, work_date)
        counts = {"slides": 0, "videos": 0, "pdf_instructions": 0, "other_files": 0}
        folders = [root_path]
        try:
            resource = await client.get_resource(root_path, limit=DISK_PAGE_SIZE)
            while folders:
                folder = folders.pop()
                folder_resource = resource if folder == root_path else await client.get_resource(
                    folder, limit=DISK_PAGE_SIZE
                )
                embedded = folder_resource.get("_embedded", {})
                if not isinstance(embedded, dict):
                    raise RuntimeError("Ответ Yandex Disk не содержит содержимое папки")
                total = int(embedded.get("total", 0))
                items = embedded.get("items", [])
                while len(items) < total:
                    page = await client.get_resource(
                        folder, limit=DISK_PAGE_SIZE, offset=len(items)
                    )
                    page_embedded = page.get("_embedded", {})
                    if not isinstance(page_embedded, dict):
                        raise RuntimeError("Ответ Yandex Disk не содержит страницу папки")
                    page_items = page_embedded.get("items", [])
                    if not page_items:
                        raise RuntimeError("Yandex Disk вернул неполную страницу папки")
                    items.extend(page_items)
                for item in items:
                    if not isinstance(item, dict):
                        continue
                    if item.get("type") == "dir":
                        item_path = item.get("path")
                        if item_path:
                            folders.append(str(item_path))
                        continue
                    name = str(item.get("name", ""))
                    extension = "." + name.rsplit(".", 1)[-1].lower() if "." in name else ""
                    if extension in SLIDE_EXTENSIONS:
                        counts["slides"] += 1
                    elif extension in VIDEO_EXTENSIONS:
                        counts["videos"] += 1
                    elif extension == ".pdf":
                        counts["pdf_instructions"] += 1
                    else:
                        counts["other_files"] += 1
        except RuntimeError:
            logger.warning(
                "Папка или её содержимое недоступны, строка пропущена; пересчет остальных продолжается | designer=%s | date=%s",
                designer,
                work_date.strftime("%d.%m.%Y"),
            )
            return None
        return DesignerOutputRow(work_date, designer, **counts)

    def _folder_path(self, designer: str, folder_date: date) -> str:
        """Формирует путь папки дизайнера в формате исходного сценария."""

        return f"{self.config.root_path}/{designer}/{folder_date.strftime('%d.%m.%Y')}"
