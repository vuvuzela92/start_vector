"""Бизнес-сервис учета выработки дизайнеров."""

from __future__ import annotations

import asyncio
import logging
from datetime import date, timedelta

from src_oop.jobs.yandex_designer_output.api_client import YandexDiskClient
from src_oop.jobs.yandex_designer_output.config import YandexDesignerOutputConfig
from src_oop.jobs.yandex_designer_output.models import DesignerOutputRow
from src_oop.jobs.yandex_designer_output.repository import DesignerOutputRepository

logger = logging.getLogger(__name__)


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

    async def run(self) -> int:
        """Запускает полный сценарий ежедневной выработки дизайнеров.

        Бизнес-сценарий: список дизайнеров читается из корневой папки,
        для каждого обеспечивается папка на завтра, затем по сегодняшним
        папкам собирается число файлов и добавляется дневной срез в таблицу.
        """

        today = date.today()
        tomorrow = today + timedelta(days=1)
        async with YandexDiskClient(self.config) as client:
            root = await client.get_resource(self.config.root_path)
            designers = self._extract_designers(root)
            await asyncio.gather(
                *(client.create_folder(self._folder_path(designer, tomorrow)) for designer in designers)
            )
            rows = await asyncio.gather(
                *(self._get_designer_row(client, designer, today) for designer in designers)
            )

        prepared_rows = [row for row in rows if row is not None]
        written_rows = self.repository.save(prepared_rows)
        logger.info(
            "Сценарий выработки дизайнеров завершён | designers=%s | written_rows=%s | date=%s",
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
        """Получает количество файлов дизайнера за указанную дату."""

        try:
            resource = await client.get_resource(self._folder_path(designer, work_date))
        except RuntimeError:
            logger.warning(
                "Папка дизайнера недоступна, строка пропущена | designer=%s | date=%s",
                designer,
                work_date.strftime("%d.%m.%Y"),
            )
            return None
        embedded = resource.get("_embedded", {})
        total = embedded.get("total", 0) if isinstance(embedded, dict) else 0
        return DesignerOutputRow(work_date, designer, int(total))

    def _folder_path(self, designer: str, folder_date: date) -> str:
        """Формирует путь папки дизайнера в формате исходного сценария."""

        return f"{self.config.root_path}/{designer}/{folder_date.strftime('%d.%m.%Y')}"
