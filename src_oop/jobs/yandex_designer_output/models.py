"""Модели данных job учета дизайнерской выработки."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date


@dataclass(frozen=True, slots=True)
class DesignerOutputRow:
    """Представляет одну строку статистики для Google Sheets.

    Бизнес-сценарий: строка связывает дату рабочей папки, дизайнера и число
    файлов, доступных в его папке за день.
    """

    work_date: date
    designer: str
    file_count: int

    def as_dict(self) -> dict[str, object]:
        """Преобразует статистику в колонки старой Google-таблицы."""

        return {
            "дата": self.work_date.strftime("%d.%m.%Y"),
            "дизайнер": self.designer,
            "количество": self.file_count,
        }
