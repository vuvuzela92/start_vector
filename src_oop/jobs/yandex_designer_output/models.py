"""Модели данных job учета дизайнерской выработки."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date


@dataclass(frozen=True, slots=True)
class DesignerOutputRow:
    """Представляет одну строку статистики для Google Sheets.

    Бизнес-сценарий: строка связывает дату рабочей папки, дизайнера и
    количество сданных слайдов, видео, PDF-инструкций и прочих файлов.
    """

    work_date: date
    designer: str
    slides: int
    videos: int
    pdf_instructions: int
    other_files: int

    def as_dict(self) -> dict[str, object]:
        """Преобразует дневную выработку дизайнера в колонки новой вкладки."""

        return {
            "дата": self.work_date.strftime("%d.%m.%Y"),
            "дизайнер": self.designer,
            "слайды": self.slides,
            "видео": self.videos,
            "PDF-инструкции": self.pdf_instructions,
            "прочие файлы": self.other_files,
        }
