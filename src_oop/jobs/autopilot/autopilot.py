import pandas as pd

from src_oop.core.my_gspread import GoogleTabs
from src_oop.jobs.autopilot.config import autopilot_gs, unit_gs


class Autopilot:
    def __init__(self):
        """
        Инициализирует старый сервис переноса индивидуальных условий.

        Бизнес-логика:
        хранит имена таблиц UNIT и ПУ, но не открывает Google Sheets до первого обращения,
        чтобы не создавать внешние соединения при простом импорте модуля.
        """
        self._table_from_name = unit_gs.get("title")
        self._sheet_from_name = unit_gs.get("unit_sheet")

        self._table_to_name = autopilot_gs.get("title")
        self._sheet_to_name = autopilot_gs.get("ic_sheet")

        self._google_connect_from = None
        self._google_connect_to = None

    @property
    def google_connect_from(self):
        """
        Лениво открывает исходный лист UNIT.

        Бизнес-логика:
        UNIT является источником индивидуальных условий, которые переносятся в ПУ.
        Подключение создается только при фактическом чтении данных.
        """
        if self._google_connect_from is None:
            print(f"Инициализация подключения к {self._table_from_name}")
            self._google_connect_from = GoogleTabs(
                self._table_from_name,
                self._sheet_from_name,
            )
        return self._google_connect_from

    @property
    def google_connect_to(self):
        """
        Лениво открывает целевой лист ПУ для индивидуальных условий.

        Бизнес-логика:
        сюда записывается результат переноса UNIT-данных для работы автопилота.
        """
        if self._google_connect_to is None:
            print(f"Инициализация подключения к {self._table_to_name}")
            self._google_connect_to = GoogleTabs(
                self._table_to_name,
                self._sheet_to_name,
            )
        return self._google_connect_to

    def get_unit_data(self):
        """
        Читает и подготавливает из UNIT поля индивидуальных условий.

        Бизнес-логика:
        выбирает только нужные для листа ИУ_ИНФО колонки: артикул, цену для клиента,
        маржу и ФБС. Артикул приводится к числу для корректной дальнейшей работы в ПУ.
        """
        unit_table = self.google_connect_from
        unit_data = unit_table.sheet_title.get_all_values()

        headers = unit_data[0]
        rows = unit_data[1:]
        df = pd.DataFrame(rows, columns=headers)

        df["Артикул"] = pd.to_numeric(df["Артикул"], errors="coerce")

        df_short = df[["Артикул", "Цена для клиента", "Мар", "ФБС"]]
        return df_short
