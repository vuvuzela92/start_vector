from src_oop.jobs.autopilot.config import unit_gs, autopilot_gs
from src_oop.core.my_gspread import GoogleTabs

import pandas as pd

class Autopilot:
    def __init__(self):
        # Сохраняем только конфигурацию, не создавая подключений сразу
        self._table_from_name = unit_gs.get("title")
        self._sheet_from_name = unit_gs.get("unit_sheet")
        
        self._table_to_name = autopilot_gs.get("title")
        self._sheet_to_name = autopilot_gs.get("ic_sheet")
        
        # Здесь будем хранить сами объекты соединений после их активации
        self._google_connect_from = None
        self._google_connect_to = None

    @property
    def google_connect_from(self):
        """Ленивое подключение к исходной таблице"""
        if self._google_connect_from is None:
            print(f"Инициализация подключения к {self._table_from_name}")
            self._google_connect_from = GoogleTabs(self._table_from_name, self._sheet_from_name)
        return self._google_connect_from

    @property
    def google_connect_to(self):
        """Ленивое подключение к целевой таблице"""
        if self._google_connect_to is None:
            print(f"Инициализация подключения к {self._table_to_name}")
            self._google_connect_to = GoogleTabs(self._table_to_name, self._sheet_to_name)
        return self._google_connect_to

    def get_unit_data(self):
        # Теперь обращение к self.google_connect_from вызовет создание подключения
        unit_table = self.google_connect_from 
        unit_data = unit_table.sheet_title.get_all_values()
        
        headers = unit_data[0]
        rows = unit_data[1:]
        df = pd.DataFrame(rows, columns=headers)
        
        # Приводим типы
        df['Артикул'] = pd.to_numeric(df['Артикул'], errors='coerce')
        
        df_short = df[['Артикул', 'Цена для клиента', 'Мар', 'ФБС']]
        return df_short