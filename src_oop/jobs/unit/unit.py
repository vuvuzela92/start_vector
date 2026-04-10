from src_oop.core.database import Database
from src_oop.jobs.unit.queries import query_adv_spend

class UnitEconomics:
    def __init__(self):
        self.database = Database()
    
    def get_adv_spend(self):
        """Получаем данные о рекламных расходах по статьям за вчерашний день"""
        return self.database.read_sql_to_dict(query_adv_spend)