import logging
import pandas as pd

from src_oop.jobs.conditional_calculations.repository import GetDataFromDB
from src_oop.jobs.conditional_calculations.tables_scheme import conditional_calculations
from src_oop.core.database import Database

 
logger = logging.getLogger(__name__)

class ConditionalCalculations:
    """Класс для создания Условного расчета"""
    def __init__(self, repo: GetDataFromDB, engine):
        self.engine = engine
        self.repo = GetDataFromDB(engine)


    def conditional_calculation_to_db_run(self, days_ago: int = 30, days_to: int = 1):
        """Функция получает данные по Условному расчету и добавляет их в БД"""
        df = self.repo.execute_conditional_calculations(days_ago, days_to)

        if df.empty:
            logger.warning("Нет данных для записи")
            return
        
        scheme = conditional_calculations.get("columns")
        table = conditional_calculations.get("title")
        keys = conditional_calculations.get("unique_keys")

        Database.sync_data_to_postgres(
            engine=self.engine,
            table_name=table,
            data=df,
            schema_definition=scheme,
            unique_keys=keys
        )
        

