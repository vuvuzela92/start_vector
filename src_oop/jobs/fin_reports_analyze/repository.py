import pandas as pd
from sqlalchemy import text
from src_oop.jobs.fin_reports_analyze.queries import query_wild_frm_products, query_cash_flow_writeoffs, query_monthly_report, query_stock_analyze, query_outcomes_detalize, query_fin_deductions_mv, query_daily_fin_reports_deductions_agg

from src_oop.core.database import Database

class FinReportsAnalyze:
    """Класс для получения аналитики по фин отчетам из БД."""
    
    def __init__(self, engine=None):
        if engine:
            self.engine = engine
        else:
            self.engine = Database.get_engine()

    def get_df_from_db(self, query: str):
        """Универсальная функция для получения DataFrame из БД по произвольному SQL-запросу"""
        query = text(query)
        # Возвращаем результат в виде DataFrame от SQL-запроса
        return Database.read_sql_to_dataframe(query)  
    
    def get_update_cash_flow_writeoffs(self):
        """ Получение данных по списаниям денежных средств из 1С Анализ_фин_отчетов_Вектор для выгрузки в гугл-таблицу"""
        query = text(query_cash_flow_writeoffs)
        return Database.read_sql_to_dataframe(query)  
            
    def get_monthly_profit_report(self) -> pd.DataFrame:
        """Формирует отчет по чистой прибыли и расходам за месяц."""
        # Оборачиваем строку в text() для SQLAlchemy 2.0+
        query = text(query_monthly_report)
        # Возвращаем результат в виде DataFrame от SQL-запроса
        return Database.read_sql_to_dataframe(query)
        

    def get_outcomes_detalize(self):
        """Таблица Расходы: Детализация"""
        query = text(query_outcomes_detalize)
        # Возвращаем результат в виде DataFrame от SQL-запроса
        return Database.read_sql_to_dataframe(query)


    def get_fin_deductions_mv(self):
        "Удержания: Детализация"
        query = text(query_fin_deductions_mv)
        # Возвращаем результат в виде DataFrame от SQL-запроса
        return Database.read_sql_to_dataframe(query)


    def get_daily_fin_reports_deductions_agg(self):
        "Удержания: Детализация"
        query = text(query_daily_fin_reports_deductions_agg)
        # Возвращаем результат в виде DataFrame от SQL-запроса
        return Database.read_sql_to_dataframe(query)
    
    def get_wild_frm_products(self):
            "Удержания: Детализация"
            query = text(query_wild_frm_products)
            # Возвращаем результат в виде DataFrame от SQL-запроса
            df = Database.read_sql_to_dataframe(query)
            return df['id'].to_list()
                     