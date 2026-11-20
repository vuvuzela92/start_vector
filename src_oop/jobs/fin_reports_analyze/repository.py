import logging
from datetime import date, timedelta

import pandas as pd
from sqlalchemy import text
from src_oop.jobs.fin_reports_analyze.queries import (
    query_cash_flow_writeoffs,
    query_daily_fin_reports_deductions_agg,
    query_fin_deductions_mv,
    query_fin_reports_art_analyze,
    query_fin_reports_art_analyze_export,
    query_monthly_report,
    query_outcomes_detalize,
    query_stock_analyze,
    query_wild_frm_products,
)

from src_oop.core.database import Database

logger = logging.getLogger(__name__)

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

    def get_fin_reports_art_analyze(self) -> pd.DataFrame:
        """Читает готовую витрину построчных финансовых отчётов для публикации.

        Бизнес-сценарий: после пересчёта PostgreSQL-витрины возвращает явный
        набор финансовых показателей в порядке дат, чтобы выгрузка в лист
        «по_отчетно» содержала только актуальные поля отчёта.
        """
        return Database.read_sql_to_dataframe(
            text(query_fin_reports_art_analyze_export)
        )
        

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

    def refresh_fin_reports_art_analyze(self, *, full_history: bool = False) -> int:
        """Создаёт или обновляет витрину финансового анализа по отчётам реализации.

        При первом запуске загружает историю с 1 января 2025 года. Если таблица
        уже содержит только более позднюю историю, однократно дозагружает начало
        периода. Обновляет схему месяцем, выручкой без НДС и детализацией
        удержаний; ставка НДС определяется по кабинету и дате отчёта. В обычных
        запусках заменяет последние 14 завершённых дней. При `full_history=True`
        пересчитывает все отчёты с 1 января 2025 года помесячными транзакциями,
        чтобы применять правила небольшими этапами. Дополнительно относит
        удержания к рекламе, сервисам, кредитам или прочим основаниям, сохраняя
        исходные знаки сумм и не изменяя общую сумму удержаний. В «Итого к оплате»
        учитываются вычеты за логистику, штрафы, удержания, платную приёмку
        (`acceptance`), хранение, кешбэк, комиссию программы лояльности,
        изменение графика выплат и корректировку вознаграждения WB.
        """
        table_name = "public.fin_reports_art_analyze"
        seed_date = date(2025, 1, 1)
        insert_columns = (
            "account",
            "realizationreport_id",
            "date_from",
            "date_to",
            "report_create_dt",
            "report_type",
            "report_type_label",
            "currency",
            "agreed_discount_pct",
            "month",
            "revenue",
            "revenue_without_vat",
            "dashboard_sale",
            "loyalty_discount_compensation",
            "loyalty_points_withheld",
            "loyalty_program_cost",
            "payment_schedule_adjustment",
            "wb_reward_adjustment",
            "logistics",
            "storage_fee",
            "acquiring_fee",
            "penalties",
            "deductions",
            "payout",
            "total_to_pay",
            "wb_commission",
            "wb_commission_without_vat",
            "wb_commission_pct",
            "retail_price_disc",
            "paid_acceptance",
            "credit_transfers",
            "to_client_cancel",
            "from_client_cancel",
            "from_client_return",
            "to_client_sale",
            "purchase_cost_sales",
            "purchase_cost_returns",
            "purchase_cost_total",
            "margin_before_cost_pct",
            "gp_after_wb",
            "gp_after_wb_pct",
            "deductions_ads",
            "deductions_services",
            "deductions_credits",
            "deductions_other",
        )
        query_parameters = {"start_date": seed_date, "end_date": seed_date}
        create_table_query = (
            f"CREATE TABLE IF NOT EXISTS {table_name} AS "
            f"{query_fin_reports_art_analyze} WITH NO DATA"
        )

        with self.engine.begin() as connection:
            connection.execute(text(create_table_query), query_parameters)
            connection.execute(text(
                f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS month TEXT"
            ))
            connection.execute(text(
                f"ALTER TABLE {table_name} "
                "ADD COLUMN IF NOT EXISTS revenue_without_vat NUMERIC(18, 2)"
            ))
            dashboard_columns = {
                "report_create_dt": "DATE",
                "report_type": "SMALLINT",
                "report_type_label": "TEXT",
                "currency": "TEXT",
                "agreed_discount_pct": "NUMERIC(18, 2)",
                "dashboard_sale": "NUMERIC(18, 2)",
                "loyalty_discount_compensation": "NUMERIC(18, 2)",
                "loyalty_points_withheld": "NUMERIC(18, 2)",
                "loyalty_program_cost": "NUMERIC(18, 2)",
                "payment_schedule_adjustment": "NUMERIC(18, 2)",
                "wb_reward_adjustment": "NUMERIC(18, 2)",
                "wb_commission_without_vat": "NUMERIC(18, 2)",
                "deductions_ads": "NUMERIC(18, 2)",
                "deductions_services": "NUMERIC(18, 2)",
                "deductions_credits": "NUMERIC(18, 2)",
                "deductions_other": "NUMERIC(18, 2)",
            }
            for column_name, column_type in dashboard_columns.items():
                connection.execute(text(
                    f"ALTER TABLE {table_name} "
                    f"ADD COLUMN IF NOT EXISTS {column_name} {column_type}"
                ))
            connection.execute(text(
                f"UPDATE {table_name} "
                "SET month = TO_CHAR(date_to, 'MM-YYYY') "
                "WHERE month IS DISTINCT FROM TO_CHAR(date_to, 'MM-YYYY')"
            ))
            connection.execute(text(
                "CREATE INDEX IF NOT EXISTS ix_fin_reports_art_analyze_date_from "
                f"ON {table_name} (date_from)"
            ))

            earliest_date = connection.execute(text(
                f"SELECT MIN(date_from) FROM {table_name}"
            )).scalar_one()
            date_range = connection.execute(text(
                "SELECT CURRENT_DATE - 1 AS end_date, "
                "GREATEST(DATE '2025-01-01', CURRENT_DATE - 14) "
                "AS window_start"
            )).mappings().one()
            history_needs_backfill = earliest_date is None or earliest_date > seed_date
            start_date = (
                seed_date
                if full_history or history_needs_backfill
                else date_range["window_start"]
            )
            end_date = date_range["end_date"]
            parameters = {"start_date": start_date, "end_date": end_date}

        insert_query = text(
            f"INSERT INTO {table_name} ({', '.join(insert_columns)}) "
            f"{query_fin_reports_art_analyze}"
        )
        delete_query = text(
            f"DELETE FROM {table_name} "
            "WHERE date_from BETWEEN :start_date AND :end_date"
        )
        batch_start = start_date
        inserted_rows = 0
        while batch_start <= end_date:
            next_month = batch_start.replace(day=28) + timedelta(days=4)
            batch_end = min(
                next_month.replace(day=1) - timedelta(days=1),
                end_date,
            )
            batch_parameters = {
                "start_date": batch_start,
                "end_date": batch_end,
            }
            with self.engine.begin() as connection:
                connection.execute(delete_query, batch_parameters)
                result = connection.execute(insert_query, batch_parameters)
            batch_rows = result.rowcount or 0
            inserted_rows += batch_rows
            logger.info(
                "Обновлён месяц витрины финансовых отчётов | "
                "start_date=%s | end_date=%s | rows=%s",
                batch_start,
                batch_end,
                batch_rows,
            )
            batch_start = batch_end + timedelta(days=1)

        logger.info(
            "Витрина финансового анализа обновлена | table=%s | "
            "start_date=%s | end_date=%s | rows=%s",
            table_name,
            start_date,
            end_date,
            inserted_rows,
        )
        return inserted_rows
                     
