# src_oop/jobs/analytics/repository.py
import logging

from sqlalchemy import inspect, text
from sqlalchemy.types import TypeEngine

from src_oop.core.database import Database
from src_oop.jobs.conditional_calculations.tables_scheme import conditional_calculations

logger = logging.getLogger(__name__)


class ConditionalCalculationsRepository:
    def __init__(self, days_ago: int = 207, days_to: int = 1):
        self.days_ago = days_ago
        self.days_to = days_to
    
    def execute_conditional_calculations(self):
        """Пересчитывает дневную витрину Условного расчета за указанное окно.

        Бизнес-сценарий: собирает базовые показатели продаж из
        `orders_articles_analyze` и дополняет их финансовыми удержаниями WB из
        `daily_fin_reports_full` по бизнес-дате фин. отчета `date_from`.
        Штрафы, итог к оплате и кредитные перечисления считаются
        на уровне дня и аккаунта, чтобы в БД и выгрузке была одна строка
        `date + account` с полной финансовой картиной.
        """
        query = text(f"""
        WITH orders AS (
            SELECT
                UPPER(TRIM(o.account)) AS account,
                SUM(o.orders_sum_rub) AS orders_sum,
                ROUND(SUM(o.sales_revenue_rep)) AS sales_sum,
                ROUND(SUM(o.profit_by_cond_orders)) AS profit_by_ind_cond_orders,
                ROUND(SUM(o.profit_by_cond_sales)) AS profit_by_ind_cond_sales,
                SUM(o.sales_count_rep) AS sales_count,
                SUM(o.orders_count) AS order_count,
                SUM(o.adv_spend) AS adv_spend,
                SUM(o.bonuses) AS bonuses,
                ROUND(
                    SUM(o.profit_by_cond_sales) - SUM(o.adv_spend)
                ) AS profit_cond_sales_minus_adv_spend,
                ROUND(SUM(o.orders_count * o.purchase_price)) AS cost_price_orders,
                ROUND(SUM(o.sales_count_rep * o.purchase_price)) AS cost_price_sales,
                SUM(o.profit_by_orders) AS general_profit_orders,
                o.date
            FROM orders_articles_analyze o
            WHERE o.date BETWEEN CURRENT_DATE - INTERVAL '{self.days_ago} days'
                AND CURRENT_DATE - INTERVAL '{self.days_to} days'
                AND o.account != '0'
                AND o.account IS NOT NULL
                AND o.account != 'NaN'
            GROUP BY
                UPPER(TRIM(o.account)),
                o.date
        ),
        fin_report AS (
            SELECT
                DATE(f.date_from) AS date,
                UPPER(TRIM(f.account)) AS account,
                ROUND(SUM(COALESCE(f.penalty, 0))) AS penalties,
                ROUND(
                    SUM(
                        CASE
                            WHEN f.doc_type_name = 'Продажа'
                                THEN COALESCE(f.ppvz_for_pay, 0)
                            ELSE 0
                        END
                    )
                    - SUM(
                        CASE
                            WHEN f.doc_type_name = 'Возврат'
                                THEN COALESCE(f.ppvz_for_pay, 0)
                            ELSE 0
                        END
                    )
                    - SUM(COALESCE(f.delivery_rub, 0))
                    - SUM(COALESCE(f.penalty, 0))
                    - SUM(COALESCE(f.deduction, 0))
                ) AS total_to_pay,
                ROUND(
                    SUM(
                        CASE
                            WHEN f.bonus_type_name ILIKE '%заём%'
                                OR f.bonus_type_name ILIKE '%займ%'
                                OR f.bonus_type_name ILIKE '%кредит%'
                                THEN COALESCE(f.deduction, 0)
                            ELSE 0
                        END
                    )
                ) AS credit_transfers
            FROM daily_fin_reports_full f
            WHERE DATE(f.date_from) BETWEEN CURRENT_DATE - INTERVAL '{self.days_ago} days'
                AND CURRENT_DATE - INTERVAL '{self.days_to} days'
                AND f.account IS NOT NULL
                AND f.account != '0'
                AND f.account != 'NaN'
            GROUP BY
                DATE(f.date_from),
                UPPER(TRIM(f.account))
        )
        SELECT
            COALESCE(orders.account, fin_report.account) AS account,
            COALESCE(orders.orders_sum, 0) AS orders_sum,
            COALESCE(orders.sales_sum, 0) AS sales_sum,
            COALESCE(
                orders.profit_by_ind_cond_orders,
                0
            ) AS profit_by_ind_cond_orders,
            COALESCE(
                orders.profit_by_ind_cond_sales,
                0
            ) AS profit_by_ind_cond_sales,
            COALESCE(orders.sales_count, 0) AS sales_count,
            COALESCE(orders.order_count, 0) AS order_count,
            COALESCE(orders.adv_spend, 0) AS adv_spend,
            COALESCE(orders.bonuses, 0) AS bonuses,
            COALESCE(
                orders.profit_cond_sales_minus_adv_spend,
                0
            ) AS profit_cond_sales_minus_adv_spend,
            COALESCE(orders.cost_price_orders, 0) AS cost_price_orders,
            COALESCE(orders.cost_price_sales, 0) AS cost_price_sales,
            COALESCE(orders.general_profit_orders, 0) AS general_profit_orders,
            COALESCE(fin_report.penalties, 0) AS penalties,
            COALESCE(fin_report.total_to_pay, 0) AS total_to_pay,
            COALESCE(fin_report.credit_transfers, 0) AS credit_transfers,
            COALESCE(orders.date, fin_report.date) AS date
        FROM orders
        FULL OUTER JOIN fin_report
            ON fin_report.date = orders.date
            AND fin_report.account = orders.account;
    """)

        return Database.read_sql_to_dataframe(query)
    

    def get_conditional_calculations(self):
        """Получает данные Условного расчета для выгрузки в Google Sheets.

        Бизнес-сценарий: отдает уже сохраненную в БД витрину, включая штрафы,
        итог к оплате и кредитные перечисления, чтобы лист был консистентен с
        таблицей `conditions_calculation`. Порядок колонок зафиксирован явно:
        старые поля остаются на прежних местах, а новые финансовые метрики
        добавляются в конец, чтобы не ломать формулы в Google Sheets.
        """
        query = text(f"""
                    SELECT
                        cc.account,
                        cc.orders_sum,
                        cc.sales_sum,
                        cc.profit_by_ind_cond_orders,
                        cc.profit_by_ind_cond_sales,
                        cc.sales_count,
                        cc.order_count,
                        cc.adv_spend,
                        cc.bonuses,
                        cc.profit_cond_sales_minus_adv_spend,
                        cc.cost_price_orders,
                        cc.cost_price_sales,
                        cc.general_profit_orders,
                        cc.date,
                        cc.penalties,
                        cc.total_to_pay,
                        cc.credit_transfers
                    FROM conditions_calculation cc
        WHERE cc.date >= '2025-12-01'
        ORDER BY cc.date ASC,
            cc.account;
        """)
        return Database.read_sql_to_dataframe(query)

    def ensure_conditions_calculation_columns(self) -> None:
        """Добавляет недостающие колонки в существующую таблицу Условного расчета.

        Бизнес-сценарий: историческая таблица `conditions_calculation` могла
        быть создана до появления финансовых полей. Метод доводит схему до
        актуальной перед upsert, чтобы новые штрафы, итог к оплате и кредитные
        перечисления записывались без ручной миграции.
        """
        engine = Database.get_engine()
        table_name = conditional_calculations["title"]
        inspector = inspect(engine)
        if not inspector.has_table(table_name):
            logger.info(
                "Таблица Условного расчета еще не создана: схему создаст "
                "общий механизм записи | table=%s",
                table_name,
            )
            return

        existing_columns = set(self._get_existing_column_types(table_name))

        for column_name, column_type in conditional_calculations["columns"].items():
            if column_name in existing_columns:
                continue

            compiled_type = self._compile_sqlalchemy_type(
                column_type=column_type,
                dialect=engine.dialect,
            )
            alter_sql = text(
                f'ALTER TABLE "{table_name}" '
                f'ADD COLUMN IF NOT EXISTS "{column_name}" {compiled_type}'
            )
            with engine.begin() as connection:
                connection.execute(alter_sql)
            logger.info(
                "В таблицу Условного расчета добавлена недостающая колонка | "
                "table=%s | column=%s | type=%s",
                table_name,
                column_name,
                compiled_type,
            )

    def _get_existing_column_types(self, table_name: str) -> dict[str, TypeEngine]:
        """Читает фактические колонки витрины перед безопасным расширением схемы.

        Бизнес-правило: job добавляет только отсутствующие поля и не меняет уже
        существующие колонки, чтобы не затронуть накопленные расчеты.
        """
        inspector = inspect(Database.get_engine())
        return {
            column["name"]: column["type"]
            for column in inspector.get_columns(table_name)
        }

    def _compile_sqlalchemy_type(self, column_type, dialect) -> str:
        """Преобразует SQLAlchemy-тип в SQL для `ALTER TABLE`.

        Бизнес-сценарий: схема Условного расчета описана теми же типами, что и
        общий upsert, поэтому helper позволяет переиспользовать ее для
        автоматического добавления финансовых колонок.
        """
        resolved_type = column_type
        if isinstance(column_type, type) and issubclass(column_type, TypeEngine):
            resolved_type = column_type()
        return resolved_type.compile(dialect=dialect)
