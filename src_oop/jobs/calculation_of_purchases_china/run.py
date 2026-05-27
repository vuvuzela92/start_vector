import logging

from src_oop.jobs.calculation_of_purchases_china.calculation_by_china_suppliers import CalculationByChinaSuppliers
from src_oop.jobs.calculation_of_purchases_china.orders_white_balance_analytics import (
    OrdersWhiteBalanceAnalyticsService,
)

logger = logging.getLogger(__name__)


def transport_quarterly_plan_to_pivot() -> None:
    calculation = CalculationByChinaSuppliers()
    df_quarterly = calculation.get_quarterly_plan_data()

    if df_quarterly.empty:
        logger.warning("Поквартальный план пуст. Обновление целевого листа пропущено.")
        return

    calculation.set_data(calculation.target_connect, df_quarterly)
    logger.info("Поквартальный план перенесен в сводный лист по поставщикам.")


def update_orders_white_balance_analytics() -> None:
    service = OrdersWhiteBalanceAnalyticsService()
    df_balance = service.run()
    logger.info("Orders white balance analytics updated: %s.", df_balance.shape)
