import logging

from src_oop.jobs.calculation_of_purchases_china.calculation_by_china_suppliers import CalculationByChinaSuppliers

logger = logging.getLogger(__name__)


def transport_quarterly_plan_to_pivot() -> None:
    calculation = CalculationByChinaSuppliers()
    df_quarterly = calculation.get_quarterly_plan_data()

    if df_quarterly.empty:
        logger.warning("Поквартальный план пуст. Обновление целевого листа пропущено.")
        return

    calculation.set_data(calculation.target_connect, df_quarterly)
    logger.info("Поквартальный план перенесен в сводный лист по поставщикам.")
