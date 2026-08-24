import logging

import pandas as pd

from src_oop.jobs.calculation_of_purchases_china.config import (
    payments_calendar,
)
from src_oop.jobs.calculation_of_purchases_china.orders_white_balance_analytics import (
    OrdersWhiteBalanceAnalyticsService,
)
from src_oop.jobs.calculation_of_purchases_china.ved_balance_analytics import (
    VedBalanceAnalyticsService,
)

logger = logging.getLogger(__name__)


def _build_combined_balance_with_ved() -> tuple[pd.DataFrame, VedBalanceAnalyticsService]:
    """
    Собирает единый DataFrame платежного календаря без записи в Google Sheets.

    Возвращает:
        Кортеж из объединенного DataFrame и экземпляра `VedBalanceAnalyticsService`,
        который уже содержит вспомогательные методы подготовки и выгрузки результата.

    Зачем выделено отдельно:
        Основной production-сценарий сначала считает white- и VED-части
        отдельно, а затем объединяет их в единую структуру платежного
        календаря. Вынесение в helper защищает этот шаг от дублирования
        и позволяет держать orchestration в одном месте.
    """
    orders_service = OrdersWhiteBalanceAnalyticsService()
    ved_service = VedBalanceAnalyticsService()

    balance_df = orders_service.run(upload=False)
    ved_balance_df = ved_service.run()

    alignment_result = ved_service.align_to_balance_columns(
        ved_balance_df=ved_balance_df,
        balance_columns=balance_df.columns.tolist(),
    )

    combined_balance_df = pd.concat(
        [balance_df, alignment_result.df_aligned],
        ignore_index=True,
    )

    duplicate_risk_df = ved_service.build_duplicate_risk_report(ved_balance_df)
    duplicate_stage_numbers = ved_service.get_duplicate_risk_stage_numbers()

    logger.info("Размер balance_df после расчета белых заказов: %s", balance_df.shape)
    logger.info("Размер ved_balance_df после расчета ВЭД: %s", ved_balance_df.shape)
    logger.info("Размер combined_balance_df после объединения: %s", combined_balance_df.shape)
    logger.info(
        "Колонки ВЭД, отсутствующие в структуре balance_df: %s",
        alignment_result.missing_columns,
    )
    logger.info(
        "Лишние колонки ВЭД относительно структуры balance_df: %s",
        alignment_result.extra_columns,
    )

    if duplicate_stage_numbers:
        logger.warning(
            "Найдены этапы VED с совпадающими mappings исходных колонок: %s. "
            "Это важно проверить бизнесом, потому что такие этапы могут "
            "сформировать одинаковые строки платежей.",
            duplicate_stage_numbers,
        )

    if not duplicate_risk_df.empty:
        logger.warning(
            "Найдены потенциальные дубли по этапам VED с совпадающими mappings: %s строк. "
            "Это не исправляется автоматически и требует проверки на стороне бизнес-логики.",
            duplicate_risk_df.shape[0],
        )

    return combined_balance_df, ved_service


def transport_quarterly_plan_to_pivot() -> None:
    """Переносит поквартальный план в сводную таблицу по поставщикам."""
    from src_oop.jobs.calculation_of_purchases_china.calculation_by_china_suppliers import (
        CalculationByChinaSuppliers,
    )

    calculation = CalculationByChinaSuppliers()
    df_quarterly = calculation.get_quarterly_plan_data()

    if df_quarterly.empty:
        logger.warning("Поквартальный план пуст. Обновление целевого листа пропущено.")
        return

    calculation.set_data(calculation.target_connect, df_quarterly)
    logger.info("Поквартальный план перенесен в сводный лист по поставщикам.")


def update_payments_analyze_with_ved() -> None:
    """
    Обновляет production-лист платежного календаря по объединенной white + VED аналитике.

    Что делает функция:
    - считает часть по белым заказам без промежуточной выгрузки;
    - считает VED-часть по листу `ОТЧЁТ_2.0`;
    - приводит VED-данные к общей структуре платежной аналитики;
    - объединяет обе части в один итоговый DataFrame;
    - добавляет служебные колонки для платежного календаря;
    - записывает результат в `payments_calendar / Аналитика_платежей`.

    Это основной и единственный актуальный entrypoint для CLI и webhook.
    Старые вспомогательные режимы выгрузки убраны, чтобы в проекте
    оставался один понятный сценарий обновления боевой аналитики.
    """
    combined_balance_df, ved_service = _build_combined_balance_with_ved()
    df_upload = ved_service.prepare_dataframe_for_upload(combined_balance_df)
    ved_service.upload_to_sheet(
        df_upload=df_upload,
        target_table_name=payments_calendar["title"],
        target_sheet_name=payments_calendar["analytic_sheet"],
    )
