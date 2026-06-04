import logging

import pandas as pd

from src_oop.jobs.calculation_of_purchases_china.config import delivery_calculation_china
from src_oop.jobs.calculation_of_purchases_china.orders_white_balance_analytics import (
    OrdersWhiteBalanceAnalyticsService,
)
from src_oop.jobs.calculation_of_purchases_china.ved_balance_analytics import (
    VedBalanceAnalyticsService,
)

logger = logging.getLogger(__name__)


def _build_combined_balance_with_ved() -> tuple[pd.DataFrame, VedBalanceAnalyticsService]:
    """
    Собирает объединенный DataFrame по белым заказам и VED без записи в Google Sheets.

    Возвращает:
        Кортеж из объединенного DataFrame и экземпляра `VedBalanceAnalyticsService`,
        который уже содержит вспомогательные методы подготовки и выгрузки результата.

    Зачем выделено отдельно:
        Тестовый и production-режимы должны строить один и тот же combined DataFrame.
        Отличаться между ними должен только целевой лист выгрузки.
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

    logger.info("balance_df shape: %s", balance_df.shape)
    logger.info("ved_balance_df shape: %s", ved_balance_df.shape)
    logger.info("combined_balance_df shape: %s", combined_balance_df.shape)
    logger.info("VED missing columns vs balance_df: %s", alignment_result.missing_columns)
    logger.info("VED extra columns vs balance_df: %s", alignment_result.extra_columns)

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


def update_orders_white_balance_analytics() -> None:
    """Запускает штатный расчет аналитики платежей по белым заказам."""
    service = OrdersWhiteBalanceAnalyticsService()
    df_balance = service.run()
    logger.info("Orders white balance analytics updated: %s.", df_balance.shape)


def update_test_balance_with_ved() -> None:
    """
    Выполняет тестовый pipeline объединения `balance_df` и `ved_balance_df`.

    Что делает функция:
    - считает обычный `balance_df` по белым заказам, но не выгружает его
      в production-лист;
    - отдельно считает `ved_balance_df` по таблице ВЭД;
    - приводит VED-часть к структуре `balance_df`;
    - объединяет оба результата через `pd.concat`;
    - пишет объединенный DataFrame только в `delivery_calculation_china / test_sheet`.

    Почему функция нужна отдельно:
    - это изолированная точка запуска для отладки VED-логики;
    - она не меняет поведение штатного `update_orders_white_balance_analytics`;
    - ее удобно вызывать из CLI, не включая VED в production-поток раньше времени.

    Что считается нормальным результатом:
    - `balance_df` строится как раньше;
    - `ved_balance_df` успешно приводится к той же структуре;
    - в логах нет критических ошибок валидации;
    - тестовая выгрузка происходит только в `test_sheet`.
    """
    combined_balance_df, ved_service = _build_combined_balance_with_ved()

    # Финальная запись идет только в тестовый лист, чтобы безопасно проверить результат.
    df_upload = ved_service.prepare_dataframe_for_upload(combined_balance_df)
    ved_service.upload_to_test_sheet(df_upload)


def update_payments_analyze_with_ved() -> None:
    """
    Выполняет production pipeline объединения `balance_df` и `ved_balance_df`.

    Что делает функция:
    - считает white-only часть без отдельной production-выгрузки;
    - отдельно считает VED-часть;
    - выравнивает VED по структуре white-аналитики;
    - объединяет обе части в один итоговый DataFrame;
    - пишет combined результат в `delivery_calculation_china / payments_analyze_sheet`.

    Ограничение:
        Функция не меняет white-only сценарий и не трогает тестовую combined-выгрузку.
        Это отдельная production-команда для webhook и CLI.
    """
    combined_balance_df, ved_service = _build_combined_balance_with_ved()
    df_upload = ved_service.prepare_dataframe_for_upload(combined_balance_df)
    ved_service.upload_to_sheet(
        df_upload=df_upload,
        target_sheet_name=delivery_calculation_china["payments_analyze_sheet"],
    )
