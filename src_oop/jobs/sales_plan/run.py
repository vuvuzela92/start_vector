from __future__ import annotations

import logging

from src_oop.jobs.sales_plan.repository import (
    SalesPlanAccountingCategoryRepository,
    SalesPlanManagerReferenceRepository,
    SalesWildStatusDailyRepository,
)

logger = logging.getLogger(__name__)


def sync_sales_plan_manager_reference_to_db() -> None:
    """Сохраняет ежедневный снимок справочника `Категория-Менеджер` в PostgreSQL.

    Бизнес-сценарий:
    функция запускает первый слой для будущего плана продаж. Она не считает
    сам план, а фиксирует в БД актуальное состояние предметов, менеджеров и
    `wild` из ПУ, чтобы затем можно было строить расчеты и исторические итоги
    по управленческому справочнику.
    """

    repository = SalesPlanManagerReferenceRepository()
    result = repository.sync_snapshot()
    logger.info(
        "Загрузка справочника категорий и менеджеров завершена | snapshot_date=%s | source_rows=%s | rows_after_cleanup=%s | duplicate_rows=%s | written_rows=%s",
        result.snapshot_date,
        result.source_rows,
        result.rows_after_cleanup,
        result.duplicate_rows,
        result.written_rows,
    )


def sync_sales_plan_accounting_category_reference_to_db() -> None:
    """Синхронизирует текущий справочник учетной категории по `wild` в PostgreSQL.

    Бизнес-сценарий:
    функция поддерживает отдельный бизнес-слой для плана продаж, где каждый
    `wild` должен быть связан только с одной учетной категорией. Таблица
    обновляется по вкладке `Поквартально` без исторического накопления и
    сохраняет `created_at` для первого появления товара в справочнике.
    """

    repository = SalesPlanAccountingCategoryRepository()
    result = repository.sync_reference()
    logger.info(
        "Загрузка справочника учетной категории завершена | source_rows=%s | rows_after_cleanup=%s | inserted_rows=%s | updated_rows=%s | deleted_rows=%s",
        result.source_rows,
        result.rows_after_cleanup,
        result.inserted_rows,
        result.updated_rows,
        result.deleted_rows,
    )


def sync_sales_wild_status_daily_to_db() -> None:
    """Сохраняет ежедневный snapshot активности `wild` в PostgreSQL.

    Бизнес-сценарий:
    функция запускает накопление дневных статусов для будущего правила
    обнуления плана продаж. Источником служит `Поквартально`, а итоговая
    таблица хранит по каждому `wild` признак `is_active` на конкретную дату.
    """

    repository = SalesWildStatusDailyRepository()
    result = repository.sync_snapshot()
    logger.info(
        "Загрузка дневного snapshot статусов wild завершена | snapshot_date=%s | source_rows=%s | rows_after_cleanup=%s | duplicate_rows=%s | written_rows=%s",
        result.snapshot_date,
        result.source_rows,
        result.rows_after_cleanup,
        result.duplicate_rows,
        result.written_rows,
    )
