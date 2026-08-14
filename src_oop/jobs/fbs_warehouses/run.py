from __future__ import annotations

import asyncio
import json
import logging
import os
from pathlib import Path

import pandas as pd

from src_oop.jobs.fbs_warehouses.config import (
    ACCOUNT_ENV,
    IMPORT_SOURCE_PATH_ENV,
    OFFICE_ID_ENV,
    OUR_WAREHOUSE_ID_ENV,
    OUTPUT_PATH_ENV,
    WAREHOUSE_ID_ENV,
    WAREHOUSE_NAME_ENV,
)
from src_oop.jobs.fbs_warehouses.repository import FBSWarehousesRepository
from src_oop.jobs.fbs_warehouses.service import FBSWarehousesService

logger = logging.getLogger(__name__)

DEFAULT_CREATED_WAREHOUSE_PATH = Path(__file__).parent / "files" / "created_warehouse.json"
DEFAULT_SYNCED_WAREHOUSES_PATH = Path(__file__).parent / "files" / "synced_warehouses.json"


def _coerce_required_int(value: int | str | None, parameter_name: str) -> int:
    """Приводит обязательный числовой параметр WB к int для операций создания и удаления склада."""
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.strip():
        try:
            return int(value)
        except ValueError as error:
            raise ValueError(f"Параметр {parameter_name} должен быть целым числом.") from error
    raise ValueError(f"Параметр {parameter_name} обязателен.")


def _coerce_required_str(value: str | None, parameter_name: str) -> str:
    """Проверяет обязательный текстовый параметр WB, чтобы не создать склад без аккаунта или названия."""
    if isinstance(value, str) and value.strip():
        return value.strip()
    raise ValueError(f"Параметр {parameter_name} обязателен.")


def _coerce_optional_int(value: int | str | None, parameter_name: str) -> int | None:
    """Приводит необязательный числовой параметр WB к int, сохраняя пустое значение как неизвестное."""
    if value is None:
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        if not value.strip():
            return None
        try:
            return int(value)
        except ValueError as error:
            raise ValueError(f"Параметр {parameter_name} должен быть целым числом.") from error
    raise TypeError(f"Параметр {parameter_name} должен иметь тип int, str или None.")


def _print_summary(summary_payload: dict[str, object]) -> None:
    """Печатает и при необходимости сохраняет сводку операции WB без потери русских символов."""
    json_text = json.dumps(summary_payload, ensure_ascii=False, indent=2)
    output_path_value = os.getenv(OUTPUT_PATH_ENV)
    if output_path_value:
        output_path = Path(output_path_value)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json_text, encoding="utf-8-sig")
        logger.info(
            "Полный результат операции WB сохранен в UTF-8 | path=%s",
            output_path,
        )
    print(json_text)


def _read_created_warehouse_payload(source_path: Path) -> dict[str, object]:
    """Читает JSON ответа WB после создания склада, чтобы первично заполнить справочник warehouses_fbs."""
    if not source_path.exists():
        raise FileNotFoundError(f"Файл с результатом создания склада не найден: {source_path}")
    return json.loads(source_path.read_text(encoding="utf-8-sig"))


def _build_warehouse_rows_from_created_payload(
    payload: dict[str, object],
    warehouse_id: int,
    warehouse_name: str,
    wb_office_id: int | None,
) -> list[dict[str, object]]:
    """Готовит строки warehouses_fbs из ответа WB, сохраняя общий ID нашего склада для всех аккаунтов."""
    results = payload.get("results")
    if not isinstance(results, list):
        raise ValueError("В JSON создания склада нет списка results.")

    rows: list[dict[str, object]] = []
    for result in results:
        if not isinstance(result, dict):
            continue
        account = result.get("account")
        create_payload = result.get("payload")
        if not isinstance(account, str) or not isinstance(create_payload, dict):
            continue
        wb_warehouse_id = create_payload.get("id")
        if not isinstance(wb_warehouse_id, int):
            raise ValueError(
                f"В ответе WB для account={account} нет числового payload.id."
            )
        rows.append(
            {
                "warehouse_id": warehouse_id,
                "warehouse_name": warehouse_name,
                "account": account.strip(),
                "wb_warehouse_id": wb_warehouse_id,
                "wb_office_id": wb_office_id,
                "status": "active",
                "create_payload": create_payload,
            }
        )

    if not rows:
        raise ValueError("В JSON создания склада нет строк, пригодных для записи.")
    return rows


def _save_created_warehouse_summary_to_db(
    summary_payload: dict[str, object],
    warehouse_name: str,
    wb_office_id: int | None,
    warehouse_id: int | None,
) -> dict[str, object]:
    """Сохраняет результат создания WB-склада в `warehouses_fbs` сразу после успешного API-вызова.

    Бизнес-сценарий: создание склада и фиксация его связки в нашей БД должны быть одной операцией,
    чтобы пользователь не переносил вручную WB `warehouseId` и не путал его с `officeId`.
    Если склад уже существует как логический склад системы, пользователь задает общий
    `WB_FBS_OUR_WAREHOUSE_ID`; если это новый логический склад, система присваивает следующий ID.
    """
    repository = FBSWarehousesRepository()
    resolved_warehouse_id = warehouse_id
    if resolved_warehouse_id is None:
        resolved_warehouse_id = repository.get_next_warehouse_id()

    rows = _build_warehouse_rows_from_created_payload(
        payload=summary_payload,
        warehouse_id=resolved_warehouse_id,
        warehouse_name=warehouse_name,
        wb_office_id=wb_office_id,
    )
    save_result = repository.save(
        dataframe=pd.DataFrame(rows),
        warehouse_id=resolved_warehouse_id,
    )
    logger.info(
        "Созданный FBS-склад автоматически записан в warehouses_fbs | warehouse_id=%s | rows=%s",
        save_result.warehouse_id,
        save_result.written_rows,
    )
    return {
        "warehouse_id": save_result.warehouse_id,
        "warehouse_name": warehouse_name,
        "written_rows": save_result.written_rows,
        "rows": rows,
    }


def _find_existing_warehouse_payload(
    source_path: Path,
    account: str,
    wb_warehouse_id: int,
) -> dict[str, object] | None:
    """Ищет существующий WB-склад в файле синхронизации, чтобы привязать его к нашему справочнику."""
    if not source_path.exists():
        return None

    payload = json.loads(source_path.read_text(encoding="utf-8-sig"))
    results = payload.get("results")
    if not isinstance(results, list):
        return None

    for account_result in results:
        if not isinstance(account_result, dict):
            continue
        if account_result.get("account") != account:
            continue

        warehouses = account_result.get("unmatched_warehouses", [])
        if not isinstance(warehouses, list):
            continue
        for warehouse in warehouses:
            if not isinstance(warehouse, dict):
                continue
            if warehouse.get("id") == wb_warehouse_id:
                return warehouse
    return None


async def list_wb_offices_async(account: str | None = None) -> None:
    """Запускает получение офисов WB для выбора `officeId` при создании FBS-склада продавца."""
    resolved_account = account or os.getenv(ACCOUNT_ENV)
    logger.info("Старт получения офисов WB для FBS-склада | account=%s", resolved_account)
    summary = await FBSWarehousesService().list_offices(account=resolved_account)
    _print_summary(_summary_to_dict(summary))
    logger.info(
        "Получение офисов WB завершено | accounts_total=%s | retries_used=%s",
        summary.accounts_total,
        summary.retries_used,
    )


async def list_fbs_warehouses_async(account: str | None = None) -> None:
    """Запускает получение FBS-складов продавца, чтобы увидеть текущие `warehouseId`."""
    resolved_account = account or os.getenv(ACCOUNT_ENV)
    logger.info("Старт получения FBS-складов WB | account=%s", resolved_account)
    summary = await FBSWarehousesService().list_warehouses(account=resolved_account)
    _print_summary(_summary_to_dict(summary))
    logger.info(
        "Получение FBS-складов WB завершено | accounts_total=%s | retries_used=%s",
        summary.accounts_total,
        summary.retries_used,
    )


async def sync_fbs_warehouses_from_wb_async(account: str | None = None) -> None:
    """Дозаполняет warehouses_fbs актуальными данными WB по складам текущего аккаунта."""
    resolved_account = _coerce_required_str(account or os.getenv(ACCOUNT_ENV), "account")
    logger.info(
        "Старт синхронизации справочника FBS-складов из WB | account=%s",
        resolved_account,
    )
    service_summary = await FBSWarehousesService().list_warehouses(account=resolved_account)
    repository = FBSWarehousesRepository()

    sync_results = []
    for account_result in service_summary.results:
        if not isinstance(account_result.payload, list):
            raise RuntimeError(
                f"WB вернул неожиданный формат складов для account={account_result.account}"
            )
        sync_result = repository.update_existing_from_wb(
            account=account_result.account,
            warehouses_payload=account_result.payload,
        )
        sync_results.append(sync_result)

    _print_summary(
        {
            "operation": "sync_fbs_warehouses_from_wb",
            "accounts_total": len(sync_results),
            "results": [
                {
                    "account": result.account,
                    "api_rows": result.api_rows,
                    "updated_rows": result.updated_rows,
                    "unmatched_rows": len(result.unmatched_warehouses),
                    "unmatched_warehouses": result.unmatched_warehouses,
                }
                for result in sync_results
            ],
        }
    )
    logger.info(
        "Синхронизация справочника FBS-складов из WB завершена | account=%s | accounts_total=%s",
        resolved_account,
        len(sync_results),
    )


async def create_fbs_warehouse_async(
    account: str | None = None,
    office_id: int | str | None = None,
    name: str | None = None,
) -> None:
    """Запускает создание FBS-склада WB по выбранному офису для будущего управления остатками."""
    resolved_account = _coerce_required_str(account or os.getenv(ACCOUNT_ENV), "account")
    resolved_office_id = _coerce_required_int(office_id or os.getenv(OFFICE_ID_ENV), "office_id")
    resolved_name = _coerce_required_str(name or os.getenv(WAREHOUSE_NAME_ENV), "name")

    logger.info(
        "Старт создания FBS-склада WB | account=%s | office_id=%s | name=%s",
        resolved_account,
        resolved_office_id,
        resolved_name,
    )
    summary = await FBSWarehousesService().create_warehouse(
        account=resolved_account,
        office_id=resolved_office_id,
        name=resolved_name,
    )
    summary_payload = _summary_to_dict(summary)
    resolved_warehouse_id = _coerce_optional_int(
        os.getenv(OUR_WAREHOUSE_ID_ENV),
        "warehouse_id",
    )
    database_import = _save_created_warehouse_summary_to_db(
        summary_payload=summary_payload,
        warehouse_name=resolved_name,
        wb_office_id=resolved_office_id,
        warehouse_id=resolved_warehouse_id,
    )
    summary_payload["database_import"] = database_import
    _print_summary(summary_payload)
    logger.info(
        "Создание FBS-склада WB завершено и записано в БД | account=%s | warehouse_id=%s | retries_used=%s",
        resolved_account,
        database_import["warehouse_id"],
        summary.retries_used,
    )


async def delete_fbs_warehouse_async(
    account: str | None = None,
    warehouse_id: int | str | None = None,
) -> None:
    """Запускает удаление FBS-склада WB, который больше не нужен для управления остатками."""
    resolved_account = _coerce_required_str(account or os.getenv(ACCOUNT_ENV), "account")
    resolved_warehouse_id = _coerce_required_int(
        warehouse_id or os.getenv(WAREHOUSE_ID_ENV),
        "warehouse_id",
    )

    logger.info(
        "Старт удаления FBS-склада WB | account=%s | warehouse_id=%s",
        resolved_account,
        resolved_warehouse_id,
    )
    summary = await FBSWarehousesService().delete_warehouse(
        account=resolved_account,
        warehouse_id=resolved_warehouse_id,
    )
    delete_result = FBSWarehousesRepository().mark_deleted(
        account=resolved_account,
        wb_warehouse_id=resolved_warehouse_id,
    )
    _print_summary(_summary_to_dict(summary))
    logger.info(
        "Удаление FBS-склада WB завершено | account=%s | warehouse_id=%s | retries_used=%s | db_updated_rows=%s",
        resolved_account,
        resolved_warehouse_id,
        summary.retries_used,
        delete_result.updated_rows,
    )


def list_wb_offices(account: str | None = None) -> None:
    """Синхронный entrypoint для списка офисов WB, из которых выбирается склад привязки."""
    asyncio.run(list_wb_offices_async(account=account))


def list_fbs_warehouses(account: str | None = None) -> None:
    """Синхронный entrypoint для списка FBS-складов продавца WB."""
    asyncio.run(list_fbs_warehouses_async(account=account))


def sync_fbs_warehouses_from_wb(account: str | None = None) -> None:
    """Синхронный entrypoint дозаполнения warehouses_fbs из текущего списка складов WB."""
    asyncio.run(sync_fbs_warehouses_from_wb_async(account=account))


def create_fbs_warehouse(
    account: str | None = None,
    office_id: int | str | None = None,
    name: str | None = None,
) -> None:
    """Синхронный entrypoint создания FBS-склада продавца WB."""
    asyncio.run(
        create_fbs_warehouse_async(
            account=account,
            office_id=office_id,
            name=name,
        )
    )


def delete_fbs_warehouse(
    account: str | None = None,
    warehouse_id: int | str | None = None,
) -> None:
    """Синхронный entrypoint удаления FBS-склада продавца WB."""
    asyncio.run(delete_fbs_warehouse_async(account=account, warehouse_id=warehouse_id))


def import_created_fbs_warehouse(
    source_path: str | None = None,
    warehouse_id: int | str | None = None,
    warehouse_name: str | None = None,
    wb_office_id: int | str | None = None,
) -> None:
    """Создает/обновляет таблицу warehouses_fbs из JSON ответа WB после создания склада.

    Бизнес-сценарий: зафиксировать связь между нашим стабильным `warehouse_id`
    и WB `warehouseId` конкретного аккаунта, чтобы будущий контур управления
    остатками мог отправлять данные на правильный склад каждого кабинета.
    """
    resolved_source_path = Path(
        source_path or os.getenv(IMPORT_SOURCE_PATH_ENV) or DEFAULT_CREATED_WAREHOUSE_PATH
    )
    resolved_name = _coerce_required_str(
        warehouse_name or os.getenv(WAREHOUSE_NAME_ENV),
        "warehouse_name",
    )
    resolved_office_id = _coerce_optional_int(
        wb_office_id or os.getenv(OFFICE_ID_ENV),
        "wb_office_id",
    )

    repository = FBSWarehousesRepository()
    resolved_warehouse_id = _coerce_optional_int(
        warehouse_id or os.getenv(OUR_WAREHOUSE_ID_ENV),
        "warehouse_id",
    )
    if resolved_warehouse_id is None:
        resolved_warehouse_id = repository.get_next_warehouse_id()

    payload = _read_created_warehouse_payload(resolved_source_path)
    rows = _build_warehouse_rows_from_created_payload(
        payload=payload,
        warehouse_id=resolved_warehouse_id,
        warehouse_name=resolved_name,
        wb_office_id=resolved_office_id,
    )
    save_result = repository.save(
        dataframe=pd.DataFrame(rows),
        warehouse_id=resolved_warehouse_id,
    )
    _print_summary(
        {
            "operation": "import_created_fbs_warehouse",
            "source_path": str(resolved_source_path),
            "warehouse_id": save_result.warehouse_id,
            "warehouse_name": resolved_name,
            "written_rows": save_result.written_rows,
            "rows": rows,
        }
    )


def import_existing_fbs_warehouse(
    source_path: str | None = None,
    account: str | None = None,
    warehouse_id: int | str | None = None,
    warehouse_name: str | None = None,
    wb_warehouse_id: int | str | None = None,
    wb_office_id: int | str | None = None,
) -> None:
    """Добавляет существующий WB-склад как новый или выбранный наш логический FBS-склад.

    Бизнес-сценарий: если склад уже есть в личном кабинете WB, но отсутствует
    в `warehouses_fbs`, мы явно присваиваем ему наш стабильный `warehouse_id`.
    Это нужно, чтобы будущая загрузка остатков управляла складом через общий
    бизнес-ID, а в WB отправляла аккаунтный `wb_warehouse_id`.
    """
    resolved_account = _coerce_required_str(account or os.getenv(ACCOUNT_ENV), "account")
    resolved_source_path = Path(
        source_path or os.getenv(IMPORT_SOURCE_PATH_ENV) or DEFAULT_SYNCED_WAREHOUSES_PATH
    )
    resolved_wb_warehouse_id = _coerce_required_int(
        wb_warehouse_id or os.getenv(WAREHOUSE_ID_ENV),
        "wb_warehouse_id",
    )
    source_payload = _find_existing_warehouse_payload(
        source_path=resolved_source_path,
        account=resolved_account,
        wb_warehouse_id=resolved_wb_warehouse_id,
    )

    resolved_name = (
        warehouse_name
        or os.getenv(WAREHOUSE_NAME_ENV)
        or (source_payload or {}).get("name")
    )
    resolved_name = _coerce_required_str(
        resolved_name if isinstance(resolved_name, str) else None,
        "warehouse_name",
    )
    resolved_office_id = _coerce_optional_int(
        wb_office_id
        or os.getenv(OFFICE_ID_ENV)
        or (source_payload or {}).get("officeId"),
        "wb_office_id",
    )

    repository = FBSWarehousesRepository()
    resolved_warehouse_id = _coerce_optional_int(
        warehouse_id or os.getenv(OUR_WAREHOUSE_ID_ENV),
        "warehouse_id",
    )
    if resolved_warehouse_id is None:
        resolved_warehouse_id = repository.get_next_warehouse_id()

    create_payload = source_payload or {
        "id": resolved_wb_warehouse_id,
        "name": resolved_name,
        "officeId": resolved_office_id,
    }
    row = {
        "warehouse_id": resolved_warehouse_id,
        "warehouse_name": resolved_name,
        "account": resolved_account,
        "wb_warehouse_id": resolved_wb_warehouse_id,
        "wb_office_id": resolved_office_id,
        "status": "active",
        "create_payload": create_payload,
    }
    save_result = repository.save(
        dataframe=pd.DataFrame([row]),
        warehouse_id=resolved_warehouse_id,
    )
    _print_summary(
        {
            "operation": "import_existing_fbs_warehouse",
            "source_path": str(resolved_source_path),
            "warehouse_id": save_result.warehouse_id,
            "warehouse_name": resolved_name,
            "written_rows": save_result.written_rows,
            "row": row,
        }
    )


def _summary_to_dict(summary) -> dict[str, object]:
    """Преобразует сводку операции WB в JSON-структуру для проверки результата человеком."""
    return {
        "operation": summary.operation,
        "accounts_total": summary.accounts_total,
        "retries_used": summary.retries_used,
        "results": [
            {
                "account": result.account,
                "retries_used": result.retries_used,
                "payload": result.payload,
            }
            for result in summary.results
        ],
    }
