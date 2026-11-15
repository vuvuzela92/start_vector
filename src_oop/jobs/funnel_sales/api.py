from __future__ import annotations

import logging
from datetime import date

from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel

from src_oop.core.database import Database

logger = logging.getLogger(__name__)

app = FastAPI(
    title="Воронка продаж",
    description="API для чтения данных из воронки продаж.",
    version="1.0.0",
)


class LatestBrandRecord(BaseModel):
    """Строка с последней датой данных для пары wild и brand_name."""

    wild: str | None
    brand_name: str
    date: date | None


@app.get(
    "/v1/funnel/brands/latest",
    response_model=list[LatestBrandRecord],
    summary="Получить последние даты воронки по брендам",
    tags=["Данные о бренде товара"]
)
def get_latest_funnel_brands() -> list[LatestBrandRecord]:
    """Возвращает последнюю доступную дату для каждой пары wild и brand_name.

    Endpoint обслуживает чтение актуальности брендов из витрины `funnel_daily`.
    Для каждой пары выбирается строка с максимальной датой, а пустые названия
    брендов исключаются согласно исходному SQL-запросу.
    """
    query = """
        SELECT DISTINCT ON (fd.wild, fd.brand_name)
            fd.wild,
            fd.brand_name,
            fd.date
        FROM funnel_daily AS fd
        WHERE fd.brand_name != ''
          AND fd.brand_name IS NOT NULL
        ORDER BY fd.wild, fd.brand_name, fd.date DESC
    """

    try:
        rows = Database.read_sql_to_dict(query)
    except Exception as error:
        logger.exception(
            "Не удалось получить последние даты воронки по брендам из PostgreSQL."
        )
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Не удалось получить данные воронки из базы данных.",
        ) from error

    return [LatestBrandRecord.model_validate(row) for row in rows]
