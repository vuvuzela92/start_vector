import logging
import os
import secrets
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
from fastapi import FastAPI, Header, HTTPException, Response, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from src_oop.jobs.calculation_of_purchases_china.orders_white_balance_analytics import (
    OrdersWhiteBalanceAnalyticsService,
)

APP_ROOT = Path(__file__).resolve().parents[1]
PROJECT_ROOT = Path(__file__).resolve().parents[3]
load_dotenv(APP_ROOT / ".env")
load_dotenv(PROJECT_ROOT / ".env")

logger = logging.getLogger(__name__)

WEBHOOK_TOKEN_ENV = "GOOGLE_SHEETS_WEBHOOK_TOKEN"

app = FastAPI(title="Payments Analytics API")


class JobRunResponse(BaseModel):
    status: str
    message: str
    rows_uploaded: int | None
    started_at: str
    finished_at: str


def _get_expected_token() -> str:
    token = os.getenv(WEBHOOK_TOKEN_ENV)
    if not token:
        logger.error("Environment variable %s is not configured.", WEBHOOK_TOKEN_ENV)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Webhook token is not configured",
        )
    return token


def _extract_bearer_token(authorization: str | None) -> str | None:
    if not authorization:
        return None

    scheme, _, token = authorization.partition(" ")
    if scheme.lower() != "bearer" or not token:
        return None
    return token.strip()


def _verify_webhook_token(
    authorization: str | None,
    x_webhook_token: str | None,
) -> None:
    expected_token = _get_expected_token()
    provided_token = _extract_bearer_token(authorization) or x_webhook_token

    if not provided_token or not secrets.compare_digest(provided_token, expected_token):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid webhook token",
        )


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post(
    "/jobs/calculation-of-purchases-china/payments-analyze/run",
    response_model=JobRunResponse,
)
def run_payments_analyze_job(
    authorization: str | None = Header(default=None),
    x_webhook_token: str | None = Header(default=None),
) -> JobRunResponse | Response:
    _verify_webhook_token(
        authorization=authorization,
        x_webhook_token=x_webhook_token,
    )

    started_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    try:
        service = OrdersWhiteBalanceAnalyticsService()
        df_balance = service.run()
    except Exception as error:
        finished_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.exception("Payments analytics job failed.")
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={
                "status": "error",
                "message": str(error),
                "rows_uploaded": None,
                "started_at": started_at,
                "finished_at": finished_at,
            },
        )

    finished_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return JobRunResponse(
        status="success",
        message="Аналитика платежей успешно обновлена",
        rows_uploaded=len(df_balance),
        started_at=started_at,
        finished_at=finished_at,
    )
