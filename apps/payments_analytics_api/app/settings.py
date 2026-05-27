import os
from pathlib import Path

from dotenv import load_dotenv

APP_DIR = Path(__file__).resolve().parents[1]

load_dotenv()
load_dotenv(APP_DIR / ".env")

GOOGLE_SHEETS_WEBHOOK_TOKEN = os.getenv("GOOGLE_SHEETS_WEBHOOK_TOKEN")

PAYMENTS_ANALYZE_PROJECT_DIR = Path(
    os.getenv("PAYMENTS_ANALYZE_PROJECT_DIR", "/app/project")
).resolve()

PAYMENTS_ANALYZE_COMMAND = os.getenv(
    "PAYMENTS_ANALYZE_COMMAND",
    "python main.py update_orders_white_balance_analytics",
)

PAYMENTS_ANALYZE_TIMEOUT_SECONDS = int(
    os.getenv("PAYMENTS_ANALYZE_TIMEOUT_SECONDS", "900")
)

API_HOST = os.getenv("API_HOST", "0.0.0.0")
API_PORT = int(os.getenv("API_PORT", "8000"))
