import os
from pathlib import Path

from dotenv import load_dotenv

APP_DIR = Path(__file__).resolve().parent.parent

load_dotenv()
load_dotenv(APP_DIR / ".env")

GOOGLE_SHEETS_WEBHOOK_TOKEN = os.getenv("GOOGLE_SHEETS_WEBHOOK_TOKEN")

def _detect_local_project_dir() -> Path:
    for path in APP_DIR.parents:
        if (path / "main.py").exists() and (path / "src_oop").exists():
            return path
    return APP_DIR


default_project_dir = (
    Path("/app/project") if os.name != "nt" else _detect_local_project_dir()
)

PAYMENTS_ANALYZE_PROJECT_DIR = Path(
    os.getenv("PAYMENTS_ANALYZE_PROJECT_DIR", str(default_project_dir))
).resolve()

PAYMENTS_ANALYZE_COMMAND = os.getenv(
    "PAYMENTS_ANALYZE_COMMAND",
    "python -c \"from src_oop.jobs.calculation_of_purchases_china.run import update_orders_white_balance_analytics; update_orders_white_balance_analytics()\"",
)

PAYMENTS_ANALYZE_TIMEOUT_SECONDS = int(
    os.getenv("PAYMENTS_ANALYZE_TIMEOUT_SECONDS", "900")
)
