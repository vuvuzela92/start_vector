import logging
import os
import shlex
import subprocess
from pathlib import Path

from app.settings import (
    PAYMENTS_ANALYZE_COMMAND,
    PAYMENTS_ANALYZE_PROJECT_DIR,
    PAYMENTS_ANALYZE_TIMEOUT_SECONDS,
)

logger = logging.getLogger(__name__)
MAX_OUTPUT_LEN = 5000
DEFAULT_MAIN_COMMAND = "python main.py update_payments_analyze_with_ved"
PRODUCTION_DIRECT_JOB_COMMAND = (
    "python -c "
    "\"from src_oop.jobs.calculation_of_purchases_china.run import "
    "update_payments_analyze_with_ved; "
    "update_payments_analyze_with_ved()\""
)
LEGACY_MAIN_COMMAND = "python main.py update_orders_white_balance_analytics"
LEGACY_DIRECT_JOB_COMMAND = (
    "python -c "
    "\"from src_oop.jobs.calculation_of_purchases_china.run import "
    "update_orders_white_balance_analytics; "
    "update_orders_white_balance_analytics()\""
)
LEGACY_DIRECT_SERVICE_COMMAND = (
    "python -c "
    "\"from src_oop.jobs.calculation_of_purchases_china.orders_white_balance_analytics "
    "import OrdersWhiteBalanceAnalyticsService; "
    "OrdersWhiteBalanceAnalyticsService().run()\""
)


def _truncate_output(output: str | None) -> str:
    if not output:
        return ""
    if len(output) <= MAX_OUTPUT_LEN:
        return output
    return output[:MAX_OUTPUT_LEN] + "\n... output truncated ..."


def _resolve_project_dir() -> Path:
    project_dir = PAYMENTS_ANALYZE_PROJECT_DIR
    if project_dir.exists() and project_dir.is_dir():
        return project_dir

    # Local development fallback for Windows/non-container runs.
    # In Docker, /app/project is expected to be mounted and should exist.
    search_roots = [Path.cwd(), *Path.cwd().parents, Path(__file__).resolve(), *Path(__file__).resolve().parents]
    for candidate in search_roots:
        if (candidate / "main.py").exists() and (candidate / "src_oop").exists():
            logger.warning(
                "Configured project directory does not exist: %s. "
                "Using detected local project directory: %s",
                project_dir,
                candidate,
            )
            return candidate

    raise NotADirectoryError(f"Project directory does not exist: {project_dir}")


def run_payments_analyze_command() -> subprocess.CompletedProcess[str]:
    command_to_run = PAYMENTS_ANALYZE_COMMAND.strip()
    if command_to_run == DEFAULT_MAIN_COMMAND:
        command_to_run = PRODUCTION_DIRECT_JOB_COMMAND
    elif command_to_run in {LEGACY_MAIN_COMMAND, LEGACY_DIRECT_JOB_COMMAND}:
        command_to_run = LEGACY_DIRECT_SERVICE_COMMAND

    command = shlex.split(command_to_run, posix=True)
    project_dir = _resolve_project_dir()

    logger.info(
        "Running payments analytics command in %s: %s",
        project_dir,
        command_to_run,
    )

    result = subprocess.run(
        command,
        cwd=project_dir,
        env=os.environ.copy(),
        capture_output=True,
        text=True,
        timeout=PAYMENTS_ANALYZE_TIMEOUT_SECONDS,
        check=False,
    )

    result.stdout = _truncate_output(result.stdout)
    result.stderr = _truncate_output(result.stderr)
    return result
