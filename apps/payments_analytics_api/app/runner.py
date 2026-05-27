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
    raise NotADirectoryError(f"Project directory does not exist: {project_dir}")


def run_payments_analyze_command() -> subprocess.CompletedProcess[str]:
    command = shlex.split(PAYMENTS_ANALYZE_COMMAND, posix=True)
    project_dir = _resolve_project_dir()

    logger.info(
        "Running payments analytics command in %s: %s",
        project_dir,
        PAYMENTS_ANALYZE_COMMAND,
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
