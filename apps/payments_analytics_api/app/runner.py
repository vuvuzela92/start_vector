import logging
import shlex
import subprocess

from app.settings import (
    PAYMENTS_ANALYZE_COMMAND,
    PAYMENTS_ANALYZE_PROJECT_DIR,
    PAYMENTS_ANALYZE_TIMEOUT_SECONDS,
)

logger = logging.getLogger(__name__)


def run_payments_analyze_command() -> subprocess.CompletedProcess[str]:
    command = shlex.split(PAYMENTS_ANALYZE_COMMAND, posix=True)

    logger.info(
        "Running payments analytics command in %s: %s",
        PAYMENTS_ANALYZE_PROJECT_DIR,
        PAYMENTS_ANALYZE_COMMAND,
    )

    return subprocess.run(
        command,
        cwd=PAYMENTS_ANALYZE_PROJECT_DIR,
        capture_output=True,
        text=True,
        timeout=PAYMENTS_ANALYZE_TIMEOUT_SECONDS,
        check=True,
    )
