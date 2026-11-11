"""Ограничивает время выполнения запланированной бизнес-задачи одним часом."""

from __future__ import annotations

import argparse
import logging
import subprocess
import sys
from pathlib import Path

LOGGER = logging.getLogger(__name__)
DEFAULT_TIMEOUT_SECONDS = 60 * 60


def run_task(python_path: Path, script_path: Path, arguments: list[str], timeout: int) -> int:
    """Запускает бизнес-сценарий и останавливает его процессы при превышении лимита.

    Сценарий обслуживает регламентные выгрузки Start Vector: дочерний Python
    процесс вместе с порождёнными им процессами не должен занимать расписание
    дольше заданного часа.
    """
    command = [str(python_path), str(script_path), *arguments]
    try:
        process = subprocess.Popen(
            command,
            cwd=Path.cwd(),
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            creationflags=subprocess.CREATE_NO_WINDOW if sys.platform == "win32" else 0,
        )
    except OSError as error:
        LOGGER.error("Не удалось запустить бизнес-задачу: %s", type(error).__name__)
        return 1

    try:
        return process.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        LOGGER.error("Бизнес-задача превысила лимит %s секунд; останавливается дерево процессов.", timeout)
        try:
            subprocess.run(
                ["taskkill.exe", "/PID", str(process.pid), "/T", "/F"],
                check=False,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=30,
            )
        except (OSError, subprocess.TimeoutExpired) as error:
            LOGGER.error("Не удалось штатно завершить дерево процессов: %s", type(error).__name__)
            process.kill()
        process.wait()
        return 124


def main() -> int:
    """Разбирает параметры launcher и запускает один полный бизнес-сценарий."""
    parser = argparse.ArgumentParser(description="Запуск задачи Планировщика с часовым лимитом.")
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT_SECONDS)
    parser.add_argument("python_path", type=Path)
    parser.add_argument("script_path", type=Path)
    parser.add_argument("arguments", nargs=argparse.REMAINDER)
    options = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    return run_task(options.python_path, options.script_path, options.arguments, options.timeout)


if __name__ == "__main__":
    raise SystemExit(main())
