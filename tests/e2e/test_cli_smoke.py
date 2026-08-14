from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.e2e

PROJECT_ROOT = Path(__file__).resolve().parents[2]
MAIN_PATH = PROJECT_ROOT / "main.py"


def _build_subprocess_env() -> dict[str, str]:
    """Готовит безопасное окружение для smoke-проверок CLI без искажения русскоязычного вывода.

    В бизнес-сценарии это защищает e2e-проверки реестра задач от ложных падений, связанных
    не с логикой CLI, а с кодировкой консольного вывода на Windows.
    """

    env = os.environ.copy()
    env["PYTHONIOENCODING"] = "utf-8"
    return env


def test_cli_help_returns_success() -> None:
    """Проверяет, что оператор может получить справку CLI без запуска бизнес-задач.

    Этот smoke-сценарий страхует базовый путь входа в приложение: загрузку реестра задач,
    инициализацию парсера аргументов и вывод подсказки по использованию.
    """

    result = subprocess.run(
        [sys.executable, str(MAIN_PATH), "--help"],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        env=_build_subprocess_env(),
        check=False,
    )

    assert result.returncode == 0
    assert "usage:" in result.stdout.lower()
    assert "--help" in result.stdout


def test_cli_without_arguments_shows_help_and_error_code() -> None:
    """Проверяет, что CLI не запускает сценарий без имени задачи и подсказывает формат запуска.

    Для бизнеса это важно как защита операционного сценария: случайный запуск без параметров
    должен завершаться управляемо и не переходить к выполнению задач с внешними интеграциями.
    """

    result = subprocess.run(
        [sys.executable, str(MAIN_PATH)],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        env=_build_subprocess_env(),
        check=False,
    )

    combined_output = f"{result.stdout}\n{result.stderr}".lower()

    assert result.returncode == 1
    assert "usage:" in combined_output
