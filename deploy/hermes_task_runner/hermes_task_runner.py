#!/usr/bin/env python3
"""Ограниченный SSH-launcher задач Start Vector для контура Hermes."""

from __future__ import annotations

import logging
import os
import re
import shlex
import stat
import subprocess
import sys
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from io import TextIOWrapper
from pathlib import Path


CONFIG_PATH = Path("/etc/start-vector/hermes-runner.conf")
ALLOWED_TASKS_PATH = Path("/etc/start-vector/hermes-allowed-tasks.txt")
FLOCK_EXECUTABLE = "/usr/bin/flock"
# Код временной недоступности задачи: второй запуск не является ошибкой бизнес-расчёта.
LOCK_CONFLICT_EXIT_CODE = 75
CONFIG_KEYS = {
    "PROJECT_DIR",
    "PYTHON_EXECUTABLE",
    "ENV_FILE",
    "LOG_DIR",
    "TASK_TIMEOUT_SECONDS",
}
ENVIRONMENT_KEY_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
# Дочерняя задача не наследует SSH-окружение, чтобы клиент не влиял на импорт модулей и пути.
TASK_ENVIRONMENT_BASE = {
    "PATH": "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
    "PYTHONUNBUFFERED": "1",
}


class ConfigurationError(Exception):
    """Сигнализирует, что защищённая конфигурация запуска недоступна или некорректна."""


@dataclass(frozen=True)
class RunnerSettings:
    """Хранит фиксированные параметры безопасного запуска бизнес-задач проекта."""

    project_dir: Path
    python_executable: Path
    env_file: Path
    log_dir: Path
    task_timeout_seconds: int


def validate_root_owned_file(path: Path) -> None:
    """Защищает правило: Hermes не может менять allowlist или путь запуска на сервере."""
    try:
        file_stat = path.stat()
    except FileNotFoundError as error:
        raise ConfigurationError(f"Не найден обязательный файл {path}") from error

    if not stat.S_ISREG(file_stat.st_mode):
        raise ConfigurationError(f"Путь {path} должен быть обычным файлом")
    if file_stat.st_uid != 0:
        raise ConfigurationError(f"Владельцем файла {path} должен быть root")
    if file_stat.st_mode & (stat.S_IWGRP | stat.S_IWOTH):
        raise ConfigurationError(f"Файл {path} не должен быть доступен для записи группе или всем")


def read_key_value_file(path: Path, allowed_keys: set[str] | None = None) -> dict[str, str]:
    """Читает конфигурацию без shell-подстановок, защищая запуск от исполнения строк из env."""
    values: dict[str, str] = {}
    for line_number, raw_line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            raise ConfigurationError(f"Некорректная строка {line_number} в {path}")

        key, value = line.split("=", maxsplit=1)
        key = key.strip()
        if not ENVIRONMENT_KEY_PATTERN.fullmatch(key):
            raise ConfigurationError(f"Некорректное имя параметра в строке {line_number} файла {path}")
        if allowed_keys is not None and key not in allowed_keys:
            raise ConfigurationError(f"Недопустимый параметр {key} в {path}")
        if key in values:
            raise ConfigurationError(f"Параметр {key} повторяется в {path}")

        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"\"", "'"}:
            value = value[1:-1]
        values[key] = value
    return values


def load_settings(config_path: Path = CONFIG_PATH) -> RunnerSettings:
    """Загружает фиксированные пути запуска, чтобы SSH-клиент не мог их подменить."""
    validate_root_owned_file(config_path)
    config = read_key_value_file(config_path, CONFIG_KEYS)
    missing_keys = CONFIG_KEYS.difference(config)
    if missing_keys:
        raise ConfigurationError(
            f"В {config_path} отсутствуют параметры: {', '.join(sorted(missing_keys))}"
        )

    try:
        timeout = int(config["TASK_TIMEOUT_SECONDS"])
    except ValueError as error:
        raise ConfigurationError("TASK_TIMEOUT_SECONDS должен быть целым числом") from error
    if timeout <= 0:
        raise ConfigurationError("TASK_TIMEOUT_SECONDS должен быть больше нуля")

    settings = RunnerSettings(
        project_dir=Path(config["PROJECT_DIR"]),
        python_executable=Path(config["PYTHON_EXECUTABLE"]),
        env_file=Path(config["ENV_FILE"]),
        log_dir=Path(config["LOG_DIR"]),
        task_timeout_seconds=timeout,
    )
    if not settings.project_dir.is_dir():
        raise ConfigurationError("Каталог проекта недоступен")
    if not settings.python_executable.is_file():
        raise ConfigurationError("Интерпретатор виртуального окружения недоступен")
    if not settings.env_file.is_file():
        raise ConfigurationError("Файл окружения недоступен")
    return settings


def parse_ssh_command(command: str) -> str:
    """Принимает только ``run <task>``, исключая shell-команды и дополнительные аргументы."""
    try:
        parts = shlex.split(command)
    except ValueError as error:
        raise ConfigurationError("Команда SSH имеет некорректное экранирование") from error
    if len(parts) != 2 or parts[0] != "run":
        raise ConfigurationError("Разрешён только формат команды: run <task>")
    task_name = parts[1]
    if not re.fullmatch(r"[a-z][a-z0-9_]{0,99}", task_name):
        raise ConfigurationError("Имя задачи содержит недопустимые символы")
    return task_name


def load_allowed_tasks(path: Path = ALLOWED_TASKS_PATH) -> set[str]:
    """Загружает утверждённый администратором перечень бизнес-задач, доступных Hermes."""
    validate_root_owned_file(path)
    tasks = {
        line.strip()
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    }
    if not tasks:
        raise ConfigurationError("Белый список задач пуст")
    for task_name in tasks:
        parse_ssh_command(f"run {task_name}")
    return tasks


def load_task_environment(env_file: Path) -> dict[str, str]:
    """Подготавливает изолированное окружение из защищённого env для запуска бизнес-задачи."""
    environment = TASK_ENVIRONMENT_BASE.copy()
    environment.update(read_key_value_file(env_file))
    return environment


def create_log_file(log_dir: Path, job_id: str) -> tuple[Path, TextIOWrapper]:
    """Создаёт закрытый лог, чтобы диагностика не раскрывала Hermes полный вывод задачи."""
    log_dir.mkdir(mode=0o750, parents=True, exist_ok=True)
    log_path = log_dir / f"{datetime.now(UTC):%Y%m%dT%H%M%SZ}_{job_id}.log"
    descriptor = os.open(log_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    return log_path, os.fdopen(descriptor, "w", encoding="utf-8")


def run_task(
    settings: RunnerSettings,
    task_name: str,
    allowed_tasks: set[str],
) -> tuple[str, int]:
    """Запускает разрешённую задачу без параллельного дубля и сохраняет вывод для аудита.

    Бизнес-правило: две одновременные выгрузки одной задачи не должны параллельно
    перезаписывать Google Sheets или одни и те же расчётные данные.
    """
    if task_name not in allowed_tasks:
        raise ConfigurationError("Эта задача не разрешена для запуска Hermes")

    job_id = uuid.uuid4().hex
    log_path, log_file = create_log_file(settings.log_dir, job_id)
    logger = logging.getLogger("hermes_task_runner")
    logger.info("Hermes запросил запуск задачи | task=%s | job_id=%s", task_name, job_id)
    lock_path = settings.log_dir / f"{task_name}.lock"
    command = [
        FLOCK_EXECUTABLE,
        "--nonblock",
        "--conflict-exit-code",
        str(LOCK_CONFLICT_EXIT_CODE),
        str(lock_path),
        str(settings.python_executable),
        "main.py",
        task_name,
    ]
    try:
        with log_file:
            log_file.write(f"job_id={job_id}\ntask={task_name}\nstarted_at={datetime.now(UTC).isoformat()}\n")
            process = subprocess.run(
                command,
                cwd=settings.project_dir,
                env=load_task_environment(settings.env_file),
                stdout=log_file,
                stderr=subprocess.STDOUT,
                text=True,
                timeout=settings.task_timeout_seconds,
                check=False,
            )
            log_file.write(f"finished_at={datetime.now(UTC).isoformat()}\nexit_code={process.returncode}\n")
    except subprocess.TimeoutExpired:
        logger.warning("Задача Hermes остановлена по таймауту | task=%s | job_id=%s", task_name, job_id)
        return job_id, 124

    if process.returncode == LOCK_CONFLICT_EXIT_CODE:
        logger.info(
            "Задача Hermes не запущена: предыдущий экземпляр ещё выполняется | task=%s | job_id=%s",
            task_name,
            job_id,
        )
        return job_id, LOCK_CONFLICT_EXIT_CODE

    logger.info(
        "Задача Hermes завершена | task=%s | job_id=%s | exit_code=%s | log=%s",
        task_name,
        job_id,
        process.returncode,
        log_path,
    )
    return job_id, process.returncode


def main() -> int:
    """Запускает безопасный сценарий Hermes: проверяет запрос и вызывает задачу из белого списка."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    try:
        task_name = parse_ssh_command(os.environ.get("SSH_ORIGINAL_COMMAND", ""))
        settings = load_settings()
        job_id, exit_code = run_task(settings, task_name, load_allowed_tasks())
    except ConfigurationError as error:
        logging.getLogger("hermes_task_runner").warning(
            "Запрос Hermes отклонён | причина=%s", error
        )
        print(f"status=rejected reason={error}")
        return 2
    except OSError as error:
        logging.getLogger("hermes_task_runner").exception(
            "Не удалось подготовить запуск Hermes | error_type=%s", type(error).__name__
        )
        print("status=failed reason=server_error")
        return 1

    status = "success" if exit_code == 0 else "busy" if exit_code == LOCK_CONFLICT_EXIT_CODE else "failed"
    print(f"status={status} job_id={job_id} exit_code={exit_code}")
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
