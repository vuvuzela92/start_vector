"""Настройка логирования для модулей на базе src_oop."""

from pathlib import Path
from datetime import datetime, timedelta
import logging
import os
import sys

from loguru import logger


PROJECT_ROOT = Path(__file__).resolve().parents[2]
LOG_DIR = PROJECT_ROOT / "logs"
LOG_FILE = "app.log"
LOG_RETENTION = timedelta(days=1)


class InterceptHandler(logging.Handler):
    """Перенаправляет сообщения стандартного logging в loguru."""

    def emit(self, record: logging.LogRecord) -> None:
        try:
            level = logger.level(record.levelname).name
        except ValueError:
            level = record.levelno

        frame, depth = logging.currentframe(), 2
        while frame and frame.f_code.co_filename == logging.__file__:
            frame = frame.f_back
            depth += 1

        logger.opt(depth=depth, exception=record.exc_info).log(
            level,
            record.getMessage(),
        )


def _remove_expired_log_files(active_log_path: Path) -> int:
    """Удаляет старые файлы приложения, не прерывая задачи из-за занятых логов.

    Очистка защищает ежедневные запуски от накопления логов других процессов.
    Активный файл и диагностические CSV не затрагиваются; ошибки доступа
    журналируются, а число успешно удалённых файлов возвращается вызывающему коду.
    """
    cutoff = datetime.now().timestamp() - LOG_RETENTION.total_seconds()
    candidates = [*LOG_DIR.glob("app_*.log"), LOG_DIR / LOG_FILE]
    removed_count = 0

    for path in candidates:
        if path == active_log_path:
            continue
        try:
            if path.stat().st_mtime < cutoff:
                path.unlink()
                removed_count += 1
        except FileNotFoundError:
            continue
        except OSError as error:
            logger.warning(
                "Не удалось удалить устаревший лог; задача продолжает работу | file={} | error_type={}",
                path.name,
                type(error).__name__,
            )

    return removed_count


def setup_logger():
    """
    Настраивает и возвращает логгер приложения.

    Если отдельной конфигурации для логов нет, используются значения по умолчанию:
    - директория логов: <корень проекта>/logs
    - файлы логов: app_<pid>_<время>.log, по одному на запуск процесса.
    Файл ротируется по достижении 10 МБ; логи старше суток удаляются при
    следующем запуске. Стандартный logging перенаправляется в loguru.
    """

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    log_file_path = LOG_DIR / f"app_{os.getpid()}_{run_id}.log"

    logger.remove()
    logger.add(
        sys.stdout,
        level="INFO",
        format=(
            "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
            "<level>{level: <8}</level> | <level>{message}</level>"
        ),
    )

    logger.add(
        log_file_path,
        level="INFO",
        rotation="10 MB",
        retention="1 day",
        encoding="utf-8",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {message}",
    )
    removed_count = _remove_expired_log_files(log_file_path)

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    root_logger.handlers = [
        handler for handler in root_logger.handlers
        if not isinstance(handler, InterceptHandler)
    ]
    root_logger.addHandler(InterceptHandler())

    logger.info(
        "Логгер настроен | file={} | rotation=10 MB | retention=1 сутки | removed_files={}",
        log_file_path,
        removed_count,
    )
    return logger
