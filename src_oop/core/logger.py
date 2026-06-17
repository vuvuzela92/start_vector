"""Настройка логирования для модулей на базе src_oop."""

from pathlib import Path
import logging
import sys
from logging.handlers import RotatingFileHandler

from loguru import logger


PROJECT_ROOT = Path(__file__).resolve().parents[2]
LOG_DIR = PROJECT_ROOT / "logs"
LOG_FILE = "app.log"


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


def setup_logger():
    """
    Настраивает и возвращает логгер приложения.

    Если отдельной конфигурации для логов нет, используются значения по умолчанию:
    - директория логов: <корень проекта>/logs
    - файл логов: app.log
    """

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_file_path = LOG_DIR / LOG_FILE

    logger.remove()
    logger.add(
        sys.stdout,
        level="INFO",
        format=(
            "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
            "<level>{level: <8}</level> | <level>{message}</level>"
        ),
    )

    file_handler = RotatingFileHandler(
        log_file_path,
        maxBytes=10 * 1024 * 1024,
        backupCount=0,
        encoding="utf-8",
    )
    file_handler.setFormatter(
        logging.Formatter(
            "{asctime} | {levelname:<8} | {message}",
            "%Y-%m-%d %H:%M:%S",
            style="{",
        )
    )
    file_handler.setLevel(logging.INFO)

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    root_logger.handlers = [
        handler
        for handler in root_logger.handlers
        if not isinstance(handler, (RotatingFileHandler, InterceptHandler))
    ]
    root_logger.addHandler(file_handler)
    root_logger.addHandler(InterceptHandler())

    logger.info("Логгер настроен, файл: {} (макс. 10 МБ)", log_file_path)
    return logger
