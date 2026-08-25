"""Сервис Telegram-запуска job add_new_items."""

from __future__ import annotations

import asyncio
import logging
import re
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from src_oop.jobs.add_new_items.telegram_config import AddNewItemsTelegramSettings
from src_oop.jobs.add_new_items.telegram_models import (
    ActiveRunInfo,
    TaskRunResult,
    TelegramLaunchActor,
)

logger = logging.getLogger(__name__)

MOSCOW_TZ = ZoneInfo("Europe/Moscow")
_SENSITIVE_PATTERNS = (
    re.compile(r"(?i)(token\s*[=:]\s*)(\S+)"),
    re.compile(r"(?i)(authorization\s*[=:]\s*)(\S+)"),
    re.compile(r"(?i)(cookie\s*[=:]\s*)(\S+)"),
    re.compile(r"(?i)(password\s*[=:]\s*)(\S+)"),
    re.compile(r"(?i)(secret\s*[=:]\s*)(\S+)"),
    re.compile(r"(?i)(dsn\s*[=:]\s*)(\S+)"),
)
_ADD_NEW_ITEMS_RESULT_PATTERN = re.compile(
    r"AddNewItemsResult\("
    r"loaded_cards=(?P<loaded_cards>\d+), "
    r"added_to_sopost=(?P<added_to_sopost>\d+), "
    r"added_to_unit_main=(?P<added_to_unit_main>\d+), "
    r"added_to_autopilot=(?P<added_to_autopilot>\d+), "
    r"added_to_competitors=(?P<added_to_competitors>\d+), "
    r"added_to_products=(?P<added_to_products>\d+)"
    r"\)"
)


@dataclass(frozen=True, slots=True)
class ParsedAddNewItemsStats:
    """Хранит распознанную из логов сводку выполнения add_new_items.

    Helper-модель нужна только для Telegram-уведомлений: она превращает сырую
    строку `AddNewItemsResult(...)` в человекочитаемую сводку по целевым местам
    записи, чтобы оператор понимал масштаб выполненного переноса.
    """

    loaded_cards: int
    added_to_sopost: int
    added_to_unit_main: int
    added_to_autopilot: int
    added_to_competitors: int
    added_to_products: int


class AddNewItemsTelegramService:
    """Управляет запуском add_new_items из Telegram без параллельных дублей.

    Сервис обслуживает главный бизнес-сценарий Telegram-части job: сотрудник
    из разрешённой группы запускает перенос новых товаров, получает быстрый
    статус старта и затем итог задачи прямо в чате, не заходя на сервер.
    """

    def __init__(
        self,
        settings: AddNewItemsTelegramSettings | None = None,
    ) -> None:
        """Инициализирует модульные настройки и внутреннее состояние запуска.

        Внутренние поля держат текущий активный запуск и последний результат,
        чтобы команды `/status` и `/last_result` могли отвечать без обращения к
        БД и без повторного запуска основной job.
        """
        self.settings = settings or AddNewItemsTelegramSettings.from_env()
        self._state_lock = asyncio.Lock()
        self._active_run: ActiveRunInfo | None = None
        self._active_task: asyncio.Task[None] | None = None
        self._last_result: TaskRunResult | None = None
        self._project_root = Path(__file__).resolve().parents[3]

    async def start_run(
        self,
        *,
        bot,
        actor: TelegramLaunchActor,
        chat_id: int,
    ) -> str:
        """Пытается запустить add_new_items и возвращает ответ для чата.

        Бизнес-правило здесь строгое: одновременно может идти только один
        перенос новых товаров, иначе возрастает риск дублей и гонок записи в
        Google Sheets и products. Если запуск уже активен, сервис вежливо
        отказывает и просит дождаться завершения.
        """
        async with self._state_lock:
            if self._active_task is not None and not self._active_task.done():
                assert self._active_run is not None
                return self._build_already_running_text(self._active_run)

            started_at = datetime.now(tz=MOSCOW_TZ)
            self._active_run = ActiveRunInfo(
                started_at=started_at,
                requested_by=actor.display_name,
                chat_id=chat_id,
            )
            self._active_task = asyncio.create_task(
                self._run_task_in_background(
                    bot=bot,
                    actor=actor,
                    chat_id=chat_id,
                    started_at=started_at,
                )
            )

        logger.info(
            "Telegram-бот принял запуск add_new_items | chat_id=%s | user_id=%s | actor=%s",
            chat_id,
            actor.user_id,
            actor.display_name,
        )
        return (
            "Добавление товаров запущено.\n\n"
            f"Кто запустил: {actor.display_name}\n"
            f"Старт: {self._format_dt(started_at)}\n"
            "Итог придет отдельным сообщением в этот чат.\n"
            "Промежуточный статус: /status"
        )

    async def get_status_text(self) -> str:
        """Возвращает человекочитаемый статус текущего запуска для команды `/status`.

        Команда нужна операторам, чтобы быстро понять, свободен ли бот для
        нового запуска или перенос новых товаров уже выполняется прямо сейчас.
        """
        async with self._state_lock:
            active_run = self._active_run
            active_task = self._active_task

        if active_run is not None and active_task is not None and not active_task.done():
            return (
                "Статус: в работе\n\n"
                f"Кто запустил: {active_run.requested_by}\n"
                f"Старт: {self._format_dt(active_run.started_at)}\n"
                "Итог придет отдельным сообщением после завершения."
            )

        if self._last_result is None:
            return "Статус: сейчас активного запуска нет."

        return (
            "Статус: сейчас активного запуска нет.\n\n"
            "Последний результат:\n"
            f"{self._format_result_text(self._last_result)}"
        )

    async def get_last_result_text(self) -> str:
        """Возвращает итог последнего завершённого запуска для команды `/last_result`.

        Этот сценарий нужен, когда сотрудник зашел в чат позже старта job и
        хочет посмотреть результат без ожидания следующего запуска.
        """
        async with self._state_lock:
            last_result = self._last_result

        if last_result is None:
            return "Запусков `add_new_items_run` через этого бота пока не было."

        return self._format_result_text(last_result)

    async def _run_task_in_background(
        self,
        *,
        bot,
        actor: TelegramLaunchActor,
        chat_id: int,
        started_at: datetime,
    ) -> None:
        """Выполняет CLI-задачу в фоне и отправляет итог в Telegram после завершения.

        Вынесение запуска в background-задачу защищает UX бота: команда отвечает
        сразу, а длительный перенос новых товаров продолжается отдельно и не
        блокирует получение следующих сообщений.
        """
        try:
            result = await self._execute_subprocess(actor=actor, started_at=started_at)
        except Exception as error:
            logger.exception(
                "Фоновый запуск add_new_items через Telegram завершился внутренней ошибкой | error_type=%s",
                type(error).__name__,
            )
            result = self._build_internal_error_result(
                actor=actor,
                started_at=started_at,
                error_type=type(error).__name__,
            )

        async with self._state_lock:
            self._last_result = result
            self._active_run = None
            self._active_task = None

        try:
            await bot.send_message(
                chat_id=chat_id,
                text=self._format_result_text(result),
                disable_web_page_preview=True,
            )
        except Exception as error:
            logger.error(
                "Не удалось отправить итог add_new_items в Telegram | chat_id=%s | error_type=%s",
                chat_id,
                type(error).__name__,
            )

    async def _execute_subprocess(
        self,
        *,
        actor: TelegramLaunchActor,
        started_at: datetime,
    ) -> TaskRunResult:
        """Запускает `python main.py add_new_items_run` в отдельном процессе.

        Отдельный subprocess сохраняет полное соответствие ручному CLI-запуску и
        изолирует бота от возможного падения job. Это ключевое бизнес-правило
        для безопасного запуска с сервера из общей Telegram-группы.
        """
        command = [sys.executable, "main.py", "add_new_items_run"]
        logger.info(
            "Запускаем add_new_items через отдельный процесс | command=%s | cwd=%s",
            command,
            self._project_root,
        )
        process = await asyncio.create_subprocess_exec(
            *command,
            cwd=str(self._project_root),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

        timed_out = False
        try:
            stdout, stderr = await asyncio.wait_for(
                process.communicate(),
                timeout=self.settings.subprocess_timeout_seconds,
            )
        except asyncio.TimeoutError:
            timed_out = True
            logger.error(
                "Процесс add_new_items превысил таймаут Telegram-бота | timeout_seconds=%s",
                self.settings.subprocess_timeout_seconds,
            )
            process.kill()
            stdout, stderr = await process.communicate()

        finished_at = datetime.now(tz=MOSCOW_TZ)
        exit_code = process.returncode
        combined_output = self._merge_process_output(stdout=stdout, stderr=stderr)
        log_excerpt = self._build_log_excerpt(combined_output)
        duration_seconds = max(int((finished_at - started_at).total_seconds()), 0)

        if timed_out:
            return TaskRunResult(
                status="timeout",
                started_at=started_at,
                finished_at=finished_at,
                requested_by=actor.display_name,
                exit_code=exit_code,
                duration_seconds=duration_seconds,
                summary="Статус: остановлено по таймауту",
                details_text=(
                    "Процесс не уложился в лимит времени.\n"
                    "Часть строк могла успеть записаться.\n"
                    "Проверьте флаги `Добавлено в MAIN (tested)`, "
                    "`Добавлено в Автопилот`, `Добавлено в products`."
                ),
                log_excerpt=log_excerpt,
            )

        parsed_stats = self._extract_add_new_items_stats(combined_output)
        if exit_code == 0:
            return TaskRunResult(
                status="success",
                started_at=started_at,
                finished_at=finished_at,
                requested_by=actor.display_name,
                exit_code=exit_code,
                duration_seconds=duration_seconds,
                summary="Статус: завершено",
                details_text=self._build_success_details(parsed_stats),
                log_excerpt=log_excerpt,
            )

        return TaskRunResult(
            status="failed",
            started_at=started_at,
            finished_at=finished_at,
            requested_by=actor.display_name,
            exit_code=exit_code,
            duration_seconds=duration_seconds,
            summary="Статус: ошибка",
            details_text=self._build_failure_details(parsed_stats),
            log_excerpt=log_excerpt,
        )

    def _build_internal_error_result(
        self,
        *,
        actor: TelegramLaunchActor,
        started_at: datetime,
        error_type: str,
    ) -> TaskRunResult:
        """Формирует безопасный результат, если сбой произошел внутри самого бота.

        Этот helper нужен, чтобы оператор в чате все равно получил финальный
        ответ, даже если subprocess не успел стартовать или фоновая задача
        сломалась до получения стандартного exit code.
        """
        finished_at = datetime.now(tz=MOSCOW_TZ)
        duration_seconds = max(int((finished_at - started_at).total_seconds()), 0)
        return TaskRunResult(
            status="failed",
            started_at=started_at,
            finished_at=finished_at,
            requested_by=actor.display_name,
            exit_code=None,
            duration_seconds=duration_seconds,
            summary="Статус: ошибка запуска",
            details_text=(
                "Серверный процесс мог не стартовать или завершиться нестандартно.\n"
                "Проверьте лог и флаги добавления в исходной таблице."
            ),
            log_excerpt=f"Внутренняя ошибка бота: {error_type}",
        )

    @staticmethod
    def _merge_process_output(*, stdout: bytes, stderr: bytes) -> str:
        """Объединяет stdout и stderr процесса в один безопасно декодированный текст.

        Бизнес-смысл объединения простой: оператору удобнее видеть один хвост
        лога, чем разбираться, в какой поток упал traceback конкретной job.
        """
        stdout_text = stdout.decode("utf-8", errors="replace").strip()
        stderr_text = stderr.decode("utf-8", errors="replace").strip()
        parts = [part for part in (stdout_text, stderr_text) if part]
        return "\n".join(parts)

    def _build_log_excerpt(self, raw_output: str) -> str:
        """Готовит укороченный и обезличенный хвост лога для Telegram-ответа.

        Helper защищает два бизнес-правила сразу: не перегружать чат слишком
        длинным текстом и не пересылать в Telegram чувствительные данные из
        окружения или служебных трейсбеков.
        """
        if not raw_output.strip():
            return "Дополнительный лог процесса отсутствует."

        safe_output = self._mask_sensitive_values(raw_output)
        tail_lines = safe_output.splitlines()[-self.settings.log_tail_lines :]
        excerpt = "\n".join(tail_lines).strip()
        if len(excerpt) > self.settings.telegram_log_max_length:
            excerpt = excerpt[-self.settings.telegram_log_max_length :]
            excerpt = f"...\n{excerpt}"
        return excerpt or "Дополнительный лог процесса отсутствует."

    def _format_result_text(self, result: TaskRunResult) -> str:
        """Форматирует итог запуска в короткое сообщение для рабочей группы.

        Итоговый текст должен быть понятен без чтения серверных логов: участник
        группы сразу видит статус, инициатора, длительность и хвост лога, если
        что-то пошло не так.
        """
        lines = [
            result.summary,
            "",
            f"Кто запустил: {result.requested_by}",
            f"Старт: {self._format_dt(result.started_at)}",
            f"Финиш: {self._format_dt(result.finished_at)}",
            f"Длительность: {self._format_duration(result.duration_seconds)}",
        ]
        if result.exit_code is not None:
            lines.append(f"Код завершения: {result.exit_code}")
        if result.details_text:
            lines.extend(["", result.details_text])
        if result.log_excerpt:
            lines.extend(["", "Хвост лога:", result.log_excerpt])
        return "\n".join(lines)

    @staticmethod
    def _build_already_running_text(active_run: ActiveRunInfo) -> str:
        """Формирует ответ, если job уже выполняется другим участником группы.

        Этот текст защищает основную бизнес-логику от дублей: бот явно сообщает,
        что второй запуск сейчас запрещен, и показывает автора текущего процесса.
        """
        started_at = active_run.started_at.strftime("%d.%m.%Y %H:%M:%S")
        return (
            "Статус: уже выполняется\n\n"
            f"Кто запустил: {active_run.requested_by}\n"
            f"Старт: {started_at}\n"
            "Дождитесь итогового сообщения и повторите попытку позже."
        )

    @staticmethod
    def _format_dt(value: datetime) -> str:
        """Преобразует дату во внутренний человекочитаемый формат для Telegram.

        Единый формат времени нужен, чтобы статусы и итоги запусков одинаково
        читались в группе и не требовали отдельного пояснения сотрудникам.
        """
        return value.strftime("%d.%m.%Y %H:%M:%S")

    @staticmethod
    def _mask_sensitive_values(text: str) -> str:
        """Маскирует типовые секреты в логе перед отправкой в Telegram.

        Даже если traceback или служебный вывод случайно включит токен, cookie
        или пароль, helper старается скрыть значение и оставить только
        диагностически полезный префикс поля.
        """
        masked_text = text
        for pattern in _SENSITIVE_PATTERNS:
            masked_text = pattern.sub(r"\1***", masked_text)
        return masked_text

    @staticmethod
    def _extract_add_new_items_stats(raw_output: str) -> ParsedAddNewItemsStats | None:
        """Извлекает из лога счетчики итогового результата add_new_items.

        Job пишет в лог строку `AddNewItemsResult(...)`. Если она есть, бот
        использует ее для более понятной сводки в финальном Telegram-сообщении,
        чтобы команда видела масштаб переноса без чтения серверной консоли.
        """
        match = _ADD_NEW_ITEMS_RESULT_PATTERN.search(raw_output)
        if match is None:
            return None

        return ParsedAddNewItemsStats(
            loaded_cards=int(match.group("loaded_cards")),
            added_to_sopost=int(match.group("added_to_sopost")),
            added_to_unit_main=int(match.group("added_to_unit_main")),
            added_to_autopilot=int(match.group("added_to_autopilot")),
            added_to_competitors=int(match.group("added_to_competitors")),
            added_to_products=int(match.group("added_to_products")),
        )

    @staticmethod
    def _build_success_details(parsed_stats: ParsedAddNewItemsStats | None) -> str:
        """Собирает пояснение для успешного завершения add_new_items.

        Даже если процесс завершился без системной ошибки, перенос по бизнесу
        может быть частичным: часть строк уже существовала или запись в один из
        контуров могла не состояться. Поэтому бот всегда напоминает проверять
        итоговые флаги `да/нет` в исходной таблице.
        """
        lines = ["Проверьте итоговые флаги в источнике: MAIN, Автопилот, products."]
        if parsed_stats is not None:
            lines.extend(
                [
                    "",
                    "Сводка:",
                    f"Строк в обработке: {parsed_stats.loaded_cards}",
                    f"Сопост: {parsed_stats.added_to_sopost}",
                    f"MAIN (tested): {parsed_stats.added_to_unit_main}",
                    f"Автопилот: {parsed_stats.added_to_autopilot}",
                    f"Конкуренты: {parsed_stats.added_to_competitors}",
                    f"products: {parsed_stats.added_to_products}",
                ]
            )
        return "\n".join(lines)

    @staticmethod
    def _build_failure_details(parsed_stats: ParsedAddNewItemsStats | None) -> str:
        """Собирает пояснение для случая, когда add_new_items завершился с ошибкой.

        При ошибке часть записей могла уже успеть пройти до сбоя. Поэтому бот
        напоминает о возможном частичном результате и, если доступны счетчики,
        показывает их как ориентир для ручной проверки.
        """
        lines = [
            "Процесс завершился с ошибкой, но часть данных могла записаться.",
            "Проверьте итоговые флаги в источнике: MAIN, Автопилот, products.",
        ]
        if parsed_stats is not None:
            lines.extend(
                [
                    "",
                    "Сводка:",
                    f"Строк в обработке: {parsed_stats.loaded_cards}",
                    f"Сопост: {parsed_stats.added_to_sopost}",
                    f"MAIN (tested): {parsed_stats.added_to_unit_main}",
                    f"Автопилот: {parsed_stats.added_to_autopilot}",
                    f"Конкуренты: {parsed_stats.added_to_competitors}",
                    f"products: {parsed_stats.added_to_products}",
                ]
            )
        return "\n".join(lines)

    @staticmethod
    def _format_duration(duration_seconds: int) -> str:
        """Преобразует длительность в короткий человекочитаемый формат.

        В Telegram-итоге длительность должна считываться с одного взгляда,
        поэтому helper убирает лишнюю точность и показывает только значимые
        единицы времени.
        """
        minutes, seconds = divmod(duration_seconds, 60)
        hours, minutes = divmod(minutes, 60)
        if hours:
            return f"{hours} ч {minutes} мин"
        if minutes:
            return f"{minutes} мин {seconds} сек"
        return f"{seconds} сек"
