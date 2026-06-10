"""Тонкие entrypoint-функции для запуска модуля актов WB.

Файл не подключён к `tasks_registry.py` и не должен содержать бизнес-логику
pipeline. Его задача — подготовить период запуска, ограничить набор аккаунтов,
создать сервис и вызвать нужный orchestration-метод.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable, Mapping
from datetime import date, timedelta

from src_oop.core.utils_general import load_api_tokens
from src_oop.jobs.wb_api.acceptance_acts.models import JobRunResult
from src_oop.jobs.wb_api.acceptance_acts.service import AcceptanceActsService

logger = logging.getLogger(__name__)

DEFAULT_DAYS_BACK = 28


def _coerce_date(value: date | str | None, parameter_name: str) -> date | None:
    """Преобразует входной параметр периода к объекту `date`.

    Поддерживаются:
    - `date`;
    - строка в формате `YYYY-MM-DD`;
    - `None`.
    """
    if value is None:
        return None

    if isinstance(value, date):
        return value

    if isinstance(value, str):
        try:
            return date.fromisoformat(value)
        except ValueError as error:
            raise ValueError(
                f"Параметр {parameter_name} должен быть датой в формате YYYY-MM-DD."
            ) from error

    raise TypeError(
        f"Параметр {parameter_name} должен иметь тип date, str или None."
    )


def _resolve_period(
    date_from: date | str | None = None,
    date_to: date | str | None = None,
    days_back: int | None = None,
) -> tuple[date, date]:
    """Разрешает период запуска entrypoint-функции.

    Если период не передан явно, используется прозрачный дефолт:
    - `date_to = today`;
    - `date_from = today - 28 days`.

    Параметр `days_back` нужен только как совместимый shortcut для запуска jobs
    по относительному периоду. Если переданы `date_from` или `date_to`, они
    имеют приоритет.
    """
    resolved_date_from = _coerce_date(date_from, "date_from")
    resolved_date_to = _coerce_date(date_to, "date_to")

    if days_back is not None:
        if days_back < 0:
            raise ValueError("Параметр days_back не может быть отрицательным.")
        if resolved_date_from is None and resolved_date_to is None:
            resolved_date_to = date.today()
            resolved_date_from = resolved_date_to - timedelta(days=days_back)

    if resolved_date_to is None:
        resolved_date_to = date.today()

    if resolved_date_from is None:
        resolved_date_from = resolved_date_to - timedelta(days=DEFAULT_DAYS_BACK)

    if resolved_date_from > resolved_date_to:
        raise ValueError("Параметр date_from не может быть позже date_to.")

    return resolved_date_from, resolved_date_to


def _filter_tokens_by_accounts(
    tokens_by_account: Mapping[str, str],
    accounts: Iterable[str] | None,
) -> dict[str, str]:
    """Фильтрует словарь токенов по запрошенному набору аккаунтов.

    Если `accounts is None`, возвращается копия исходного словаря.
    Если после фильтрации не осталось ни одного аккаунта, выбрасывается
    `ValueError`, чтобы не запускать пустой job молча.
    """
    filtered_tokens = {
        account: token
        for account, token in tokens_by_account.items()
        if isinstance(account, str)
        and account.strip()
        and isinstance(token, str)
        and token.strip()
    }

    if accounts is None:
        return dict(filtered_tokens)

    normalized_accounts = tuple(
        account.strip()
        for account in accounts
        if isinstance(account, str) and account.strip()
    )
    if not normalized_accounts:
        raise ValueError(
            "Параметр accounts передан, но не содержит ни одного валидного имени аккаунта."
        )

    requested_accounts = set(normalized_accounts)
    result = {
        account: token
        for account, token in filtered_tokens.items()
        if account in requested_accounts
    }

    missing_accounts = sorted(requested_accounts - set(result))
    if missing_accounts:
        logger.warning(
            "Часть запрошенных аккаунтов не найдена в токенах WB: missing_accounts=%s",
            missing_accounts,
        )

    if not result:
        raise ValueError(
            "После фильтрации не осталось ни одного аккаунта для запуска acceptance_acts."
        )

    return result


def _prepare_tokens_by_accounts(
    accounts: Iterable[str] | None,
) -> dict[str, str]:
    """Загружает токены проекта и при необходимости ограничивает их аккаунтами."""
    loaded_tokens = load_api_tokens()
    if not isinstance(loaded_tokens, Mapping):
        raise TypeError("load_api_tokens() должен возвращать Mapping account -> token.")

    return _filter_tokens_by_accounts(
        tokens_by_account=loaded_tokens,
        accounts=accounts,
    )


async def run_fbo_acceptance_acts(
    date_from: date | str | None = None,
    date_to: date | str | None = None,
    days_back: int | None = None,
    dry_run: bool = False,
    accounts: list[str] | tuple[str, ...] | set[str] | None = None,
) -> JobRunResult:
    """Запускает новый OOP-entrypoint для ФБО-актов WB.

    Функция не содержит бизнес-логики и только:
    - разрешает период запуска;
    - загружает токены стандартным механизмом проекта;
    - при необходимости ограничивает запуск набором `accounts`;
    - создаёт `AcceptanceActsService`;
    - вызывает `service.run_fbo(...)`;
    - возвращает `JobRunResult`.

    Если период не передан, используется дефолтный интервал:
    `today - 28 days .. today`.

    Если `accounts=None`, запуск использует все доступные аккаунты из токенов.
    """
    resolved_date_from, resolved_date_to = _resolve_period(
        date_from=date_from,
        date_to=date_to,
        days_back=days_back,
    )
    tokens_by_account = _prepare_tokens_by_accounts(accounts)

    logger.info(
        "Старт entrypoint acceptance_acts FBO: period=%s..%s dry_run=%s requested_accounts=%s filtered_accounts=%s",
        resolved_date_from.isoformat(),
        resolved_date_to.isoformat(),
        dry_run,
        sorted(accounts) if accounts is not None else None,
        len(tokens_by_account),
    )

    service = AcceptanceActsService(dry_run=dry_run)
    result = await service.run_fbo(
        date_from=resolved_date_from,
        date_to=resolved_date_to,
        tokens_by_account=tokens_by_account,
    )

    logger.info(
        "Завершён entrypoint acceptance_acts FBO: accounts=%s documents=%s excel_files=%s written_rows=%s warnings=%s errors=%s dry_run=%s",
        result.accounts_total,
        result.documents_downloaded,
        result.excel_files_found,
        result.written_rows,
        len(result.warnings),
        len(result.errors),
        dry_run,
    )
    return result


async def run_fbs_acceptance_acts(
    date_from: date | str | None = None,
    date_to: date | str | None = None,
    days_back: int | None = None,
    dry_run: bool = False,
    accounts: list[str] | tuple[str, ...] | set[str] | None = None,
) -> JobRunResult:
    """Запускает новый OOP-entrypoint для ФБС-актов WB.

    Если период не передан, используется дефолтный интервал:
    `today - 28 days .. today`.

    Если `accounts=None`, запуск использует все доступные аккаунты из токенов.
    """
    resolved_date_from, resolved_date_to = _resolve_period(
        date_from=date_from,
        date_to=date_to,
        days_back=days_back,
    )
    tokens_by_account = _prepare_tokens_by_accounts(accounts)

    logger.info(
        "Старт entrypoint acceptance_acts FBS: period=%s..%s dry_run=%s requested_accounts=%s filtered_accounts=%s",
        resolved_date_from.isoformat(),
        resolved_date_to.isoformat(),
        dry_run,
        sorted(accounts) if accounts is not None else None,
        len(tokens_by_account),
    )

    service = AcceptanceActsService(dry_run=dry_run)
    result = await service.run_fbs(
        date_from=resolved_date_from,
        date_to=resolved_date_to,
        tokens_by_account=tokens_by_account,
    )

    logger.info(
        "Завершён entrypoint acceptance_acts FBS: accounts=%s documents=%s excel_files=%s written_rows=%s warnings=%s errors=%s dry_run=%s",
        result.accounts_total,
        result.documents_downloaded,
        result.excel_files_found,
        result.written_rows,
        len(result.warnings),
        len(result.errors),
        dry_run,
    )
    return result


async def run_all_acceptance_acts(
    date_from: date | str | None = None,
    date_to: date | str | None = None,
    days_back: int | None = None,
    dry_run: bool = False,
    accounts: list[str] | tuple[str, ...] | set[str] | None = None,
) -> JobRunResult:
    """Запускает новый OOP-entrypoint для совместной обработки ФБО и ФБС.

    Если период не передан, используется дефолтный интервал:
    `today - 28 days .. today`.

    Если `accounts=None`, запуск использует все доступные аккаунты из токенов.
    """
    resolved_date_from, resolved_date_to = _resolve_period(
        date_from=date_from,
        date_to=date_to,
        days_back=days_back,
    )
    tokens_by_account = _prepare_tokens_by_accounts(accounts)

    logger.info(
        "Старт entrypoint acceptance_acts all: period=%s..%s dry_run=%s requested_accounts=%s filtered_accounts=%s",
        resolved_date_from.isoformat(),
        resolved_date_to.isoformat(),
        dry_run,
        sorted(accounts) if accounts is not None else None,
        len(tokens_by_account),
    )

    service = AcceptanceActsService(dry_run=dry_run)
    result = await service.run_all(
        date_from=resolved_date_from,
        date_to=resolved_date_to,
        tokens_by_account=tokens_by_account,
    )

    logger.info(
        "Завершён entrypoint acceptance_acts all: accounts=%s documents=%s excel_files=%s written_rows=%s warnings=%s errors=%s dry_run=%s",
        result.accounts_total,
        result.documents_downloaded,
        result.excel_files_found,
        result.written_rows,
        len(result.warnings),
        len(result.errors),
        dry_run,
    )
    return result
