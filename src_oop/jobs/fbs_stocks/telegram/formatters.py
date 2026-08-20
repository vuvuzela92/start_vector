from __future__ import annotations

from collections import Counter
from datetime import datetime

from src_oop.jobs.fbs_stocks.telegram.models import FBSJobFailureContext, FBSNotificationEvent


def format_dry_run_message(
    job_name: str,
    account_scope: str,
    checked_rows: int,
    prepared_rows: int,
    skipped_rows: int,
    happened_at: datetime,
) -> str:
    """Формирует Telegram-сообщение о dry-run запуске без реальной отправки в WB.

    Бизнес-сценарий: оператор должен сразу понимать, что прогон завершился штатно, но фактическая
    запись на WB была отключена и данные на сайте не менялись.
    """
    return (
        "FBS / DRY-RUN\n\n"
        f"Сценарий: {job_name}\n"
        f"ЛК: {account_scope}\n"
        "Статус: ПРОДАКТ-РЕЖИМ ОТКЛЮЧЕН\n\n"
        "Данные на WB не отправлялись.\n"
        f"Обработано строк: {checked_rows}\n"
        f"Подготовлено команд: {prepared_rows}\n"
        f"Пропущено строк: {skipped_rows}\n\n"
        f"Время: {happened_at.strftime('%Y-%m-%d %H:%M:%S')}"
    )


def format_full_failure_message(context: FBSJobFailureContext) -> str:
    """Формирует Telegram-сообщение о полном срыве FBS-сценария.

    Бизнес-сценарий: если задача оборвалась целиком, пользователю нужна краткая и понятная сводка
    без трассировки Python, чтобы быстро понять, какой сценарий не выполнился и по какой причине.
    """
    lines = [
        "FBS / ОШИБКА",
        "",
        f"Сценарий: {context.job_name}",
        f"ЛК: {context.account_scope}",
        "Статус: выполнение прервано",
        "",
        f"Причина: {context.reason}",
        f"Тип ошибки: {context.error_type}",
    ]
    if context.detail:
        lines.extend(["", f"Деталь: {context.detail}"])
    lines.extend(["", f"Время: {context.happened_at.strftime('%Y-%m-%d %H:%M:%S')}"])
    return "\n".join(lines)


def format_partial_failure_message(
    job_name: str,
    account_scope: str,
    events: list[FBSNotificationEvent],
    happened_at: datetime,
) -> str:
    """Формирует итоговую сводку частичных сбоев по всему FBS-прогону.

    Бизнес-сценарий: при частичном успехе оператору важно увидеть, сколько проблем осталось и какие
    причины были основными, не проваливаясь сразу в десятки отдельных сообщений.
    """
    reason_counter = Counter(
        f"{event.reason_code}: {event.reason}"
        for event in events
    )
    top_reasons = "\n".join(
        f"- {reason}: {count}"
        for reason, count in reason_counter.most_common(5)
    )
    failed_articles = len({event.article_id for event in events if event.article_id is not None})
    return (
        "FBS / ЧАСТИЧНЫЙ СБОЙ\n\n"
        f"Сценарий: {job_name}\n"
        f"ЛК: {account_scope}\n\n"
        f"Проблемных событий: {len(events)}\n"
        f"Затронуто артикулов: {failed_articles}\n\n"
        "Основные причины:\n"
        f"{top_reasons}\n\n"
        f"Время: {happened_at.strftime('%Y-%m-%d %H:%M:%S')}"
    )


def format_wild_summary_message(
    job_name: str,
    account: str,
    wild: str,
    events: list[FBSNotificationEvent],
    happened_at: datetime,
    max_article_examples: int,
) -> str:
    """Формирует сводку массовой проблемы по одному `wild`.

    Бизнес-сценарий: если один и тот же сбой затронул сразу несколько строк одного `wild`, оператору
    удобнее получить одну агрегированную карточку проблемы, а не отдельное сообщение на каждый SKU.
    """
    article_examples = [
        str(article_id)
        for article_id in sorted(
            {
                event.article_id
                for event in events
                if event.article_id is not None
            }
        )[:max_article_examples]
    ]
    warehouse_names = sorted(
        {
            event.warehouse_name
            for event in events
            if event.warehouse_name
        }
    )
    first_event = events[0]
    return (
        "FBS / ПРОБЛЕМА ПО WILD\n\n"
        f"Сценарий: {job_name}\n"
        f"ЛК: {account}\n"
        f"wild: {wild}\n\n"
        f"Строк с проблемой: {len(events)}\n"
        f"Артикулов затронуто: {len(set(article_examples))}\n"
        f"Складов затронуто: {len(warehouse_names)}\n\n"
        f"Причина: {first_event.reason}\n"
        f"Деталь: {first_event.detail or 'без дополнительной детали'}\n\n"
        "Примеры артикулов:\n"
        f"{', '.join(article_examples) if article_examples else 'нет данных'}\n\n"
        f"Время: {happened_at.strftime('%Y-%m-%d %H:%M:%S')}"
    )


def format_article_failure_message(
    event: FBSNotificationEvent,
    happened_at: datetime,
) -> str:
    """Формирует точечное уведомление по одному артикулу FBS.

    Бизнес-сценарий: единичные проблемы не должны растворяться в общей сводке. Оператору нужно
    видеть конкретный артикул, ЛК и склад, чтобы быстро проверить точечный сбой на стороне WB.
    """
    lines = [
        "FBS / ПРОБЛЕМА ПО АРТИКУЛУ",
        "",
        f"Сценарий: {event.job_name}",
        f"ЛК: {event.account or 'не указан'}",
    ]
    if event.article_id is not None:
        lines.append(f"Артикул: {event.article_id}")
    if event.wild:
        lines.append(f"wild: {event.wild}")
    lines.extend([""])
    if event.warehouse_name:
        lines.append(f"Склад: {event.warehouse_name}")
    if event.wb_warehouse_id is not None:
        lines.append(f"WB warehouse id: {event.wb_warehouse_id}")
    if event.wb_office_id is not None:
        lines.append(f"WB office id: {event.wb_office_id}")
    lines.extend(
        [
            "",
            f"Причина: {event.reason}",
            f"Деталь: {event.detail or 'без дополнительной детали'}",
            "",
            f"Время: {happened_at.strftime('%Y-%m-%d %H:%M:%S')}",
        ]
    )
    return "\n".join(lines)
