"""Общие Telegram-клавиатуры, не привязанные к конкретному бизнес-модулю."""

from __future__ import annotations


def build_back_keyboard(callback_data: str):
    """Строит простую inline-кнопку «Назад» для интерактивных сценариев.

    Кнопка нужна как единый UX-элемент для навигации по Telegram-ботам проекта.
    Импорт `aiogram` выполняется лениво, чтобы инфраструктурный код не ломал
    остальные сценарии в среде, где пакет ещё не установлен.
    """
    from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ Назад", callback_data=callback_data)]
        ]
    )


def build_commands_reply_keyboard(*rows: tuple[str, ...]):
    """Строит обычную экранную клавиатуру из набора строк-команд.

    Клавиатура нужна для ботов, где пользователям удобнее нажимать на видимые
    кнопки, чем помнить текст slash-команд. Helper остается общим, чтобы разные
    Telegram-модули проекта могли собирать свои наборы кнопок без дублирования.
    """
    from aiogram.types import KeyboardButton, ReplyKeyboardMarkup

    keyboard = [
        [KeyboardButton(text=button_text) for button_text in row if button_text]
        for row in rows
        if row
    ]
    return ReplyKeyboardMarkup(
        keyboard=keyboard,
        resize_keyboard=True,
        one_time_keyboard=False,
    )
