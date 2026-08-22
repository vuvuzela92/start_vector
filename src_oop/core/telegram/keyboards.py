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
