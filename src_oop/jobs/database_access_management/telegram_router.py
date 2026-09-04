"""Telegram-вход руководителей в сервис управления доступами."""

from __future__ import annotations

import asyncio
import logging

from src_oop.jobs.database_access_management.config import DatabaseAccessManagementSettings
from src_oop.jobs.database_access_management.executor import (
    EnvironmentSecretResolver,
    PostgreSQLGrantExecutor,
)
from src_oop.jobs.database_access_management.models import AccessGrantRequest
from src_oop.jobs.database_access_management.postgresql_adapter import PostgreSQLAccessAdapter
from src_oop.jobs.database_access_management.postgresql_inventory import (
    PostgreSQLAccessInventory,
)
from src_oop.jobs.database_access_management.repository import AccessGrantRepository
from src_oop.jobs.database_access_management.telegram_config import (
    DatabaseAccessTelegramSettings,
)

logger = logging.getLogger(__name__)
_TELEGRAM_SQL_CHUNK_LENGTH = 3500


def _is_manager_chat(chat_id: int, settings: DatabaseAccessTelegramSettings) -> bool:
    """Проверяет, что команда пришла из разрешённой управляющей группы."""

    return chat_id in settings.manager_chat_ids


def create_database_access_router():
    """Создаёт router с закрытым стартовым меню для руководителей.

    Функция запускает первый Telegram-сценарий MVP: до подключения форм выдачи
    и отзыва бот проверяет отдельный allow-list и показывает только безопасное
    меню доступных управленческих действий.
    """

    from aiogram import F, Router
    from aiogram.filters import Command, CommandObject, CommandStart
    from aiogram.fsm.context import FSMContext
    from aiogram.fsm.state import State, StatesGroup
    from aiogram.types import (
        CallbackQuery,
        InlineKeyboardButton,
        InlineKeyboardMarkup,
        KeyboardButton,
        Message,
        ReplyKeyboardMarkup,
    )

    class GrantForm(StatesGroup):
        """Хранит выбор руководителя до создания одного распоряжения."""

        target = State()
        level = State()
        schema = State()
        details = State()
        login_created = State()

    class DeleteUserForm(StatesGroup):
        """Хранит ожидание логина для удаления учётной записи."""

        login = State()

    class RevokeForm(StatesGroup):
        """Хранит ожидание логина для выбора активного доступа к отзыву."""

        login = State()

    class MassDeleteForm(StatesGroup):
        """Хранит ожидание списка логинов для массового удаления."""

        logins = State()

    router = Router(name="database_access_management")
    settings = DatabaseAccessTelegramSettings.from_env()

    async def send_sql_plan(message: Message, title: str, sql_plan: str) -> None:
        """Отправляет SQL-план частями, не превышая лимит сообщения Telegram.

        Функция обслуживает ручную выдачу доступа: полный план прав может быть
        длиннее лимита Telegram, поэтому команды разбиваются только по границам
        строк и остаются пригодными для последовательного копирования.
        """

        sql_chunks: list[str] = []
        current_lines: list[str] = []
        current_length = 0
        for line in sql_plan.splitlines():
            line_length = len(line) + 1
            if current_lines and current_length + line_length > _TELEGRAM_SQL_CHUNK_LENGTH:
                sql_chunks.append("\n".join(current_lines))
                current_lines = []
                current_length = 0
            current_lines.append(line)
            current_length += line_length
        if current_lines:
            sql_chunks.append("\n".join(current_lines))

        for index, chunk in enumerate(sql_chunks, start=1):
            await message.answer(
                f"{title}, часть {index}/{len(sql_chunks)}:\n```sql\n{chunk}\n```",
                parse_mode="Markdown",
            )
    reply_keyboard = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="➕ Выдать доступ"), KeyboardButton(text="📋 Активные доступы")],
            [KeyboardButton(text="🔎 Инвентаризация PostgreSQL"), KeyboardButton(text="⛔ Отозвать доступ")],
            [KeyboardButton(text="🗑 Удалить пользователя")],
            [KeyboardButton(text="🗑 Массовое удаление")],
        ],
        resize_keyboard=True,
        input_field_placeholder="Выберите действие или введите команду",
    )

    @router.message(CommandStart())
    async def start(message: Message) -> None:
        """Проверяет руководителя и показывает начальное меню управления доступами."""

        if not _is_manager_chat(message.chat.id, settings):
            logger.warning("Отклонён вход в Telegram-бот управления доступами | chat_id=%s", message.chat.id)
            await message.answer("Доступ к управлению доступами не разрешён.")
            return
        await message.answer(
            "Управление доступами PostgreSQL\n\n"
            "Доступные действия:\n"
            "• /grant — выдать доступ\n"
            "• /accesses — показать активные доступы\n"
            "• /revoke <номер_распоряжения> — отозвать доступ\n\n"
            "Смена роли выполняется через выдачу новой роли и отзыв прежнего доступа.",
            reply_markup=reply_keyboard,
        )
        await message.answer(
            "Быстрые действия:",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(text="➕ Выдать доступ", callback_data="dam_menu:grant")],
                    [InlineKeyboardButton(text="📋 Активные доступы", callback_data="dam_menu:accesses")],
                    [InlineKeyboardButton(text="🔎 Найти существующие доступы", callback_data="dam_menu:inventory")],
                    [InlineKeyboardButton(text="⛔ Отозвать доступ", callback_data="dam_menu:revoke")],
                    [InlineKeyboardButton(text="🗑 Удалить пользователя", callback_data="dam_menu:delete")],
                    [InlineKeyboardButton(text="🗑 Массовое удаление", callback_data="dam_menu:mass_delete")],
                ]
            ),
        )

    @router.message(F.text == "➕ Выдать доступ")
    async def grant_button(message: Message, state: FSMContext) -> None:
        """Открывает форму выдачи доступа по нажатию постоянной кнопки."""

        await start_grant(message, state)

    @router.message(F.text == "📋 Активные доступы")
    async def accesses_button(message: Message) -> None:
        """Сразу показывает все активные доступы, выданные сервисом."""

        if not _is_manager_chat(message.chat.id, settings):
            return
        service_settings = DatabaseAccessManagementSettings.from_env()
        repository = AccessGrantRepository.from_database_url(
            service_settings.database_url, schema_name=service_settings.schema_name
        )
        grants = repository.list_active_grants()
        if not grants:
            await message.answer("Активные PostgreSQL-доступы, выданные сервисом, не найдены.")
            return
        lines = ["Все активные PostgreSQL-доступы:"]
        lines.extend(
            f"• {grant['login_name']} — {grant['database_name']} — {grant['access_level']}"
            for grant in grants
        )
        await message.answer("\n".join(lines))

    @router.message(F.text == "🔎 Инвентаризация PostgreSQL")
    async def inventory_button(message: Message) -> None:
        """Сразу запускает read-only инвентаризацию существующих доступов."""

        if not _is_manager_chat(message.chat.id, settings):
            return
        await message.answer("Ищу существующих пользователей PostgreSQL…")
        service_settings = DatabaseAccessManagementSettings.from_env()
        inventory = PostgreSQLAccessInventory(service_settings.database_url)
        accesses = await asyncio.to_thread(inventory.list_users_and_role_memberships)
        if not accesses:
            await message.answer("В PostgreSQL не найдены прикладные пользователи.")
            return
        lines = ["Пользователи PostgreSQL и их роли:"]
        lines.extend(
            f"• {item.login_name} → {item.role_name or 'роль не назначена'}"
            for item in accesses[:100]
        )
        if len(accesses) > 100:
            lines.append(f"Показаны первые 100 из {len(accesses)} записей.")
        await message.answer("\n".join(lines))

    @router.message(F.text == "⛔ Отозвать доступ")
    async def revoke_button(message: Message, state: FSMContext) -> None:
        """Запрашивает логин для выбора выданного сервисом доступа к отзыву."""

        if not _is_manager_chat(message.chat.id, settings):
            return
        await state.set_state(RevokeForm.login)
        await message.answer("Введите логин пользователя, доступ которого нужно отозвать.")

    @router.message(RevokeForm.login)
    async def choose_grant_for_revocation(message: Message, state: FSMContext) -> None:
        """Показывает кнопки отзыва активных распоряжений выбранного логина."""

        if not _is_manager_chat(message.chat.id, settings):
            await state.clear()
            return
        login_name = (message.text or "").strip()
        service_settings = DatabaseAccessManagementSettings.from_env()
        repository = AccessGrantRepository.from_database_url(
            service_settings.database_url, schema_name=service_settings.schema_name
        )
        grants = repository.list_active_grants(login_name)
        await state.clear()
        if not grants:
            await message.answer("У этого логина нет активных доступов, выданных сервисом.")
            return
        keyboard = InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(
                    text=f"Отозвать {grant['database_name']} — {grant['access_level']}",
                    callback_data=f"dam_revoke:{grant['id']}",
                )]
                for grant in grants
            ]
        )
        await message.answer("Выберите доступ для отзыва:", reply_markup=keyboard)

    @router.message(F.text == "🗑 Удалить пользователя")
    async def delete_user_button(message: Message, state: FSMContext) -> None:
        """Запрашивает логин для безвозвратного удаления учётной записи."""

        if not _is_manager_chat(message.chat.id, settings):
            return
        await state.set_state(DeleteUserForm.login)
        await message.answer("Введите логин пользователя для удаления. Операция необратима.")

    @router.message(F.text == "🗑 Массовое удаление")
    async def mass_delete_button(message: Message, state: FSMContext) -> None:
        """Запрашивает список логинов для последовательного массового удаления."""

        if not _is_manager_chat(message.chat.id, settings):
            return
        await state.set_state(MassDeleteForm.logins)
        await message.answer(
            "Отправьте логины для удаления: каждый с новой строки. Максимум 50. "
            "Операция необратима."
        )

    @router.message(MassDeleteForm.logins)
    async def delete_users_from_list(message: Message, state: FSMContext) -> None:
        """Удаляет список логинов по одному и возвращает итог для каждой строки."""

        if not _is_manager_chat(message.chat.id, settings):
            await state.clear()
            return
        login_names = list(dict.fromkeys(
            line.strip() for line in (message.text or "").splitlines() if line.strip()
        ))
        if not login_names:
            await message.answer("Не найдено ни одного логина. Отправьте список ещё раз.")
            return
        if len(login_names) > 50:
            await message.answer("За один запуск можно удалить не более 50 логинов.")
            return
        await state.clear()
        service_settings = DatabaseAccessManagementSettings.from_env()
        repository = AccessGrantRepository.from_database_url(
            service_settings.database_url, schema_name=service_settings.schema_name
        )
        executor = PostgreSQLGrantExecutor(
            repository=repository, secret_resolver=EnvironmentSecretResolver()
        )
        await message.answer(f"Начинаю удаление {len(login_names)} пользователей…")
        results: list[str] = []
        for login_name in login_names:
            deleted = await asyncio.to_thread(executor.delete_user, login_name)
            results.append(f"{'✅' if deleted else '❌'} {login_name}")
        await message.answer("Итог массового удаления:\n" + "\n".join(results))

    @router.message(DeleteUserForm.login)
    async def delete_user_from_button(message: Message, state: FSMContext) -> None:
        """Удаляет пользователя, логин которого руководитель ввёл после кнопки."""

        if not _is_manager_chat(message.chat.id, settings):
            await state.clear()
            return
        login_name = (message.text or "").strip()
        if not login_name:
            await message.answer("Введите непустой логин пользователя.")
            return
        service_settings = DatabaseAccessManagementSettings.from_env()
        repository = AccessGrantRepository.from_database_url(
            service_settings.database_url, schema_name=service_settings.schema_name
        )
        executor = PostgreSQLGrantExecutor(
            repository=repository, secret_resolver=EnvironmentSecretResolver()
        )
        await message.answer(f"Удаляю пользователя {login_name}…")
        deleted = await asyncio.to_thread(executor.delete_user, login_name)
        await state.clear()
        await message.answer(
            f"Пользователь {login_name} удалён." if deleted else "Не удалось удалить пользователя."
        )

    @router.callback_query(lambda query: query.data and query.data.startswith("dam_menu:"))
    async def handle_menu(query: CallbackQuery) -> None:
        """Обрабатывает кнопки главного меню без выполнения опасных действий."""

        if query.message is None or not _is_manager_chat(query.message.chat.id, settings):
            await query.answer("Доступ не разрешён", show_alert=True)
            return
        action = query.data.removeprefix("dam_menu:")
        prompts = {
            "grant": "Введите /grant, чтобы выбрать базу и роль.",
            "accesses": "Введите /accesses, чтобы увидеть управляемые сервисом доступы.",
            "revoke": "Сначала найдите номер: /accesses, затем /revoke <номер>.",
            "delete": "Введите /delete_user <логин> для безвозвратного удаления пользователя.",
            "mass_delete": "Нажмите постоянную кнопку «🗑 Массовое удаление».",
        }
        if action != "inventory":
            prompt = prompts.get(action)
            if prompt is None:
                await query.answer("Неизвестное действие", show_alert=True)
                return
            await query.message.answer(prompt)
            await query.answer()
            return
        await query.answer("Ищу существующие доступы…")
        await query.message.answer("Ищу существующие PostgreSQL-доступы…")
        service_settings = DatabaseAccessManagementSettings.from_env()
        inventory = PostgreSQLAccessInventory(service_settings.database_url)
        accesses = await asyncio.to_thread(inventory.list_users_and_role_memberships)
        if not accesses:
            await query.message.answer("В PostgreSQL не найдены прикладные пользователи.")
        else:
            lines = ["Пользователи PostgreSQL и их роли:"]
            lines.extend(
                f"• {item.login_name} → {item.role_name or 'роль не назначена'}"
                for item in accesses[:100]
            )
            if len(accesses) > 100:
                lines.append(f"Показаны первые 100 из {len(accesses)} записей.")
            await query.message.answer("\n".join(lines))

    @router.message(Command("grant"))
    async def start_grant(message: Message, state: FSMContext) -> None:
        """Показывает руководителю зарегистрированные PostgreSQL-цели."""

        if not _is_manager_chat(message.chat.id, settings):
            await message.answer("Доступ к управлению доступами не разрешён.")
            return
        service_settings = DatabaseAccessManagementSettings.from_env()
        repository = AccessGrantRepository.from_database_url(
            service_settings.database_url, schema_name=service_settings.schema_name
        )
        targets = repository.list_active_database_targets(engine_name="postgresql")
        if not targets:
            await message.answer("Нет зарегистрированных PostgreSQL-баз.")
            return
        keyboard = InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text=target.display_name, callback_data=f"dam_target:{target.target_id}")]
                for target in targets
            ]
        )
        await state.set_state(GrantForm.target)
        await message.answer("Выберите PostgreSQL-базу:", reply_markup=keyboard)

    @router.message(Command("accesses"))
    async def list_accesses(message: Message, command: CommandObject) -> None:
        """Показывает активные доступы логина без раскрытия секретов."""

        if not _is_manager_chat(message.chat.id, settings):
            await message.answer("Доступ к управлению доступами не разрешён.")
            return
        login_name = (command.args or "").strip() or None
        service_settings = DatabaseAccessManagementSettings.from_env()
        repository = AccessGrantRepository.from_database_url(
            service_settings.database_url, schema_name=service_settings.schema_name
        )
        grants = repository.list_active_grants(login_name)
        if not grants:
            await message.answer("Активные PostgreSQL-доступы не найдены.")
            return
        lines = [
            f"Активные доступы для {login_name}:"
            if login_name
            else "Все активные PostgreSQL-доступы:"
        ]
        for grant in grants:
            lines.append(
                f"• {grant['database_name']} — {grant['access_level']}\n"
                f"  распоряжение: {grant['id']}"
            )
        lines.append("Фильтр: /accesses <логин>\nДля отзыва: /revoke <номер_распоряжения>")
        await message.answer("\n".join(lines))

    @router.message(Command("revoke"))
    async def revoke_access(message: Message, command: CommandObject) -> None:
        """Запускает точечный технический отзыв активного PostgreSQL-доступа."""

        if not _is_manager_chat(message.chat.id, settings):
            await message.answer("Доступ к управлению доступами не разрешён.")
            return
        grant_id = (command.args or "").strip()
        if not grant_id:
            await message.answer("Укажите номер распоряжения: /revoke <номер>")
            return
        service_settings = DatabaseAccessManagementSettings.from_env()
        repository = AccessGrantRepository.from_database_url(
            service_settings.database_url, schema_name=service_settings.schema_name
        )
        executor = PostgreSQLGrantExecutor(
            repository=repository, secret_resolver=EnvironmentSecretResolver()
        )
        revoked = await asyncio.to_thread(executor.revoke, grant_id)
        await message.answer(
            "Доступ отозван." if revoked else "Не удалось отозвать доступ. Проверьте его статус."
        )

    @router.callback_query(lambda query: query.data and query.data.startswith("dam_revoke:"))
    async def revoke_grant_from_button(query: CallbackQuery) -> None:
        """Отзывает выбранный в интерфейсе активный доступ PostgreSQL."""

        if query.message is None or not _is_manager_chat(query.message.chat.id, settings):
            await query.answer("Доступ не разрешён", show_alert=True)
            return
        grant_id = query.data.removeprefix("dam_revoke:")
        await query.answer("Отзываю доступ…")
        service_settings = DatabaseAccessManagementSettings.from_env()
        repository = AccessGrantRepository.from_database_url(
            service_settings.database_url, schema_name=service_settings.schema_name
        )
        executor = PostgreSQLGrantExecutor(
            repository=repository, secret_resolver=EnvironmentSecretResolver()
        )
        revoked = await asyncio.to_thread(executor.revoke, grant_id)
        await query.message.answer("Доступ отозван." if revoked else "Не удалось отозвать доступ.")

    @router.message(Command("delete_user"))
    async def delete_user(message: Message, command: CommandObject) -> None:
        """Удаляет непривилегированную учётную запись PostgreSQL по логину."""

        if not _is_manager_chat(message.chat.id, settings):
            await message.answer("Доступ к управлению доступами не разрешён.")
            return
        login_name = (command.args or "").strip()
        if not login_name:
            await message.answer("Укажите логин: /delete_user ivanov_i")
            return
        service_settings = DatabaseAccessManagementSettings.from_env()
        repository = AccessGrantRepository.from_database_url(
            service_settings.database_url, schema_name=service_settings.schema_name
        )
        executor = PostgreSQLGrantExecutor(
            repository=repository, secret_resolver=EnvironmentSecretResolver()
        )
        deleted = await asyncio.to_thread(executor.delete_user, login_name)
        await message.answer(
            f"Пользователь {login_name} удалён." if deleted else "Не удалось удалить пользователя."
        )

    @router.callback_query(GrantForm.target, lambda query: query.data and query.data.startswith("dam_target:"))
    async def select_target(query: CallbackQuery, state: FSMContext) -> None:
        """Запоминает цель и запрашивает реквизиты персонального доступа."""

        if query.message is None or not _is_manager_chat(query.message.chat.id, settings):
            await query.answer("Доступ не разрешён", show_alert=True)
            return
        target_id = query.data.removeprefix("dam_target:")
        await state.update_data(target_id=target_id)
        keyboard = InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="Чтение схемы", callback_data="dam_level:read_all")],
                [InlineKeyboardButton(text="Запись в схему", callback_data="dam_level:write")],
                [InlineKeyboardButton(text="Управление схемой", callback_data="dam_level:full_access")],
                [InlineKeyboardButton(text="Чтение отдельных таблиц", callback_data="dam_level:read_tables")],
                [InlineKeyboardButton(text="Полный доступ ко всем схемам", callback_data="dam_level:full_all")],
            ]
        )
        await state.set_state(GrantForm.level)
        await query.message.answer("Выберите роль:", reply_markup=keyboard)
        await query.answer()

    @router.callback_query(GrantForm.level, lambda query: query.data and query.data.startswith("dam_level:"))
    async def select_level(query: CallbackQuery, state: FSMContext) -> None:
        """Запоминает роль и объясняет формат реквизитов для выбранной области."""

        if query.message is None or not _is_manager_chat(query.message.chat.id, settings):
            await query.answer("Доступ не разрешён", show_alert=True)
            return
        selected_level = query.data.removeprefix("dam_level:")
        if selected_level == "full_all":
            await state.update_data(level="full_access", schema_name=None)
            await state.set_state(GrantForm.details)
            await query.message.answer("Отправьте логин пользователя PostgreSQL\n\nПример: ivanov_i")
            await query.answer()
            return
        await query.answer("Загружаю схемы…")
        service_settings = DatabaseAccessManagementSettings.from_env()
        adapter = PostgreSQLAccessAdapter(service_settings.database_url)
        try:
            schema_names = await asyncio.to_thread(adapter.list_user_schemas)
        except Exception as error:
            logger.error(
                "Не удалось получить схемы PostgreSQL для выдачи доступа | error_type=%s",
                type(error).__name__,
            )
            await query.message.answer("Не удалось загрузить схемы PostgreSQL. Попробуйте ещё раз.")
            await state.clear()
            return
        if not schema_names:
            await query.message.answer("В целевой базе не найдены прикладные схемы.")
            await state.clear()
            return
        await state.update_data(level=selected_level)
        await state.set_state(GrantForm.schema)
        keyboard = InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text=schema_name, callback_data=f"dam_schema:{schema_name}")]
                for schema_name in schema_names
            ]
        )
        await query.message.answer("Выберите схему:", reply_markup=keyboard)

    @router.callback_query(GrantForm.schema, lambda query: query.data and query.data.startswith("dam_schema:"))
    async def select_schema(query: CallbackQuery, state: FSMContext) -> None:
        """Сохраняет выбранную схему и запрашивает логин с таблицами при необходимости."""

        if query.message is None or not _is_manager_chat(query.message.chat.id, settings):
            await query.answer("Доступ не разрешён", show_alert=True)
            return
        schema_name = query.data.removeprefix("dam_schema:")
        form_data = await state.get_data()
        await state.update_data(schema_name=schema_name)
        await state.set_state(GrantForm.details)
        prompt = (
            "Отправьте логин, затем по одной таблице на строку"
            if form_data["level"] == "read_tables"
            else "Отправьте логин пользователя PostgreSQL"
        )
        await query.message.answer(f"{prompt}\n\nПример: ivanov_i")
        await query.answer()

    @router.message(GrantForm.details)
    async def create_read_all_grant(message: Message, state: FSMContext) -> None:
        """Создаёт распоряжение чтения всех данных для выбранной PostgreSQL-базы."""

        if not _is_manager_chat(message.chat.id, settings):
            await state.clear()
            return
        parts = [part.strip() for part in (message.text or "").split("|")]
        form_data = await state.get_data()
        level = form_data["level"]
        if level == "read_tables":
            parts = [line.strip() for line in (message.text or "").splitlines() if line.strip()]
        expected_count = 2 if level == "read_tables" else 1
        if len(parts) < expected_count or not all(parts):
            await message.answer("Формат сообщения не соответствует выбранной роли.")
            return
        login_name = parts[0]
        display_name = login_name
        if level == "read_tables":
            schema_name = form_data["schema_name"]
            table_names = parts[1:]
            reason = "Выдача доступа через Telegram-бота"
            scope = {
                "database": "",
                "schema_name": schema_name,
                "tables": table_names,
            }
        else:
            reason = "Выдача доступа через Telegram-бота"
            scope = {"database": "", "schema_name": form_data.get("schema_name")}
        service_settings = DatabaseAccessManagementSettings.from_env()
        repository = AccessGrantRepository.from_database_url(
            service_settings.database_url, schema_name=service_settings.schema_name
        )
        targets = repository.list_active_database_targets(engine_name="postgresql")
        target = next((item for item in targets if item.target_id == form_data["target_id"]), None)
        if target is None:
            await state.clear()
            await message.answer("Выбранная база больше недоступна. Начните заново: /grant")
            return
        scope["database"] = target.database_name
        try:
            grant = repository.create_pending_grant(
                AccessGrantRequest(
                    principal={"principal_id": login_name, "principal_type": "human", "login_name": login_name, "display_name": display_name, "secret_ref": "manual://password"},
                    target_id=target.target_id, engine="postgresql", level=level,
                    scope=scope, reason=reason,
                    requested_by=str(message.from_user.id if message.from_user else message.chat.id),
                )
            )
        except (LookupError, ValueError) as error:
            await message.answer(f"Не удалось создать распоряжение: {error}")
            return
        adapter = PostgreSQLAccessAdapter(service_settings.database_url)
        await state.update_data(
            grant_id=grant.id,
            login_name=login_name,
            target_id=target.target_id,
            level=level,
            scope=scope,
            reason=reason,
            requested_by=str(message.from_user.id if message.from_user else message.chat.id),
        )
        await state.set_state(GrantForm.login_created)
        await message.answer(
            f"Распоряжение {grant.id} зарегистрировано. Сначала создайте учётную запись "
            "командой ниже, подставив пароль. После успешного выполнения нажмите «Создано»."
        )
        await send_sql_plan(
            message,
            "Команда создания учётной записи",
            adapter.build_create_login_statement(login_name),
        )
        await message.answer(
            "Продолжить выдачу доступа?",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(text="✅ Создано", callback_data="dam_manual:login_created")],
                    [InlineKeyboardButton(text="↩️ Вернуться назад", callback_data="dam_manual:cancel")],
                ]
            ),
        )

    @router.callback_query(GrantForm.login_created, lambda query: query.data == "dam_manual:login_created")
    async def confirm_login_created(query: CallbackQuery, state: FSMContext) -> None:
        """Выдаёт права существующему логину после нажатия «Создано»."""

        if query.message is None or not _is_manager_chat(query.message.chat.id, settings):
            await query.answer("Доступ не разрешён", show_alert=True)
            return
        await query.answer("Выдаю права…")
        form_data = await state.get_data()
        service_settings = DatabaseAccessManagementSettings.from_env()
        repository = AccessGrantRepository.from_database_url(
            service_settings.database_url, schema_name=service_settings.schema_name
        )
        executor = PostgreSQLGrantExecutor(
            repository=repository, secret_resolver=EnvironmentSecretResolver()
        )
        granted = await asyncio.to_thread(executor.execute_for_existing_login, form_data["grant_id"])
        login_name = form_data["login_name"]
        await state.clear()
        await query.message.answer(
            f"Доступ для {login_name} выдан и отмечен активным."
            if granted
            else f"Не удалось выдать доступ для {login_name}. Проверьте, что учётная запись создана."
        )

    @router.callback_query(
        GrantForm.login_created,
        lambda query: query.data == "dam_manual:cancel",
    )
    async def cancel_manual_grant(query: CallbackQuery, state: FSMContext) -> None:
        """Отменяет незавершённое распоряжение и возвращает к выбору базы."""

        if query.message is None or not _is_manager_chat(query.message.chat.id, settings):
            await query.answer("Доступ не разрешён", show_alert=True)
            return
        form_data = await state.get_data()
        service_settings = DatabaseAccessManagementSettings.from_env()
        repository = AccessGrantRepository.from_database_url(
            service_settings.database_url, schema_name=service_settings.schema_name
        )
        cancelled = repository.cancel_pending_grant(
            form_data["grant_id"], actor_id=form_data["requested_by"]
        )
        await state.clear()
        await query.answer()
        await query.message.answer(
            "Распоряжение отменено. Выберите базу для новой выдачи доступа."
            if cancelled else "Распоряжение уже нельзя отменить: оно не находится в ожидании."
        )
        if cancelled:
            await start_grant(query.message, state)

    return router
