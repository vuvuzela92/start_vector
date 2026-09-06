"""Telegram-вход руководителей в сервис управления доступами."""

from __future__ import annotations

import asyncio
import logging

from sqlalchemy.exc import SQLAlchemyError

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

    class ActiveAccessForm(StatesGroup):
        """Хранит логин и выбор базы для точечного просмотра доступов сотрудника."""

        login = State()
        target = State()

    class InventoryForm(StatesGroup):
        """Хранит выбор базы для инвентаризации существующих PostgreSQL-ролей."""

        target = State()

    class DeleteUserForm(StatesGroup):
        """Хранит логин и подтверждение удаления одной учётной записи."""

        login = State()
        confirmation = State()

    class RevokeForm(StatesGroup):
        """Хранит логин и выбор активного доступа для безопасного отзыва."""

        login = State()
        selection = State()

    class MassDeleteForm(StatesGroup):
        """Хранит список логинов и подтверждение массового удаления."""

        logins = State()
        confirmation = State()

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

    async def request_active_access_target(
        message: Message,
        state: FSMContext,
        login_name: str,
    ) -> None:
        """Предлагает базу для чтения действующих доступов без изменений прав.

        Функция обслуживает единый сценарий «Активные доступы»: руководитель
        вводит логин сотрудника, затем выбирает зарегистрированную
        PostgreSQL-базу. Логин сохраняется только во временном состоянии
        Telegram-диалога.
        """

        service_settings = DatabaseAccessManagementSettings.from_env()
        repository = AccessGrantRepository.from_database_url(
            service_settings.database_url, schema_name=service_settings.schema_name
        )
        targets = repository.list_active_database_targets(engine_name="postgresql")
        if not targets:
            await message.answer("Нет зарегистрированных PostgreSQL-баз.")
            return
        await state.update_data(active_access_login_name=login_name)
        await state.set_state(ActiveAccessForm.target)
        await message.answer(
            "Выберите PostgreSQL-базу для просмотра действующих доступов:",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[
                    [
                        InlineKeyboardButton(
                            text=target.display_name,
                            callback_data=f"dam_access_target:{target.target_id}",
                        )
                    ]
                    for target in targets
                ] + [[InlineKeyboardButton(text="↩️ Назад", callback_data="dam_back:active_login")]]
            ),
        )

    async def request_active_access_login(message: Message, state: FSMContext) -> None:
        """Запрашивает логин для точечного просмотра действующих доступов.

        Функция обслуживает кнопку и команду «Активные доступы»: без логина
        бот не выполняет широкий запрос по всем сотрудникам, а ожидает явный
        идентификатор сотрудника для последующего выбора PostgreSQL-базы.
        """

        await state.set_state(ActiveAccessForm.login)
        await message.answer(
            "Введите логин сотрудника для просмотра его действующих доступов.",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(text="↩️ Назад", callback_data="dam_back:menu")]
                ]
            ),
        )

    async def request_inventory_target(message: Message, state: FSMContext) -> None:
        """Предлагает базу для инвентаризации её существующих логинов и ролей.

        Функция обслуживает общий read-only сценарий «Инвентаризация
        PostgreSQL»: руководитель сначала явно выбирает зарегистрированную
        целевую базу, чтобы результаты не смешивали доступы разных систем.
        """

        service_settings = DatabaseAccessManagementSettings.from_env()
        repository = AccessGrantRepository.from_database_url(
            service_settings.database_url, schema_name=service_settings.schema_name
        )
        targets = repository.list_active_database_targets(engine_name="postgresql")
        if not targets:
            await message.answer("Нет зарегистрированных PostgreSQL-баз.")
            return
        await state.set_state(InventoryForm.target)
        await message.answer(
            "Выберите PostgreSQL-базу для инвентаризации:",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[
                    [
                        InlineKeyboardButton(
                            text=target.display_name,
                            callback_data=f"dam_inventory_target:{target.target_id}",
                        )
                    ]
                    for target in targets
                ] + [[InlineKeyboardButton(text="↩️ Назад", callback_data="dam_back:menu")]]
            ),
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
            "• /accesses <логин> — показать действующие доступы сотрудника\n"
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
    async def accesses_button(message: Message, state: FSMContext) -> None:
        """Запускает выбор БД для просмотра действующих доступов сотрудников."""

        if not _is_manager_chat(message.chat.id, settings):
            return
        await request_active_access_login(message, state)

    @router.message(F.text == "🔎 Инвентаризация PostgreSQL")
    async def inventory_button(message: Message, state: FSMContext) -> None:
        """Запускает выбор базы для общей инвентаризации PostgreSQL-ролей."""

        if not _is_manager_chat(message.chat.id, settings):
            return
        await request_inventory_target(message, state)

    @router.callback_query(
        InventoryForm.target,
        lambda query: query.data and query.data.startswith("dam_inventory_target:"),
    )
    async def show_inventory_for_target(
        query: CallbackQuery,
        state: FSMContext,
    ) -> None:
        """Показывает логины и роли, существующие в выбранной PostgreSQL-базе.

        Метод обслуживает инвентаризацию уже выданных прав независимо от того,
        создавал ли их Telegram-бот. Запросы ограничены чтением системного
        каталога PostgreSQL, а ссылка на административный секрет не попадает в
        ответ пользователю или журнал.
        """

        if query.message is None or not _is_manager_chat(query.message.chat.id, settings):
            await query.answer("Доступ не разрешён", show_alert=True)
            return
        target_id = query.data.removeprefix("dam_inventory_target:")
        await query.answer("Загружаю инвентаризацию…")
        service_settings = DatabaseAccessManagementSettings.from_env()
        repository = AccessGrantRepository.from_database_url(
            service_settings.database_url, schema_name=service_settings.schema_name
        )
        try:
            admin_secret_ref = repository.get_active_target_admin_secret_ref(
                target_id,
                engine_name="postgresql",
            )
            database_url = EnvironmentSecretResolver().resolve_database_url(admin_secret_ref)
            inventory = PostgreSQLAccessInventory(database_url)
            accesses = await asyncio.to_thread(inventory.list_users_and_role_memberships)
        except (LookupError, OSError, RuntimeError, SQLAlchemyError, ValueError) as error:
            logger.error(
                "Не удалось выполнить инвентаризацию PostgreSQL | "
                "target_id=%s | error_type=%s",
                target_id,
                type(error).__name__,
            )
            await state.clear()
            await query.message.answer("Не удалось загрузить инвентаризацию выбранной базы.")
            return

        await state.clear()
        if not accesses:
            await query.message.answer("В выбранной PostgreSQL-базе не найдены прикладные пользователи.")
            return
        lines = [f"Пользователи PostgreSQL базы {target_id} и их роли:"]
        lines.extend(
            f"• {item.login_name} → {item.role_name or 'роль не назначена'}"
            for item in accesses[:100]
        )
        if len(accesses) > 100:
            lines.append(f"Показаны первые 100 из {len(accesses)} записей.")
        await query.message.answer("\n".join(lines))

    @router.message(ActiveAccessForm.login)
    async def choose_login_for_active_accesses(message: Message, state: FSMContext) -> None:
        """Сохраняет логин сотрудника и предлагает базу для проверки его ролей."""

        if not _is_manager_chat(message.chat.id, settings):
            await state.clear()
            return
        login_name = (message.text or "").strip()
        if not login_name:
            await message.answer("Введите непустой логин сотрудника.")
            return
        await request_active_access_target(message, state, login_name)

    @router.callback_query(
        ActiveAccessForm.target,
        lambda query: query.data and query.data.startswith("dam_access_target:"),
    )
    async def show_active_accesses_for_target(
        query: CallbackQuery,
        state: FSMContext,
    ) -> None:
        """Показывает существующие логины и роли выбранной PostgreSQL-базы.

        Метод обслуживает просмотр действующих доступов, включая выданные до
        внедрения Telegram-бота. Он выполняет только чтение системного каталога
        PostgreSQL и не показывает административный секрет или строку
        подключения даже при ошибке доступа к выбранной базе.
        """

        if query.message is None or not _is_manager_chat(query.message.chat.id, settings):
            await query.answer("Доступ не разрешён", show_alert=True)
            return
        target_id = query.data.removeprefix("dam_access_target:")
        form_data = await state.get_data()
        login_name = form_data.get("active_access_login_name")
        if not isinstance(login_name, str):
            await state.clear()
            await query.answer("Данные формы устарели", show_alert=True)
            return
        await query.answer("Загружаю действующие доступы…")
        service_settings = DatabaseAccessManagementSettings.from_env()
        repository = AccessGrantRepository.from_database_url(
            service_settings.database_url, schema_name=service_settings.schema_name
        )
        try:
            admin_secret_ref = repository.get_active_target_admin_secret_ref(
                target_id,
                engine_name="postgresql",
            )
            database_url = EnvironmentSecretResolver().resolve_database_url(admin_secret_ref)
            inventory = PostgreSQLAccessInventory(database_url)
            accesses = await asyncio.to_thread(inventory.list_users_and_role_memberships)
            managed_table_scopes = repository.list_active_read_table_scopes(
                login_name,
                target_id,
            )
        except (LookupError, OSError, RuntimeError, SQLAlchemyError, ValueError) as error:
            logger.error(
                "Не удалось прочитать действующие доступы PostgreSQL | "
                "target_id=%s | error_type=%s",
                target_id,
                type(error).__name__,
            )
            await state.clear()
            await query.message.answer("Не удалось загрузить действующие доступы выбранной базы.")
            return

        accesses = [item for item in accesses if item.login_name == login_name]
        await state.clear()
        if not accesses:
            await query.message.answer(
                f"Для логина {login_name} в выбранной PostgreSQL-базе доступы не найдены."
            )
            return
        lines = [f"Действующие PostgreSQL-доступы {login_name} в базе {target_id}:"]
        for item in accesses[:100]:
            line = f"• {item.login_name} → {item.role_name or 'роль не назначена'}"
            if item.role_name is not None:
                table_names = managed_table_scopes.get(item.role_name)
                if table_names:
                    line += f"\n  таблицы: {', '.join(table_names)}"
            lines.append(line)
        if len(accesses) > 100:
            lines.append(f"Показаны первые 100 из {len(accesses)} записей.")
        await query.message.answer("\n".join(lines))

    @router.message(F.text == "⛔ Отозвать доступ")
    async def revoke_button(message: Message, state: FSMContext) -> None:
        """Запрашивает логин для выбора выданного сервисом доступа к отзыву."""

        if not _is_manager_chat(message.chat.id, settings):
            return
        await state.set_state(RevokeForm.login)
        await message.answer(
            "Введите логин пользователя, доступ которого нужно отозвать.",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(text="↩️ Назад", callback_data="dam_back:menu")]
                ]
            ),
        )

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
        if not grants:
            await state.clear()
            await message.answer("У этого логина нет активных доступов, выданных сервисом.")
            return
        keyboard = InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(
                    text=f"Отозвать {grant['database_name']} — {grant['access_level']}",
                    callback_data=f"dam_revoke:{grant['id']}",
                )]
                for grant in grants
            ] + [[InlineKeyboardButton(text="↩️ Назад", callback_data="dam_back:revoke_login")]]
        )
        await state.set_state(RevokeForm.selection)
        await message.answer("Выберите доступ для отзыва:", reply_markup=keyboard)

    @router.message(F.text == "🗑 Удалить пользователя")
    async def delete_user_button(message: Message, state: FSMContext) -> None:
        """Запрашивает логин для безвозвратного удаления учётной записи."""

        if not _is_manager_chat(message.chat.id, settings):
            return
        await state.set_state(DeleteUserForm.login)
        await message.answer(
            "Введите логин пользователя для удаления. Операция необратима.",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(text="↩️ Назад", callback_data="dam_back:menu")]
                ]
            ),
        )

    @router.message(F.text == "🗑 Массовое удаление")
    async def mass_delete_button(message: Message, state: FSMContext) -> None:
        """Запрашивает список логинов для последовательного массового удаления."""

        if not _is_manager_chat(message.chat.id, settings):
            return
        await state.set_state(MassDeleteForm.logins)
        await message.answer(
            "Отправьте логины для удаления: каждый с новой строки. Максимум 50. "
            "Операция необратима.",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(text="↩️ Назад", callback_data="dam_back:menu")]
                ]
            ),
        )

    @router.message(MassDeleteForm.logins)
    async def delete_users_from_list(message: Message, state: FSMContext) -> None:
        """Запрашивает подтверждение массового удаления указанного списка логинов.

        Метод защищает учётные записи от случайной отправки списка в Telegram:
        до нажатия кнопки подтверждения SQL-команды не выполняются, а список
        остаётся только во временном состоянии диалога руководителя.
        """

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
        await state.update_data(login_names=login_names)
        await state.set_state(MassDeleteForm.confirmation)
        await message.answer(
            "Будут безвозвратно удалены учётные записи:\n"
            + "\n".join(f"• {login_name}" for login_name in login_names)
            + "\n\nПодтвердите удаление.",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(text="🗑 Подтвердить удаление", callback_data="dam_delete:mass_confirm")],
                    [InlineKeyboardButton(text="↩️ Назад", callback_data="dam_back:mass_delete")],
                ]
            ),
        )

    @router.message(DeleteUserForm.login)
    async def delete_user_from_button(message: Message, state: FSMContext) -> None:
        """Запрашивает подтверждение удаления логина, введённого руководителем.

        Метод не запускает `DROP ROLE` сразу после ввода, чтобы случайная ошибка
        в логине не приводила к необратимому закрытию доступа сотрудника.
        """

        if not _is_manager_chat(message.chat.id, settings):
            await state.clear()
            return
        login_name = (message.text or "").strip()
        if not login_name:
            await message.answer("Введите непустой логин пользователя.")
            return
        await state.update_data(login_name=login_name)
        await state.set_state(DeleteUserForm.confirmation)
        await message.answer(
            f"Будет безвозвратно удалён пользователь {login_name}. Подтвердите удаление.",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(text="🗑 Подтвердить удаление", callback_data="dam_delete:single_confirm")],
                    [InlineKeyboardButton(text="↩️ Назад", callback_data="dam_back:single_delete")],
                ]
            ),
        )

    @router.callback_query(DeleteUserForm.confirmation, lambda query: query.data == "dam_delete:single_confirm")
    async def confirm_single_user_deletion(query: CallbackQuery, state: FSMContext) -> None:
        """Удаляет одного пользователя только после явного подтверждения руководителя."""

        if query.message is None or not _is_manager_chat(query.message.chat.id, settings):
            await query.answer("Доступ не разрешён", show_alert=True)
            return
        form_data = await state.get_data()
        login_name = form_data.get("login_name")
        if not isinstance(login_name, str):
            await state.clear()
            await query.answer("Данные формы устарели", show_alert=True)
            return
        await query.answer("Удаляю пользователя…")
        service_settings = DatabaseAccessManagementSettings.from_env()
        repository = AccessGrantRepository.from_database_url(
            service_settings.database_url, schema_name=service_settings.schema_name
        )
        executor = PostgreSQLGrantExecutor(
            repository=repository, secret_resolver=EnvironmentSecretResolver()
        )
        deleted = await asyncio.to_thread(executor.delete_user, login_name)
        await state.clear()
        await query.message.answer(
            f"Пользователь {login_name} удалён, связанные активные доступы закрыты."
            if deleted
            else "Не удалось удалить пользователя."
        )

    @router.callback_query(MassDeleteForm.confirmation, lambda query: query.data == "dam_delete:mass_confirm")
    async def confirm_mass_user_deletion(query: CallbackQuery, state: FSMContext) -> None:
        """Удаляет список пользователей только после явного подтверждения руководителя."""

        if query.message is None or not _is_manager_chat(query.message.chat.id, settings):
            await query.answer("Доступ не разрешён", show_alert=True)
            return
        form_data = await state.get_data()
        login_names = form_data.get("login_names")
        if not isinstance(login_names, list) or not all(isinstance(name, str) for name in login_names):
            await state.clear()
            await query.answer("Данные формы устарели", show_alert=True)
            return
        await query.answer("Удаляю пользователей…")
        service_settings = DatabaseAccessManagementSettings.from_env()
        repository = AccessGrantRepository.from_database_url(
            service_settings.database_url, schema_name=service_settings.schema_name
        )
        executor = PostgreSQLGrantExecutor(
            repository=repository, secret_resolver=EnvironmentSecretResolver()
        )
        results: list[str] = []
        for login_name in login_names:
            deleted = await asyncio.to_thread(executor.delete_user, login_name)
            results.append(f"{'✅' if deleted else '❌'} {login_name}")
        await state.clear()
        await query.message.answer("Итог массового удаления:\n" + "\n".join(results))

    @router.callback_query(lambda query: query.data and query.data.startswith("dam_back:"))
    async def go_back(query: CallbackQuery, state: FSMContext) -> None:
        """Возвращает руководителя на предыдущий шаг без запуска SQL-команд.

        Навигация обслуживает безопасное заполнение форм: действие «Назад»
        меняет только временное состояние Telegram-диалога и не создаёт,
        не изменяет и не удаляет учётные записи или права в PostgreSQL.
        """

        if query.message is None or not _is_manager_chat(query.message.chat.id, settings):
            await query.answer("Доступ не разрешён", show_alert=True)
            return
        destination = query.data.removeprefix("dam_back:")
        if destination == "menu":
            await state.clear()
            await query.answer()
            await query.message.answer("Вы вернулись в главное меню.", reply_markup=reply_keyboard)
            return
        if destination == "single_delete":
            await state.set_state(DeleteUserForm.login)
            await query.answer()
            await query.message.answer("Введите другой логин пользователя для удаления.")
            return
        if destination == "mass_delete":
            await state.set_state(MassDeleteForm.logins)
            await query.answer()
            await query.message.answer("Отправьте новый список логинов для удаления.")
            return
        if destination == "revoke_login":
            await state.set_state(RevokeForm.login)
            await query.answer()
            await query.message.answer("Введите другой логин для поиска доступов.")
            return
        if destination == "active_login":
            await state.set_state(ActiveAccessForm.login)
            await query.answer()
            await query.message.answer("Введите другой логин сотрудника.")
            return
        if destination == "grant_target":
            await state.set_state(GrantForm.target)
            await query.answer()
            await start_grant(query.message, state)
            return
        if destination == "grant_level":
            await state.set_state(GrantForm.level)
            await query.answer()
            await query.message.answer(
                "Выберите роль:",
                reply_markup=InlineKeyboardMarkup(
                    inline_keyboard=[
                        [InlineKeyboardButton(text="Чтение схемы", callback_data="dam_level:read_all")],
                        [InlineKeyboardButton(text="Запись в схему", callback_data="dam_level:write")],
                        [InlineKeyboardButton(text="Управление схемой", callback_data="dam_level:full_access")],
                        [InlineKeyboardButton(text="Чтение отдельных таблиц", callback_data="dam_level:read_tables")],
                        [InlineKeyboardButton(text="Полный доступ ко всем схемам", callback_data="dam_level:full_all")],
                        [InlineKeyboardButton(text="↩️ Назад", callback_data="dam_back:grant_target")],
                    ]
                ),
            )
            return
        if destination == "grant_schema":
            service_settings = DatabaseAccessManagementSettings.from_env()
            adapter = PostgreSQLAccessAdapter(service_settings.database_url)
            try:
                schema_names = await asyncio.to_thread(adapter.list_user_schemas)
            except Exception as error:
                logger.error(
                    "Не удалось получить схемы PostgreSQL при возврате в форме доступа | "
                    "error_type=%s",
                    type(error).__name__,
                )
                await state.clear()
                await query.answer()
                await query.message.answer("Не удалось загрузить схемы PostgreSQL. Начните заново: /grant")
                return
            if not schema_names:
                await state.clear()
                await query.answer()
                await query.message.answer("В целевой базе не найдены прикладные схемы.")
                return
            await state.set_state(GrantForm.schema)
            await query.answer()
            await query.message.answer(
                "Выберите схему:",
                reply_markup=InlineKeyboardMarkup(
                    inline_keyboard=[
                        [InlineKeyboardButton(text=schema_name, callback_data=f"dam_schema:{schema_name}")]
                        for schema_name in schema_names
                    ] + [[InlineKeyboardButton(text="↩️ Назад", callback_data="dam_back:grant_level")]]
                ),
            )
            return
        await query.answer("Предыдущий шаг больше недоступен", show_alert=True)

    @router.callback_query(lambda query: query.data == "dam_delete:cancel")
    async def cancel_user_deletion(query: CallbackQuery, state: FSMContext) -> None:
        """Отменяет неподтверждённое удаление до выполнения команд PostgreSQL."""

        if query.message is None or not _is_manager_chat(query.message.chat.id, settings):
            await query.answer("Доступ не разрешён", show_alert=True)
            return
        await state.clear()
        await query.answer()
        await query.message.answer("Удаление отменено. Учётные записи не изменены.")

    @router.callback_query(lambda query: query.data and query.data.startswith("dam_menu:"))
    async def handle_menu(query: CallbackQuery, state: FSMContext) -> None:
        """Обрабатывает кнопки главного меню без выполнения опасных действий."""

        if query.message is None or not _is_manager_chat(query.message.chat.id, settings):
            await query.answer("Доступ не разрешён", show_alert=True)
            return
        action = query.data.removeprefix("dam_menu:")
        prompts = {
            "grant": "Введите /grant, чтобы выбрать базу и роль.",
            "revoke": "Сначала найдите номер: /accesses, затем /revoke <номер>.",
            "delete": "Введите /delete_user <логин> для безвозвратного удаления пользователя.",
            "mass_delete": "Нажмите постоянную кнопку «🗑 Массовое удаление».",
        }
        if action == "accesses":
            await request_active_access_login(query.message, state)
            await query.answer()
            return
        if action == "inventory":
            await inventory_button(query.message, state)
            await query.answer()
            return
        if action != "inventory":
            prompt = prompts.get(action)
            if prompt is None:
                await query.answer("Неизвестное действие", show_alert=True)
                return
            await query.message.answer(prompt)
            await query.answer()
            return

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
            ] + [[InlineKeyboardButton(text="↩️ Назад", callback_data="dam_back:menu")]]
        )
        await state.set_state(GrantForm.target)
        await message.answer("Выберите PostgreSQL-базу:", reply_markup=keyboard)

    @router.message(Command("accesses"))
    async def list_accesses(
        message: Message,
        command: CommandObject,
        state: FSMContext,
    ) -> None:
        """Запускает просмотр действующих доступов конкретного логина.

        Команда обслуживает тот же сценарий, что и кнопка «Активные доступы»,
        и принимает логин как необязательный аргумент. Если аргумент не указан,
        бот запрашивает его до выбора PostgreSQL-базы. Команда не обращается к
        журналу заявок сервиса.
        """

        if not _is_manager_chat(message.chat.id, settings):
            await message.answer("Доступ к управлению доступами не разрешён.")
            return
        login_name = (command.args or "").strip() or None
        if login_name is None:
            await request_active_access_login(message, state)
            return
        await request_active_access_target(message, state, login_name)

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
    async def revoke_grant_from_button(query: CallbackQuery, state: FSMContext) -> None:
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
        await state.clear()
        await query.message.answer("Доступ отозван." if revoked else "Не удалось отозвать доступ.")

    @router.message(Command("delete_user"))
    async def delete_user(
        message: Message,
        command: CommandObject,
        state: FSMContext,
    ) -> None:
        """Запрашивает подтверждение удаления логина из команды руководителя.

        Метод защищает командный сценарий наравне с кнопкой интерфейса: ввод
        `/delete_user` не должен запускать необратимый `DROP ROLE` без явного
        нажатия кнопки подтверждения.
        """

        if not _is_manager_chat(message.chat.id, settings):
            await message.answer("Доступ к управлению доступами не разрешён.")
            return
        login_name = (command.args or "").strip()
        if not login_name:
            await message.answer("Укажите логин: /delete_user ivanov_i")
            return
        await state.update_data(login_name=login_name)
        await state.set_state(DeleteUserForm.confirmation)
        await message.answer(
            f"Будет безвозвратно удалён пользователь {login_name}. Подтвердите удаление.",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(text="🗑 Подтвердить удаление", callback_data="dam_delete:single_confirm")],
                    [InlineKeyboardButton(text="↩️ Назад", callback_data="dam_back:single_delete")],
                ]
            ),
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
                [InlineKeyboardButton(text="↩️ Назад", callback_data="dam_back:grant_target")],
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
            await query.message.answer(
                "Отправьте логин пользователя PostgreSQL\n\nПример: ivanov_i",
                reply_markup=InlineKeyboardMarkup(
                    inline_keyboard=[
                        [InlineKeyboardButton(text="↩️ Назад", callback_data="dam_back:grant_level")]
                    ]
                ),
            )
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
            ] + [[InlineKeyboardButton(text="↩️ Назад", callback_data="dam_back:grant_level")]]
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
        await query.message.answer(
            f"{prompt}\n\nПример: ivanov_i",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(text="↩️ Назад", callback_data="dam_back:grant_schema")]
                ]
            ),
        )
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
