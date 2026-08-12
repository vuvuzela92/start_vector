from __future__ import annotations

import logging
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field

import aiohttp

from src_oop.core.utils_general import load_api_tokens
from src_oop.jobs.fbs_warehouses.client import WBFBSWarehousesClient

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class AccountOperationResult:
    """Итог операции по одному кабинету WB в сценарии управления FBS-складами."""

    account: str
    payload: dict | list | None
    retries_used: int = 0


@dataclass(slots=True)
class WarehousesOperationSummary:
    """Сводка ручной операции над FBS-складами WB по одному или нескольким кабинетам."""

    operation: str
    accounts_total: int = 0
    retries_used: int = 0
    results: list[AccountOperationResult] = field(default_factory=list)


class FBSWarehousesService:
    """Оркестрирует выбор офисов WB, создание и удаление FBS-складов продавца."""

    def __init__(
        self,
        client: WBFBSWarehousesClient | None = None,
        tokens_loader: Callable[[], Mapping[str, str]] | None = None,
    ) -> None:
        """Собирает зависимости сценария управления складами без глобального состояния."""
        self.client = client or WBFBSWarehousesClient()
        self.tokens_loader = tokens_loader or load_api_tokens

    async def list_offices(self, account: str | None = None) -> WarehousesOperationSummary:
        """Получает офисы WB, чтобы пользователь мог выбрать `officeId` для создания FBS-склада."""
        tokens_by_account = self._resolve_tokens(account=account)
        summary = WarehousesOperationSummary(
            operation="list_offices",
            accounts_total=len(tokens_by_account),
        )
        async with aiohttp.ClientSession() as session:
            for account_name, token in tokens_by_account.items():
                result = await self.client.fetch_offices(
                    session=session,
                    account=account_name,
                    token=token,
                )
                self._append_result(summary, account_name, result.payload, result.retries_used)
        return summary

    async def list_warehouses(self, account: str | None = None) -> WarehousesOperationSummary:
        """Получает FBS-склады продавца, чтобы сверить `warehouseId` перед удалением или обновлением остатков."""
        tokens_by_account = self._resolve_tokens(account=account)
        summary = WarehousesOperationSummary(
            operation="list_warehouses",
            accounts_total=len(tokens_by_account),
        )
        async with aiohttp.ClientSession() as session:
            for account_name, token in tokens_by_account.items():
                result = await self.client.fetch_warehouses(
                    session=session,
                    account=account_name,
                    token=token,
                )
                self._append_result(summary, account_name, result.payload, result.retries_used)
        return summary

    async def create_warehouse(
        self,
        account: str,
        office_id: int,
        name: str,
    ) -> WarehousesOperationSummary:
        """Создает FBS-склад продавца в выбранном кабинете WB по заранее выбранному офису WB."""
        if not name.strip():
            raise ValueError("Название FBS-склада WB не может быть пустым.")

        token = self._resolve_single_token(account=account)
        summary = WarehousesOperationSummary(operation="create_warehouse", accounts_total=1)
        async with aiohttp.ClientSession() as session:
            result = await self.client.create_warehouse(
                session=session,
                account=account.strip(),
                token=token,
                office_id=office_id,
                name=name.strip(),
            )
            self._append_result(summary, account.strip(), result.payload, result.retries_used)
        return summary

    async def delete_warehouse(
        self,
        account: str,
        warehouse_id: int,
    ) -> WarehousesOperationSummary:
        """Удаляет FBS-склад продавца WB из выбранного кабинета, чтобы исключить его из контура остатков."""
        token = self._resolve_single_token(account=account)
        summary = WarehousesOperationSummary(operation="delete_warehouse", accounts_total=1)
        async with aiohttp.ClientSession() as session:
            result = await self.client.delete_warehouse(
                session=session,
                account=account.strip(),
                token=token,
                warehouse_id=warehouse_id,
            )
            self._append_result(summary, account.strip(), result.payload, result.retries_used)
        return summary

    def _resolve_tokens(self, account: str | None) -> dict[str, str]:
        """Загружает WB-токены и при необходимости ограничивает ручную операцию одним кабинетом."""
        loaded_tokens = self.tokens_loader()
        if not isinstance(loaded_tokens, Mapping):
            raise TypeError("load_api_tokens() должен возвращать Mapping account -> token.")

        tokens_by_account = {
            account_name.strip(): token.strip()
            for account_name, token in loaded_tokens.items()
            if isinstance(account_name, str)
            and account_name.strip()
            and isinstance(token, str)
            and token.strip()
        }

        if account is None:
            return tokens_by_account

        normalized_account = account.strip()
        if normalized_account in tokens_by_account:
            return {normalized_account: tokens_by_account[normalized_account]}

        raise ValueError(f"Аккаунт '{account}' не найден в токенах WB.")

    def _resolve_single_token(self, account: str) -> str:
        """Возвращает один токен WB, защищая создание и удаление складов от запуска по всем кабинетам."""
        if not account.strip():
            raise ValueError("Для создания или удаления FBS-склада нужно указать аккаунт WB.")
        return self._resolve_tokens(account=account)[account.strip()]

    def _append_result(
        self,
        summary: WarehousesOperationSummary,
        account: str,
        payload: dict | list | None,
        retries_used: int,
    ) -> None:
        """Добавляет результат кабинета в сводку ручной операции над складами WB."""
        summary.retries_used += retries_used
        summary.results.append(
            AccountOperationResult(
                account=account,
                payload=payload,
                retries_used=retries_used,
            )
        )