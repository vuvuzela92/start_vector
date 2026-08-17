from __future__ import annotations

import logging
from collections.abc import Callable, Mapping, Sequence
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

    async def list_offices(self, account: str | Sequence[str] | None = None) -> WarehousesOperationSummary:
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

    async def list_warehouses(
        self,
        account: str | Sequence[str] | None = None,
    ) -> WarehousesOperationSummary:
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
        account: str | Sequence[str] | None,
        office_id: int,
        name: str,
        skip_existing_by_account: dict[str, dict[str, object]] | None = None,
    ) -> WarehousesOperationSummary:
        """Создает FBS-склад продавца по выбранному офису WB в одном или нескольких кабинетах.

        Бизнес-правило: если для аккаунта уже есть активная связка нашего `warehouse_id` в
        `warehouses_fbs`, WB API не вызывается, чтобы не создавать дубли складов.
        """
        if not name.strip():
            raise ValueError("Название FBS-склада WB не может быть пустым.")

        tokens_by_account = self._resolve_tokens(account=account)
        summary = WarehousesOperationSummary(
            operation="create_warehouse",
            accounts_total=len(tokens_by_account),
        )
        normalized_skip = skip_existing_by_account or {}
        async with aiohttp.ClientSession() as session:
            for account_name, token in tokens_by_account.items():
                existing_payload = normalized_skip.get(account_name.casefold())
                if existing_payload is not None:
                    logger.info(
                        "Создание FBS-склада пропущено: активный склад уже есть в warehouses_fbs | account=%s | warehouse_id=%s | wb_warehouse_id=%s",
                        account_name,
                        existing_payload.get("warehouse_id"),
                        existing_payload.get("wb_warehouse_id"),
                    )
                    self._append_result(
                        summary,
                        account_name,
                        {
                            "status": "skipped_existing",
                            "reason": "active_warehouse_exists",
                            "existing": existing_payload,
                        },
                        retries_used=0,
                    )
                    continue

                result = await self.client.create_warehouse(
                    session=session,
                    account=account_name,
                    token=token,
                    office_id=office_id,
                    name=name.strip(),
                )
                self._append_result(summary, account_name, result.payload, result.retries_used)
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
                account=self._normalize_account_name(account),
                token=token,
                warehouse_id=warehouse_id,
            )
            self._append_result(
                summary,
                self._normalize_account_name(account),
                result.payload,
                result.retries_used,
            )
        return summary

    def _resolve_tokens(self, account: str | Sequence[str] | None) -> dict[str, str]:
        """Загружает WB-токены и при необходимости ограничивает операцию списком кабинетов.

        Бизнес-сценарий: создание складов можно запускать на одном ЛК, на списке ЛК или на всех
        аккаунтах из токенов. Для каждого указанного ЛК наличие токена обязательно. Названия ЛК
        приводятся к uppercase, чтобы `старт0854`, `Старт0854` и `СТАРТ0854` считались одним ЛК.
        """
        loaded_tokens = self.tokens_loader()
        if not isinstance(loaded_tokens, Mapping):
            raise TypeError("load_api_tokens() должен возвращать Mapping account -> token.")

        tokens_by_account = {
            self._normalize_account_name(account_name): token.strip()
            for account_name, token in loaded_tokens.items()
            if isinstance(account_name, str)
            and account_name.strip()
            and isinstance(token, str)
            and token.strip()
        }
        tokens_by_lookup = {
            account_name.casefold(): (account_name, token)
            for account_name, token in tokens_by_account.items()
        }

        if account is None:
            return tokens_by_account

        requested_accounts = (
            [self._normalize_account_name(account)]
            if isinstance(account, str)
            else [
                self._normalize_account_name(account_name)
                for account_name in account
                if account_name.strip()
            ]
        )
        selected_tokens: dict[str, str] = {}
        missing_accounts: list[str] = []
        for account_name in requested_accounts:
            resolved_token = tokens_by_lookup.get(account_name.casefold())
            if resolved_token is not None:
                resolved_account, token = resolved_token
                selected_tokens[resolved_account] = token
            else:
                missing_accounts.append(account_name)

        if missing_accounts:
            raise ValueError(f"Аккаунты не найдены в токенах WB: {missing_accounts}")
        return selected_tokens

    def _resolve_single_token(self, account: str) -> str:
        """Возвращает один токен WB, защищая создание и удаление складов от запуска по всем кабинетам."""
        if not account.strip():
            raise ValueError("Для создания или удаления FBS-склада нужно указать аккаунт WB.")
        resolved_tokens = self._resolve_tokens(account=account)
        return next(iter(resolved_tokens.values()))

    def _normalize_account_name(self, account: str) -> str:
        """Приводит название ЛК к единому виду для поиска токена и записи складов в БД."""
        return account.strip().upper()

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
