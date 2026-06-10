"""API-слой для работы с документами WB.

На этом этапе реализуется только инфраструктурный streaming-клиент для
WB Documents API. Логика распаковки архивов, Excel parsing, нормализации и
записи в БД намеренно остаётся вне этого файла.
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator, Iterator
from datetime import date, datetime, timezone
from math import ceil

import aiohttp

from src_oop.core.scraper import HTTPClient
from src_oop.jobs.wb_api.acceptance_acts.config import (
    ACT_TYPE_FBO,
    ACT_TYPE_FBS,
    DOCUMENT_BATCH_SIZE,
    DOCUMENTS_DOWNLOAD_ALL_URL,
    DOCUMENTS_LIST_URL,
    FBO_DOCUMENT_CATEGORY,
    FBS_DOCUMENT_CATEGORY,
    REQUEST_TIMEOUT_SECONDS,
    RETRY_ATTEMPTS,
    RETRY_BASE_DELAY_SECONDS,
    ActType,
)
from src_oop.jobs.wb_api.acceptance_acts.models import (
    DownloadedDocumentBatch,
    WBDocumentMeta,
)

logger = logging.getLogger(__name__)


class WBActsClient:
    """Streaming-клиент для работы с WB Documents API.

    Клиент отвечает только за:
    - получение списка документов WB;
    - скачивание документов батчами;
    - потоковую выдачу скачанных архивов через async generator.

    Клиент не должен заниматься:
    - распаковкой base64 и zip;
    - чтением Excel;
    - нормализацией строк;
    - записью в БД.

    Такой контракт позволяет использовать слой как транспортный адаптер
    поверх существующего `HTTPClient` проекта.
    """

    def __init__(self, session: aiohttp.ClientSession | None = None) -> None:
        """Инициализирует каркас API-клиента.

        Args:
            session: Будущая HTTP-сессия для переиспользования соединений.
        """
        self.session = session

    async def list_documents(
        self,
        account: str,
        token: str,
        begin_date: date,
        end_date: date,
        category: str,
        expected_act_type: ActType,
    ) -> list[WBDocumentMeta]:
        """Возвращает список документов WB по аккаунту и категории.

        Метод делает только запрос к `documents/list`, обрабатывает пагинацию,
        фильтрует документы без `serviceName` и возвращает структурированные
        метаданные. Ошибки API не должны приводить к падению процесса на этом
        слое: в спорных случаях метод возвращает пустой список и пишет
        диагностический лог.
        """
        logger.info(
            "Запрос списка документов WB: account=%s act_type=%s category=%s period=%s..%s",
            account,
            expected_act_type,
            category,
            begin_date.isoformat(),
            end_date.isoformat(),
        )

        offset = 0
        documents: list[WBDocumentMeta] = []

        async with self._get_session() as session:
            client = self._build_http_client(
                session=session,
                account=account,
                token=token,
            )

            while True:
                params = {
                    "beginTime": begin_date.isoformat(),
                    "endTime": end_date.isoformat(),
                    "limit": DOCUMENT_BATCH_SIZE,
                    "offset": offset,
                }
                if category:
                    params["category"] = category

                payload = await client.get(
                    DOCUMENTS_LIST_URL,
                    params=params,
                    retries=RETRY_ATTEMPTS,
                    delay=RETRY_BASE_DELAY_SECONDS,
                )

                if payload is None:
                    logger.error(
                        "Не удалось получить список документов WB: account=%s category=%s offset=%s",
                        account,
                        category,
                        offset,
                    )
                    break

                batch_documents = self._extract_documents_from_payload(payload)
                if batch_documents is None:
                    logger.error(
                        "Неожиданный payload списка документов WB: account=%s category=%s offset=%s",
                        account,
                        category,
                        offset,
                    )
                    break

                if not batch_documents:
                    if offset == 0:
                        logger.info(
                            "Список документов WB пуст: account=%s act_type=%s category=%s",
                            account,
                            expected_act_type,
                            category,
                        )
                    break

                models_batch = self._build_document_models(
                    account=account,
                    category=category,
                    expected_act_type=expected_act_type,
                    raw_documents=batch_documents,
                )
                documents.extend(models_batch)

                logger.info(
                    "Получена страница документов WB: account=%s category=%s offset=%s raw_count=%s usable_count=%s",
                    account,
                    category,
                    offset,
                    len(batch_documents),
                    len(models_batch),
                )

                if len(batch_documents) < DOCUMENT_BATCH_SIZE:
                    break

                offset += DOCUMENT_BATCH_SIZE

        logger.info(
            "Список документов WB собран: account=%s act_type=%s category=%s documents=%s",
            account,
            expected_act_type,
            category,
            len(documents),
        )
        return documents

    async def list_fbo_documents(
        self,
        account: str,
        token: str,
        begin_date: date,
        end_date: date,
    ) -> list[WBDocumentMeta]:
        """Возвращает метаданные документов ФБО за указанный период.

        Метод фиксирует категорию `act-income` и тип акта `fbo`, оставляя
        orchestration-слою только выбор периода и аккаунта.
        """
        return await self.list_documents(
            account=account,
            token=token,
            begin_date=begin_date,
            end_date=end_date,
            category=FBO_DOCUMENT_CATEGORY,
            expected_act_type=ACT_TYPE_FBO,
        )

    async def list_fbs_documents(
        self,
        account: str,
        token: str,
        begin_date: date,
        end_date: date,
    ) -> list[WBDocumentMeta]:
        """Возвращает метаданные документов ФБС за указанный период.

        Метод фиксирует категорию `act-income-mp` и тип акта `fbs`.
        """
        return await self.list_documents(
            account=account,
            token=token,
            begin_date=begin_date,
            end_date=end_date,
            category=FBS_DOCUMENT_CATEGORY,
            expected_act_type=ACT_TYPE_FBS,
        )

    async def download_documents_batch(
        self,
        account: str,
        token: str,
        expected_act_type: ActType,
        service_names: list[str],
        batch_index: int,
    ) -> DownloadedDocumentBatch:
        """Скачивает один батч документов через `documents/download/all`.

        Метод валидирует входной батч, отправляет запрос к WB и возвращает
        только транспортный результат `DownloadedDocumentBatch`. Здесь
        намеренно нет попыток распаковать base64 payload.
        """
        if not service_names:
            raise ValueError("Батч документов не может быть пустым.")
        if len(service_names) > DOCUMENT_BATCH_SIZE:
            raise ValueError(
                f"Размер батча превышает лимит {DOCUMENT_BATCH_SIZE} документов."
            )

        payload = {
            "params": [
                {"extension": "xlsx", "serviceName": service_name}
                for service_name in service_names
            ]
        }

        logger.info(
            "Скачивание батча документов WB: account=%s act_type=%s batch_index=%s batch_size=%s",
            account,
            expected_act_type,
            batch_index,
            len(service_names),
        )

        async with self._get_session() as session:
            client = self._build_http_client(
                session=session,
                account=account,
                token=token,
            )
            response_payload = await client.post(
                DOCUMENTS_DOWNLOAD_ALL_URL,
                json=payload,
                retries=RETRY_ATTEMPTS,
                delay=RETRY_BASE_DELAY_SECONDS,
            )

        if response_payload is None:
            raise ValueError(
                f"WB API не вернул payload для скачивания батча account={account} batch_index={batch_index}"
            )

        base64_payload = self._extract_document_binary(response_payload)
        if not base64_payload:
            raise ValueError(
                f"В ответе WB нет документа для account={account} batch_index={batch_index}"
            )

        logger.info(
            "Батч документов WB успешно скачан: account=%s act_type=%s batch_index=%s batch_size=%s",
            account,
            expected_act_type,
            batch_index,
            len(service_names),
        )

        return DownloadedDocumentBatch(
            account=account,
            expected_act_type=expected_act_type,
            service_names=service_names,
            base64_payload=base64_payload,
            batch_index=batch_index,
            downloaded_at=self._utcnow(),
        )

    async def iter_downloaded_batches(
        self,
        account: str,
        token: str,
        begin_date: date,
        end_date: date,
        expected_act_type: ActType,
    ) -> AsyncIterator[DownloadedDocumentBatch]:
        """Постепенно отдаёт скачанные батчи документов для одного аккаунта.

        Алгоритм работы:
        1. Получить список документов нужного типа.
        2. Извлечь `service_name`.
        3. Разбить документы на батчи по `DOCUMENT_BATCH_SIZE`.
        4. Скачать один батч.
        5. Сразу `yield` результат дальше.

        Важно: метод не накапливает скачанные архивы в одном большом списке.
        """
        logger.info(
            "Старт потокового скачивания батчей WB: account=%s act_type=%s period=%s..%s",
            account,
            expected_act_type,
            begin_date.isoformat(),
            end_date.isoformat(),
        )

        if expected_act_type == ACT_TYPE_FBO:
            documents = await self.list_fbo_documents(
                account=account,
                token=token,
                begin_date=begin_date,
                end_date=end_date,
            )
        elif expected_act_type == ACT_TYPE_FBS:
            documents = await self.list_fbs_documents(
                account=account,
                token=token,
                begin_date=begin_date,
                end_date=end_date,
            )
        else:
            raise ValueError(f"Неподдерживаемый тип акта: {expected_act_type}")

        service_names = [
            document.service_name.strip()
            for document in documents
            if document.service_name and document.service_name.strip()
        ]

        logger.info(
            "Подготовлен список serviceName для скачивания: account=%s act_type=%s documents=%s service_names=%s",
            account,
            expected_act_type,
            len(documents),
            len(service_names),
        )

        if not service_names:
            logger.warning(
                "Для скачивания нет документов с serviceName: account=%s act_type=%s",
                account,
                expected_act_type,
            )
            return

        total_batches = ceil(len(service_names) / DOCUMENT_BATCH_SIZE)
        for batch_index, service_names_batch in enumerate(
            self._batch_service_names(service_names),
            start=1,
        ):
            logger.info(
                "Скачивание batch %s/%s: account=%s act_type=%s batch_size=%s",
                batch_index,
                total_batches,
                account,
                expected_act_type,
                len(service_names_batch),
            )
            try:
                downloaded_batch = await self.download_documents_batch(
                    account=account,
                    token=token,
                    expected_act_type=expected_act_type,
                    service_names=service_names_batch,
                    batch_index=batch_index,
                )
            except ValueError as error:
                logger.error(
                    "Ошибка скачивания батча документов WB: account=%s act_type=%s batch_index=%s error=%s",
                    account,
                    expected_act_type,
                    batch_index,
                    error,
                )
                continue

            yield downloaded_batch

    def _build_http_client(
        self,
        session: aiohttp.ClientSession,
        account: str,
        token: str,
    ) -> HTTPClient:
        """Создаёт `HTTPClient` в стиле существующей инфраструктуры проекта."""
        return HTTPClient(
            session=session,
            api_key=token,
            account=account,
            timeout=REQUEST_TIMEOUT_SECONDS,
        )

    def _extract_documents_from_payload(
        self,
        payload: dict[str, object],
    ) -> list[dict[str, object]] | None:
        """Извлекает список документов из ответа `documents/list`.

        Метод нужен, чтобы изолировать работу с вложенным JSON и аккуратно
        различать пустой ответ и неожиданный формат payload.
        """
        data = payload.get("data")
        if not isinstance(data, dict):
            return None

        documents = data.get("documents")
        if documents is None:
            return []
        if not isinstance(documents, list):
            return None

        normalized_documents: list[dict[str, object]] = []
        for document in documents:
            if isinstance(document, dict):
                normalized_documents.append(document)
            else:
                logger.warning(
                    "Пропущен документ WB с неожиданным форматом: type=%s",
                    type(document).__name__,
                )
        return normalized_documents

    def _build_document_models(
        self,
        account: str,
        category: str,
        expected_act_type: ActType,
        raw_documents: list[dict[str, object]],
    ) -> list[WBDocumentMeta]:
        """Строит модели `WBDocumentMeta`, пропуская элементы без serviceName."""
        models: list[WBDocumentMeta] = []
        for raw_document in raw_documents:
            service_name = raw_document.get("serviceName")
            if not isinstance(service_name, str) or not service_name.strip():
                logger.warning(
                    "Пропущен документ WB без serviceName: account=%s act_type=%s payload_keys=%s",
                    account,
                    expected_act_type,
                    sorted(raw_document.keys()),
                )
                continue

            created_at = self._parse_document_created_at(raw_document)
            models.append(
                WBDocumentMeta(
                    account=account,
                    service_name=service_name.strip(),
                    category=category,
                    expected_act_type=expected_act_type,
                    created_at=created_at,
                    raw_payload=raw_document,
                )
            )
        return models

    def _parse_document_created_at(
        self,
        raw_document: dict[str, object],
    ) -> datetime | None:
        """Пытается извлечь дату/время создания документа из known-полей WB.

        На этом этапе значение используется только как дополнительная мета-
        информация и не должно приводить к падению клиента, если формат WB
        окажется другим.
        """
        for field_name in ("createdAt", "createdDate", "date", "created_at"):
            raw_value = raw_document.get(field_name)
            if not isinstance(raw_value, str) or not raw_value.strip():
                continue
            try:
                normalized = raw_value.replace("Z", "+00:00")
                return datetime.fromisoformat(normalized)
            except ValueError:
                continue
        return None

    def _extract_document_binary(self, payload: dict[str, object]) -> str | None:
        """Извлекает base64-документ из ответа `documents/download/all`."""
        data = payload.get("data")
        if not isinstance(data, dict):
            return None

        document_value = data.get("document")
        if not isinstance(document_value, str) or not document_value.strip():
            return None
        return document_value

    def _batch_service_names(
        self,
        service_names: list[str],
    ) -> Iterator[list[str]]:
        """Разбивает `service_name` на батчи фиксированного размера."""
        for index in range(0, len(service_names), DOCUMENT_BATCH_SIZE):
            yield service_names[index : index + DOCUMENT_BATCH_SIZE]

    def _utcnow(self) -> datetime:
        """Возвращает текущий UTC timestamp для `DownloadedDocumentBatch`."""
        return datetime.now(timezone.utc)

    def _get_session(self) -> "_SessionManager":
        """Возвращает менеджер сессии, не создающий запросов при импорте."""
        return _SessionManager(self.session)


class _SessionManager:
    """Вспомогательный async context manager для безопасной работы с сессией.

    Если внешняя `ClientSession` уже передана в клиент, менеджер переиспользует
    её и не закрывает. Если сессии нет, она создаётся только на время вызова
    метода и после этого корректно закрывается.
    """

    def __init__(self, session: aiohttp.ClientSession | None) -> None:
        self._provided_session = session
        self._created_session: aiohttp.ClientSession | None = None

    async def __aenter__(self) -> aiohttp.ClientSession:
        if self._provided_session is not None:
            return self._provided_session

        self._created_session = aiohttp.ClientSession()
        return self._created_session

    async def __aexit__(self, exc_type, exc, exc_tb) -> None:
        if self._created_session is not None:
            await self._created_session.close()
