from __future__ import annotations

import asyncio
import logging
from collections.abc import Mapping
from datetime import date

import aiohttp

from src_oop.jobs.bukh_docs.config import (
    DOCUMENT_CATEGORIES,
    DOCUMENT_DOWNLOAD_URL,
    DOCUMENT_LIST_URL,
    DOCUMENTS_BATCH_LIMIT,
    DOWNLOAD_TIMEOUT_SECONDS,
    REQUEST_TIMEOUT_SECONDS,
    RETRY_ATTEMPTS,
    RETRY_BASE_DELAY_SECONDS,
)
from src_oop.jobs.bukh_docs.models import DocumentRequest, DownloadedDocumentsPayload

logger = logging.getLogger(__name__)


class WBDocsClient:
    """Клиент WB Documents API для получения и скачивания бухгалтерских документов."""

    def __init__(self, session: aiohttp.ClientSession | None = None) -> None:
        self._session = session

    async def list_documents_for_account(
        self,
        account: str,
        token: str,
        date_from: date,
        date_to: date,
    ) -> list[DocumentRequest]:
        documents: list[DocumentRequest] = []

        async with self._get_session() as session:
            for category in DOCUMENT_CATEGORIES:
                logger.info(
                    "Запрос списка документов WB: account=%s category=%s period=%s..%s",
                    account,
                    category,
                    date_from.isoformat(),
                    date_to.isoformat(),
                )
                offset = 0
                while True:
                    params = {
                        "beginTime": date_from.isoformat(),
                        "endTime": date_to.isoformat(),
                        "category": category,
                        "limit": DOCUMENTS_BATCH_LIMIT,
                        "offset": offset,
                    }
                    payload = await self._request_json(
                        session=session,
                        method="GET",
                        url=DOCUMENT_LIST_URL,
                        token=token,
                        account=account,
                        params=params,
                        timeout_seconds=REQUEST_TIMEOUT_SECONDS,
                    )
                    if payload is None:
                        logger.warning(
                            "Не удалось получить страницу списка документов WB: account=%s category=%s offset=%s",
                            account,
                            category,
                            offset,
                        )
                        break

                    batch_documents = self._extract_documents(payload)
                    if not batch_documents:
                        logger.info(
                            "Пустая страница списка документов WB: account=%s category=%s offset=%s",
                            account,
                            category,
                            offset,
                        )
                        break

                    prepared_batch = self._build_document_requests(
                        account=account,
                        category=category,
                        raw_documents=batch_documents,
                    )
                    documents.extend(prepared_batch)
                    logger.info(
                        "Получена страница документов WB: account=%s category=%s offset=%s raw_documents=%s prepared_documents=%s",
                        account,
                        category,
                        offset,
                        len(batch_documents),
                        len(prepared_batch),
                    )

                    if len(batch_documents) < DOCUMENTS_BATCH_LIMIT:
                        break
                    offset += DOCUMENTS_BATCH_LIMIT

        logger.info(
            "Собран список документов WB: account=%s documents=%s",
            account,
            len(documents),
        )
        return documents

    async def download_documents_for_account(
        self,
        account: str,
        token: str,
        document_requests: list[DocumentRequest],
    ) -> DownloadedDocumentsPayload | None:
        if not document_requests:
            logger.info("Пропуск скачивания документов WB: account=%s пустой список документов", account)
            return None

        request_body = {
            "params": [
                {
                    "extension": document.extension,
                    "serviceName": document.service_name,
                }
                for document in document_requests
            ]
        }

        async with self._get_session() as session:
            payload = await self._request_json(
                session=session,
                method="POST",
                url=DOCUMENT_DOWNLOAD_URL,
                token=token,
                account=account,
                json=request_body,
                timeout_seconds=DOWNLOAD_TIMEOUT_SECONDS,
            )

        if payload is None:
            logger.error(
                "Не удалось скачать документы WB: account=%s requested_documents=%s",
                account,
                len(document_requests),
            )
            return None

        data = payload.get("data")
        if not isinstance(data, dict):
            logger.error("WB download payload без data: account=%s", account)
            return None

        base64_document = data.get("document")
        if not isinstance(base64_document, str) or not base64_document.strip():
            logger.error("WB download payload без document: account=%s", account)
            return None

        logger.info(
            "Документы WB скачаны: account=%s requested_documents=%s",
            account,
            len(document_requests),
        )
        return DownloadedDocumentsPayload(
            account=account,
            document_requests=document_requests,
            base64_document=base64_document,
        )

    def _build_document_requests(
        self,
        account: str,
        category: str,
        raw_documents: list[dict[str, object]],
    ) -> list[DocumentRequest]:
        documents: list[DocumentRequest] = []
        for raw_document in raw_documents:
            service_name = raw_document.get("serviceName")
            if not isinstance(service_name, str) or not service_name.strip():
                logger.warning(
                    "Пропущен документ WB без serviceName: account=%s category=%s keys=%s",
                    account,
                    category,
                    sorted(raw_document.keys()),
                )
                continue

            extensions = raw_document.get("extensions")
            if isinstance(extensions, list) and extensions:
                extension_value = extensions[0]
            else:
                extension_value = "xlsx"

            if not isinstance(extension_value, str) or not extension_value.strip():
                extension_value = "xlsx"

            documents.append(
                DocumentRequest(
                    account=account,
                    doc_type=category,
                    service_name=service_name.strip(),
                    extension=extension_value.strip(),
                )
            )
        return documents

    def _extract_documents(
        self,
        payload: dict[str, object],
    ) -> list[dict[str, object]]:
        data = payload.get("data")
        if not isinstance(data, dict):
            return []

        documents = data.get("documents")
        if not isinstance(documents, list):
            return []

        return [document for document in documents if isinstance(document, dict)]

    async def _request_json(
        self,
        session: aiohttp.ClientSession,
        method: str,
        url: str,
        token: str,
        account: str,
        timeout_seconds: int,
        params: Mapping[str, object] | None = None,
        json: dict[str, object] | None = None,
    ) -> dict[str, object] | None:
        headers = {"Authorization": token}

        for attempt in range(1, RETRY_ATTEMPTS + 1):
            try:
                async with session.request(
                    method=method,
                    url=url,
                    headers=headers,
                    params=params,
                    json=json,
                    timeout=aiohttp.ClientTimeout(total=timeout_seconds),
                ) as response:
                    if response.status == 200:
                        payload = await response.json()
                        if isinstance(payload, dict):
                            return payload
                        logger.error(
                            "Некорректный JSON-ответ WB: account=%s method=%s url=%s",
                            account,
                            method,
                            url,
                        )
                        return None

                    if response.status in (400, 429, 503):
                        error_detail = await self._read_error(response)
                        logger.warning(
                            "Повторяем запрос WB: account=%s method=%s status=%s attempt=%s/%s detail=%s",
                            account,
                            method,
                            response.status,
                            attempt,
                            RETRY_ATTEMPTS,
                            error_detail,
                        )
                        await asyncio.sleep(RETRY_BASE_DELAY_SECONDS * attempt)
                        continue

                    error_detail = await self._read_error(response)
                    logger.error(
                        "WB API error: account=%s status=%s method=%s url=%s detail=%s",
                        account,
                        response.status,
                        method,
                        url,
                        error_detail,
                    )
                    return None
            except (aiohttp.ClientError, asyncio.TimeoutError) as error:
                logger.warning(
                    "Сбой запроса WB: account=%s method=%s url=%s attempt=%s/%s error=%s",
                    account,
                    method,
                    url,
                    attempt,
                    RETRY_ATTEMPTS,
                    error,
                )
                if attempt == RETRY_ATTEMPTS:
                    return None
                await asyncio.sleep(RETRY_BASE_DELAY_SECONDS * attempt)

        return None

    async def _read_error(self, response: aiohttp.ClientResponse) -> str:
        if response.content_type == "application/json":
            try:
                payload = await response.json()
            except Exception:
                return await response.text()
            if isinstance(payload, dict):
                detail = payload.get("detail")
                if isinstance(detail, str):
                    return detail
        return await response.text()

    def _get_session(self) -> "_SessionManager":
        return _SessionManager(self._session)


class _SessionManager:
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
