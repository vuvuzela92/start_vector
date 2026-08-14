"""Нормализация возвратов покупателей WB перед записью в Google Sheets."""

from __future__ import annotations

import json
import logging
import re
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime

import pandas as pd

from src_oop.jobs.returns_to_customers.config import (
    COLUMN_RENAME_MAP,
    DATABASE_COLUMN_RENAME_MAP,
    DB_COLUMNS,
    DB_DATETIME_COLUMNS,
    DB_INTEGER_COLUMNS,
    ENUM_TEXT_MAPS,
    PRIORITY_COLUMNS,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class ClaimEnvelope:
    """Контейнер одной заявки, который сохраняет кабинет и состояние архива рядом с ответом WB."""

    account: str
    is_archive: bool
    claim: dict


class BuyersReturnsNormalizer:
    """Подготавливает витрину возвратов в удобном для Google Sheets бизнес-виде."""

    def normalize(
        self,
        claim_envelopes: Iterable[ClaimEnvelope],
    ) -> pd.DataFrame:
        """Преобразует заявки WB в плоскую таблицу с русскими заголовками для аналитиков."""
        rows = [
            self._flatten_claim(envelope, serialize_lists=True)
            for envelope in claim_envelopes
        ]
        if not rows:
            logger.info("Для выгрузки возвратов покупателей нет строк после нормализации.")
            return pd.DataFrame(columns=list(PRIORITY_COLUMNS))

        dataframe = pd.DataFrame.from_records(rows)
        dataframe["updated_at_export"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        renamed_columns = {
            column: COLUMN_RENAME_MAP.get(column, self._fallback_column_title(column))
            for column in dataframe.columns
        }
        dataframe = dataframe.rename(columns=renamed_columns)
        dataframe = self._order_columns(dataframe)
        dataframe = dataframe.astype(object).where(pd.notnull(dataframe), "")

        logger.info(
            "Нормализация возвратов покупателей завершена | rows=%s | columns=%s",
            len(dataframe.index),
            list(dataframe.columns),
        )
        return dataframe

    def normalize_for_database(
        self,
        claim_envelopes: Iterable[ClaimEnvelope],
    ) -> pd.DataFrame:
        """Готовит технический DataFrame для PostgreSQL, сохраняя коды, тексты и исходные поля заявки."""
        rows = [
            self._flatten_claim(envelope, serialize_lists=False)
            for envelope in claim_envelopes
        ]
        if not rows:
            return pd.DataFrame(columns=list(DB_COLUMNS))

        dataframe = pd.DataFrame.from_records(rows)
        dataframe["updated_at_export"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        dataframe = dataframe.rename(columns=DATABASE_COLUMN_RENAME_MAP)
        dataframe = self._enrich_database_identifiers(dataframe)

        for column in DB_COLUMNS:
            if column not in dataframe.columns:
                dataframe[column] = pd.NA

        for column in DB_INTEGER_COLUMNS:
            numeric_series = pd.to_numeric(dataframe[column], errors="coerce")
            if column == "price":
                dataframe[column] = numeric_series.round().astype("Int64")
                continue
            dataframe[column] = numeric_series.astype("Int64")

        for column in DB_DATETIME_COLUMNS:
            dataframe[column] = pd.to_datetime(dataframe[column], errors="coerce")

        dataframe = self._fill_required_legacy_datetime_fields(dataframe)

        if "is_archive" in dataframe.columns:
            dataframe["is_archive"] = dataframe["is_archive"].map(
                lambda value: None if pd.isna(value) else bool(value)
            )

        dataframe = dataframe.loc[:, list(DB_COLUMNS)].astype(object)
        if "created_at" in dataframe.columns:
            dataframe = dataframe.drop(columns=["created_at"])
        dataframe = dataframe.mask(pd.isna(dataframe), None)
        logger.info(
            "Подготовлен технический DataFrame возвратов для PostgreSQL | rows=%s | columns=%s",
            len(dataframe.index),
            list(dataframe.columns),
        )
        return dataframe

    def _fill_required_legacy_datetime_fields(
        self,
        dataframe: pd.DataFrame,
    ) -> pd.DataFrame:
        """Дозаполняет обязательные legacy-даты для `public.claims`, если WB не прислал исходное поле.

        Бизнес-правило: унаследованная таблица `claims` требует обязательные даты
        `dt`, `order_dt`, `dt_update` и `delivery_dt`, но API WB иногда не присылает одну
        из них. Чтобы не ронять всю выгрузку, берем ближайшую доступную дату из той же
        заявки как технический fallback для upsert.
        """
        enriched_dataframe = dataframe.copy()
        datetime_fallbacks: dict[str, tuple[str, ...]] = {
            "dt": ("dt_update", "order_dt", "delivery_dt"),
            "order_dt": ("delivery_dt", "dt", "dt_update"),
            "dt_update": ("dt", "order_dt", "delivery_dt"),
            "delivery_dt": ("order_dt", "dt", "dt_update"),
        }

        for target_column, fallback_columns in datetime_fallbacks.items():
            if target_column not in enriched_dataframe.columns:
                continue

            target_series = enriched_dataframe[target_column]
            missing_before = int(target_series.isna().sum())
            if missing_before == 0:
                continue

            for fallback_column in fallback_columns:
                if fallback_column not in enriched_dataframe.columns:
                    continue
                target_series = target_series.fillna(enriched_dataframe[fallback_column])

            enriched_dataframe[target_column] = target_series
            restored_count = missing_before - int(target_series.isna().sum())
            if restored_count > 0:
                logger.info(
                    "Восстановлены обязательные даты для legacy-таблицы claims | column=%s | restored_rows=%s",
                    target_column,
                    restored_count,
                )

        return enriched_dataframe

    def _enrich_database_identifiers(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        """Готовит идентификаторы для `public.claims`, чтобы строковый id WB сохранялся как ключ, а числовой claim_id заполнялся только когда он действительно существует."""
        enriched_dataframe = dataframe.copy()
        raw_id_series = enriched_dataframe.get("id")
        if raw_id_series is None:
            return enriched_dataframe

        string_ids = raw_id_series.map(self._normalize_string_identifier)
        enriched_dataframe["id"] = string_ids
        enriched_dataframe["claim_id"] = raw_id_series.map(self._extract_numeric_claim_id)
        return enriched_dataframe

    def _flatten_claim(
        self,
        envelope: ClaimEnvelope,
        serialize_lists: bool,
    ) -> dict[str, object]:
        """Разворачивает заявку WB в плоскую строку, сохраняя списки либо как JSON для листа, либо как Python-списки для БД."""
        flattened: dict[str, object] = {
            "account": envelope.account,
            "is_archive": envelope.is_archive,
        }
        self._flatten_mapping(
            source=envelope.claim,
            target=flattened,
            parent_key="",
            serialize_lists=serialize_lists,
        )
        self._add_enum_text_fields(flattened)
        return flattened

    def _flatten_mapping(
        self,
        source: dict,
        target: dict[str, object],
        parent_key: str,
        serialize_lists: bool,
    ) -> None:
        """Рекурсивно разворачивает словари и подбирает формат списков под целевой сценарий выгрузки."""
        for key, value in source.items():
            composed_key = f"{parent_key}_{key}" if parent_key else str(key)
            if isinstance(value, dict):
                self._flatten_mapping(
                    source=value,
                    target=target,
                    parent_key=composed_key,
                    serialize_lists=serialize_lists,
                )
                continue
            if isinstance(value, list):
                if serialize_lists:
                    target[composed_key] = self._serialize_complex_value(value)
                else:
                    target[composed_key] = value
                continue
            target[composed_key] = value

    def _serialize_complex_value(self, value: object) -> str:
        """Сериализует вложенные массивы и объекты, чтобы Google Sheets не терял детали возврата."""
        return json.dumps(value, ensure_ascii=False)

    def _normalize_string_identifier(self, raw_value: object) -> str:
        """Преобразует первичный идентификатор заявки WB в стабильную строку для обязательной колонки `claims.id`."""
        if raw_value in (None, ""):
            return ""
        return str(raw_value).strip()

    def _extract_numeric_claim_id(self, raw_value: object) -> int | None:
        """Извлекает числовой id заявки только для строк, где WB действительно прислал число, чтобы не засорять `claim_id` псевдозначениями."""
        if raw_value in (None, ""):
            return None
        raw_text = str(raw_value).strip()
        if not raw_text:
            return None
        if re.fullmatch(r"\d+", raw_text) is None:
            return None
        return int(raw_text)

    def _add_enum_text_fields(self, flattened: dict[str, object]) -> None:
        """Добавляет текстовые расшифровки кодов WB, чтобы лист был понятен без чтения документации."""
        for field_name, field_map in ENUM_TEXT_MAPS.items():
            if field_name not in flattened:
                continue

            flattened[f"{field_name}_text"] = self._resolve_enum_text(
                field_name=field_name,
                raw_value=flattened[field_name],
                field_map=field_map,
            )

    def _resolve_enum_text(
        self,
        field_name: str,
        raw_value: object,
        field_map: dict[int, str],
    ) -> str:
        """Преобразует код WB в текст и помечает случаи, когда локальный справочник еще не заполнен."""
        if raw_value in (None, ""):
            return ""

        try:
            normalized_value = int(raw_value)
        except (TypeError, ValueError):
            return str(raw_value)

        if normalized_value in field_map:
            return field_map[normalized_value]

        logger.warning(
            "Для поля возвратов WB пока нет текстовой расшифровки кода | field=%s | code=%s",
            field_name,
            normalized_value,
        )
        return f"Неизвестный код: {normalized_value}"

    def _fallback_column_title(self, column_name: str) -> str:
        """Даёт русское название допполям, которые WB может добавить без изменения нашего кода."""
        return f"Доп. поле: {column_name}"

    def _order_columns(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        """Ставит ключевые бизнес-колонки в начало листа, а редкие дополнительные поля оставляет справа."""
        existing_priority = [
            column for column in PRIORITY_COLUMNS if column in dataframe.columns
        ]
        tail_columns = [
            column for column in dataframe.columns if column not in existing_priority
        ]
        return dataframe.loc[:, [*existing_priority, *tail_columns]]
