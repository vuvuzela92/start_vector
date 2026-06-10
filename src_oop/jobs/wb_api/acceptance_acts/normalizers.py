"""Нормализаторы строк актов приёма-передачи WB."""

from __future__ import annotations

import logging
import re
from collections.abc import Iterator
from datetime import date, datetime

import pandas as pd

from src_oop.jobs.wb_api.acceptance_acts.config import (
    FBO_DEFAULT_KIZ,
    FBO_DEFAULT_QUANTITY,
    FBO_DEFAULT_SHK_ID,
    FBO_DOCUMENT_NUMBER_REGEX,
    FBO_OUTPUT_COLUMNS,
    FBS_DEFAULT_QUANTITY,
    FBS_DOCUMENT_NUMBER_REGEX,
    FBS_OUTPUT_COLUMNS,
    NORMALIZED_ROWS_CHUNK_SIZE,
)
from src_oop.jobs.wb_api.acceptance_acts.models import (
    ExcelParseResult,
    NormalizedFboRow,
    NormalizedFbsRow,
)

logger = logging.getLogger(__name__)

TOTAL_ROW_MARKER = "Итого"


class FboAcceptanceNormalizer:
    """Нормализатор строк ФБО для таблицы `acceptance_fbo_acts_new`."""

    def normalize(self, parse_result: ExcelParseResult) -> list[NormalizedFboRow]:
        """Преобразует один `ExcelParseResult` в список нормализованных строк ФБО."""
        dataframe = self._prepare_dataframe(parse_result)
        if dataframe is None:
            return []

        input_rows = len(dataframe)
        dataframe = self._apply_defaults(dataframe, parse_result)
        dataframe = self._coerce_types(dataframe, parse_result)
        filtered_dataframe = self._filter_valid_rows(dataframe, parse_result)

        normalized_rows: list[NormalizedFboRow] = []
        dropped_rows = 0

        for row_index, row in filtered_dataframe.iterrows():
            normalized_row = self._build_normalized_row(
                row=row,
                parse_result=parse_result,
                row_index=row_index,
            )
            if normalized_row is None:
                dropped_rows += 1
                continue
            normalized_rows.append(normalized_row)

        logger.info(
            "Нормализация ФБО завершена: account=%s document=%s document_number=%s act_type=%s "
            "input_rows=%s filtered_rows=%s normalized_rows=%s dropped_rows=%s",
            parse_result.account,
            parse_result.document_name,
            parse_result.document_number,
            parse_result.actual_act_type or parse_result.expected_act_type,
            input_rows,
            len(filtered_dataframe),
            len(normalized_rows),
            dropped_rows,
        )

        if not normalized_rows:
            logger.warning(
                "ФБО документ не дал ни одной нормализованной строки: account=%s document=%s",
                parse_result.account,
                parse_result.document_name,
            )

        return normalized_rows

    def iter_normalized_chunks(
        self,
        parse_result: ExcelParseResult,
        chunk_size: int = NORMALIZED_ROWS_CHUNK_SIZE,
    ) -> Iterator[list[NormalizedFboRow]]:
        """Отдаёт нормализованные строки ФБО чанками для одного документа."""
        normalized_rows = self.normalize(parse_result)
        if chunk_size <= 0:
            raise ValueError("chunk_size должен быть положительным.")

        for start in range(0, len(normalized_rows), chunk_size):
            yield normalized_rows[start : start + chunk_size]

    def _prepare_dataframe(self, parse_result: ExcelParseResult) -> pd.DataFrame | None:
        """Готовит копию canonical dataframe для безопасной нормализации."""
        dataframe = parse_result.canonical_dataframe
        if dataframe is None or dataframe.empty:
            logger.warning(
                "ФБО normalizer получил пустой canonical_dataframe: account=%s document=%s",
                parse_result.account,
                parse_result.document_name,
            )
            return None

        prepared = dataframe.copy()
        for column_name in FBO_OUTPUT_COLUMNS:
            if column_name not in prepared.columns:
                prepared[column_name] = pd.NA
        return prepared

    def _apply_defaults(
        self,
        dataframe: pd.DataFrame,
        parse_result: ExcelParseResult,
    ) -> pd.DataFrame:
        """Применяет значения по умолчанию для ФБО."""
        result = dataframe.copy()

        result["quantity"] = result["quantity"].where(
            result["quantity"].notna(),
            FBO_DEFAULT_QUANTITY,
        )
        result["kiz"] = result["kiz"].fillna(FBO_DEFAULT_KIZ)
        result["kiz"] = result["kiz"].replace("", FBO_DEFAULT_KIZ)
        result["shk_id"] = result["shk_id"].where(
            result["shk_id"].notna(),
            FBO_DEFAULT_SHK_ID,
        )

        document_number = self._resolve_document_number(parse_result)
        if document_number is not None:
            result["document_number"] = result["document_number"].where(
                result["document_number"].notna(),
                document_number,
            )
            result["document_number"] = result["document_number"].replace(
                "",
                document_number,
            )

        document_date = self._resolve_document_date(parse_result, result)
        if document_date is not None:
            result["date"] = result["date"].where(result["date"].notna(), document_date)

        result["document"] = result["document"].where(
            result["document"].notna(),
            parse_result.document_name,
        )
        result["account"] = result["account"].where(
            result["account"].notna(),
            parse_result.account,
        )
        return result

    def _coerce_types(
        self,
        dataframe: pd.DataFrame,
        parse_result: ExcelParseResult,
    ) -> pd.DataFrame:
        """Приводит значения ФБО к типам, совместимым с БД."""
        result = dataframe.copy()

        result["num"] = pd.to_numeric(result["num"], errors="coerce").fillna(0).astype(int)

        quantity_numeric = pd.to_numeric(result["quantity"], errors="coerce")
        invalid_quantity_mask = result["quantity"].notna() & quantity_numeric.isna()
        if invalid_quantity_mask.any():
            logger.warning(
                "ФБО quantity не удалось привести к int, подставляю 1: account=%s document=%s rows=%s",
                parse_result.account,
                parse_result.document_name,
                invalid_quantity_mask[invalid_quantity_mask].index.tolist(),
            )
        result["quantity"] = quantity_numeric.fillna(FBO_DEFAULT_QUANTITY).astype(int)

        shk_numeric = pd.to_numeric(result["shk_id"], errors="coerce")
        invalid_shk_mask = result["shk_id"].notna() & shk_numeric.isna()
        if invalid_shk_mask.any():
            logger.warning(
                "ФБО shk_id не удалось привести к int, подставляю 0: account=%s document=%s rows=%s",
                parse_result.account,
                parse_result.document_name,
                invalid_shk_mask[invalid_shk_mask].index.tolist(),
            )
        result["shk_id"] = shk_numeric.fillna(FBO_DEFAULT_SHK_ID).astype(int)

        result["date"] = pd.to_datetime(result["date"], dayfirst=True, errors="coerce").dt.date

        for column_name in (
            "product_name",
            "unit",
            "barcode",
            "vendor_code",
            "size",
            "kiz",
            "box_barcode",
            "document",
            "document_number",
            "account",
        ):
            result[column_name] = result[column_name].map(self._to_clean_string)

        return result

    def _filter_valid_rows(
        self,
        dataframe: pd.DataFrame,
        parse_result: ExcelParseResult,
    ) -> pd.DataFrame:
        """Оставляет только строки ФБО с непустым box_barcode."""
        mask = dataframe["box_barcode"].map(self._has_text_value)
        filtered = dataframe[mask].copy().reset_index(drop=True)

        logger.info(
            "Фильтрация строк ФБО по box_barcode: account=%s document=%s before=%s after=%s",
            parse_result.account,
            parse_result.document_name,
            len(dataframe),
            len(filtered),
        )
        return filtered

    def _build_normalized_row(
        self,
        row: pd.Series,
        parse_result: ExcelParseResult,
        row_index: int,
    ) -> NormalizedFboRow | None:
        """Собирает одну нормализованную строку ФБО или пропускает её с warning."""
        document_number = self._pick_row_document_number(row, parse_result)
        document_date = self._pick_row_date(row, parse_result)
        vendor_code = self._to_clean_string(row.get("vendor_code"))
        box_barcode = self._to_clean_string(row.get("box_barcode"))

        if not vendor_code:
            logger.warning(
                "Пропускаю строку ФБО без vendor_code: account=%s document=%s row=%s",
                parse_result.account,
                parse_result.document_name,
                row_index,
            )
            return None
        if not box_barcode:
            logger.warning(
                "Пропускаю строку ФБО без box_barcode: account=%s document=%s row=%s",
                parse_result.account,
                parse_result.document_name,
                row_index,
            )
            return None
        if not document_number:
            logger.warning(
                "Пропускаю строку ФБО без document_number: account=%s document=%s row=%s",
                parse_result.account,
                parse_result.document_name,
                row_index,
            )
            return None
        if document_date is None:
            logger.warning(
                "Пропускаю строку ФБО без date: account=%s document=%s row=%s",
                parse_result.account,
                parse_result.document_name,
                row_index,
            )
            return None

        return NormalizedFboRow(
            num=int(row.get("num", 0)),
            product_name=self._to_clean_string(row.get("product_name")),
            unit=self._to_clean_string(row.get("unit")),
            barcode=self._to_clean_string(row.get("barcode")),
            vendor_code=vendor_code,
            size=self._to_clean_string(row.get("size")),
            kiz=self._to_clean_string(row.get("kiz")) or FBO_DEFAULT_KIZ,
            box_barcode=box_barcode,
            quantity=int(row.get("quantity", FBO_DEFAULT_QUANTITY)),
            document=self._to_clean_string(row.get("document")) or parse_result.document_name,
            document_number=document_number,
            date=document_date,
            shk_id=int(row.get("shk_id", FBO_DEFAULT_SHK_ID)),
            account=self._to_clean_string(row.get("account")) or parse_result.account,
        )

    def _resolve_document_number(self, parse_result: ExcelParseResult) -> str | None:
        """Возвращает document_number для ФБО с приоритетом legacy-regex."""
        extracted = self._extract_document_number(parse_result.document_name)
        return extracted or parse_result.document_number

    def _resolve_document_date(
        self,
        parse_result: ExcelParseResult,
        dataframe: pd.DataFrame,
    ) -> date | None:
        """Возвращает дату документа из parse_result или из колонки date."""
        if parse_result.document_date is not None:
            return parse_result.document_date

        if "date" not in dataframe.columns:
            return None

        parsed = pd.to_datetime(dataframe["date"], dayfirst=True, errors="coerce")
        first_valid = parsed.dropna()
        if first_valid.empty:
            return None
        return first_valid.iloc[0].date()

    def _extract_document_number(self, document_name: str) -> str | None:
        """Извлекает document_number для ФБО по legacy regex."""
        match = re.search(FBO_DOCUMENT_NUMBER_REGEX, document_name, flags=re.IGNORECASE)
        if match:
            return match.group(1)
        return None

    def _pick_row_document_number(
        self,
        row: pd.Series,
        parse_result: ExcelParseResult,
    ) -> str | None:
        """Выбирает document_number для конкретной строки."""
        row_value = self._to_clean_string(row.get("document_number"))
        if row_value:
            return row_value
        return self._resolve_document_number(parse_result)

    def _pick_row_date(
        self,
        row: pd.Series,
        parse_result: ExcelParseResult,
    ) -> date | None:
        """Выбирает date для конкретной строки."""
        row_date = row.get("date")
        if isinstance(row_date, datetime):
            return row_date.date()
        if isinstance(row_date, date):
            return row_date
        if row_date is not None and not pd.isna(row_date):
            parsed = pd.to_datetime(row_date, dayfirst=True, errors="coerce")
            if not pd.isna(parsed):
                return parsed.date()
        return parse_result.document_date

    def _to_clean_string(self, value: object) -> str:
        """Безопасно приводит значение к строке без мусорных пустот."""
        if value is None or pd.isna(value):
            return ""
        return str(value).strip()

    def _has_text_value(self, value: object) -> bool:
        """Проверяет, что значение непустое текстово."""
        return bool(self._to_clean_string(value))


class FbsAcceptanceNormalizer:
    """Нормализатор строк ФБС для таблицы `acceptance_fbs_acts_new`."""

    def normalize(self, parse_result: ExcelParseResult) -> list[NormalizedFbsRow]:
        """Преобразует один `ExcelParseResult` в список нормализованных строк ФБС."""
        dataframe = self._prepare_dataframe(parse_result)
        if dataframe is None:
            return []

        input_rows = len(dataframe)
        dataframe = self._drop_total_rows(dataframe, parse_result)
        dataframe = self._coerce_types(dataframe, parse_result)
        filtered_dataframe = self._filter_valid_rows(dataframe, parse_result)

        normalized_rows: list[NormalizedFbsRow] = []
        dropped_rows = 0

        for row_index, row in filtered_dataframe.iterrows():
            normalized_row = self._build_normalized_row(
                row=row,
                parse_result=parse_result,
                row_index=row_index,
            )
            if normalized_row is None:
                dropped_rows += 1
                continue
            normalized_rows.append(normalized_row)

        logger.info(
            "Нормализация ФБС завершена: account=%s document=%s document_number=%s act_type=%s "
            "input_rows=%s filtered_rows=%s normalized_rows=%s dropped_rows=%s",
            parse_result.account,
            parse_result.document_name,
            parse_result.document_number,
            parse_result.actual_act_type or parse_result.expected_act_type,
            input_rows,
            len(filtered_dataframe),
            len(normalized_rows),
            dropped_rows,
        )

        if not normalized_rows:
            logger.warning(
                "ФБС документ не дал ни одной нормализованной строки: account=%s document=%s",
                parse_result.account,
                parse_result.document_name,
            )

        return normalized_rows

    def iter_normalized_chunks(
        self,
        parse_result: ExcelParseResult,
        chunk_size: int = NORMALIZED_ROWS_CHUNK_SIZE,
    ) -> Iterator[list[NormalizedFbsRow]]:
        """Отдаёт нормализованные строки ФБС чанками для одного документа."""
        normalized_rows = self.normalize(parse_result)
        if chunk_size <= 0:
            raise ValueError("chunk_size должен быть положительным.")

        for start in range(0, len(normalized_rows), chunk_size):
            yield normalized_rows[start : start + chunk_size]

    def _prepare_dataframe(self, parse_result: ExcelParseResult) -> pd.DataFrame | None:
        """Готовит копию canonical dataframe для нормализации ФБС."""
        dataframe = parse_result.canonical_dataframe
        if dataframe is None or dataframe.empty:
            logger.warning(
                "ФБС normalizer получил пустой canonical_dataframe: account=%s document=%s",
                parse_result.account,
                parse_result.document_name,
            )
            return None

        prepared = dataframe.copy()
        for column_name in FBS_OUTPUT_COLUMNS:
            if column_name not in prepared.columns:
                prepared[column_name] = pd.NA
        return prepared

    def _drop_total_rows(
        self,
        dataframe: pd.DataFrame,
        parse_result: ExcelParseResult,
    ) -> pd.DataFrame:
        """Удаляет служебные строки `Итого` из ФБС-данных."""
        if "sticker" not in dataframe.columns:
            return dataframe.copy()

        total_mask = dataframe["sticker"].map(self._is_total_marker)
        filtered = dataframe[~total_mask].copy().reset_index(drop=True)

        logger.info(
            "Удаление строк 'Итого' для ФБС: account=%s document=%s before=%s after=%s",
            parse_result.account,
            parse_result.document_name,
            len(dataframe),
            len(filtered),
        )
        return filtered

    def _coerce_types(
        self,
        dataframe: pd.DataFrame,
        parse_result: ExcelParseResult,
    ) -> pd.DataFrame:
        """Приводит значения ФБС к типам старой реализации."""
        result = dataframe.copy()

        result["num"] = pd.to_numeric(result["num"], errors="coerce").fillna(0).astype(int)

        quantity_numeric = pd.to_numeric(result["quantity"], errors="coerce")
        invalid_quantity_mask = result["quantity"].notna() & quantity_numeric.isna()
        if invalid_quantity_mask.any():
            logger.warning(
                "ФБС quantity не удалось привести к числу, подставляю 0: account=%s document=%s rows=%s",
                parse_result.account,
                parse_result.document_name,
                invalid_quantity_mask[invalid_quantity_mask].index.tolist(),
            )
        result["quantity"] = quantity_numeric.fillna(FBS_DEFAULT_QUANTITY).astype(int)
        result["date"] = pd.to_datetime(result["date"], dayfirst=True, errors="coerce").dt.date

        for column_name in (
            "order_number",
            "unit",
            "sticker",
            "document",
            "document_number",
            "account",
        ):
            result[column_name] = result[column_name].map(self._to_clean_string)

        document_number = self._resolve_document_number(parse_result)
        if document_number is not None:
            result["document_number"] = result["document_number"].replace("", document_number)
            result["document_number"] = result["document_number"].fillna(document_number)

        if parse_result.document_date is not None:
            result["date"] = result["date"].where(result["date"].notna(), parse_result.document_date)

        result["document"] = result["document"].where(
            result["document"].notna(),
            parse_result.document_name,
        )
        result["account"] = result["account"].where(
            result["account"].notna(),
            parse_result.account,
        )
        return result

    def _filter_valid_rows(
        self,
        dataframe: pd.DataFrame,
        parse_result: ExcelParseResult,
    ) -> pd.DataFrame:
        """Оставляет только строки ФБС с непустым sticker."""
        mask = dataframe["sticker"].map(self._has_text_value)
        filtered = dataframe[mask].copy().reset_index(drop=True)

        logger.info(
            "Фильтрация строк ФБС по sticker: account=%s document=%s before=%s after=%s",
            parse_result.account,
            parse_result.document_name,
            len(dataframe),
            len(filtered),
        )
        return filtered

    def _build_normalized_row(
        self,
        row: pd.Series,
        parse_result: ExcelParseResult,
        row_index: int,
    ) -> NormalizedFbsRow | None:
        """Собирает одну нормализованную строку ФБС или пропускает её с warning."""
        document_number = self._pick_row_document_number(row, parse_result)
        document_date = self._pick_row_date(row, parse_result)
        order_number = self._to_clean_string(row.get("order_number"))
        sticker = self._to_clean_string(row.get("sticker"))

        if not order_number:
            logger.warning(
                "Пропускаю строку ФБС без order_number: account=%s document=%s row=%s",
                parse_result.account,
                parse_result.document_name,
                row_index,
            )
            return None
        if not sticker:
            logger.warning(
                "Пропускаю строку ФБС без sticker: account=%s document=%s row=%s",
                parse_result.account,
                parse_result.document_name,
                row_index,
            )
            return None
        if not document_number:
            logger.warning(
                "Пропускаю строку ФБС без document_number: account=%s document=%s row=%s",
                parse_result.account,
                parse_result.document_name,
                row_index,
            )
            return None
        if document_date is None:
            logger.warning(
                "Пропускаю строку ФБС без date: account=%s document=%s row=%s",
                parse_result.account,
                parse_result.document_name,
                row_index,
            )
            return None

        return NormalizedFbsRow(
            num=int(row.get("num", 0)),
            order_number=order_number,
            unit=self._to_clean_string(row.get("unit")),
            sticker=sticker,
            quantity=int(row.get("quantity", FBS_DEFAULT_QUANTITY)),
            document=self._to_clean_string(row.get("document")) or parse_result.document_name,
            document_number=document_number,
            date=document_date,
            account=self._to_clean_string(row.get("account")) or parse_result.account,
        )

    def _resolve_document_number(self, parse_result: ExcelParseResult) -> str | None:
        """Возвращает document_number для ФБС с приоритетом legacy-regex."""
        extracted = self._extract_document_number(parse_result.document_name)
        return extracted or parse_result.document_number

    def _extract_document_number(self, document_name: str) -> str | None:
        """Извлекает document_number для ФБС по legacy regex."""
        match = re.search(FBS_DOCUMENT_NUMBER_REGEX, document_name, flags=re.IGNORECASE)
        if match:
            return match.group(1)
        return None

    def _pick_row_document_number(
        self,
        row: pd.Series,
        parse_result: ExcelParseResult,
    ) -> str | None:
        """Выбирает document_number для конкретной строки ФБС."""
        row_value = self._to_clean_string(row.get("document_number"))
        if row_value:
            return row_value
        return self._resolve_document_number(parse_result)

    def _pick_row_date(
        self,
        row: pd.Series,
        parse_result: ExcelParseResult,
    ) -> date | None:
        """Выбирает date для конкретной строки ФБС."""
        row_date = row.get("date")
        if isinstance(row_date, datetime):
            return row_date.date()
        if isinstance(row_date, date):
            return row_date
        if row_date is not None and not pd.isna(row_date):
            parsed = pd.to_datetime(row_date, dayfirst=True, errors="coerce")
            if not pd.isna(parsed):
                return parsed.date()
        return parse_result.document_date

    def _to_clean_string(self, value: object) -> str:
        """Безопасно приводит значение к строке."""
        if value is None or pd.isna(value):
            return ""
        return str(value).strip()

    def _has_text_value(self, value: object) -> bool:
        """Проверяет непустоту значения."""
        return bool(self._to_clean_string(value))

    def _is_total_marker(self, value: object) -> bool:
        """Проверяет, является ли значение служебной строкой `Итого`."""
        return self._to_clean_string(value).casefold() == TOTAL_ROW_MARKER.casefold()
