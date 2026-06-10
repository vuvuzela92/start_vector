"""Валидаторы для результатов Excel parsing актов WB."""

from __future__ import annotations

import logging

import pandas as pd

from src_oop.jobs.wb_api.acceptance_acts.config import (
    ACT_TYPE_FBO,
    ACT_TYPE_FBS,
    FBO_REQUIRED_FIELDS,
    FBS_REQUIRED_FIELDS,
    ParseStatus,
)
from src_oop.jobs.wb_api.acceptance_acts.models import (
    ExcelParseResult,
    ValidationResult,
)

logger = logging.getLogger(__name__)


class AcceptanceActsValidator:
    """Проверяет, можно ли передавать результат parsing дальше в normalizer.

    Слой отвечает только за минимальные требования к распознанной таблице:
    наличие dataframe, базовых колонок, непустых ключевых значений и
    классификацию в `success` / `partial` / `failed`.
    """

    def validate_parse_result(self, parse_result: ExcelParseResult) -> ValidationResult:
        """Валидирует общий результат парсинга Excel-файла.

        `success` означает, что найден минимально пригодный набор данных для
        дальнейшей нормализации. `partial` означает, что таблица распознана,
        но есть предупреждения. `failed` означает, что продолжать обработку
        небезопасно или бессмысленно.
        """
        errors = list(parse_result.errors)
        warnings = list(parse_result.warnings)
        missing_columns: list[str] = []

        if parse_result.status == "failed":
            if not errors:
                errors.append("Parse result уже помечен как failed.")
            return self._build_validation_result(
                status="failed",
                errors=errors,
                warnings=warnings,
                missing_columns=missing_columns,
            )

        dataframe = parse_result.canonical_dataframe
        if dataframe is None:
            errors.append("Отсутствует canonical_dataframe.")
            return self._build_validation_result(
                status="failed",
                errors=errors,
                warnings=warnings,
                missing_columns=missing_columns,
            )

        if dataframe.empty:
            errors.append("canonical_dataframe пустой.")
            return self._build_validation_result(
                status="failed",
                errors=errors,
                warnings=warnings,
                missing_columns=missing_columns,
            )

        validation_act_type = parse_result.actual_act_type or parse_result.expected_act_type

        if parse_result.actual_act_type is None:
            warnings.append("actual_act_type не определён, используется expected_act_type для базовой проверки.")
        elif parse_result.actual_act_type != parse_result.expected_act_type:
            warnings.append(
                "actual_act_type не совпадает с expected_act_type: "
                f"{parse_result.expected_act_type} != {parse_result.actual_act_type}."
            )

        missing_columns = self.validate_required_columns(
            dataframe=dataframe,
            act_type=validation_act_type,
        )
        if missing_columns:
            errors.append(
                "Отсутствуют обязательные колонки: " + ", ".join(missing_columns)
            )

        missing_value_fields = self.validate_required_values(
            dataframe=dataframe,
            act_type=validation_act_type,
        )
        if missing_value_fields:
            errors.append(
                "Нет пригодных значений в обязательных полях: "
                + ", ".join(missing_value_fields)
            )

        if parse_result.document_number is None:
            warnings.append("document_number отсутствует.")
        if parse_result.document_date is None:
            warnings.append("date отсутствует.")

        status = self.classify_result(
            parse_result=parse_result,
            errors=errors,
            warnings=warnings,
            missing_columns=missing_columns,
        )

        logger.info(
            "Валидация Excel parse result завершена: document=%s account=%s expected=%s actual=%s "
            "parse_status=%s validation_status=%s missing_columns=%s warnings=%s errors=%s",
            parse_result.document_name,
            parse_result.account,
            parse_result.expected_act_type,
            parse_result.actual_act_type,
            parse_result.status,
            status,
            missing_columns,
            len(warnings),
            len(errors),
        )

        return self._build_validation_result(
            status=status,
            errors=errors,
            warnings=warnings,
            missing_columns=missing_columns,
        )

    def validate_required_columns(
        self,
        dataframe: pd.DataFrame,
        act_type: str,
    ) -> list[str]:
        """Возвращает список обязательных колонок, которых нет в dataframe.

        Метод не мутирует dataframe и не пытается исправлять структуру. Он
        только диагностирует отсутствие минимального набора полей из config.
        """
        required_columns = self._get_required_fields(act_type)
        dataframe_columns = set(dataframe.columns.tolist())
        return [
            column_name
            for column_name in required_columns
            if column_name not in dataframe_columns
        ]

    def validate_required_values(
        self,
        dataframe: pd.DataFrame,
        act_type: str,
    ) -> list[str]:
        """Проверяет наличие непустых значений по минимально важным полям.

        Для каждого обязательного поля метод проверяет, что в колонке есть
        хотя бы одно пригодное значение. Строки не фильтруются и не удаляются.
        """
        missing_value_fields: list[str] = []

        for column_name in self._get_required_fields(act_type):
            if column_name not in dataframe.columns:
                missing_value_fields.append(column_name)
                continue

            if not self._has_any_non_empty_value(dataframe[column_name]):
                missing_value_fields.append(column_name)

        return missing_value_fields

    def classify_result(
        self,
        parse_result: ExcelParseResult,
        errors: list[str],
        warnings: list[str],
        missing_columns: list[str],
    ) -> ParseStatus:
        """Классифицирует итог в `success`, `partial` или `failed`.

        `failed` возвращается при фатальных проблемах структуры или данных.
        `partial` возвращается, когда файл ещё диагностически полезен, но есть
        warnings или mismatch типов. `success` означает отсутствие критики.
        """
        dataframe = parse_result.canonical_dataframe

        if parse_result.status == "failed":
            return "failed"
        if dataframe is None or dataframe.empty:
            return "failed"

        if errors:
            if missing_columns and len(missing_columns) < len(self._get_required_fields(
                parse_result.actual_act_type or parse_result.expected_act_type
            )):
                return "partial"
            return "failed"

        if warnings:
            return "partial"
        return "success"

    def _get_required_fields(self, act_type: str) -> tuple[str, ...]:
        """Возвращает минимально обязательные поля для указанного типа акта."""
        if act_type == ACT_TYPE_FBO:
            return FBO_REQUIRED_FIELDS
        if act_type == ACT_TYPE_FBS:
            return FBS_REQUIRED_FIELDS
        return tuple()

    def _has_any_non_empty_value(self, series: pd.Series) -> bool:
        """Проверяет, что в колонке есть хотя бы одно непустое значение."""
        normalized = series.map(self._normalize_value_for_presence_check)
        return normalized.notna().any()

    def _normalize_value_for_presence_check(self, value: object) -> object | None:
        """Нормализует значение для проверки непустоты без бизнес-обработки."""
        if value is None:
            return None
        if pd.isna(value):
            return None
        if isinstance(value, str):
            stripped = value.strip()
            return stripped or None
        return value

    def _build_validation_result(
        self,
        status: ParseStatus,
        errors: list[str],
        warnings: list[str],
        missing_columns: list[str],
    ) -> ValidationResult:
        """Собирает единый объект результата валидации."""
        return ValidationResult(
            status=status,
            is_valid=status == "success",
            errors=self._deduplicate_messages(errors),
            warnings=self._deduplicate_messages(warnings),
            missing_columns=sorted(set(missing_columns)),
        )

    def _deduplicate_messages(self, messages: list[str]) -> list[str]:
        """Убирает повторы, сохраняя исходный порядок сообщений."""
        seen: set[str] = set()
        result: list[str] = []
        for message in messages:
            if message in seen:
                continue
            seen.add(message)
            result.append(message)
        return result
