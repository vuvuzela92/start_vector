from __future__ import annotations

from datetime import date

import pandas as pd

from src_oop.jobs.sales_plan.repository import (
    SalesPlanAccountingCategoryRepository,
    SalesPlanManagerReferenceRepository,
    SalesWildStatusDailyRepository,
)


def test_prepare_snapshot_dataframe_normalizes_and_deduplicates_rows() -> None:
    """Проверяет, что snapshot-справочник очищает ключи и сворачивает дубли предмет + менеджер.

    Бизнес-сценарий:
    дневной справочник менеджеров должен сохранять только уникальные пары
    `предмет -> менеджер` на дату снимка, даже если в источнике одна и та же
    связка встречается много раз на разных товарах.
    """

    source_dataframe = pd.DataFrame(
        {
            "Предмет": ["Весы ", "Весы", " ", "Вафельницы"],
            "Менеджер": [" Мадина Хидирова", "Мадина Хидирова", "Нет менеджера", "Нет менеджера"],
            "Артикул": ["222870754.0", "222870755", "", "222870163"],
        }
    )

    repository = SalesPlanManagerReferenceRepository.__new__(SalesPlanManagerReferenceRepository)

    prepared_dataframe, duplicate_rows = repository._prepare_snapshot_dataframe(
        dataframe=source_dataframe,
        snapshot_date=date(2026, 8, 28),
    )

    assert duplicate_rows == 1
    assert prepared_dataframe["manager_name"].tolist() == [
        "Мадина Хидирова",
        "Нет менеджера",
    ]
    assert prepared_dataframe["subject_name"].tolist() == ["Весы", "Вафельницы"]


def test_validate_required_columns_raises_for_changed_sheet_header() -> None:
    """Проверяет явную остановку загрузки при изменении бизнес-шапки листа.

    Бизнес-сценарий:
    если в ПУ переименуют ключевую колонку справочника, задача должна
    завершаться с понятной ошибкой, а не записывать неполный снимок в БД.
    """

    source_dataframe = pd.DataFrame({"Предмет": ["Весы"], "Менеджер": ["Мадина Хидирова"]})

    repository = SalesPlanManagerReferenceRepository.__new__(SalesPlanManagerReferenceRepository)

    try:
        repository._validate_required_columns(source_dataframe)
    except ValueError as error:
        assert "обязательные колонки" in str(error)
    else:
        raise AssertionError("Ожидалась ошибка при отсутствии обязательных колонок.")


def test_prepare_accounting_category_dataframe_raises_on_wild_conflict() -> None:
    """Проверяет остановку загрузки, если один `wild` попал в разные предметы.

    Бизнес-сценарий:
    учетная категория нужна именно для того, чтобы один и тот же `wild`
    присутствовал в плане только один раз. Конфликтный источник должен
    подсвечиваться ошибкой, а не записываться молча.
    """

    source_dataframe = pd.DataFrame(
        {
            "wild": ["wild100", "wild100", "wild200"],
            "предмет": ["Весы", "Блендеры", "Вафельницы"],
            "3 квартал, шт 2026": ["300", "300", ""],
            "цена продажная плановая": ["1000", "1000", ""],
        }
    )

    repository = SalesPlanAccountingCategoryRepository.__new__(SalesPlanAccountingCategoryRepository)

    try:
        repository._prepare_reference_dataframe(source_dataframe)
    except ValueError as error:
        assert "несколькими предметами" in str(error)
    else:
        raise AssertionError("Ожидалась ошибка при конфликте предметов у одного wild.")


def test_build_accounting_category_payload_preserves_created_at() -> None:
    """Проверяет сохранение `created_at` при ежедневном обновлении справочника.

    Бизнес-сценарий:
    справочник учетной категории не копит историю по дням, но должен помнить
    дату первого появления `wild`, даже если предмет позже будет уточняться.
    """

    prepared_dataframe = pd.DataFrame(
        {
            "wild": ["wild100", "wild200"],
            "subject_name": ["Весы", "Блендеры"],
            "quarter_3_units_2026": [100.0, None],
            "plan_price": [1234.56, None],
        }
    )
    existing_reference = pd.DataFrame(
        {
            "wild": ["wild100"],
            "subject_name": ["Весы old"],
            "created_at": [pd.Timestamp("2026-08-01 09:00:00")],
        }
    )

    repository = SalesPlanAccountingCategoryRepository.__new__(SalesPlanAccountingCategoryRepository)
    payload_dataframe, inserted_rows, updated_rows = repository._build_database_payload(
        prepared_dataframe=prepared_dataframe,
        existing_reference=existing_reference,
    )

    assert inserted_rows == 1
    assert updated_rows == 1
    assert payload_dataframe.loc[payload_dataframe["wild"] == "wild100", "created_at"].iloc[0] == pd.Timestamp(
        "2026-08-01 09:00:00"
    )
    assert payload_dataframe.loc[
        payload_dataframe["wild"] == "wild100", "quarter_3_units_2026"
    ].iloc[0] == 100.0
    assert payload_dataframe.loc[
        payload_dataframe["wild"] == "wild100", "plan_price"
    ].iloc[0] == 1234.56
    assert pd.isna(
        payload_dataframe.loc[
            payload_dataframe["wild"] == "wild200", "quarter_3_units_2026"
        ].iloc[0]
    )
    assert pd.isna(
        payload_dataframe.loc[
            payload_dataframe["wild"] == "wild200", "plan_price"
        ].iloc[0]
    )
    assert pd.notna(
        payload_dataframe.loc[payload_dataframe["wild"] == "wild200", "created_at"].iloc[0]
    )


def test_prepare_accounting_category_dataframe_divides_3q_units_by_three() -> None:
    """Проверяет расчет среднемесячного значения из поля `3 квартал, шт 2026`.

    Бизнес-сценарий:
    справочник учетной категории должен хранить для 3 квартала 2026 значение
    на месяц, а не весь квартал. Поэтому непустой квартальный план делится на
    3, округляется до 2 знаков после запятой, а пустое значение остается
    `NULL`.
    """

    source_dataframe = pd.DataFrame(
        {
            "wild": ["wild100", "wild200"],
            "предмет": ["Весы", "Блендеры"],
            "3 квартал, шт 2026": ["20000", ""],
            "цена продажная плановая": ["6512052", ""],
        }
    )

    repository = SalesPlanAccountingCategoryRepository.__new__(SalesPlanAccountingCategoryRepository)
    prepared_dataframe = repository._prepare_reference_dataframe(source_dataframe)

    assert prepared_dataframe.loc[
        prepared_dataframe["wild"] == "wild100", "quarter_3_units_2026"
    ].iloc[0] == 6666.67
    assert prepared_dataframe.loc[
        prepared_dataframe["wild"] == "wild100", "plan_price"
    ].iloc[0] == 6512052.00
    assert pd.isna(
        prepared_dataframe.loc[
            prepared_dataframe["wild"] == "wild200", "quarter_3_units_2026"
        ].iloc[0]
    )
    assert pd.isna(
        prepared_dataframe.loc[
            prepared_dataframe["wild"] == "wild200", "plan_price"
        ].iloc[0]
    )


def test_prepare_sales_wild_status_daily_snapshot_maps_statuses() -> None:
    """Проверяет подготовку дневного snapshot-а статусов `wild` для плана продаж.

    Бизнес-сценарий:
    правило обнуления плана будет считать дни, когда товар был активен в
    продаже. Поэтому snapshot должен оставлять один `wild` на дату и
    преобразовывать статус `активно` в `true`, а остальные статусы в `false`.
    """

    source_dataframe = pd.DataFrame(
        {
            "wild": ["wild100", "wild100", "wild200", "wild300", ""],
            "Статус вилд": ["новинка", "активно", "закрыто", "вывод", "активно"],
        }
    )

    repository = SalesWildStatusDailyRepository.__new__(SalesWildStatusDailyRepository)
    prepared_dataframe, duplicate_rows = repository._prepare_snapshot_dataframe(
        dataframe=source_dataframe,
        snapshot_date=date(2026, 8, 28),
    )

    assert duplicate_rows == 1
    assert prepared_dataframe["wild"].tolist() == ["wild100", "wild200", "wild300"]
    assert prepared_dataframe["is_active"].tolist() == [True, False, False]
    assert prepared_dataframe["date"].tolist() == [
        date(2026, 8, 28),
        date(2026, 8, 28),
        date(2026, 8, 28),
    ]
