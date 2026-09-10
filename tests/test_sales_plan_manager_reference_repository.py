from __future__ import annotations

from datetime import date

import pandas as pd

from src_oop.jobs.sales_plan.repository import (
    SalesPlanAccountingCategoryRepository,
    SalesPlanManagerReferenceRepository,
    SalesPlanReportRepository,
    SalesWildStatusDailyRepository,
)


def test_prepare_sales_plan_report_dataframe_clears_null_markers() -> None:
    """Проверяет очистку пропусков перед публикацией витрины в Google Sheets.

    Бизнес-сценарий:
    в готовом плане продаж отсутствие цены или процента не должно выглядеть
    как текст `[NULL]`. Такие значения выгружаются как визуально пустые
    ячейки, а числовые показатели сохраняются без изменения.
    """

    source_dataframe = pd.DataFrame(
        {
            "plan_price": [None, "[NULL]", " [NULL] ", 1200],
            "fact_orders_rub": [pd.NA, 10, 20, 30],
        }
    )

    prepared_dataframe = SalesPlanReportRepository._prepare_dataframe_for_sheet(
        source_dataframe
    )

    assert prepared_dataframe.to_dict(orient="list") == {
        "plan_price": ["", "", "", 1200],
        "fact_orders_rub": ["", 10, 20, 30],
    }


def test_add_updated_at_column_sets_one_timestamp_for_all_report_rows() -> None:
    """Проверяет добавление времени публикации к каждой строке витрины.

    Бизнес-сценарий:
    поле `updatet_at` должно показывать момент актуализации отчета. Все строки
    одной выгрузки получают одинаковое значение, чтобы их можно было считать
    единым снимком данных.
    """

    source_dataframe = pd.DataFrame({"wild": ["wild100", "wild200"]})

    prepared_dataframe = SalesPlanReportRepository._add_updated_at_column(
        source_dataframe
    )

    assert "updatet_at" in prepared_dataframe.columns
    assert prepared_dataframe["updatet_at"].nunique() == 1
    assert pd.notna(prepared_dataframe["updatet_at"]).all()
    assert isinstance(prepared_dataframe.loc[0, "updatet_at"], str)


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


def test_prepare_snapshot_dataframe_keeps_first_manager_for_duplicate_subject() -> None:
    """Проверяет сохранение первого менеджера при повторении предмета в источнике.

    Бизнес-сценарий:
    в дневном справочнике один предмет может иметь только одного менеджера.
    Если лист временно содержит два назначения, для стабильной загрузки в БД
    сохраняется первое назначение в порядке строк Google Sheets.
    """

    source_dataframe = pd.DataFrame(
        {
            "Предмет": ["Дозаторы для ванной", "Дозаторы для ванной"],
            "Менеджер": ["Анастасия Гусакова", "Мадина Хидирова"],
            "Артикул": ["wild100", "wild200"],
        }
    )

    repository = SalesPlanManagerReferenceRepository.__new__(SalesPlanManagerReferenceRepository)
    prepared_dataframe, duplicate_rows = repository._prepare_snapshot_dataframe(
        dataframe=source_dataframe,
        snapshot_date=date(2026, 9, 8),
    )

    assert duplicate_rows == 1
    assert prepared_dataframe[["subject_name", "manager_name"]].to_dict(orient="records") == [
        {
            "subject_name": "Дозаторы для ванной",
            "manager_name": "Анастасия Гусакова",
        }
    ]


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


def test_prepare_accounting_category_dataframe_maps_missing_price_marker_to_null() -> None:
    """Проверяет преобразование служебной метки отсутствующей цены в `NULL`.

    Бизнес-сценарий:
    значение `нет цены` не является числом и не должно останавливать дневную
    синхронизацию справочника. Оно означает отсутствие плановой цены, поэтому
    в БД сохраняется как `NULL` без подстановки фиктивного нуля.
    """

    source_dataframe = pd.DataFrame(
        {
            "wild": ["wild100"],
            "предмет": ["Весы"],
            "3 квартал, шт 2026": ["300"],
            "цена продажная плановая": ["нет цены"],
        }
    )

    repository = SalesPlanAccountingCategoryRepository.__new__(SalesPlanAccountingCategoryRepository)
    prepared_dataframe = repository._prepare_reference_dataframe(source_dataframe)

    assert pd.isna(prepared_dataframe.loc[0, "plan_price"])


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


def test_build_backfill_dataframe_keeps_only_confirmed_days_for_changed_wild() -> None:
    """Проверяет безопасное восстановление статусов в пропущенный период.

    Бизнес-сценарий:
    стабильный статус между 1 и 8 сентября можно перенести на все пропущенные
    дни. Если статус изменился, задача добавляет только дни с заказами и не
    заменяет неизвестную историю значением `false` или текущим статусом.
    """

    snapshots_dataframe = pd.DataFrame(
        {
            "date": [
                date(2026, 9, 1),
                date(2026, 9, 1),
                date(2026, 9, 8),
                date(2026, 9, 8),
            ],
            "wild": ["wild100", "wild200", "wild100", "wild200"],
            "is_active": [True, False, True, True],
        }
    )
    funnel_dataframe = pd.DataFrame(
        {
            "date": [date(2026, 9, 2), date(2026, 9, 4)],
            "wild": ["wild200", "wild200"],
        }
    )
    existing_dataframe = pd.DataFrame(columns=["date", "wild"])

    repository = SalesWildStatusDailyRepository.__new__(SalesWildStatusDailyRepository)
    (
        payload_dataframe,
        stable_wilds,
        changed_status_wilds,
        confirmed_order_days,
        skipped_existing_rows,
    ) = repository._build_backfill_dataframe(
        snapshots_dataframe=snapshots_dataframe,
        funnel_dataframe=funnel_dataframe,
        existing_dataframe=existing_dataframe,
    )

    assert stable_wilds == 1
    assert changed_status_wilds == 1
    assert confirmed_order_days == 2
    assert skipped_existing_rows == 0
    assert len(payload_dataframe.index) == 8
    assert payload_dataframe.loc[
        payload_dataframe["wild"] == "wild100", "is_active"
    ].tolist() == [True] * 6
    assert payload_dataframe.loc[
        payload_dataframe["wild"] == "wild200", "date"
    ].tolist() == [date(2026, 9, 2), date(2026, 9, 4)]
    assert payload_dataframe.loc[
        payload_dataframe["wild"] == "wild200", "is_active"
    ].tolist() == [True, True]
