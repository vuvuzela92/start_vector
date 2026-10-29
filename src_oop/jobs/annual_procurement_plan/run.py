# Импорт внутренних модулей
import logging

from src_oop.jobs.annual_procurement_plan.annual_procurement_plan import AnnualProcurementPlan
from src_oop.core.utils_general import clean_currency_value
# Импорт внешних библиотек
import pandas as pd

logger = logging.getLogger(__name__)

def _select_orders_columns(df_source: pd.DataFrame, required_columns: list[str]) -> pd.DataFrame:
    """Возвращает набор колонок заказа для выгрузки в годовой план закупа.

    Вспомогательная функция защищает основной сценарий переноса заказов в
    вкладку ``БД_ЗАКАЗЫ``: если одна из ожидаемых колонок временно отсутствует
    в листе-источнике, она создается пустой, чтобы обновление не прерывалось и
    структура выгрузки оставалась стабильной.
    """
    df_result = df_source.copy()
    missing_columns = [column for column in required_columns if column not in df_result.columns]

    for column in missing_columns:
        df_result[column] = ""

    return df_result[required_columns]


def _append_seller_price(
    df_orders: pd.DataFrame,
    df_seller_price: pd.DataFrame,
) -> pd.DataFrame:
    """Добавляет в выгрузку заказов самую свежую цену WB по артикулу продавца.

    Вспомогательная функция защищает сценарий подготовки вкладки ``БД_ЗАКАЗЫ``.
    Она нормализует ключи `wild` и `local_vendor_code`, чтобы корректно
    соединить табличные заказы с последней доступной ценой WB, не меняя состав самих
    строк заказа и не прерывая выгрузку, если по части артикулов цена в БД
    отсутствует.
    """
    df_result = df_orders.copy()

    if df_seller_price.empty:
        df_result["Цена WB"] = ""
        return df_result

    prepared_orders = df_result.copy()
    prepared_prices = df_seller_price.copy()

    prepared_orders["_merge_wild"] = (
        prepared_orders["wild"].fillna("").astype(str).str.strip()
    )
    prepared_prices["_merge_wild"] = (
        prepared_prices["local_vendor_code"].fillna("").astype(str).str.strip()
    )

    prepared_prices = prepared_prices[["_merge_wild", "seller_price"]].drop_duplicates(
        subset=["_merge_wild"],
        keep="last",
    )

    merged = prepared_orders.merge(
        prepared_prices,
        on="_merge_wild",
        how="left",
    )
    merged["Цена WB"] = merged["seller_price"].fillna("")
    return merged.drop(columns=["_merge_wild", "seller_price"])


def _append_purchase_price(
    df_orders: pd.DataFrame,
    df_purchase_price: pd.DataFrame,
) -> pd.DataFrame:
    """Добавляет в выгрузку заказов актуальную закупочную стоимость.

    Вспомогательная функция обслуживает тот же бизнес-сценарий, что и
    ``purchase_price_update``: подмешивает к строкам `БД_ЗАКАЗЫ` итоговый
    `price_per_item`, рассчитанный по последней валидной поставке для каждого
    `local_vendor_code`. Соединение выполняется по нормализованному ключу
    `wild`, чтобы различия в пробелах не ломали финальную выгрузку.
    """
    df_result = df_orders.copy()

    if df_purchase_price.empty:
        df_result["Стоимость в закупке (руб.)"] = ""
        return df_result

    prepared_orders = df_result.copy()
    prepared_prices = df_purchase_price.copy()

    prepared_orders["_merge_wild"] = (
        prepared_orders["wild"].fillna("").astype(str).str.strip()
    )
    prepared_prices["_merge_wild"] = (
        prepared_prices["local_vendor_code"].fillna("").astype(str).str.strip()
    )

    prepared_prices = prepared_prices[["_merge_wild", "price_per_item"]].drop_duplicates(
        subset=["_merge_wild"],
        keep="last",
    )

    merged = prepared_orders.merge(
        prepared_prices,
        on="_merge_wild",
        how="left",
    )
    merged["Стоимость в закупке (руб.)"] = merged["price_per_item"].fillna("")
    return merged.drop(columns=["_merge_wild", "price_per_item"])


def _append_quarterly_seller_price(
    df_quarterly: pd.DataFrame,
    df_seller_price: pd.DataFrame,
    target_column_name: str,
) -> pd.DataFrame:
    """Подмешивает самую свежую цену WB в квартальный план по ключу `wild`.

    Вспомогательная функция обслуживает отдельный сценарий обновления листа
    ``Поквартально``. Значение записывается только в целевую колонку
    `цена продажная плановая`. Если по товару в БД нет цены, ранее введенное
    вручную значение в ячейке сохраняется без изменений.
    """
    df_result = df_quarterly.copy()

    if df_seller_price.empty:
        if target_column_name not in df_result.columns:
            df_result[target_column_name] = ""
        return df_result

    prepared_quarterly = df_result.copy()
    prepared_prices = df_seller_price.copy()
    # PostgreSQL NUMERIC приходит как Decimal/object; перед записью в Sheets
    # приводим только новые цены к обычному числовому типу.
    prepared_prices["seller_price"] = pd.to_numeric(
        prepared_prices["seller_price"],
        errors="coerce",
    )

    prepared_quarterly["_merge_wild"] = (
        prepared_quarterly["wild"].fillna("").astype(str).str.strip()
    )
    prepared_prices["_merge_wild"] = (
        prepared_prices["local_vendor_code"].fillna("").astype(str).str.strip()
    )
    prepared_prices = prepared_prices[["_merge_wild", "seller_price"]].drop_duplicates(
        subset=["_merge_wild"],
        keep="last",
    )

    merged = prepared_quarterly.merge(
        prepared_prices,
        on="_merge_wild",
        how="left",
    )
    # Не очищаем ручные значения квартального плана, когда цена WB не найдена.
    merged[target_column_name] = merged["seller_price"].where(
        merged["seller_price"].notna(),
        merged[target_column_name],
    )
    return merged.drop(columns=["_merge_wild", "seller_price"])


def _append_quarterly_white_plan_cost(
    df_quarterly: pd.DataFrame,
    df_white_orders: pd.DataFrame,
    source_column_name: str,
    target_column_name: str,
) -> pd.DataFrame:
    """Подмешивает плановую себестоимость из листа белых заказов в квартальный план.

    Вспомогательная функция защищает бизнес-сценарий, где закупщик хочет видеть
    в листе ``Поквартально`` ориентир по себестоимости для каждого `wild`.
    Если один и тот же `wild` встречается в белых заказах несколько раз,
    используется последнее положительное значение из текущего снимка листа,
    кроме строк со статусом ``в планах``.
    Перед записью себестоимость очищается от валютных префиксов и лишних
    символов, чтобы Google Sheets воспринимал результат как число, а не как
    текст вроде ``р.428,10``. При отсутствии данных в источнике ранее
    заполненная вручную ячейка квартального плана не изменяется.
    """
    df_result = df_quarterly.copy()

    status_column_name = "Статус"
    required_columns = {"wild", source_column_name, status_column_name}
    if (
        df_white_orders.empty
        or not required_columns.issubset(df_white_orders.columns)
    ):
        if target_column_name not in df_result.columns:
            df_result[target_column_name] = ""
        return df_result

    prepared_quarterly = df_result.copy()
    prepared_white_orders = df_white_orders[
        ["wild", source_column_name, status_column_name]
    ].copy()

    prepared_quarterly["_merge_wild"] = (
        prepared_quarterly["wild"].fillna("").astype(str).str.strip()
    )
    prepared_white_orders["_merge_wild"] = (
        prepared_white_orders["wild"].fillna("").astype(str).str.strip()
    )
    prepared_white_orders["_clean_cost_value"] = (
        prepared_white_orders[source_column_name].apply(clean_currency_value)
    )
    prepared_white_orders["_normalized_status"] = (
        prepared_white_orders[status_column_name]
        .fillna("")
        .astype(str)
        .str.strip()
        .str.casefold()
    )
    prepared_white_orders = prepared_white_orders.loc[
        (prepared_white_orders["_merge_wild"] != "")
        & (prepared_white_orders["_clean_cost_value"] > 0)
        & (prepared_white_orders["_normalized_status"] != "в планах")
    ]
    prepared_white_orders = prepared_white_orders[
        ["_merge_wild", "_clean_cost_value"]
    ].drop_duplicates(
        subset=["_merge_wild"],
        keep="last",
    )

    merged = prepared_quarterly.merge(
        prepared_white_orders,
        on="_merge_wild",
        how="left",
    )
    # Не очищаем ручные значения квартального плана, когда себестоимость не найдена.
    merged[target_column_name] = merged["_clean_cost_value"].where(
        merged["_clean_cost_value"].notna(),
        merged[target_column_name],
    )
    return merged.drop(columns=["_merge_wild", "_clean_cost_value"])


def _append_quarterly_launch_date(
    df_quarterly: pd.DataFrame,
    df_new_items: pd.DataFrame,
    target_column_name: str,
) -> pd.DataFrame:
    """Подмешивает дату запуска товара в квартальный план по ключу `wild`.

    Вспомогательная функция обслуживает перенос из вкладки `НОВИНКИ`.
    Используется последнее непустое значение `Запуск` для каждого `WILD`;
    если дата запуска не найдена, существующее значение в `Поквартально`
    сохраняется без изменений.
    """
    df_result = df_quarterly.copy()
    if df_new_items.empty:
        return df_result

    prepared_quarterly = df_result.copy()
    source_columns = {
        str(column).replace("\u00a0", " ").strip().casefold(): column
        for column in df_new_items.columns
    }
    wild_column = source_columns.get("wild")
    launch_column = source_columns.get("запуск")
    if wild_column is None or launch_column is None:
        logger.warning(
            "Не найдены колонки WILD и Запуск во вкладке НОВИНКИ; "
            "даты запуска не обновлены, существующие значения сохранены"
        )
        return df_result

    prepared_new_items = df_new_items[[wild_column, launch_column]].copy()
    prepared_new_items.columns = ["WILD", "Запуск"]
    prepared_quarterly["_merge_wild"] = (
        prepared_quarterly["wild"]
        .fillna("")
        .astype(str)
        .str.replace("\u00a0", " ", regex=False)
        .str.strip()
        .str.casefold()
    )
    prepared_new_items["_merge_wild"] = (
        prepared_new_items["WILD"]
        .fillna("")
        .astype(str)
        .str.replace("\u00a0", " ", regex=False)
        .str.strip()
        .str.casefold()
    )
    prepared_new_items["_launch_date"] = (
        prepared_new_items["Запуск"].fillna("").astype(str).str.strip()
    )
    prepared_new_items = prepared_new_items.loc[
        (prepared_new_items["_merge_wild"] != "")
        & (prepared_new_items["_launch_date"] != "")
    ]
    prepared_new_items = prepared_new_items[
        ["_merge_wild", "_launch_date"]
    ].drop_duplicates(subset=["_merge_wild"], keep="last")

    merged = prepared_quarterly.merge(
        prepared_new_items,
        on="_merge_wild",
        how="left",
    )
    if target_column_name not in merged.columns:
        merged[target_column_name] = ""
    merged[target_column_name] = merged["_launch_date"].where(
        merged["_launch_date"].notna(),
        merged[target_column_name],
    )
    logger.info(
        "Для квартального плана сопоставлено дат запуска: %s",
        int(merged["_launch_date"].notna().sum()),
    )
    return merged.drop(columns=["_merge_wild", "_launch_date"])


def transport_data_to_annual_procurement_plan():
    """Переносит заказы из рабочих листов в вкладку ``БД_ЗАКАЗЫ`` годового плана.

    Сценарий объединяет данные из листов ``Заказы белые ТЕСТ`` и ``Заказы``,
    сохраняет только бизнес-значимые колонки заказа, очищает сумму заказа,
    исключает отмененные и завершенные статусы и формирует итоговую выгрузку
    для дальнейшей работы закупки и аналитики. Поле ``Поставщик`` также
    включается в итоговую структуру выгрузки.
    """
    # Создаем экземпляр класса и получаем данные
    plan = AnnualProcurementPlan()
    df_white_orders = plan.get_white_orders_data()
    # Выбираем нужные колонки
    choosen_orders_columns = plan.choosen_orders_columns
    df_white_orders_short = _select_orders_columns(df_white_orders, choosen_orders_columns)
    # Получаем датафрейм от листа Заказы в таблице Расчет поставки Китай_по обороту
    df_orders = plan.get_orders_data()
    df_orders_short = _select_orders_columns(df_orders, choosen_orders_columns)
    # Объединяем датафреймы вертикально
    df_merge = pd.concat([
        df_white_orders_short.reset_index(drop=True), 
        df_orders_short.reset_index(drop=True)
    ], ignore_index=True)
    # Убираем знаки валюты из колонки
    df_merge['Сумма заказа, RMB'] = df_merge['Сумма заказа, RMB'].apply(clean_currency_value)
    # Выбираем статусы для фильтрации
    cancel_statuses = plan.cancel_statuses
    # Фильтрация
    df_merge = df_merge.loc[~df_merge['Статус'].isin(cancel_statuses)]
    # Используем последнюю доступную цену WB по артикулу без ограничения по дате.
    df_seller_price = plan.get_seller_price_data()
    df_merge = _append_seller_price(df_merge, df_seller_price)
    # Дополняем итоговую выгрузку актуальной закупочной стоимостью.
    df_purchase_price = plan.get_purchase_price_data()
    df_merge = _append_purchase_price(df_merge, df_purchase_price)
    df_merge = _select_orders_columns(df_merge, choosen_orders_columns)
    # Обновляем данные в таблице Годовой план закупа 2026
    plan.set_data(plan.google_connect_to, df_merge)


def transport_unit_data_to_annual_procurement_plan():
    """Обновляет юнит-данные и даты запуска товаров в годовом плане закупа.

    Сценарий сохраняет агрегированный остаток ФБО во вкладке
    ``Данные_Юнитки``, затем читает `WILD` и `Запуск` из вкладки ``НОВИНКИ``
    панели продаж и точечно обновляет `Дата запуска в продажу` во вкладке
    ``Поквартально``. При отсутствии даты исходное значение в квартальном
    плане сохраняется.
    """
    # Создаем экземпляр класса и получаем данные
    plan = AnnualProcurementPlan()
    df_unit = plan.get_unit_data()
    df_unit_short = df_unit[plan.unit_cols]
    # Используем str.replace с регулярным выражением
    df_unit_short['ФБО'] = (
        df_unit_short['ФБО']
        .astype(str)
        .str.replace(r'[\s\xa0]+', '', regex=True)
    )
    # errors='coerce' превратит некорректные строки в NaN
    df_unit_short['ФБО'] = pd.to_numeric(df_unit_short['ФБО'], errors='coerce')
    
    # Заполняем пустоты нулями и приводим к int
    df_unit_short['ФБО'] = df_unit_short['ФБО'].fillna(0).astype(int)
    df_unit_short = df_unit_short.groupby('wild').agg({  
        'ФБО': 'sum'
    }).reset_index()
    # Обновляем данные в таблице Годовой план закупа 2026
    plan.set_data(plan.annual_plan_connect_to_unit_sheet, df_unit_short)

    df_quarterly = plan.get_quarterly_plan_data()
    df_new_items = plan.get_new_items_data()
    df_quarterly = _append_quarterly_launch_date(
        df_quarterly=df_quarterly,
        df_new_items=df_new_items,
        target_column_name="Дата запуска в продажу",
    )
    plan.annual_plan_connect_to_quarter_sheet.update_column_by_name_at_header_row(
        column_name="Дата запуска в продажу",
        data_to_write=df_quarterly["Дата запуска в продажу"].tolist(),
        header_row_number=4,
        data_start_row_number=5,
    )

def transport_supplies_data_to_annual_procurement_plan():
    # Создаем экземпляр класса и получаем данные
    plan = AnnualProcurementPlan()
    df_supplies = plan.get_supplies_data()
    # Обновляем данные в таблице Годовой план закупа 2026
    plan.set_data(plan.annual_plan_connect_to_supply_sheet, df_supplies)

def transport_parfume_data_to_annual_procurement_plan():
    # Создаем экземпляр класса и получаем данные
    plan = AnnualProcurementPlan()
    df_parfume = plan.get_parfume_data()
    # Обновляем данные в таблице Годовой план закупа 2026
    plan.set_data(plan.annual_plan_connect_to_parfume_sheet, df_parfume)


def update_quarterly_prices_to_annual_procurement_plan() -> None:
    """Обновляет цены и дни без остатков в годовом плане закупа.

    Сценарий читает квартальный план из таблицы ``Годовой план закупа 2026``,
    находит для каждой строки с `wild` самую свежую `Цена WB` из БД и
    плановую себестоимость из листа ``Заказы белые ТЕСТ`` по колонке
    ``Себестоимость 1 шт. в руб ПЛАН``, а затем точечно записывает значения в
    колонки ``цена продажная плановая`` и ``себестоимость 2025-2026``. Если по
    товару нет данных в источнике, существующее, в том числе ручное, значение
    в квартальном плане сохраняется без изменений. Затем тот же запуск
    выгружает агрегированные дни с остатком и без остатка во вкладку
    ``Дней с остатком`` и актуальную цену в колонку ``цена продажная плановая``
    листа ``Заказы белые ТЕСТ``. При отсутствии цены ручное значение в белых
    заказах также сохраняется.
    Пустой результат второй выгрузки не очищает уже заполненную вкладку.
    """
    plan = AnnualProcurementPlan()
    df_quarterly = plan.get_quarterly_plan_data()

    if df_quarterly.empty:
        logger.warning(
            "Обновление ценовых колонок листа Поквартально пропущено: в листе нет строк данных."
        )
        return

    df_seller_price = plan.get_seller_price_data()
    df_white_orders = plan.get_white_orders_data()

    df_quarterly = _append_quarterly_seller_price(
        df_quarterly=df_quarterly,
        df_seller_price=df_seller_price,
        target_column_name=plan.quarter_price_column,
    )
    df_quarterly = _append_quarterly_white_plan_cost(
        df_quarterly=df_quarterly,
        df_white_orders=df_white_orders,
        source_column_name=plan.white_plan_cost_column,
        target_column_name=plan.quarter_cost_column,
    )

    quarter_connector = plan.annual_plan_connect_to_quarter_sheet
    quarter_connector.update_column_by_name_at_header_row(
        column_name=plan.quarter_price_column,
        data_to_write=df_quarterly[plan.quarter_price_column].tolist(),
        header_row_number=4,
        data_start_row_number=5,
    )
    quarter_connector.update_column_by_name_at_header_row(
        column_name=plan.quarter_cost_column,
        data_to_write=df_quarterly[plan.quarter_cost_column].tolist(),
        header_row_number=4,
        data_start_row_number=5,
    )
    logger.info(
        "Ценовые колонки листа Поквартально обновлены | rows=%s | price_column=%s | cost_column=%s",
        len(df_quarterly.index),
        plan.quarter_price_column,
        plan.quarter_cost_column,
    )

    white_orders_with_price = _append_quarterly_seller_price(
        df_quarterly=df_white_orders,
        df_seller_price=df_seller_price,
        target_column_name="цена продажная плановая",
    )
    plan.google_connect_white.update_column_by_name_at_header_row(
        column_name="цена продажная плановая",
        data_to_write=white_orders_with_price["цена продажная плановая"].tolist(),
        header_row_number=4,
        data_start_row_number=5,
    )
    logger.info(
        "Цена продажная плановая обновлена в белых заказах | rows=%s",
        len(white_orders_with_price.index),
    )

    days_without_stocks = plan.get_days_without_stocks_data()

    if days_without_stocks.empty:
        logger.warning(
            "Выгрузка дней без остатков пропущена: запрос не вернул данных."
        )
        return

    plan.set_data(
        plan.annual_plan_connect_to_days_without_stocks_sheet,
        days_without_stocks,
    )
    logger.info(
        "Выгрузка дней без остатков завершена | rows=%s | sheet=%s",
        len(days_without_stocks.index),
        "Дней с остатком",
    )
