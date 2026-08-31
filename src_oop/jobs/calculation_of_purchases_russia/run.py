from src_oop.jobs.calculation_of_purchases_russia.calculation_of_purchases_russia import (
    Calculation_of_purchases_russia,
)
from src_oop.jobs.calculation_of_purchases_russia.config import query


def set_orders_quantity() -> None:
    """Обновляет лист `БД_Заказы` количеством заказов за последние 7 дней.

    Бизнес-сценарий:
    функция запускает регламентную выгрузку ежедневного количества заказов по
    `wild` в закупочную таблицу России, чтобы команда закупок видела свежую
    динамику спроса в отдельной витрине Google Sheets.
    """

    calc = Calculation_of_purchases_russia()
    df = calc.db.read_sql_to_dataframe(query)
    df["date"] = df["date"].astype(str)
    calc.google_connect.set_df_to_google(df)


def transport_orders_and_supply() -> None:
    """Перезаписывает лист `Заказы_и_поступления` сводом заказов и поступлений.

    Бизнес-сценарий:
    функция публикует в закупочную таблицу России объединённую витрину
    заказов поставщикам, фактических поступлений и возвратов, чтобы бизнес
    мог сверять цепочку поставки в одном листе без ручных объединений.
    """

    calc = Calculation_of_purchases_russia()
    df = calc.get_orders_and_supplies_data()
    df = df.fillna(0)
    calc.set_data(calc.google_connect_to_purchsase_russia_table, df)


def update_penalties_in_gs_purchase_russia() -> None:
    """Обновляет в листе расчёта колонки штрафов и виртуальных остатков.

    Бизнес-сценарий:
    функция запускает точечный сценарий для листа `Расчет закупки`, где нужно
    подтянуть штрафы из БД и виртуальные остатки из UNIT, не затирая остальную
    пользовательскую структуру листа.
    """

    calc = Calculation_of_purchases_russia()
    calc.update_penalties_and_virtual_stock()


def update_supplies_1c_in_purchase_russia() -> None:
    """Обновляет лист `Приходы_1С` в таблице `Расчет закупки Россия`.

    Бизнес-сценарий:
    функция запускает полный контур перенесённой legacy-выгрузки приходов 1С:
    читает валидные поступления из PostgreSQL, форматирует даты, сохраняет
    русские заголовки колонок и публикует результат в Google Sheets с отметкой
    времени обновления в ячейке `A1`.
    """

    calc = Calculation_of_purchases_russia()
    calc.update_supplies_1c_sheet()


# python -m src_oop.jobs.calculation_of_purchases_russia.run
