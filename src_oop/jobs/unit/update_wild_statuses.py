from src_oop.jobs.calculation_of_purchases_russia.calculation_of_purchases_russia import Calculation_of_purchases_russia
from src_oop.jobs.unit.unit import UnitEconomics

import pandas as pd


def update_wild_statuses():
    # Подключаюсь к гугл-таблице Расчет закупки Россия
    calc = Calculation_of_purchases_russia()
    # Считываем данные из листа Статичный лист статусы
    statuses_table = calc.google_connect_statuses.sheet_title.get_all_values()

    # Определяем с какой строки начинаются заголовки и данные
    headers = statuses_table[0]
    rows = statuses_table[1:]
    # Приводим данные к дафрейму
    df_statuses = pd.DataFrame(rows, columns=headers)
    # Оставляем только те колонкиЮ что нам нужны
    status_cols = ['wild', 'статус вилд']
    df_statuses_short = df_statuses[status_cols]
    # Удаляем полный датафрейм для оптимизации памяти
    del df_statuses
    # Получаем данные из Google Sheets и преобразуем их в DataFrame
    unit_economics = UnitEconomics()
    unit_table = unit_economics.google_connect.sheet_title.get_all_values()
    headers = unit_table[0]
    rows = unit_table[1:]
    df_unit = pd.DataFrame(rows, columns=headers)
    # Создаем датафрейм с нужными столбцами для дальнейшей работы
    df_unit_short = df_unit[['wild', 'Статус товара']]
    del df_unit
    # Отбираю только уникальные вилды
    wild_with_statuses = df_statuses_short.drop_duplicates(subset=['wild', 'статус вилд'], keep='first')
    # Объединяем таблицы (LEFT JOIN)
    # Мы присоединяем статусы к df_unit по ключу 'wild'
    df_unit_short = df_unit_short.merge(
        wild_with_statuses, 
        on='wild', 
        how='left'
    )

    # Переносим данные из 'статус вилд' в 'Статус товара'
    # Если колонка 'Статус товара' уже была в df_unit, она обновится
    df_unit_short['Статус товара'] = df_unit_short['статус вилд']

    # Удаляем лишнюю временную колонку 'статус вилд'
    df_unit_short = df_unit_short.drop(columns=['статус вилд'])
    df_unit_short = df_unit_short.fillna('')

    col_name_in_google = "Статус товара" # Как колонка называется в самом Google Sheets

    if col_name_in_google not in df_unit_short.columns:
        print(f"Ошибка: В обработанных данных нет колонки {col_name_in_google}!")
    else:
        #  Извлечение списка значений 
        # Превращаем колонку DataFrame в простой список строк
        results_list = df_unit_short[col_name_in_google].astype(str).tolist()

        # Запись в Google Таблицу ---
        try:
            # Используем твой метод для обновления всей колонки целиком
            unit_economics.google_connect.update_column_by_name(col_name_in_google, results_list)
            print(f"✅ Колонка '{col_name_in_google}' успешно обновлена в Google Sheets")
        except Exception as e:
            print(f"❌ Ошибка при записи в Google: {e}")

