from src_oop.core.database import Database
from src_oop.jobs.unit.queries import query_adv_spend
from src_oop.jobs.unit.config import unit_gs
from src_oop.core.my_gspread import GoogleTabs

import pandas as pd


def update_adv_participants_to_gs():
    # Инициализируем базу данных и получаем данные о рекламных расходах
    database = Database()
    # Получаем данные о рекламных расходах по статьям за вчерашний день
    adv_spend = database.read_sql_to_dataframe(query_adv_spend)
    # Задаем параметры для работы с Google Sheets
    table_title = unit_gs.get("title")
    sheet_title = unit_gs.get("unit_sheet")
    # Инициализируем работу с Google Sheets
    google_tabs = GoogleTabs(table_title, sheet_title)
    # Получаем данные из Google Sheets и преобразуем их в DataFrame
    sheet_data = google_tabs.sheet_title.get_all_values()
    headers = sheet_data[0]
    rows = sheet_data[1:]
    df = pd.DataFrame(rows, columns=headers)
    # Приводим столбец 'Артикул' к типу int для корректного сравнения
    df['Артикул'] = df['Артикул'].astype(int)
    # Создаем датафрейм с нужными столбцами для дальнейшей работы
    df_short = df[['Артикул', 'Реклама']]

    # 1. Получаем список всех уникальных артикулов, по которым БЫЛИ затраты
    articles_with_spend = set(adv_spend['article_id'].astype(int))

    # 2. Создаем функцию для проверки
    def check_adv(article):
        # Приводим к строке для надежности сравнения
        if int(article) in articles_with_spend:
            return "реклама"
        else:
            return ""

    # 3. Применяем функцию к колонке 'Артикул' в df_short
    df_short['Реклама'] = df_short['Артикул'].apply(check_adv)

    # Динамически находим индекс колонки "Артикул", чтобы не ошибиться
    if 'Артикул' not in headers:
        print("Ошибка: В таблице нет колонки 'Артикул'!")
    else:
        art_idx = headers.index('Артикул')

        # --- 3. Сопоставление ---
        # Важно: мы создаем список значений в том же порядке, в котором идут строки в таблице
        results_list = []
        for row in rows:
            # Проверяем артикул из текущей строки таблицы
            current_art = int(row[art_idx])
            
            if current_art in articles_with_spend:
                results_list.append("реклама")
            else:
                results_list.append("")

        # --- 4. Запись ---
        # Теперь нам всё равно, где находится колонка "Реклама", метод сам её найдет
        google_tabs.update_column_by_name("Реклама", results_list)