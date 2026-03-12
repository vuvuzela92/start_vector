from src_oop.core.database import Database
from src_oop.storage.repository.repository import GetDataFromDB
from src_oop.core.my_gspread import GoogleTabs
from src_oop.storage.google_sheets.google_sheets import fin_rep_analyze

import gspread

def get_incomes_outcomes_analyze_week():
    # 1. Получаем engine
    engine = Database.get_engine()
    # 2. Инициализируем репозиторий
    repo = GetDataFromDB(engine)
    # 3. Получаем данные
    df = repo.get_weekly_profit_report()
    df = df.fillna(0)
    df["date_to"] = df["date_to"].astype(str)
    df["end_date"] = df["end_date"].astype(str)

    # Создаем соединение с гугл-таблицей
    google_table = fin_rep_analyze.get("title")
    table_sheet = fin_rep_analyze.get("incomes_outcomes_analyze_week_sheet")

    # Создаем соединение с гугл-таблицей
    try:
        # Создаем соединение с гугл-таблицей
        google_connect = GoogleTabs(table_title=google_table, sheet_title=table_sheet)
        # Вставляем данные в гугл-таблицу
        google_connect._send_df_to_google(df, google_connect.sheet_title)
    except gspread.exceptions.SpreadsheetNotFound:
        print(f"Не найдена таблица {google_table}")
    except gspread.exceptions.WorksheetNotFound as e:
        print(f"Не найден лист {table_sheet} в таблице {google_table}")
    except StopIteration:
        print(f"Не найден лист {table_sheet} в таблице {google_table}")
    except RuntimeError as e:
        print(f"Ошибка подключения: {e}")

if __name__ == "__main__":
    get_incomes_outcomes_analyze_week()
    