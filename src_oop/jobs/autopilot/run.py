from src_oop.jobs.autopilot.autopilot import Autopilot

def update_individual_info():
    autopilot = Autopilot()
    # Забираем нужные данные из юнитки
    df_unit = autopilot.get_unit_data()
    # Вставляем данные в ПУ в лист ИУ_ИНФО
    autopilot_table = autopilot.google_connect_to
    autopilot_table.set_df_to_google(df_unit)