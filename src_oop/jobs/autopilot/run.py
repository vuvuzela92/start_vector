from src_oop.jobs.autopilot.autopilot import Autopilot
from src_oop.jobs.autopilot.service import AutopilotHourlyService


def update_individual_info():
    """
    Обновляет лист индивидуальных условий в ПУ данными из UNIT.

    Бизнес-логика:
    это существующий контур автопилота, который переносит в ПУ справочные поля
    по артикулам, цене для клиента, марже и ФБС без участия hourly-метрик.
    """
    autopilot = Autopilot()
    # Забираем нужные данные из юнитки.
    df_unit = autopilot.get_unit_data()
    # Вставляем данные в ПУ в лист ИУ_ИНФО.
    autopilot_table = autopilot.google_connect_to
    autopilot_table.set_df_to_google(df_unit)


async def autopilot_hourly_run():
    """
    Запускает почасовое обновление метрик панели управления автопилотом.

    Бизнес-логика:
    entrypoint для `tasks_registry.py`; выполняет полный сценарий сбора данных,
    расчетов, записи в ПУ и обновления `spp_history`.
    """
    service = AutopilotHourlyService()
    return await service.run()
