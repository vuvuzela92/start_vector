from src_oop.jobs.autopilot_daily.service import AutopilotDailyService


def autopilot_daily_run():
    """Запускает дневное обновление панели управления автопилотом.

    Бизнес-логика:
    entrypoint для `tasks_registry.py`; выполняет перенос legacy-сценария
    `autopilot_daily.py`: дневные и исторические метрики ПУ, средние позиции,
    справочные поля A:D, статус рекламы UNIT и лист Сопост с сохранением
    частичной записи метрик при ошибках Google Sheets.
    """
    service = AutopilotDailyService()
    return service.run()
