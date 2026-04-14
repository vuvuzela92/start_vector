import asyncio
from src_oop.jobs.wb_api.measurements.measurements import Measurements
from src_oop.jobs.wb_api.measurements.service import fetch_measurements_task, get_measurements

def collect_and_store_measurements():
    """Главная функция для запуска сбора данных о замерах и сохранения их в БД"""
    asyncio.run(fetch_measurements_task())

def set_measurements_to_google():
    """Функция для получения данных о замерах из БД и записи их в Google Sheets"""
    df = get_measurements()
    measurements = Measurements()
    measurements.set_data(df)

