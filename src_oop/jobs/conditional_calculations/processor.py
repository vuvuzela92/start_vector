import logging
import pandas as pd
import numpy as np
from src_oop.jobs.conditional_calculations.repository import ConditionalCalculationsRepository

logger = logging.getLogger(__name__)

class ProcessConditionalCalculation:
    def __init__(self, repo: ConditionalCalculationsRepository):
        self.repo = repo

    def process_df(self):
        # Получаем данные из репозитория
        df = self.repo.execute_conditional_calculations()

        if df is None or df.empty:
            logger.warning("Датасет для Условного расчета пустой")
            return df

        # --- ДОБАВЬТЕ ЭТОТ БЛОК ---
        # Убеждаемся, что колонка date — это именно дата без времени
        df['date'] = pd.to_datetime(df['date']).dt.date
        
        # Удаляем дубликаты по уникальным ключам, оставляя только одну запись
        # (например, последнюю встреченную)
        before_count = len(df)
        df = df.drop_duplicates(subset=['date', 'account'], keep='last')
        after_count = len(df)
        
        if before_count != after_count:
            logger.info(f"Удалено {before_count - after_count} дубликатов строк перед загрузкой")
        # --------------------------

        # 1. Очистка от бесконечностей и NaN
        df = df.replace([np.inf, -np.inf], np.nan)
        
        # 2. Проходим по всем колонкам
        for col in df.columns:
            if col.lower() not in ["date", "account"]:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(np.int64)
        
        return df