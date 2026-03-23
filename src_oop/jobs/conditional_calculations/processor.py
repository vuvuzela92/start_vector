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

        # 1. Очистка от бесконечностей и NaN
        # Заменяем их на 0, чтобы конвертация в int прошла успешно
        df = df.replace([np.inf, -np.inf], np.nan)
        
        # 2. Проходим по всем колонкам
        for col in df.columns:
            # если имя колонки не в списке исключений
            if col.lower() not in ["date", "account"]:
                # Превращаем колонку в целые числа
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(np.int64)
        
        return df