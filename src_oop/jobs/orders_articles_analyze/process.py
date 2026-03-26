import logging
import pandas as pd
from src_oop.jobs.orders_articles_analyze.repository import ArticleAnalyzeRepository

logger = logging.getLogger(__name__)

class ProcessArticleAnalyze:
    def __init__(self, repo: ArticleAnalyzeRepository):
        self.repo = repo

    def build_dataset(self, days_ago: int, days_to: int) -> pd.DataFrame:
        # Передаем даты явно!
        df_adv = self.repo.get_adv_stat(days_ago, days_to)
        df_gen = self.repo.get_general_stat(days_ago, days_to)

        if df_adv.empty:
            logger.warning("adv_stat пустой")
        if df_gen.empty:
            logger.warning("general_stat пустой")
            return pd.DataFrame()

        logger.info("Удаляем дубликаты")
        df_gen = df_gen.sort_values(by=["date", "orders_sum_rub"], ascending=[False, False]).drop_duplicates(subset=["article_id", "date"])

        self._validate_unique_keys(df_adv, "df_adv")
        self._validate_unique_keys(df_gen, "df_gen")

        df = pd.merge(df_gen, df_adv, on=["article_id", "date"], how="left")

        del df_adv
        del df_gen
        
        # Безопасное заполнение числовых колонок нулями
        numeric_cols = df.select_dtypes(include="number").columns
        df.loc[:, numeric_cols] = df[numeric_cols].fillna(0)

        return df.sort_values(by=["date", "orders_sum_rub"], ascending=[False, False]).reset_index(drop=True)

    @staticmethod
    def _validate_unique_keys(df: pd.DataFrame, name: str):
        if df.empty:
            return
        duplicates = df[df.duplicated(["article_id", "date"], keep=False)]
        if not duplicates.empty:
            raise ValueError(f"{name} содержит дубли по ['article_id', 'date']")