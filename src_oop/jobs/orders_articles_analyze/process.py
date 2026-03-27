import logging
import pandas as pd
from src_oop.jobs.orders_articles_analyze.repository import ArticleAnalyzeRepository

logger = logging.getLogger(__name__)

class ProcessArticleAnalyze:
    def __init__(self, repo: ArticleAnalyzeRepository):
        self.repo = repo

    def build_dataset(self, days_ago: int, days_to: int) -> pd.DataFrame:
        df_adv = self.repo.get_adv_stat(days_ago, days_to)
        df_gen = self.repo.get_general_stat(days_ago, days_to)
        
        # 📑 Получаем справочник товаров (без фильтра по датам)
        df_all_goods = self.repo.get_all_goods_directory()

        if df_gen.empty and df_adv.empty:
            logger.warning("Оба источника пусты.")
            return pd.DataFrame()

        logger.info("Удаляем дубликаты")
        if not df_gen.empty:
            df_gen = df_gen.sort_values(by=["date", "orders_sum_rub"], ascending=[False, False]).drop_duplicates(subset=["article_id", "date"])

        df = pd.merge(df_gen, df_adv, on=["article_id", "date"], how="outer") 

        del df_adv
        del df_gen 

        # === 📅 1. Восстанавливаем даты ===
        df['date'] = pd.to_datetime(df['date'])
        df['week_num'] = df['week_num'].fillna(df['date'].dt.isocalendar().week)
        df['month_num'] = df['month_num'].fillna(df['date'].dt.month)

        # === 📦 2. Обогащаем данными о товаре из справочника ===
        df_goods = df_all_goods.drop_duplicates(subset=['article_id']).copy()
        del df_all_goods

        cols_to_fill = ['account', 'local_vendor_code', 'subject_name']
        for col in cols_to_fill:
            mapping_dict = df_goods.set_index('article_id')[col]
            df[col] = df[col].fillna(df['article_id'].map(mapping_dict))

        # === 🔢 3. Заполняем числовые колонки нулями ===
        numeric_cols = df.select_dtypes(include="number").columns
        df.loc[:, numeric_cols] = df[numeric_cols].fillna(0)

        return df.sort_values(by=["date", "orders_sum_rub"], ascending=[False, False]).reset_index(drop=True)