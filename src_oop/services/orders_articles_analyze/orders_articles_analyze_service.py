import logging
import pandas as pd
from src_oop.storage.repository.orders_articles_analyze import ArticleAnalyze

logger = logging.getLogger(__name__)

class ArticleAnalyzeService:

    def __init__(self, repo: ArticleRepository):
        self.repo = repo

    def build_dataset(self, days_ago: int, days_to: int) -> pd.DataFrame:
        df_adv = self.repo.get_adv_stat(days_ago, days_to)
        df_gen = self.repo.get_general_stat(days_ago, days_to)

        if df_adv.empty:
            logger.warning("adv_stat пустой")

        if df_gen.empty:
            logger.warning("general_stat пустой")

        df = pd.merge(df_adv, df_gen, on=['article_id', 'date'], how='left')

        df = (
            df.drop_duplicates(subset=['date', 'article_id'])
              .fillna(0)
              .sort_values(by=['date', 'orders_sum_rub'], ascending=[False, False])
        )

        return df