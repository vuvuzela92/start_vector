from src_oop.jobs.orders_articles_analyze.repository import GetDataFromDB
from src_oop.jobs.orders_articles_analyze.service import ArticleAnalyzeService
import logging
from src_oop.jobs.orders_articles_analyze.tables_scheme import orders_articles_analyze_table
from src_oop.core.database import Database

logger = logging.getLogger(__name__)

class ArticleAnalyzeUseCase:

    def __init__(self, engine):
        self.engine = engine
        self.repo = GetDataFromDB(engine)
        self.service = ArticleAnalyzeService(self.repo)

    def orders_article_analyze_run(self, days_ago: int = 30, days_to: int = 1):
        df = self.service.build_dataset(days_ago, days_to)

        if df.empty:
            logger.warning("Нет данных для записи")
            return

        scheme = orders_articles_analyze_table.get("columns")
        table = orders_articles_analyze_table.get("title")
        keys = orders_articles_analyze_table.get("unique_keys")

        Database.sync_data_to_postgres(
            engine=self.engine,
            table_name=table,
            data=df,
            schema_definition=scheme,
            unique_keys=keys
        )

# if __name__ == "__main__":
#     engine = Database.get_engine()
#     use_case = ArticleAnalyzeUseCase(engine)
#     use_case.orders_article_analyze_run()