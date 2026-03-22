from src_oop.jobs.orders_articles_analyze.process import ProcessArticleAnalyze
import logging
from src_oop.jobs.orders_articles_analyze.tables_scheme import orders_articles_analyze_table
from src_oop.core.database import Database
from src_oop.jobs.orders_articles_analyze.repository import ArticleAnalyzeRepository

logger = logging.getLogger(__name__)

def orders_article_analyze_run(days_ago: int = 30, days_to: int = 1):
    """Запуск Артикульного анализа"""
    logger.info("Запуск получения данных по Артикульному анализу")
    repo = ArticleAnalyzeRepository()
    df = ProcessArticleAnalyze(repo).build_dataset(days_ago, days_to)

    if df.empty:
        logger.warning("Нет данных для записи")
        return

    scheme = orders_articles_analyze_table.get("columns")
    table = orders_articles_analyze_table.get("title")
    unique_keys = orders_articles_analyze_table.get("unique_keys")

    Database.sync_data_to_postgres(
        table_name=table,
        data=df,
        schema_definition=scheme,
        unique_keys=unique_keys
    )