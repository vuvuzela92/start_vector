from src_oop.jobs.orders_articles_analyze.process import ProcessArticleAnalyze
import logging
from src_oop.jobs.orders_articles_analyze.tables_scheme import orders_articles_analyze_table
from src_oop.core.database import Database
from src_oop.jobs.orders_articles_analyze.repository import ArticleAnalyzeRepository
from datetime import datetime

logger = logging.getLogger(__name__)

def orders_article_analyze_run(days_ago_total: int = 2, days_to: int = 1):
    """Запуск Артикульного анализа по дням (для экономии памяти БД)"""
    logger.info(f"Запуск артикульного анализа за период с {days_ago_total} до {days_to} дней назад")
    
    repo = ArticleAnalyzeRepository()
    processor = ProcessArticleAnalyze(repo)
    
    # Итерируемся от самого старого дня к самому новому
    for day in range(days_ago_total, days_to - 1, -1):
        # В этом цикле day — это число дней назад от СЕГОДНЯ
        # Если мы хотим обработать ОДИН конкретный день, 
        # то в SQL INTERVAL 'day' AND INTERVAL 'day' даст данные ровно за эту дату.
        
        print(f"--- 🗓️ Обработка данных за {day} дн. назад ---")
        
        # Вызываем build_dataset, где ago и to равны одному и тому же числу
        df = processor.build_dataset(days_ago=day, days_to=day)

        if df.empty:
            logger.warning(f"Нет данных за {day} дн. назад, пропускаем...")
            continue

        scheme = orders_articles_analyze_table.get("columns")
        table = orders_articles_analyze_table.get("title")
        unique_keys = orders_articles_analyze_table.get("unique_keys")
        
        print(f"✅ Запись в БД: {len(df)} строк за {day} дн. назад")
        
        Database.sync_data_to_postgres(
            table_name=table,
            data=df,
            schema_definition=scheme,
            unique_keys=unique_keys
        )

    print("🏁 Артикульный анализ успешно завершен")