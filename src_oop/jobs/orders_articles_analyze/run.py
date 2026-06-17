import logging
from datetime import datetime

import numpy as np
import pandas as pd

from src_oop.core.database import Database
from src_oop.core.logger import LOG_DIR
from src_oop.jobs.orders_articles_analyze.process import ProcessArticleAnalyze
from src_oop.jobs.orders_articles_analyze.repository import ArticleAnalyzeRepository
from src_oop.jobs.orders_articles_analyze.tables_scheme import (
    orders_articles_analyze_table,
)


logger = logging.getLogger(__name__)


def _safe_records_preview(
    df: pd.DataFrame,
    preview_columns: list[str],
    max_rows: int = 50,
) -> list[dict[str, object]]:
    available_columns = [column for column in preview_columns if column in df.columns]
    preview_df = df.loc[:, available_columns].head(max_rows).copy() if available_columns else df.head(max_rows).copy()

    for column in preview_df.columns:
        if pd.api.types.is_datetime64_any_dtype(preview_df[column]):
            preview_df[column] = preview_df[column].astype(str)

    return preview_df.replace({np.nan: None}).to_dict(orient="records")


def _filter_invalid_article_id_before_upsert(
    df: pd.DataFrame,
    task_name: str,
) -> pd.DataFrame:
    if "article_id" not in df.columns:
        raise ValueError(
            "Required column 'article_id' is missing before PostgreSQL upsert."
        )

    result_df = df.copy()
    article_series = result_df["article_id"]
    article_as_string = article_series.astype("string").str.strip().str.lower()
    numeric_article = pd.to_numeric(article_series, errors="coerce")

    invalid_mask = (
        article_series.isna()
        | article_as_string.eq("")
        | article_as_string.isin({"0", "0.0"})
        | (numeric_article.eq(0) & numeric_article.notna())
    )

    total_rows_before = len(result_df.index)
    invalid_rows = result_df.loc[invalid_mask].copy()
    invalid_count = len(invalid_rows.index)
    preview_columns = [
        "date",
        "article_id",
        "vendor_code",
        "account",
        "orders",
        "sum_price",
        "views",
        "clicks",
        "cpm",
        "updated_at",
    ]

    logger.info(
        "Invalid article_id filter started | task=%s | total_rows_before=%s | invalid_count=%s",
        task_name,
        total_rows_before,
        invalid_count,
    )

    if invalid_rows.empty:
        logger.info(
            "No invalid article_id rows found before PostgreSQL upsert | task=%s | total_rows_after=%s",
            task_name,
            total_rows_before,
        )
        return result_df

    diagnostics_dir = LOG_DIR / "diagnostics"
    diagnostics_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    diagnostics_path = diagnostics_dir / (
        f"{task_name}_invalid_article_id_{timestamp}.csv"
    )
    invalid_rows.to_csv(diagnostics_path, index=False, encoding="utf-8-sig")

    logger.warning(
        "Invalid article_id rows found before PostgreSQL upsert | task=%s | total_rows_before=%s | invalid_count=%s | diagnostics_csv=%s",
        task_name,
        total_rows_before,
        invalid_count,
        diagnostics_path,
    )
    logger.warning(
        "Invalid article_id rows preview before PostgreSQL upsert | task=%s | preview_rows=%s",
        task_name,
        _safe_records_preview(invalid_rows, preview_columns=preview_columns, max_rows=50),
    )

    filtered_df = result_df.loc[~invalid_mask].copy()
    filtered_df["article_id"] = pd.to_numeric(
        filtered_df["article_id"],
        errors="raise",
    ).astype("int64")
    logger.info(
        "Invalid article_id filter finished | task=%s | total_rows_after=%s | removed_rows=%s",
        task_name,
        len(filtered_df.index),
        invalid_count,
    )
    return filtered_df


def _log_duplicate_keys_before_upsert(
    df: pd.DataFrame,
    unique_keys: list[str],
    task_name: str,
) -> None:
    missing_keys = [column for column in unique_keys if column not in df.columns]
    if missing_keys:
        raise ValueError(
            f"Unique key columns {missing_keys} are missing in DataFrame before PostgreSQL upsert."
        )

    total_rows = len(df.index)
    unique_key_count = int(df[unique_keys].drop_duplicates().shape[0])
    duplicated_mask = df.duplicated(subset=unique_keys, keep=False)
    duplicate_rows = df.loc[duplicated_mask].copy()
    duplicate_row_count = len(duplicate_rows.index)
    duplicate_key_counts = (
        duplicate_rows.groupby(unique_keys, dropna=False)
        .size()
        .reset_index(name="duplicate_count")
        .sort_values(by="duplicate_count", ascending=False)
    )
    duplicate_key_count = len(duplicate_key_counts.index)

    logger.info(
        "Pre-upsert duplicate check | task=%s | total_rows=%s | unique_key_count=%s | duplicate_row_count=%s | duplicate_key_count=%s | unique_keys=%s",
        task_name,
        total_rows,
        unique_key_count,
        duplicate_row_count,
        duplicate_key_count,
        unique_keys,
    )

    if duplicate_rows.empty:
        logger.info(
            "No duplicates found before PostgreSQL upsert | task=%s | unique_keys=%s",
            task_name,
            unique_keys,
        )
        return

    diagnostics_dir = LOG_DIR / "diagnostics"
    diagnostics_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    diagnostics_path = diagnostics_dir / (
        f"{task_name}_duplicates_{timestamp}.csv"
    )
    duplicate_rows.to_csv(diagnostics_path, index=False, encoding="utf-8-sig")

    top_conflicts = duplicate_key_counts.head(20).to_dict(orient="records")
    logger.warning(
        "Duplicate rows found before PostgreSQL upsert | task=%s | total_rows=%s | unique_key_count=%s | duplicate_row_count=%s | duplicate_key_count=%s | diagnostics_csv=%s",
        task_name,
        total_rows,
        unique_key_count,
        duplicate_row_count,
        duplicate_key_count,
        diagnostics_path,
    )
    logger.warning(
        "Top duplicate keys before PostgreSQL upsert | task=%s | top_conflicts=%s",
        task_name,
        top_conflicts,
    )

    preview_columns_priority = [
        "date",
        "article_id",
        "vendor_code",
        "account",
        "orders",
        "sum_price",
        "views",
        "clicks",
        "updated_at",
    ]
    top_duplicate_keys = duplicate_key_counts.head(10)[unique_keys]
    duplicate_preview = duplicate_rows.merge(
        top_duplicate_keys,
        on=unique_keys,
        how="inner",
    )
    logger.warning(
        "Duplicate rows preview before PostgreSQL upsert | task=%s | preview_rows=%s",
        task_name,
        _safe_records_preview(
            duplicate_preview,
            preview_columns=preview_columns_priority,
            max_rows=50,
        ),
    )

    raise ValueError(
        "Duplicate rows by unique keys ['date', 'article_id'] found before PostgreSQL upsert. "
        "See diagnostics CSV in logs/diagnostics."
    )


def orders_article_analyze_run(days_ago_total: int = 2, days_to: int = 1) -> None:
    """Запуск артикульного анализа по дням."""

    current_stage = "initialization"
    logger.info(
        "orders_article_analyze_run started | days_ago_total=%s | days_to=%s",
        days_ago_total,
        days_to,
    )

    try:
        repo = ArticleAnalyzeRepository()
        processor = ProcessArticleAnalyze(repo)
        logger.info(
            "Execution path | step=initialized_components | repository=%s | processor=%s",
            repo.__class__.__name__,
            processor.__class__.__name__,
        )

        for day in range(days_ago_total, days_to - 1, -1):
            current_stage = f"day_{day}_start"
            logger.info(
                "Day processing started | day=%s | days_ago=%s | days_to=%s",
                day,
                day,
                day,
            )
            print(f"--- 🗓️ Обработка данных за {day} дн. назад ---")

            current_stage = f"day_{day}_build_dataset"
            logger.info("Execution path | step=build_dataset | day=%s", day)
            df = processor.build_dataset(days_ago=day, days_to=day)
            logger.info(
                "Dataset built | day=%s | rows=%s | columns=%s",
                day,
                len(df.index),
                list(df.columns),
            )

            if df.empty:
                logger.warning("Нет данных за %s дн. назад, пропускаем...", day)
                continue

            current_stage = f"day_{day}_filter_invalid_article_id_before_upsert"
            df = _filter_invalid_article_id_before_upsert(
                df=df,
                task_name="orders_article_analyze",
            )

            scheme = orders_articles_analyze_table.get("columns")
            table = orders_articles_analyze_table.get("title")
            unique_keys = orders_articles_analyze_table.get("unique_keys")
            logger.info(
                "Prepared DB sync metadata | day=%s | table=%s | unique_keys=%s | schema_columns=%s",
                day,
                table,
                unique_keys,
                list(scheme.keys()) if scheme else [],
            )

            current_stage = f"day_{day}_check_duplicates_before_upsert"
            logger.info(
                "Duplicate check started before DB sync | day=%s | table=%s | unique_keys=%s",
                day,
                table,
                unique_keys,
            )
            _log_duplicate_keys_before_upsert(
                df=df,
                unique_keys=unique_keys,
                task_name="orders_article_analyze",
            )

            print(f"✅ Запись в БД: {len(df)} строк за {day} дн. назад")

            current_stage = f"day_{day}_sync_data_to_postgres"
            logger.info(
                "DB sync started | day=%s | table=%s | rows=%s",
                day,
                table,
                len(df.index),
            )
            Database.sync_data_to_postgres(
                table_name=table,
                data=df,
                schema_definition=scheme,
                unique_keys=unique_keys,
            )
            logger.info(
                "DB sync finished | day=%s | table=%s | rows=%s",
                day,
                table,
                len(df.index),
            )
            logger.info("Day processing finished | day=%s", day)

        logger.info("orders_article_analyze_run finished successfully")
        print("🏁 Артикульный анализ успешно завершен")
    except Exception as error:
        logger.exception(
            "orders_article_analyze_run failed | current_stage=%s | error_type=%s | error=%s",
            current_stage,
            type(error).__name__,
            error,
        )
        raise
