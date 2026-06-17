import logging

import numpy as np
import pandas as pd

from src_oop.jobs.orders_articles_analyze.repository import ArticleAnalyzeRepository


logger = logging.getLogger(__name__)


class ProcessArticleAnalyze:
    def __init__(self, repo: ArticleAnalyzeRepository):
        self.repo = repo

    @staticmethod
    def _safe_preview(
        df: pd.DataFrame,
        rows: int = 3,
        max_columns: int = 12,
    ) -> list[dict[str, object]]:
        if df.empty:
            return []

        preview_df = df.head(rows).copy()
        safe_columns = [
            column
            for column in preview_df.columns
            if not any(
                token in column.lower()
                for token in ("token", "secret", "password", "email", "phone")
            )
        ]
        preview_df = preview_df.loc[:, safe_columns[:max_columns]]

        for column in preview_df.columns:
            if pd.api.types.is_datetime64_any_dtype(preview_df[column]):
                preview_df[column] = preview_df[column].astype(str)

        return preview_df.replace({np.nan: None}).to_dict(orient="records")

    def _log_dataframe_state(
        self,
        stage: str,
        df: pd.DataFrame,
        required_columns: list[str] | None = None,
        key_columns: list[str] | None = None,
    ) -> None:
        logger.info(
            "DataFrame snapshot | stage=%s | shape=%s | rows=%s | columns=%s",
            stage,
            df.shape,
            len(df.index),
            list(df.columns),
        )

        if required_columns is not None:
            missing_columns = [
                column for column in required_columns if column not in df.columns
            ]
            logger.info(
                "Required columns check | stage=%s | required=%s | missing=%s",
                stage,
                required_columns,
                missing_columns,
            )

        if key_columns is not None:
            available_key_columns = [
                column for column in key_columns if column in df.columns
            ]
            null_counts = {
                column: int(df[column].isna().sum())
                for column in available_key_columns
            }
            logger.info(
                "Key column null counts | stage=%s | null_counts=%s",
                stage,
                null_counts,
            )

        logger.info(
            "DataFrame preview | stage=%s | preview=%s",
            stage,
            self._safe_preview(df),
        )

    def _log_article_id_quality(self, stage: str, df: pd.DataFrame) -> None:
        if "article_id" not in df.columns:
            logger.warning(
                "article_id diagnostics skipped | stage=%s | reason=missing_column",
                stage,
            )
            return

        article_series = df["article_id"]
        article_as_string = article_series.astype("string").str.strip().str.lower()
        null_mask = article_series.isna()
        empty_string_mask = article_as_string.eq("")
        zero_string_mask = article_as_string.isin({"0", "0.0"})
        numeric_article = pd.to_numeric(article_series, errors="coerce")
        zero_numeric_mask = numeric_article.eq(0) & numeric_article.notna()
        invalid_mask = null_mask | empty_string_mask | zero_string_mask | zero_numeric_mask

        preview_columns_priority = [
            "date",
            "article_id",
            "account",
            "local_vendor_code",
            "subject_name",
            "views",
            "clicks",
            "cpm",
        ]
        preview_columns = [
            column for column in preview_columns_priority if column in df.columns
        ]
        invalid_preview = (
            df.loc[invalid_mask, preview_columns].head(20)
            if preview_columns
            else df.loc[invalid_mask].head(20)
        )

        logger.info(
            "article_id diagnostics | stage=%s | total_rows=%s | null_count=%s | empty_string_count=%s | zero_string_count=%s | zero_numeric_count=%s | invalid_count=%s | invalid_preview=%s",
            stage,
            len(df.index),
            int(null_mask.sum()),
            int(empty_string_mask.sum()),
            int(zero_string_mask.sum()),
            int(zero_numeric_mask.sum()),
            int(invalid_mask.sum()),
            self._safe_preview(invalid_preview, rows=20, max_columns=len(preview_columns) or 12),
        )

    def build_dataset(self, days_ago: int, days_to: int) -> pd.DataFrame:
        current_stage = "load_adv_stat"
        logger.info(
            "build_dataset started | days_ago=%s | days_to=%s",
            days_ago,
            days_to,
        )

        try:
            df_adv = self.repo.get_adv_stat(days_ago, days_to)
            self._log_dataframe_state(
                stage="source_adv_stat",
                df=df_adv,
                required_columns=["article_id", "date", "clicks", "views", "adv_spend"],
                key_columns=["article_id", "date", "clicks", "views", "adv_spend"],
            )
            self._log_article_id_quality(stage="source_adv_stat", df=df_adv)

            current_stage = "load_general_stat"
            df_gen = self.repo.get_general_stat(days_ago, days_to)
            self._log_dataframe_state(
                stage="source_general_stat",
                df=df_gen,
                required_columns=[
                    "article_id",
                    "date",
                    "orders_sum_rub",
                    "orders_count",
                ],
                key_columns=[
                    "article_id",
                    "date",
                    "account",
                    "local_vendor_code",
                    "subject_name",
                ],
            )
            self._log_article_id_quality(stage="source_general_stat", df=df_gen)

            current_stage = "load_all_goods_directory"
            df_all_goods = self.repo.get_all_goods_directory()
            self._log_dataframe_state(
                stage="source_all_goods_directory",
                df=df_all_goods,
                required_columns=[
                    "article_id",
                    "account",
                    "local_vendor_code",
                    "subject_name",
                ],
                key_columns=[
                    "article_id",
                    "account",
                    "local_vendor_code",
                    "subject_name",
                ],
            )
            self._log_article_id_quality(stage="source_all_goods_directory", df=df_all_goods)

            if df_gen.empty and df_adv.empty:
                logger.warning(
                    "Оба источника пусты | days_ago=%s | days_to=%s",
                    days_ago,
                    days_to,
                )
                return pd.DataFrame()

            current_stage = "deduplicate_general_stat"
            logger.info("Stage started | stage=%s", current_stage)
            if not df_gen.empty:
                df_gen = df_gen.sort_values(
                    by=["date", "orders_sum_rub"],
                    ascending=[False, False],
                ).drop_duplicates(subset=["article_id", "date"])
            self._log_dataframe_state(
                stage="general_stat_deduplicated",
                df=df_gen,
                required_columns=[
                    "article_id",
                    "date",
                    "orders_sum_rub",
                    "orders_count",
                ],
                key_columns=["article_id", "date"],
            )
            self._log_article_id_quality(stage="general_stat_deduplicated", df=df_gen)
            logger.info("Stage finished | stage=%s", current_stage)

            current_stage = "merge_general_and_adv"
            logger.info("Stage started | stage=%s", current_stage)
            df = pd.merge(df_gen, df_adv, on=["article_id", "date"], how="outer")
            self._log_dataframe_state(
                stage="merged_general_and_adv",
                df=df,
                required_columns=["article_id", "date"],
                key_columns=[
                    "article_id",
                    "date",
                    "account",
                    "local_vendor_code",
                    "subject_name",
                ],
            )
            self._log_article_id_quality(stage="merged_general_and_adv", df=df)
            logger.info("Stage finished | stage=%s", current_stage)

            del df_adv
            del df_gen

            current_stage = "restore_dates"
            logger.info("Stage started | stage=%s", current_stage)
            df["date"] = pd.to_datetime(df["date"])
            df["week_num"] = df["week_num"].fillna(df["date"].dt.isocalendar().week)
            df["month_num"] = df["month_num"].fillna(df["date"].dt.month)
            self._log_dataframe_state(
                stage="dates_restored",
                df=df,
                required_columns=["article_id", "date", "week_num", "month_num"],
                key_columns=["article_id", "date", "week_num", "month_num"],
            )
            self._log_article_id_quality(stage="dates_restored", df=df)
            logger.info("Stage finished | stage=%s", current_stage)

            current_stage = "prepare_goods_directory"
            logger.info("Stage started | stage=%s", current_stage)
            df_goods = df_all_goods.drop_duplicates(subset=["article_id"]).copy()
            del df_all_goods
            self._log_dataframe_state(
                stage="goods_directory_deduplicated",
                df=df_goods,
                required_columns=[
                    "article_id",
                    "account",
                    "local_vendor_code",
                    "subject_name",
                ],
                key_columns=[
                    "article_id",
                    "account",
                    "local_vendor_code",
                    "subject_name",
                ],
            )
            self._log_article_id_quality(stage="goods_directory_deduplicated", df=df_goods)
            logger.info("Stage finished | stage=%s", current_stage)

            current_stage = "enrich_from_goods_directory"
            logger.info("Stage started | stage=%s", current_stage)
            cols_to_fill = ["account", "local_vendor_code", "subject_name"]
            for col in cols_to_fill:
                missing_before = int(df[col].isna().sum()) if col in df.columns else None
                mapping_dict = df_goods.set_index("article_id")[col]
                df[col] = df[col].fillna(df["article_id"].map(mapping_dict))
                missing_after = int(df[col].isna().sum()) if col in df.columns else None
                logger.info(
                    "Column enrichment applied | column=%s | missing_before=%s | missing_after=%s",
                    col,
                    missing_before,
                    missing_after,
                )
            self._log_dataframe_state(
                stage="enriched_from_goods_directory",
                df=df,
                required_columns=[
                    "article_id",
                    "date",
                    "account",
                    "local_vendor_code",
                    "subject_name",
                ],
                key_columns=[
                    "article_id",
                    "date",
                    "account",
                    "local_vendor_code",
                    "subject_name",
                ],
            )
            self._log_article_id_quality(stage="enriched_from_goods_directory", df=df)
            logger.info("Stage finished | stage=%s", current_stage)

            current_stage = "fill_numeric_nulls"
            logger.info("Stage started | stage=%s", current_stage)
            numeric_cols = df.select_dtypes(include="number").columns
            logger.info(
                "Numeric columns detected | stage=%s | columns=%s",
                current_stage,
                list(numeric_cols),
            )
            df.loc[:, numeric_cols] = df[numeric_cols].fillna(0)
            self._log_dataframe_state(
                stage="numeric_nulls_filled_first_pass",
                df=df,
                required_columns=["article_id", "date"],
                key_columns=[
                    "article_id",
                    "date",
                    "orders_sum_rub",
                    "orders_count",
                    "adv_spend",
                ],
            )
            self._log_article_id_quality(stage="numeric_nulls_filled_first_pass", df=df)
            logger.info("Stage finished | stage=%s", current_stage)

            current_stage = "replace_infinite_values"
            logger.info("Stage started | stage=%s", current_stage)
            inf_count = int(np.isinf(df.select_dtypes(include="number").to_numpy()).sum())
            logger.info(
                "Infinite numeric values before replace | stage=%s | count=%s",
                current_stage,
                inf_count,
            )
            df = df.replace([np.inf, -np.inf], np.nan)
            self._log_article_id_quality(stage="replace_infinite_values", df=df)
            logger.info("Stage finished | stage=%s", current_stage)

            current_stage = "fill_numeric_nulls_second_pass"
            logger.info("Stage started | stage=%s", current_stage)
            numeric_cols = df.select_dtypes(include="number").columns
            df.loc[:, numeric_cols] = df[numeric_cols].fillna(0)
            self._log_dataframe_state(
                stage="numeric_nulls_filled_second_pass",
                df=df,
                required_columns=["article_id", "date"],
                key_columns=[
                    "article_id",
                    "date",
                    "orders_sum_rub",
                    "orders_count",
                    "adv_spend",
                ],
            )
            self._log_article_id_quality(stage="numeric_nulls_filled_second_pass", df=df)
            logger.info("Stage finished | stage=%s", current_stage)

            current_stage = "normalize_large_numeric_types"
            logger.info("Stage started | stage=%s", current_stage)
            converted_columns: list[str] = []
            for col in numeric_cols:
                if df[col].max() > 2147483647 or df[col].min() < -2147483648:
                    df[col] = df[col].astype(float)
                    converted_columns.append(col)
            logger.info(
                "Large numeric normalization finished | stage=%s | converted_columns=%s",
                current_stage,
                converted_columns,
            )
            self._log_article_id_quality(stage="normalize_large_numeric_types", df=df)
            logger.info("Stage finished | stage=%s", current_stage)

            current_stage = "final_sort"
            logger.info("Stage started | stage=%s", current_stage)
            result_df = df.sort_values(
                by=["date", "orders_sum_rub"],
                ascending=[False, False],
            ).reset_index(drop=True)
            self._log_dataframe_state(
                stage="final_dataset",
                df=result_df,
                required_columns=[
                    "article_id",
                    "date",
                    "orders_sum_rub",
                    "orders_count",
                ],
                key_columns=[
                    "article_id",
                    "date",
                    "account",
                    "local_vendor_code",
                    "subject_name",
                ],
            )
            self._log_article_id_quality(stage="final_dataset", df=result_df)
            logger.info("build_dataset finished | rows=%s", len(result_df.index))
            print(f"ROWS RETURNED: {len(result_df)}")
            return result_df
        except Exception as error:
            logger.exception(
                "build_dataset failed | current_stage=%s | days_ago=%s | days_to=%s | error_type=%s | error=%s",
                current_stage,
                days_ago,
                days_to,
                type(error).__name__,
                error,
            )
            raise
