import logging

import numpy as np
import pandas as pd

from src_oop.jobs.orders_articles_analyze.repository import ArticleAnalyzeRepository


logger = logging.getLogger(__name__)


class ProcessArticleAnalyze:
    """Собирает и подготавливает итоговый DataFrame для артикульного анализа."""

    def __init__(self, repo: ArticleAnalyzeRepository):
        self.repo = repo

    @staticmethod
    def _safe_preview(
        df: pd.DataFrame,
        rows: int = 3,
        max_columns: int = 12,
    ) -> list[dict[str, object]]:
        """Возвращает безопасный preview DataFrame без чувствительных колонок."""
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
        """Логирует состояние DataFrame на текущем этапе обработки."""
        logger.info(
            "Состояние DataFrame | stage=%s | shape=%s | rows=%s | columns=%s",
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
                "Проверка обязательных колонок | stage=%s | required=%s | missing=%s",
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
                "Количество пустых значений в ключевых колонках | stage=%s | null_counts=%s",
                stage,
                null_counts,
            )

        logger.info(
            "Предпросмотр DataFrame | stage=%s | preview=%s",
            stage,
            self._safe_preview(df),
        )

    def _log_article_id_quality(self, stage: str, df: pd.DataFrame) -> None:
        """Логирует качество значений article_id на выбранном этапе."""
        if "article_id" not in df.columns:
            logger.warning(
                "Диагностика article_id пропущена | stage=%s | reason=missing_column",
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
        invalid_mask = (
            null_mask
            | empty_string_mask
            | zero_string_mask
            | zero_numeric_mask
        )

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
            "Диагностика article_id | stage=%s | total_rows=%s | null_count=%s | empty_string_count=%s | zero_string_count=%s | zero_numeric_count=%s | invalid_count=%s | invalid_preview=%s",
            stage,
            len(df.index),
            int(null_mask.sum()),
            int(empty_string_mask.sum()),
            int(zero_string_mask.sum()),
            int(zero_numeric_mask.sum()),
            int(invalid_mask.sum()),
            self._safe_preview(
                invalid_preview,
                rows=20,
                max_columns=len(preview_columns) or 12,
            ),
        )

    def _ensure_unique_merge_keys(
        self,
        df: pd.DataFrame,
        *,
        stage: str,
        key_columns: list[str],
        dataset_name: str,
    ) -> None:
        """Проверяет уникальность ключей перед merge и выбрасывает ошибку при дублях."""
        duplicated_mask = df.duplicated(subset=key_columns, keep=False)
        if not duplicated_mask.any():
            logger.info(
                "Проверка ключей перед merge пройдена | stage=%s | dataset=%s | key_columns=%s | rows=%s",
                stage,
                dataset_name,
                key_columns,
                len(df.index),
            )
            return

        duplicates_df = df.loc[duplicated_mask].copy()
        preview_columns = [
            column
            for column in [
                "account",
                "date",
                "create_dt",
                "article_id",
                "sales_revenue_rep",
                "wb_commission_rep",
                "logistics",
            ]
            if column in duplicates_df.columns
        ]
        logger.error(
            "Перед merge найдены дубли по ключам | stage=%s | dataset=%s | key_columns=%s | duplicate_rows=%s | preview=%s",
            stage,
            dataset_name,
            key_columns,
            len(duplicates_df.index),
            self._safe_preview(
                duplicates_df.loc[:, preview_columns] if preview_columns else duplicates_df,
                rows=10,
                max_columns=len(preview_columns) or 12,
            ),
        )
        raise ValueError(
            f"Перед merge набора {dataset_name} найдены дубли по ключам {key_columns}. "
            "Это может привести к размножению финансовых метрик."
        )

    def _log_sales_revenue_state(
        self,
        stage: str,
        df: pd.DataFrame,
    ) -> None:
        """Логирует состояние метрики sales_revenue_rep на выбранном этапе."""
        if "sales_revenue_rep" not in df.columns:
            logger.warning(
                "Диагностика sales_revenue_rep пропущена | stage=%s | reason=missing_column",
                stage,
            )
            return

        series = pd.to_numeric(df["sales_revenue_rep"], errors="coerce")
        non_empty_mask = series.notna()
        non_zero_mask = series.fillna(0).ne(0)
        non_zero_preview_columns = [
            column
            for column in [
                "date",
                "article_id",
                "account",
                "local_vendor_code",
                "sales_revenue_rep",
                "sales_profit_cond_rep",
                "wb_commission_rep",
                "logistics",
                "sales_count_rep",
                "returns_count_rep",
            ]
            if column in df.columns
        ]
        non_zero_preview_df = df.loc[non_zero_mask, non_zero_preview_columns].head(5)

        logger.info(
            "Диагностика sales_revenue_rep | stage=%s | has_column=%s | non_empty_count=%s | non_zero_count=%s | sum=%s | min=%s | max=%s | preview=%s",
            stage,
            True,
            int(non_empty_mask.sum()),
            int(non_zero_mask.sum()),
            round(float(series.fillna(0).sum()), 2),
            None if series.dropna().empty else round(float(series.min()), 2),
            None if series.dropna().empty else round(float(series.max()), 2),
            self._safe_preview(
                non_zero_preview_df,
                rows=5,
                max_columns=len(non_zero_preview_columns) or 12,
            ),
        )

    @staticmethod
    def _normalize_account_key(series: pd.Series) -> pd.Series:
        """Возвращает нормализованный ключ аккаунта для безопасного merge."""
        normalized = series.astype("string").str.strip().str.upper()
        return normalized.fillna("")

    def build_dataset(self, days_ago: int, days_to: int) -> pd.DataFrame:
        """
        Собирает итоговый DataFrame для артикульного анализа.

        Принимает границы периода в днях, загружает данные из всех источников,
        объединяет их и возвращает отсортированный DataFrame.
        Если на любом этапе возникает ошибка, исключение пробрасывается выше.
        """
        current_stage = "load_adv_stat"
        logger.info(
            "Начата сборка итогового DataFrame | days_ago=%s | days_to=%s",
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

            current_stage = "load_fin_report_stat"
            df_fin = self.repo.get_fin_report_stat(days_ago, days_to)
            self._log_dataframe_state(
                stage="source_daily_fin_reports_full",
                df=df_fin,
                required_columns=[
                    "article_id",
                    "date",
                    "sales_revenue_rep",
                    "sales_count_rep",
                    "returns_count_rep",
                ],
                key_columns=["article_id", "date", "account", "create_dt"],
            )
            self._log_article_id_quality(stage="source_daily_fin_reports_full", df=df_fin)
            self._log_sales_revenue_state(stage="source_daily_fin_reports_full", df=df_fin)
            df_fin = df_fin.copy()
            df_fin["merge_account_key"] = self._normalize_account_key(df_fin["account"])
            self._ensure_unique_merge_keys(
                df=df_fin,
                stage="source_daily_fin_reports_full",
                key_columns=["merge_account_key", "article_id", "date"],
                dataset_name="daily_fin_reports_full",
            )

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
                    "Оба основных источника данных пусты | days_ago=%s | days_to=%s",
                    days_ago,
                    days_to,
                )
                return pd.DataFrame()

            current_stage = "deduplicate_general_stat"
            logger.info("Начат этап обработки | stage=%s", current_stage)
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
            logger.info("Этап обработки завершён | stage=%s", current_stage)

            current_stage = "merge_general_and_adv"
            logger.info("Начат этап обработки | stage=%s", current_stage)
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
            logger.info("Этап обработки завершён | stage=%s", current_stage)

            del df_adv
            del df_gen

            current_stage = "restore_dates"
            logger.info("Начат этап обработки | stage=%s", current_stage)
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
            logger.info("Этап обработки завершён | stage=%s", current_stage)

            current_stage = "prepare_goods_directory"
            logger.info("Начат этап обработки | stage=%s", current_stage)
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
            logger.info("Этап обработки завершён | stage=%s", current_stage)

            current_stage = "enrich_from_goods_directory"
            logger.info("Начат этап обработки | stage=%s", current_stage)
            cols_to_fill = ["account", "local_vendor_code", "subject_name"]
            for col in cols_to_fill:
                missing_before = int(df[col].isna().sum()) if col in df.columns else None
                mapping_dict = df_goods.set_index("article_id")[col]
                df[col] = df[col].fillna(df["article_id"].map(mapping_dict))
                missing_after = int(df[col].isna().sum()) if col in df.columns else None
                logger.info(
                    "Заполнена колонка из справочника товаров | column=%s | missing_before=%s | missing_after=%s",
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
            logger.info("Этап обработки завершён | stage=%s", current_stage)

            current_stage = "merge_daily_fin_reports_full"
            df = df.copy()
            df["merge_account_key"] = self._normalize_account_key(df["account"])
            if "date" in df_fin.columns:
                df_fin = df_fin.copy()
                # Финансовый отчёт может быть сформирован сегодня, но содержать продажи за вчера.
                # Поэтому для merge используем бизнес-дату продаж из date_from, уже сохранённую в колонке date,
                # а не create_dt. Перед merge дополнительно выравниваем тип date, иначе pandas не объединит
                # datetime64[ns] из основного DataFrame и object/date из df_fin.
                df_fin["date"] = pd.to_datetime(df_fin["date"], errors="coerce")
            logger.info(
                "Начат merge с данными daily_fin_reports_full | stage=%s | merge_keys=%s | base_rows=%s | fin_rows=%s",
                current_stage,
                ["merge_account_key", "article_id", "date"],
                len(df.index),
                len(df_fin.index),
            )
            logger.info(
                "Правило merge для daily_fin_reports_full | stage=%s | "
                "account_source=нормализованный_account | article_id_source=article_id | "
                "date_source=DATE(fin.date_from) | create_dt_usage=только_snapshot_диагностика",
                current_stage,
            )
            logger.info(
                # Эта диагностика нужна, чтобы заранее увидеть несовместимые типы и пустые ключи:
                # тогда проблема проявится в логах до merge и до записи итоговых данных в PostgreSQL.
                "Типы ключей перед merge с daily_fin_reports_full | stage=%s | "
                "base_date_dtype=%s | fin_date_dtype=%s | base_article_id_dtype=%s | fin_article_id_dtype=%s | "
                "base_date_nulls=%s | fin_date_nulls=%s",
                current_stage,
                df["date"].dtype if "date" in df.columns else None,
                df_fin["date"].dtype if "date" in df_fin.columns else None,
                df["article_id"].dtype if "article_id" in df.columns else None,
                df_fin["article_id"].dtype if "article_id" in df_fin.columns else None,
                int(df["date"].isna().sum()) if "date" in df.columns else None,
                int(df_fin["date"].isna().sum()) if "date" in df_fin.columns else None,
            )
            self._log_sales_revenue_state(stage="before_merge_daily_fin_reports_full", df=df_fin)
            fin_columns_to_merge = [
                "merge_account_key",
                "article_id",
                "date",
                "sales_revenue_rep",
                "sales_profit_cond_rep",
                "wb_commission_rep",
                "logistics",
                "sales_count_rep",
                "returns_count_rep",
                "cost_price_sales_fin_rep",
                "cost_price_returns_fin_rep",
            ]
            available_fin_columns = [
                column for column in fin_columns_to_merge if column in df_fin.columns
            ]
            fin_metrics_sum_before_merge = (
                round(
                    float(
                        pd.to_numeric(
                            df_fin["sales_revenue_rep"],
                            errors="coerce",
                        ).fillna(0).sum()
                    ),
                    2,
                )
                if "sales_revenue_rep" in df_fin.columns
                else 0.0
            )
            df = df.merge(
                df_fin.loc[:, available_fin_columns],
                on=["merge_account_key", "article_id", "date"],
                how="left",
                validate="m:1",
            )
            if {"purchase_price", "sales_count_rep"}.issubset(df.columns):
                df["cost_price_sales_fin_rep"] = (
                    pd.to_numeric(df["purchase_price"], errors="coerce").fillna(0)
                    * pd.to_numeric(df["sales_count_rep"], errors="coerce").fillna(0)
                )
            if {"purchase_price", "returns_count_rep"}.issubset(df.columns):
                df["cost_price_returns_fin_rep"] = (
                    pd.to_numeric(df["purchase_price"], errors="coerce").fillna(0)
                    * pd.to_numeric(df["returns_count_rep"], errors="coerce").fillna(0)
                )
            if {
                "sales_revenue_rep",
                "wb_commission_rep",
                "logistics",
                "cost_price_sales_fin_rep",
            }.issubset(df.columns):
                df["sales_profit_cond_rep"] = (
                    pd.to_numeric(df["sales_revenue_rep"], errors="coerce").fillna(0)
                    - pd.to_numeric(df["wb_commission_rep"], errors="coerce").fillna(0)
                    - pd.to_numeric(df["logistics"], errors="coerce").fillna(0)
                    - pd.to_numeric(df["cost_price_sales_fin_rep"], errors="coerce").fillna(0)
                )
            logger.info(
                "Merge с данными daily_fin_reports_full завершён | stage=%s | merge_keys=%s | rows_after=%s | sales_revenue_rep_sum_before_merge=%s | non_empty_sales_revenue_rep_after_merge=%s",
                current_stage,
                ["merge_account_key", "article_id", "date"],
                len(df.index),
                fin_metrics_sum_before_merge,
                int(pd.to_numeric(df["sales_revenue_rep"], errors="coerce").notna().sum())
                if "sales_revenue_rep" in df.columns
                else 0,
            )
            self._log_dataframe_state(
                stage="merged_daily_fin_reports_full",
                df=df,
                required_columns=["article_id", "date", "sales_revenue_rep"],
                key_columns=["article_id", "date", "account", "sales_revenue_rep"],
            )
            self._log_sales_revenue_state(stage="merged_daily_fin_reports_full", df=df)
            if "merge_account_key" in df.columns:
                df = df.drop(columns=["merge_account_key"])
            logger.info("Этап обработки завершён | stage=%s", current_stage)
            del df_fin

            current_stage = "fill_numeric_nulls"
            logger.info("Начат этап обработки | stage=%s", current_stage)
            numeric_cols = df.select_dtypes(include="number").columns
            logger.info(
                "Определены числовые колонки | stage=%s | columns=%s",
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
            self._log_sales_revenue_state(stage="numeric_nulls_filled_first_pass", df=df)
            logger.info("Этап обработки завершён | stage=%s", current_stage)

            current_stage = "replace_infinite_values"
            logger.info("Начат этап обработки | stage=%s", current_stage)
            inf_count = int(np.isinf(df.select_dtypes(include="number").to_numpy()).sum())
            logger.info(
                "Подсчитаны бесконечные значения в числовых колонках | stage=%s | count=%s",
                current_stage,
                inf_count,
            )
            df = df.replace([np.inf, -np.inf], np.nan)
            self._log_article_id_quality(stage="replace_infinite_values", df=df)
            self._log_sales_revenue_state(stage="replace_infinite_values", df=df)
            logger.info("Этап обработки завершён | stage=%s", current_stage)

            current_stage = "fill_numeric_nulls_second_pass"
            logger.info("Начат этап обработки | stage=%s", current_stage)
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
            self._log_sales_revenue_state(stage="numeric_nulls_filled_second_pass", df=df)
            logger.info("Этап обработки завершён | stage=%s", current_stage)

            current_stage = "normalize_large_numeric_types"
            logger.info("Начат этап обработки | stage=%s", current_stage)
            converted_columns: list[str] = []
            for col in numeric_cols:
                if df[col].max() > 2147483647 or df[col].min() < -2147483648:
                    df[col] = df[col].astype(float)
                    converted_columns.append(col)
            logger.info(
                "Завершена нормализация крупных числовых значений | stage=%s | converted_columns=%s",
                current_stage,
                converted_columns,
            )
            self._log_article_id_quality(stage="normalize_large_numeric_types", df=df)
            self._log_sales_revenue_state(stage="normalize_large_numeric_types", df=df)
            logger.info("Этап обработки завершён | stage=%s", current_stage)

            current_stage = "final_sort"
            logger.info("Начат этап обработки | stage=%s", current_stage)
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
            self._log_sales_revenue_state(stage="final_dataset", df=result_df)
            logger.info("Сборка итогового DataFrame завершена | rows=%s", len(result_df.index))
            print(f"Возвращено строк: {len(result_df)}")
            return result_df
        except Exception as error:
            logger.exception(
                "Ошибка при сборке итогового DataFrame | current_stage=%s | days_ago=%s | days_to=%s | error_type=%s | error=%s",
                current_stage,
                days_ago,
                days_to,
                type(error).__name__,
                error,
            )
            raise
