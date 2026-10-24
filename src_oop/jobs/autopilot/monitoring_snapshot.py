from __future__ import annotations

import argparse
import json
import logging
from dataclasses import dataclass
from datetime import date, datetime
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from sqlalchemy import text
from sqlalchemy.engine import Connection, Engine

from src_oop.core.database import Database
from src_oop.core.my_gspread import GoogleTabs
from src_oop.jobs.autopilot.config import (
    MONITORING_SHEET_TITLE,
    MONITORING_TABLE_ID,
    MONITORING_TABLE_TITLE,
)

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class MonitoringPublishResult:
    """Отделяет доставку архива от успеха основного обновления ПУ."""

    synced: int = 0
    conflicts: int = 0
    failed: bool = False
    locked: bool = False


class MonitoringSnapshotWriter:
    """Архивирует часы 1–23 и дневной итог 24-го часа для Google Sheets."""

    _MOSCOW_TIMEZONE = ZoneInfo("Europe/Moscow")
    _DATABASE_TABLE = "autopilot_hourly_snapshots"
    # Одна блокировка для всех экземпляров публикации в технический лист.
    _PUBLISH_LOCK = 71190426001
    _BATCH_SIZE = 100

    def __init__(self, engine: Engine | None = None) -> None:
        """Готовит архив без обращения к Sheets, чтобы квота не мешала сохранению."""
        self.engine = engine if engine is not None else Database.get_engine()
        self.connector: GoogleTabs | None = None

    def _connect_sheet(self) -> GoogleTabs:
        """Открывает пользовательскую витрину только при публикации или сверке."""
        if self.connector is None:
            self.connector = GoogleTabs(
                table_title=MONITORING_TABLE_TITLE,
                sheet_title=MONITORING_SHEET_TITLE,
                spreadsheet_id=MONITORING_TABLE_ID,
            )
        return self.connector

    def upsert_snapshot(
        self,
        orders_sum: float,
        orders_count: int,
        adv_spend: float,
        profit_by_cond_orders: float,
        snapshot_at: datetime | None = None,
    ) -> bool:
        """Сохраняет первый достоверный срез часа независимо от доступности Sheets.

        Конфликт по дате и часу разрешает замену только unavailable. Повторный
        успешный запуск не меняет значения, время и статус доставки. Нули
        допустимы, нечисловые значения и бесконечности запрещены.
        """
        snapshot_at = snapshot_at or datetime.now(self._MOSCOW_TIMEZONE)
        if snapshot_at.tzinfo is None:
            raise ValueError("Для среза мониторинга требуется время с часовым поясом.")
        snapshot_at = snapshot_at.astimezone(self._MOSCOW_TIMEZONE)
        if orders_count < 0 or int(orders_count) != orders_count:
            raise ValueError("Количество заказов должно быть целым неотрицательным.")
        saved = self._upsert_by_key(
            snapshot_date=snapshot_at.date(),
            snapshot_hour=snapshot_at.hour,
            snapshot_at=snapshot_at,
            orders_sum=orders_sum,
            orders_count=orders_count,
            adv_spend=adv_spend,
            profit_by_cond_orders=profit_by_cond_orders,
        )
        logger.info(
            "Мониторинг: %s | date=%s hour=%s snapshot_at=%s",
            "срез сохранён, ожидает публикации" if saved else "первый успешный срез уже сохранён",
            snapshot_at.date(),
            snapshot_at.hour,
            snapshot_at.isoformat(),
        )
        # Ошибка разметки пропусков не отменяет сохранение достоверного среза.
        try:
            self._mark_missing_hours(snapshot_at)
        except Exception as error:
            logger.warning(
                "Не удалось отметить пропуски мониторинга; срез сохранён, сценарий продолжается: "
                "error_type=%s",
                type(error).__name__,
            )
        return saved

    def upsert_daily_snapshot(
        self,
        report_date: date,
        orders_sum: float,
        orders_count: int,
        adv_spend: float,
        profit_by_cond_orders: float,
        snapshot_at: datetime | None = None,
    ) -> bool:
        """Сохраняет итог завершённых суток как 24-й час мониторинга.

        `report_date` является логической датой данных, а `snapshot_at` хранит
        фактическое время их получения на следующий день. Успешный итог не
        перезаписывается; запись unavailable может быть заменена полным итогом.
        """
        snapshot_at = snapshot_at or datetime.now(self._MOSCOW_TIMEZONE)
        if snapshot_at.tzinfo is None:
            raise ValueError("Для дневного среза требуется время с часовым поясом.")
        snapshot_at = snapshot_at.astimezone(self._MOSCOW_TIMEZONE)
        if snapshot_at.date() < report_date:
            raise ValueError("Время получения дневного среза не может быть раньше даты отчёта.")
        if orders_count < 0 or int(orders_count) != orders_count:
            raise ValueError("Количество заказов должно быть целым неотрицательным.")
        saved = self._upsert_by_key(
            snapshot_date=report_date,
            snapshot_hour=24,
            snapshot_at=snapshot_at,
            orders_sum=orders_sum,
            orders_count=orders_count,
            adv_spend=adv_spend,
            profit_by_cond_orders=profit_by_cond_orders,
            replace_captured=True,
        )
        logger.info(
            "Мониторинг: итог 24-го часа %s | date=%s snapshot_at=%s",
            "сохранён, ожидает публикации" if saved else "уже сохранён",
            report_date,
            snapshot_at.isoformat(),
        )
        return saved

    def _upsert_by_key(
        self,
        snapshot_date: date,
        snapshot_hour: int,
        snapshot_at: datetime,
        orders_sum: float,
        orders_count: int,
        adv_spend: float,
        profit_by_cond_orders: float,
        replace_captured: bool = False,
    ) -> bool:
        """Атомарно сохраняет срез по явному ключу даты и часа.

        Общий путь защищает правило первого успешного среза и используется как
        для hourly-часов, так и для дневного 24-го часа.
        """
        params = {
            "snapshot_date": snapshot_date,
            "snapshot_hour": snapshot_hour,
            "snapshot_at": snapshot_at,
            "orders_sum": self._money(orders_sum),
            "orders_count": orders_count,
            "adv_spend": self._money(adv_spend),
            "profit_by_cond_orders": self._money(profit_by_cond_orders),
            "replace_captured": replace_captured,
        }
        with self.engine.begin() as connection:
            return (
                connection.execute(
                    text(f"""
                INSERT INTO {self._DATABASE_TABLE} (
                    snapshot_date, snapshot_hour, snapshot_at, orders_sum,
                    orders_count, adv_spend, profit_by_cond_orders,
                    snapshot_status, recovery_note, delivery_status
                ) VALUES (
                    :snapshot_date, :snapshot_hour, :snapshot_at, :orders_sum,
                    :orders_count, :adv_spend, :profit_by_cond_orders,
                    'captured', NULL, 'pending'
                )
                ON CONFLICT (snapshot_date, snapshot_hour) DO UPDATE SET
                    snapshot_at = EXCLUDED.snapshot_at,
                    orders_sum = EXCLUDED.orders_sum,
                    orders_count = EXCLUDED.orders_count,
                    adv_spend = EXCLUDED.adv_spend,
                    profit_by_cond_orders = EXCLUDED.profit_by_cond_orders,
                    snapshot_status = 'captured', recovery_note = NULL,
                    delivery_status = 'pending', synced_at = NULL,
                    last_delivery_error = NULL, saved_at = NOW()
                WHERE {self._DATABASE_TABLE}.snapshot_status = 'unavailable'
                    OR :replace_captured = TRUE
                RETURNING snapshot_hour
            """),
                    params,
                ).scalar_one_or_none()
                is not None
            )

    @staticmethod
    def _money(value: object) -> Decimal:
        """Приводит итог к копейкам и запрещает ложные срезы с NaN или пропусками."""
        try:
            result = Decimal(str(value))
            if not result.is_finite():
                raise ValueError("Итог мониторинга должен быть конечным числом.")
            return result.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        except InvalidOperation:
            raise ValueError("Итог мониторинга не является допустимой денежной суммой.") from None

    def _mark_missing_hours(self, snapshot_at: datetime) -> None:
        """Помечает завершённые часы без срезов, не выдумывая исторические суммы.

        Для текущих суток создаются часы 1–23 до текущего запуска, а для
        предыдущей даты архива также резервируется 24-й час. Существующие
        unavailable и captured не меняются.
        """
        with self.engine.begin() as connection:
            connection.execute(
                text(f"""
                WITH days AS (
                    SELECT CAST(:snapshot_date AS DATE) AS day, :snapshot_hour - 1 AS last_hour
                    UNION ALL
                    SELECT MAX(snapshot_date), 24 FROM {self._DATABASE_TABLE}
                    WHERE snapshot_date < :snapshot_date
                )
                INSERT INTO {self._DATABASE_TABLE} (
                    snapshot_date, snapshot_hour, snapshot_at, orders_sum,
                    orders_count, adv_spend, profit_by_cond_orders,
                    snapshot_status, recovery_note, delivery_status
                )
                SELECT day, hour, :snapshot_at, NULL, NULL, NULL, NULL,
                    'unavailable', 'Достоверный почасовой срез не был сохранён', 'pending'
                FROM days CROSS JOIN LATERAL generate_series(1, last_hour) AS hour
                WHERE day IS NOT NULL
                ON CONFLICT (snapshot_date, snapshot_hour) DO NOTHING
            """),
                {
                    "snapshot_date": snapshot_at.date(),
                    "snapshot_hour": snapshot_at.hour,
                    "snapshot_at": snapshot_at,
                },
            )

    def publish_pending(self) -> MonitoringPublishResult:
        """Доставляет captured всех дат порциями по 100 под общей блокировкой.

        Транзакционная advisory-блокировка освобождается также при обрыве БД.
        После сбоя между Sheets и commit повторная сверка находит ту же строку.
        Ошибки доставки не отменяют ранее сохранённые срезы и основной сценарий.
        """
        result = MonitoringPublishResult()
        try:
            with self.engine.begin() as connection:
                locked = connection.execute(
                    text("SELECT pg_try_advisory_xact_lock(:key)"),
                    {"key": self._PUBLISH_LOCK},
                ).scalar_one()
                if not locked:
                    result.locked = True
                    logger.info(
                        "Мониторинг: публикацию выполняет другой запуск, архив ожидает доставки."
                    )
                    return result
                cursor_date, cursor_hour = "0001-01-01", -1
                while True:
                    rows = [
                        dict(row)
                        for row in connection.execute(
                            text(f"""
                        SELECT snapshot_date, snapshot_hour, orders_sum,
                            adv_spend, profit_by_cond_orders
                        FROM {self._DATABASE_TABLE}
                        WHERE snapshot_status = 'captured' AND delivery_status = 'pending'
                            AND (snapshot_date, snapshot_hour) > (CAST(:day AS DATE), :hour)
                        ORDER BY snapshot_date, snapshot_hour
                        LIMIT :batch_size
                    """),
                            {
                                "day": cursor_date,
                                "hour": cursor_hour,
                                "batch_size": self._BATCH_SIZE,
                            },
                        ).mappings()
                    ]
                    if not rows:
                        break
                    cursor_date = rows[-1]["snapshot_date"]
                    cursor_hour = rows[-1]["snapshot_hour"]
                    try:
                        statuses = self._connect_sheet().execute_with_retry(
                            "публикация архива мониторинга со сверкой ключей",
                            self._publish_batch,
                            rows,
                        )
                    except Exception as error:
                        code = type(error).__name__[:64]
                        self._record_delivery(connection, rows, [code] * len(rows))
                        result.failed = True
                        logger.warning(
                            "Мониторинг ожидает публикации после ошибки; основной сценарий продолжается: "
                            "error_type=%s rows=%s",
                            code,
                            len(rows),
                        )
                        break
                    self._record_delivery(connection, rows, statuses)
                    result.synced += statuses.count(None)
                    result.conflicts += len(statuses) - statuses.count(None)
        except Exception as error:
            result.synced = 0
            result.failed = True
            logger.error(
                "Мониторинг: доставка не подтверждена в БД, повторим в следующем запуске; "
                "основной сценарий продолжается: error_type=%s",
                type(error).__name__,
            )
        logger.info(
            "Мониторинг: итог публикации | synced=%s conflicts=%s failed=%s",
            result.synced,
            result.conflicts,
            result.failed,
        )
        return result

    def _record_delivery(
        self,
        connection: Connection,
        rows: list[dict[str, Any]],
        statuses: list[str | None],
    ) -> None:
        """Подтверждает только записанные строки; конфликт или ошибка оставляют pending."""
        connection.execute(
            text(f"""
            UPDATE {self._DATABASE_TABLE}
            SET delivery_status = CASE WHEN :error IS NULL THEN 'synced' ELSE 'pending' END,
                synced_at = CASE WHEN :error IS NULL THEN NOW() ELSE NULL END,
                last_delivery_error = :error
            WHERE snapshot_date = :day AND snapshot_hour = :hour
                AND snapshot_status = 'captured'
        """),
            [
                {"day": row["snapshot_date"], "hour": row["snapshot_hour"], "error": error}
                for row, error in zip(rows, statuses, strict=True)
            ],
        )

    def _inspect_batch(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Сверяет ключи и суммы, сохраняя ручные значения и обнаруживая дубли.

        Читаем A:B и только C:E проверяемых строк. Пустую строку с существующим
        ключом можно заполнить; частично заполненные или отличающиеся суммы
        требуют разбора. Проверяются также места для новых строк.
        """
        sheet = self._connect_sheet().sheet_title
        keys = sheet.get("A:B", value_render_option="FORMATTED_VALUE")
        by_key: dict[tuple[str, str], list[int]] = {}
        for number, row in enumerate(keys[1:], start=2):
            if len(row) >= 2:
                key = (self._normalize_date(str(row[0])), self._normalize_time(str(row[1])))
                by_key.setdefault(key, []).append(number)
        next_row = max(len(keys) + 1, 2)
        plans: list[dict[str, Any]] = []
        for row in rows:
            key = (str(row["snapshot_date"]), self._format_sheet_hour(row["snapshot_hour"]))
            matches = by_key.get(key, [])
            target = matches[0] if matches else next_row
            if not matches:
                next_row += 1
            plans.append(
                {
                    "key": key,
                    "row": target,
                    "values": [
                        *key,
                        *[
                            float(self._money(row[name]))
                            for name in (
                                "orders_sum",
                                "adv_spend",
                                "profit_by_cond_orders",
                            )
                        ],
                    ],
                    "state": "duplicate" if len(matches) > 1 else "missing",
                    "existing_key": bool(matches),
                    "actual": [],
                }
            )
        readable = [plan for plan in plans if plan["row"] <= sheet.row_count]
        existing = (
            sheet.batch_get(
                [f"C{plan['row']}:E{plan['row']}" for plan in readable],
                value_render_option="UNFORMATTED_VALUE",
            )
            if readable
            else []
        )
        for plan, values in zip(readable, existing, strict=True):
            actual = values[0] if values else []
            plan["actual"] = actual
            if plan["state"] == "duplicate":
                continue
            if not any(value not in (None, "") for value in actual):
                continue
            try:
                equal = len(actual) == 3 and all(
                    self._money(value) == self._money(expected)
                    for value, expected in zip(actual, plan["values"][2:], strict=True)
                )
            except (ValueError, TypeError):
                equal = False
            plan["state"] = "equal" if equal and plan["existing_key"] else "conflict"
        return plans

    def _publish_batch(self, rows: list[dict[str, Any]]) -> list[str | None]:
        """Сверяет лист заново и пишет фиксированные диапазоны без append.

        Если предыдущая попытка записала строку, но потеряла ответ, новая
        попытка узнает ключ и подтвердит её. Конфликты обычных часов не
        перезаписываются; для закрытого дня строка 24:00 обновляется из
        PostgreSQL, поскольку этот источник является достоверным итогом.
        """
        plans = self._inspect_batch(rows)
        sheet = self._connect_sheet().sheet_title
        # Для закрытого дня PostgreSQL является источником истины: строка 24:00
        # может содержать устаревший результат предыдущего восстановления.
        overwrite_conflicts = {
            plan["row"]
            for plan in plans
            if plan["state"] == "conflict" and plan["key"][1] == "24:00"
        }
        updates = [
            {"range": f"A{plan['row']}:E{plan['row']}", "values": [plan["values"]]}
            for plan in plans
            if plan["state"] == "missing" or plan["row"] in overwrite_conflicts
        ]
        if updates:
            required_rows = max(
                plan["row"]
                for plan in plans
                if plan["state"] == "missing" or plan["row"] in overwrite_conflicts
            )
            if required_rows > sheet.row_count:
                sheet.resize(rows=required_rows)
            sheet.batch_update(updates, value_input_option="RAW")
        statuses: list[str | None] = []
        for plan in plans:
            overwritten = plan["row"] in overwrite_conflicts
            error = (
                None
                if overwritten or plan["state"] not in ("conflict", "duplicate")
                else f"sheet_{plan['state']}"
            )
            statuses.append(error)
            if overwritten:
                logger.info(
                    "Мониторинг: конфликт 24-го часа перезаписан данными PostgreSQL | "
                    "date=%s time=%s row=%s",
                    *plan["key"],
                    plan["row"],
                )
            elif error:
                logger.warning(
                    "Мониторинг: требуется разбор строки, значения сохранены без изменений; "
                    "остальные строки публикуются | date=%s time=%s row=%s code=%s",
                    *plan["key"],
                    plan["row"],
                    error,
                )
        return statuses

    def reconcile(self) -> list[dict[str, Any]]:
        """Возвращает отчёт сверки архива и Sheets без изменения данных.

        В отчёт входят unavailable, отсутствующие строки, различия и дубли.
        Отчёт нужен перед внедрением; фактическую доставку выполняет publish_pending.
        """
        report: list[dict[str, Any]] = []
        with self.engine.connect() as connection:
            with connection.begin():
                connection.execute(text("SET TRANSACTION READ ONLY"))
                rows = [
                    dict(row)
                    for row in connection.execute(
                        text(f"""
                    SELECT snapshot_date, snapshot_hour, snapshot_status,
                        orders_sum, adv_spend, profit_by_cond_orders
                    FROM {self._DATABASE_TABLE}
                    ORDER BY snapshot_date, snapshot_hour
                """)
                    ).mappings()
                ]
        captured = [row for row in rows if row["snapshot_status"] == "captured"]
        for start in range(0, len(captured), self._BATCH_SIZE):
            plans = self._connect_sheet().execute_with_retry(
                "сверка архива мониторинга",
                self._inspect_batch,
                captured[start : start + self._BATCH_SIZE],
            )
            report.extend(plan for plan in plans if plan["state"] != "equal")
        report.extend(
            {
                "key": [
                    str(row["snapshot_date"]),
                    self._format_sheet_hour(row["snapshot_hour"]),
                ],
                "state": "unavailable",
            }
            for row in rows
            if row["snapshot_status"] != "captured"
        )
        return report

    def migrate(self) -> None:
        """Явно применяет повторяемую миграцию перед внедрением мониторинга.

        Hourly не выполняет DDL. Старые срезы получают pending для сверки,
        повтор миграции не сбрасывает уже доставленные строки.
        """
        sql = (
            Path(__file__)
            .with_name("monitoring_snapshot_migration.sql")
            .read_text(encoding="utf-8")
        )
        with self.engine.begin() as connection:
            connection.execute(
                text("SELECT pg_advisory_xact_lock(:key)"), {"key": self._PUBLISH_LOCK}
            )
            connection.execute(text(sql))
        logger.info("Миграция архива мониторинга применена.")

    @staticmethod
    def _normalize_date(value: str) -> str:
        """Сводит отображаемую дату Sheets к ISO-ключу без дублей при смене формата."""
        for fmt in ("%Y-%m-%d", "%d.%m.%Y"):
            try:
                return datetime.strptime(value.strip(), fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue
        return value.strip()

    @staticmethod
    def _normalize_time(value: str) -> str:
        """Узнаёт час в форматах HH:MM, HH:MM:SS и 24:00 для доставки."""
        if value.strip() in ("24:00", "24:00:00"):
            return "24:00"
        for fmt in ("%H:%M", "%H:%M:%S"):
            try:
                return datetime.strptime(value.strip(), fmt).strftime("%H:00")
            except ValueError:
                continue
        return value.strip()

    @staticmethod
    def _format_sheet_hour(snapshot_hour: int) -> str:
        """Формирует ключ времени для Sheets, сохраняя специальный час 24:00."""
        return "24:00" if snapshot_hour == 24 else f"{snapshot_hour:02d}:00"


def main() -> None:
    """Запускает явную миграцию, сверку или доставку архива при внедрении.

    Команда reconcile только читает БД и Sheets и выдаёт JSON-отчёт. Ошибки
    выводятся без секретов; неуспешная доставка возвращает ненулевой код.
    """
    parser = argparse.ArgumentParser(description="Обслуживание архива почасового мониторинга")
    parser.add_argument("action", choices=("migrate", "reconcile", "publish"))
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
    try:
        writer = MonitoringSnapshotWriter()
        if args.action == "migrate":
            writer.migrate()
        elif args.action == "reconcile":
            print(json.dumps(writer.reconcile(), ensure_ascii=False, indent=2))
        else:
            result = writer.publish_pending()
            if result.failed or result.conflicts or result.locked:
                raise SystemExit(1)
    except Exception as error:
        logger.error("Обслуживание мониторинга прервано: error_type=%s", type(error).__name__)
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
