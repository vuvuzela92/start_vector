from __future__ import annotations

import logging
from datetime import datetime
from zoneinfo import ZoneInfo

from sqlalchemy import text

from src_oop.core.database import Database
from src_oop.core.my_gspread import GoogleTabs
from src_oop.jobs.autopilot.config import (
    MONITORING_SHEET_TITLE,
    MONITORING_TABLE_ID,
    MONITORING_TABLE_TITLE,
)

logger = logging.getLogger(__name__)


class MonitoringSnapshotWriter:
    """Записывает почасовые итоги текущего дня в технический лист мониторинга.

    Класс получает уже рассчитанные суммы из hourly-сценария автопилота и не
    перечитывает лист «Автопилот». Поэтому технический лист использует тот же
    числовой payload, который был отправлен в текущий дневной блок ПУ.
    """

    _MOSCOW_TIMEZONE = ZoneInfo("Europe/Moscow")
    _DATABASE_TABLE = "autopilot_hourly_snapshots"

    def __init__(self) -> None:
        """Открывает технический лист, обслуживающий почасовой мониторинг.

        Подключение выполняется по постоянному spreadsheet ID, а не только по
        названию. Это защищает hourly-запуск от ручного переименования таблицы.
        В `self.worksheet` сохраняется объект конкретной вкладки, чтобы дальше
        читать и изменять только «Технический лист».
        """
        self.connector = GoogleTabs(
            table_title=MONITORING_TABLE_TITLE,
            sheet_title=MONITORING_SHEET_TITLE,
            spreadsheet_id=MONITORING_TABLE_ID,
        )
        self.worksheet = self.connector.sheet_title

    def upsert_snapshot(
        self,
        orders_sum: float,
        orders_count: int,
        adv_spend: float,
        profit_by_cond_orders: float,
        snapshot_at: datetime | None = None,
    ) -> None:
        """Создаёт или обновляет снэпшот трёх итогов за текущий час.

        Бизнес-правило: повторный запуск hourly-задачи в одном часе не должен
        добавлять дубли, поэтому ключом строки служит пара «дата + час».
        Значения берутся из тех же агрегатов, которые hourly-сценарий пишет
        в текущий дневной блок листа «Автопилот».

        Сначала читаются только строки технического листа и ищется существующая
        пара «дата + час». Если такая строка найдена, она обновляется. Если нет,
        новая строка добавляется в конец таблицы. Запись выполняется через общий
        retry-механизм Google Sheets, поэтому временный 429 или 5xx не создаёт
        сразу ложный пропуск снэпшота.
        """
        snapshot_at = snapshot_at or datetime.now(self._MOSCOW_TIMEZONE)
        # В таблицу передаются отдельные дата и час, чтобы по ним можно было
        # безопасно найти строку при повторном запуске того же часа.
        snapshot_date = snapshot_at.strftime("%Y-%m-%d")
        snapshot_time = snapshot_at.strftime("%H:00")
        # Порядок значений соответствует заголовкам A:E технического листа:
        # дата, время, сумма заказов, сумма затрат, прибыль с заказов по ИУ.
        row_values = [
            snapshot_date,
            snapshot_time,
            orders_sum,
            adv_spend,
            profit_by_cond_orders,
        ]

        self._mark_missing_hours(snapshot_at=snapshot_at)
        # Сначала сохраняем срез в PostgreSQL: ошибка квоты Google Sheets
        # не должна приводить к потере подтверждённых hourly-данных.
        self._upsert_database_snapshot(
            snapshot_at=snapshot_at,
            orders_sum=orders_sum,
            orders_count=orders_count,
            adv_spend=adv_spend,
            profit_by_cond_orders=profit_by_cond_orders,
        )

        # Читаем только целевую таблицу мониторинга. Лист «Автопилот» повторно
        # не запрашивается: это снижает нагрузку и сохраняет единый payload.
        existing_rows = self.connector.get_all_values_with_retry()
        target_row: int | None = None
        # Первая строка содержит заголовки, поэтому поиск начинается со строки 2.
        for row_number, row in enumerate(existing_rows[1:], start=2):
            if len(row) < 2:
                continue
            if (
                self._normalize_date(row[0]) == snapshot_date
                and self._normalize_time(row[1]) == snapshot_time
            ):
                target_row = row_number
                break

        if target_row is None:
            # Так как ключ «дата + час» ещё не найден, добавляем новый снэпшот
            # после существующих строк, сохраняя историю предыдущих запусков.
            self.connector._execute_google_write_with_retry(
                "добавление почасового снэпшота мониторинга",
                self.worksheet.append_row,
                row_values,
                value_input_option="USER_ENTERED",
                insert_data_option="INSERT_ROWS",
            )
            logger.info(
                "Почасовой снэпшот добавлен в технический лист мониторинга: date=%s time=%s",
                snapshot_date,
                snapshot_time,
            )
            return

        # Повторный запуск в том же часу не должен создавать вторую строку:
        # обновляем найденную запись актуальными суммами.
        self.connector._execute_google_write_with_retry(
            "обновление почасового снэпшота мониторинга",
            self.worksheet.update,
            f"A{target_row}:E{target_row}",
            [row_values],
            value_input_option="USER_ENTERED",
        )
        logger.info(
            "Почасовой снэпшот обновлён в техническом листе мониторинга: date=%s time=%s row=%s",
            snapshot_date,
            snapshot_time,
            target_row,
        )

    def _upsert_database_snapshot(
        self,
        snapshot_at: datetime,
        orders_sum: float,
        orders_count: int,
        adv_spend: float,
        profit_by_cond_orders: float,
    ) -> None:
        """Сохраняет почасовой срез в PostgreSQL с защитой от дублей.

        Бизнес-правило: PostgreSQL является резервным архивом снэпшотов, а
        Google Sheets — пользовательской витриной. Ключ «дата + час» позволяет
        повторному запуску обновить срез после временной ошибки Sheets.
        """
        snapshot_local = snapshot_at.astimezone(self._MOSCOW_TIMEZONE)
        with Database.get_engine().begin() as connection:
            connection.execute(
                text(
                    f"""
                    CREATE TABLE IF NOT EXISTS {self._DATABASE_TABLE} (
                        snapshot_date DATE NOT NULL,
                        snapshot_hour SMALLINT NOT NULL,
                        snapshot_at TIMESTAMPTZ NOT NULL,
                        orders_sum NUMERIC(18, 2) NOT NULL,
                        orders_count BIGINT NOT NULL DEFAULT 0,
                        adv_spend NUMERIC(18, 2),
                        profit_by_cond_orders NUMERIC(18, 2),
                        snapshot_status VARCHAR(16) NOT NULL DEFAULT 'captured',
                        recovery_note TEXT,
                        saved_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        CONSTRAINT uq_{self._DATABASE_TABLE}_date_hour
                            UNIQUE (snapshot_date, snapshot_hour),
                        CONSTRAINT ck_{self._DATABASE_TABLE}_hour
                            CHECK (snapshot_hour BETWEEN 0 AND 23)
                    )
                    """
                )
            )
            connection.execute(
                text(
                    f"""
                    ALTER TABLE {self._DATABASE_TABLE}
                        ADD COLUMN IF NOT EXISTS snapshot_status VARCHAR(16)
                            NOT NULL DEFAULT 'captured';
                    ALTER TABLE {self._DATABASE_TABLE}
                        ADD COLUMN IF NOT EXISTS recovery_note TEXT;
                    ALTER TABLE {self._DATABASE_TABLE}
                        ADD COLUMN IF NOT EXISTS orders_count BIGINT NOT NULL DEFAULT 0;
                    ALTER TABLE {self._DATABASE_TABLE}
                        ALTER COLUMN orders_sum DROP NOT NULL;
                    ALTER TABLE {self._DATABASE_TABLE}
                        ALTER COLUMN adv_spend DROP NOT NULL;
                    ALTER TABLE {self._DATABASE_TABLE}
                        ALTER COLUMN profit_by_cond_orders DROP NOT NULL;
                    """
                )
            )
            connection.execute(
                text(
                    f"""
                    INSERT INTO {self._DATABASE_TABLE} (
                        snapshot_date, snapshot_hour, snapshot_at,
                        orders_sum, orders_count, adv_spend, profit_by_cond_orders,
                        snapshot_status, recovery_note
                    )
                    VALUES (
                        :snapshot_date, :snapshot_hour, :snapshot_at,
                        :orders_sum, :orders_count, :adv_spend, :profit_by_cond_orders,
                        'captured', NULL
                    )
                    ON CONFLICT (snapshot_date, snapshot_hour)
                    DO UPDATE SET
                        snapshot_at = EXCLUDED.snapshot_at,
                        orders_sum = EXCLUDED.orders_sum,
                        orders_count = EXCLUDED.orders_count,
                        adv_spend = EXCLUDED.adv_spend,
                        profit_by_cond_orders = EXCLUDED.profit_by_cond_orders,
                        snapshot_status = 'captured',
                        recovery_note = NULL,
                        saved_at = NOW()
                    """
                ),
                {
                    "snapshot_date": snapshot_local.date(),
                    "snapshot_hour": snapshot_local.hour,
                    "snapshot_at": snapshot_at,
                    "orders_sum": orders_sum,
                    "orders_count": orders_count,
                    "adv_spend": adv_spend,
                    "profit_by_cond_orders": profit_by_cond_orders,
                },
            )
        logger.info(
            "Почасовой снэпшот сохранен в PostgreSQL: date=%s hour=%s",
            snapshot_local.date().isoformat(),
            snapshot_local.hour,
        )

    def _mark_missing_hours(self, snapshot_at: datetime) -> None:
        """Фиксирует пропущенные часы до текущего снэпшота.

        Бизнес-правило: пропуск не заполняется текущими накопительными
        расходами Cometa. Если исторические затраты недоступны, час получает
        статус unavailable и пояснение для последующего восстановления.
        """
        snapshot_local = snapshot_at.astimezone(self._MOSCOW_TIMEZONE)
        with Database.get_engine().begin() as connection:
            connection.execute(
                text(
                    f"""
                    CREATE TABLE IF NOT EXISTS {self._DATABASE_TABLE} (
                        snapshot_date DATE NOT NULL,
                        snapshot_hour SMALLINT NOT NULL,
                        snapshot_at TIMESTAMPTZ NOT NULL,
                        orders_sum NUMERIC(18, 2) NOT NULL,
                        orders_count BIGINT NOT NULL DEFAULT 0,
                        adv_spend NUMERIC(18, 2),
                        profit_by_cond_orders NUMERIC(18, 2),
                        snapshot_status VARCHAR(16) NOT NULL DEFAULT 'captured',
                        recovery_note TEXT,
                        saved_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        CONSTRAINT uq_{self._DATABASE_TABLE}_date_hour
                            UNIQUE (snapshot_date, snapshot_hour)
                    )
                    """
                )
            )
            connection.execute(
                text(
                    f"""
                    ALTER TABLE {self._DATABASE_TABLE}
                        ADD COLUMN IF NOT EXISTS orders_count BIGINT NOT NULL DEFAULT 0;
                    ALTER TABLE {self._DATABASE_TABLE}
                        ADD COLUMN IF NOT EXISTS snapshot_status VARCHAR(16)
                            NOT NULL DEFAULT 'captured';
                    ALTER TABLE {self._DATABASE_TABLE}
                        ADD COLUMN IF NOT EXISTS recovery_note TEXT;
                    ALTER TABLE {self._DATABASE_TABLE}
                        ALTER COLUMN orders_sum DROP NOT NULL;
                    ALTER TABLE {self._DATABASE_TABLE}
                        ALTER COLUMN adv_spend DROP NOT NULL;
                    ALTER TABLE {self._DATABASE_TABLE}
                        ALTER COLUMN profit_by_cond_orders DROP NOT NULL;
                    """
                )
            )
            rows = connection.execute(
                text(
                    f"""
                    SELECT snapshot_hour
                    FROM {self._DATABASE_TABLE}
                    WHERE snapshot_date = :snapshot_date
                    """
                ),
                {"snapshot_date": snapshot_local.date()},
            ).scalars()
            existing_hours = {int(hour) for hour in rows}
            if not existing_hours:
                return

            missing_hours = [
                hour
                for hour in range(min(existing_hours), snapshot_local.hour)
                if hour not in existing_hours
            ]
            for missing_hour in missing_hours:
                order_totals = connection.execute(
                    text(
                        """
                        SELECT
                            COALESCE(SUM(seller_price), 0) AS orders_sum,
                            COUNT(*) AS orders_count
                        FROM wb_order_feed
                        WHERE status = 'created'
                            AND created_at >= :day_start
                            AND created_at < :hour_end
                        """
                    ),
                    {
                        "day_start": snapshot_local.replace(
                            hour=0,
                            minute=0,
                            second=0,
                            microsecond=0,
                        ),
                        "hour_end": snapshot_local.replace(
                            hour=missing_hour + 1,
                            minute=0,
                            second=0,
                            microsecond=0,
                        ),
                    },
                ).mappings().one()
                connection.execute(
                    text(
                        f"""
                        INSERT INTO {self._DATABASE_TABLE} (
                            snapshot_date, snapshot_hour, snapshot_at,
                            orders_sum, orders_count, adv_spend, profit_by_cond_orders,
                            snapshot_status, recovery_note
                        )
                        VALUES (
                            :snapshot_date, :snapshot_hour, :snapshot_at,
                            :orders_sum, :orders_count, NULL, NULL, 'unavailable',
                            'Точные затраты за пропущенный час отсутствуют в историческом источнике'
                        )
                        ON CONFLICT (snapshot_date, snapshot_hour) DO NOTHING
                        """
                    ),
                    {
                        "snapshot_date": snapshot_local.date(),
                        "snapshot_hour": missing_hour,
                        "snapshot_at": snapshot_local,
                        "orders_sum": float(order_totals["orders_sum"] or 0),
                        "orders_count": int(order_totals["orders_count"] or 0),
                    },
                )
        if missing_hours:
            logger.warning(
                "Найдены пропущенные часы почасового мониторинга: date=%s hours=%s; "
                "срезы отмечены как unavailable без подстановки текущих затрат",
                snapshot_local.date().isoformat(),
                missing_hours,
            )

    @staticmethod
    def _normalize_date(value: str) -> str:
        """Приводит дату из Google Sheets к ISO-формату для поиска строки.

        Google Sheets может вернуть дату как `02.10.2026`, а добавляемая
        программой строка использует ISO-представление `2026-10-02`. Обе формы
        приводятся к одному ключу, чтобы повторный запуск находил существующую
        строку независимо от отображаемого формата ячейки.
        """
        for date_format in ("%Y-%m-%d", "%d.%m.%Y"):
            try:
                return datetime.strptime(value.strip(), date_format).strftime("%Y-%m-%d")
            except ValueError:
                continue
        return value.strip()

    @staticmethod
    def _normalize_time(value: str) -> str:
        """Приводит время листа к ключу часа `HH:00`.

        В техническом листе время хранится с точностью до часа. Нормализация
        нужна для сравнения значения из Google Sheets с ключом текущего запуска
        и предотвращает создание дубля при повторной попытке записи.
        """
        try:
            return datetime.strptime(value.strip(), "%H:%M").strftime("%H:00")
        except ValueError:
            return value.strip()
