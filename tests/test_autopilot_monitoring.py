"""Проверки архива на отдельном PostgreSQL и отказов Sheets без внешних записей."""

from __future__ import annotations

import asyncio
import os
import re
import shutil
import socket
import subprocess
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime
from pathlib import Path
from unittest.mock import AsyncMock, Mock
from zoneinfo import ZoneInfo

import gspread
import pandas as pd
import pytest
import requests
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

from src_oop.core.my_gspread import GoogleTabs
from src_oop.jobs.autopilot import service as service_module
from src_oop.jobs.autopilot.cometa_adv_spend import CometaAdvSpendClient
from src_oop.jobs.autopilot.monitoring_snapshot import MonitoringSnapshotWriter
from src_oop.jobs.autopilot.repository import AutopilotRepository
from src_oop.jobs.autopilot.service import AutopilotHourlyService
from src_oop.jobs.autopilot.sheets_writer import MetricWriteResult


@pytest.fixture(scope="session")
def postgres_engine(tmp_path_factory):
    """Поднимает временный PostgreSQL на localhost, не используя окружение рабочей БД."""
    configured = os.getenv("MONITORING_TEST_POSTGRES_BIN")
    candidates = [Path(configured)] if configured else []
    if shutil.which("pg_ctl"):
        candidates.append(Path(shutil.which("pg_ctl")).parent)
    candidates.extend(Path("C:/Program Files/PostgreSQL").glob("*/bin"))
    suffix = ".exe" if os.name == "nt" else ""
    binary = next((path for path in candidates if (path / f"initdb{suffix}").exists()), None)
    if binary is None:
        pytest.skip("Для интеграционной проверки нужен локальный PostgreSQL.")
    root = tmp_path_factory.mktemp("monitoring_postgres")
    data = root / "data"
    flags = subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0
    initialized = subprocess.run(
        [
            str(binary / f"initdb{suffix}"),
            "-D",
            str(data),
            "-U",
            "monitoring_test",
            "-A",
            "trust",
            "--no-locale",
            "--encoding=UTF8",
        ],
        capture_output=True,
        timeout=60,
        creationflags=flags,
    )
    assert initialized.returncode == 0, initialized.stderr.decode(errors="replace")
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
    pg_ctl = str(binary / f"pg_ctl{suffix}")
    engine = None
    try:
        subprocess.run(
            [
                pg_ctl,
                "-D",
                str(data),
                "-l",
                str(root / "postgres.log"),
                "-w",
                "-t",
                "30",
                "-o",
                f"-h 127.0.0.1 -p {port} -F",
                "start",
            ],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=40,
            creationflags=flags,
        )
        engine = create_engine(
            URL.create(
                "postgresql+psycopg2",
                username="monitoring_test",
                host="127.0.0.1",
                port=port,
                database="postgres",
            )
        )
        yield engine
    finally:
        if engine is not None:
            engine.dispose()
        subprocess.run(
            [pg_ctl, "-D", str(data), "-m", "immediate", "-w", "stop"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=40,
            creationflags=flags,
        )


@pytest.fixture
def archive(postgres_engine):
    """Очищает только таблицы временной БД и применяет настоящую миграцию архива."""
    with postgres_engine.begin() as connection:
        connection.execute(text("DROP TABLE IF EXISTS autopilot_hourly_snapshots, wb_order_feed"))
    writer = MonitoringSnapshotWriter(postgres_engine)
    writer.migrate()
    return writer


@pytest.fixture(autouse=True)
def no_sleep(monkeypatch):
    """Убирает реальные паузы в проверках отказов, сохраняя число попыток."""
    monkeypatch.setattr("src_oop.core.my_gspread.time.sleep", Mock())
    monkeypatch.setattr(service_module, "execute_google_write_pause", Mock())


class MemorySheet:
    """Моделирует фиксированные строки Sheets и потерю ответа после записи."""

    def __init__(self):
        """Создаёт технический лист с заголовком для проверок повторной доставки."""
        self.rows = [["Дата", "Время", "Заказы", "Расходы", "Прибыль"]]
        self.row_count = 1000
        self.writes = 0
        self.lose_reply = False
        self.unavailable = False

    def get(self, range_name, **kwargs):
        """Возвращает ключи; недоступность листа воспроизводит сетевой сбой."""
        if self.unavailable:
            raise requests.Timeout("Тестовая недоступность листа")
        assert range_name == "A:B"
        return [row[:2] for row in self.rows]

    def batch_get(self, ranges, **kwargs):
        """Возвращает значения для сверки архива перед идемпотентной записью."""
        output = []
        for cell_range in ranges:
            number = int(re.search(r"\d+", cell_range).group())
            values = self.rows[number - 1][2:5] if number <= len(self.rows) else []
            output.append([values] if values else [])
        return output

    def batch_update(self, updates, **kwargs):
        """Записывает фиксированные строки и при необходимости теряет подтверждение."""
        self.writes += 1
        for update in updates:
            number = int(re.search(r"\d+", update["range"]).group())
            while len(self.rows) < number:
                self.rows.append([])
            self.rows[number - 1] = update["values"][0]
        if self.lose_reply:
            self.lose_reply = False
            raise requests.Timeout("Ответ потерян после успешной записи")

    def resize(self, rows):
        """Задаёт абсолютный размер, позволяющий безопасно повторить расширение листа."""
        self.row_count = rows


def connect_memory(writer):
    """Подставляет лист в памяти, сохраняя реальный механизм retry GoogleTabs."""
    connector = GoogleTabs.__new__(GoogleTabs)
    connector.sheet_title = MemorySheet()
    writer.connector = connector
    return connector.sheet_title


def moment(hour=8, minute=0, day=19):
    """Фиксирует московское время для проверок почасового ключа и полуночи."""
    return datetime(2026, 10, day, hour, minute, tzinfo=ZoneInfo("Europe/Moscow"))


def capture(writer, at=None, amount=100):
    """Сохраняет полный финансовый срез без обращения к внешним источникам."""
    return writer.upsert_snapshot(amount, 2, 10, 20, snapshot_at=at or moment())


def db_rows(writer):
    """Читает архив тестовой БД для проверки сохранности и доставки срезов."""
    with writer.engine.connect() as connection:
        return [
            dict(row)
            for row in connection.execute(
                text(
                    "SELECT snapshot_date, snapshot_hour, snapshot_at, orders_sum, orders_count, "
                    "adv_spend, snapshot_status, delivery_status, synced_at, last_delivery_error "
                    "FROM autopilot_hourly_snapshots ORDER BY snapshot_date, snapshot_hour"
                )
            ).mappings()
        ]


def test_first_success_and_replacement_of_unavailable(archive):
    """Успех в :30 заполняет час; повтор не меняет первый срез, включая время."""
    capture(archive, moment(9))
    assert capture(archive, moment(8, 31), 200)
    assert not capture(archive, moment(8, 45), 300)
    row = next(row for row in db_rows(archive) if row["snapshot_hour"] == 8)
    assert row["orders_sum"] == 200
    assert row["snapshot_at"] == moment(8, 31)
    assert row["delivery_status"] == "pending"
    gap = next(row for row in db_rows(archive) if row["snapshot_hour"] == 7)
    assert gap["snapshot_status"] == "unavailable"
    assert gap["orders_sum"] is gap["orders_count"] is gap["adv_spend"] is None


def test_parallel_capture_keeps_one_winner(archive):
    """Два одновременных запуска сохраняют ровно один первый срез в настоящей БД."""
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(capture, archive, moment(), amount) for amount in (100, 200)]
        assert sorted(future.result() for future in futures) == [False, True]
    captured = [row for row in db_rows(archive) if row["snapshot_status"] == "captured"]
    assert len(captured) == 1
    assert captured[0]["orders_sum"] in (100, 200)


def test_migration_preserves_legacy_snapshots(archive):
    """Старая структура получает очередь доставки без изменения архивных значений."""
    with archive.engine.begin() as connection:
        connection.execute(
            text("""
            DROP TABLE autopilot_hourly_snapshots;
            CREATE TABLE autopilot_hourly_snapshots (
                snapshot_date DATE NOT NULL, snapshot_hour SMALLINT NOT NULL,
                snapshot_at TIMESTAMPTZ NOT NULL, orders_sum NUMERIC(18,2) NOT NULL,
                adv_spend NUMERIC(18,2), profit_by_cond_orders NUMERIC(18,2),
                saved_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE (snapshot_date, snapshot_hour)
            );
            INSERT INTO autopilot_hourly_snapshots VALUES
                ('2026-10-19', 8, '2026-10-19 08:01:00+03', 150, 10, 20, NOW())
        """)
        )
    archive.migrate()
    assert not capture(archive, moment(8, 31), 300)
    row = next(row for row in db_rows(archive) if row["snapshot_hour"] == 8)
    assert row["orders_sum"] == 150 and row["snapshot_at"] == moment(8, 1)
    assert row["delivery_status"] == "pending"
    connect_memory(archive)
    assert archive.publish_pending().synced == 1
    archive.migrate()
    assert archive.publish_pending().synced == 0


def test_startup_publishes_archive_even_when_pu_is_unavailable(monkeypatch, archive):
    """Отказ первоначального чтения ПУ не мешает доставке ранее сохранённого часа."""
    capture(archive, moment(7))
    sheet = connect_memory(archive)
    monkeypatch.setattr(service_module, "MonitoringSnapshotWriter", Mock(return_value=archive))
    monkeypatch.setattr(service_module, "GoogleTabs", Mock(side_effect=requests.Timeout()))
    with pytest.raises(requests.Timeout):
        asyncio.run(AutopilotHourlyService().run())
    assert len(sheet.rows) == 2
    assert db_rows(archive)[-1]["delivery_status"] == "synced"


def test_delivery_recovers_after_sheets_failure_and_lost_reply(archive):
    """Архив переживает недоступность Sheets и потерю ответа без повторной строки."""
    capture(archive)
    sheet = connect_memory(archive)
    sheet.unavailable = True
    assert archive.publish_pending().failed
    row = db_rows(archive)[-1]
    assert row["delivery_status"] == "pending"
    assert row["last_delivery_error"] == "Timeout"
    sheet.unavailable = False
    sheet.lose_reply = True
    result = archive.publish_pending()
    assert result.synced == 1 and not result.failed
    assert len(sheet.rows) == 2 and sheet.writes == 1
    assert archive.publish_pending().synced == 0
    assert db_rows(archive)[-1]["synced_at"] is not None


def test_commit_failure_reconciles_previously_written_row(archive, monkeypatch):
    """Сбой подтверждения в БД после записи Sheets не создаёт дубль при восстановлении."""
    capture(archive)
    sheet = connect_memory(archive)
    real_record = archive._record_delivery
    monkeypatch.setattr(archive, "_record_delivery", Mock(side_effect=RuntimeError("Сбой БД")))
    assert archive.publish_pending().failed
    assert len(sheet.rows) == 2
    monkeypatch.setattr(archive, "_record_delivery", real_record)
    assert archive.publish_pending().synced == 1
    assert sheet.writes == 1


def test_conflicts_and_duplicates_are_reported_without_overwrite(archive):
    """Сверка сохраняет ручные различия и дубли, остальные срезы доставляются."""
    capture(archive, moment(8))
    capture(archive, moment(9))
    capture(archive, moment(10))
    sheet = connect_memory(archive)
    sheet.rows.extend(
        [
            ["19.10.2026", "08:00:00", 999, 10, 20],
            ["2026-10-19", "09:00", 100, 10, 20],
            ["2026-10-19", "09:00", 100, 10, 20],
        ]
    )
    report = archive.reconcile()
    assert {row["state"] for row in report} >= {"conflict", "duplicate", "missing"}
    assert sheet.writes == 0
    result = archive.publish_pending()
    assert result.synced == 1 and result.conflicts == 2
    assert sheet.rows[1][2] == 999 and len(sheet.rows) == 5


def test_publish_respects_advisory_lock(archive):
    """Второй процесс не пишет Sheets, пока первый держит общую блокировку."""
    capture(archive)
    sheet = connect_memory(archive)
    with archive.engine.begin() as connection:
        connection.execute(
            text("SELECT pg_advisory_xact_lock(:key)"), {"key": archive._PUBLISH_LOCK}
        )
        assert archive.publish_pending().locked
        assert sheet.writes == 0
    assert archive.publish_pending().synced == 1


def test_batches_include_previous_dates_and_migration_is_repeatable(archive):
    """Пакеты больше 100 доставляются по датам, повтор миграции не сбрасывает synced."""
    with archive.engine.begin() as connection:
        connection.execute(
            text("""
            INSERT INTO autopilot_hourly_snapshots
                (snapshot_date, snapshot_hour, snapshot_at, orders_sum, adv_spend, profit_by_cond_orders)
            SELECT DATE '2026-10-01' + n / 24, n % 24, NOW(), 100, 10, 20
            FROM generate_series(0, 204) AS n
        """)
        )
    sheet = connect_memory(archive)
    assert archive.publish_pending().synced == 205
    assert sheet.writes == 3 and len(sheet.rows) == 206
    archive.migrate()
    assert archive.publish_pending().synced == 0


@pytest.mark.parametrize(
    "minutes,report_day,expected", [(0, 19, "saved"), (31, 19, "saved"), (0, 18, "skipped_date")]
)
def test_capture_accepts_late_run_and_rejects_historical(
    monkeypatch, minutes, report_day, expected
):
    """Запуск в :31 допустим, исторический report_date не создаёт текущий срез."""
    clock = Mock()
    clock.now.return_value = moment(8, minutes)
    monkeypatch.setattr(service_module, "datetime", clock)
    store = Mock()
    store.upsert_snapshot.return_value = True
    monkeypatch.setattr(service_module, "MonitoringSnapshotWriter", Mock(return_value=store))
    status = AutopilotHourlyService._write_monitoring_snapshot(
        {1: 100},
        {1: 2},
        {1: 10},
        {1: 20},
        moment(8),
        date(2026, 10, report_day),
        [1],
    )
    assert status == expected
    assert store.upsert_snapshot.call_count == (expected == "saved")


def test_midnight_and_incomplete_profit_do_not_capture(monkeypatch):
    """Сбор через полночь и неполная прибыль не закрепляют ошибочные итоги часа."""
    clock = Mock()
    clock.now.return_value = moment(0, day=20)
    monkeypatch.setattr(service_module, "datetime", clock)
    store = Mock()
    monkeypatch.setattr(service_module, "MonitoringSnapshotWriter", store)
    assert (
        AutopilotHourlyService._write_monitoring_snapshot(
            {1: 100},
            {1: 1},
            {},
            {1: 10},
            moment(23, 59),
            date(2026, 10, 19),
            [1],
        )
        == "skipped_date"
    )
    clock.now.return_value = moment()
    assert (
        AutopilotHourlyService._write_monitoring_snapshot(
            {1: 100},
            {1: 1},
            {},
            {},
            moment(),
            date(2026, 10, 19),
            [1],
        )
        == "incomplete"
    )
    store.assert_not_called()


def test_valid_empty_sources_capture_zero(monkeypatch, archive):
    """Успешные пустые источники дают нулевой срез при известном списке артикулов."""
    clock = Mock()
    clock.now.return_value = moment()
    monkeypatch.setattr(service_module, "datetime", clock)
    monkeypatch.setattr(service_module, "MonitoringSnapshotWriter", Mock(return_value=archive))
    assert (
        AutopilotHourlyService._write_monitoring_snapshot(
            {},
            {},
            {},
            {},
            moment(),
            date(2026, 10, 19),
            [1],
        )
        == "saved"
    )
    assert db_rows(archive)[-1]["orders_sum"] == 0


@pytest.mark.parametrize(
    "payload",
    [
        {"error": "сбой"},
        [None],
        [{"product_id": 1}],
        [{"product_id": 1, "budget_spent_today": "NaN"}],
    ],
)
def test_invalid_cometa_cannot_become_zero(monkeypatch, payload):
    """Ошибочный Cometa не превращается в успешный нулевой расход мониторинга."""
    monkeypatch.setenv("COMETA_API_KEY", "test-only")
    monkeypatch.setattr(CometaAdvSpendClient, "_request_json", AsyncMock(return_value=payload))
    with pytest.raises(ValueError):
        asyncio.run(CometaAdvSpendClient().fetch_today_spend([1]))


def test_cometa_valid_zero(monkeypatch):
    """Корректный ответ Cometa без расходов остаётся допустимым пустым словарём."""
    monkeypatch.setenv("COMETA_API_KEY", "test-only")
    monkeypatch.setattr(CometaAdvSpendClient, "_request_json", AsyncMock(return_value=[]))
    assert asyncio.run(CometaAdvSpendClient().fetch_today_spend([1])) == {}


def test_read_retry_honors_quota_and_retry_after(monkeypatch):
    """Чтение переживает 429, учитывая Retry-After, и не раскрывает тело исключения."""
    response = requests.Response()
    response.status_code = 429
    response._content = b'{"error":{"code":429,"message":"quota","status":"RESOURCE_EXHAUSTED"}}'
    response.headers["Retry-After"] = "90"
    error = gspread.exceptions.APIError(response)
    operation = Mock(side_effect=[error, ["данные"]])
    sleep = Mock()
    monkeypatch.setattr("src_oop.core.my_gspread.time.sleep", sleep)
    tabs = GoogleTabs.__new__(GoogleTabs)
    assert tabs.execute_with_retry("тест чтения", operation) == ["данные"]
    assert sleep.call_args.args[0] >= 90 and operation.call_count == 2
    operation = Mock(side_effect=error)
    with pytest.raises(gspread.exceptions.APIError):
        tabs.execute_with_retry("тест исчерпания квоты", operation)
    assert operation.call_count == 4


def test_unit_reads_shifted_columns_in_two_requests():
    """Смещённые колонки UNIT читаются пакетно, скидка сохраняется как доля."""
    tabs = GoogleTabs.__new__(GoogleTabs)
    tabs.sheet_title = Mock(row_count=20)
    tabs.sheet_title.row_values.return_value = [
        "Скидка ВБ факт",
        "Артикул",
        "Свободный остаток\n(сервис)1",
    ]
    tabs.sheet_title.batch_get.return_value = [
        [["123"], ["456"]],
        [["6\u00a0772"], [""]],
        [["32%"], ["0"]],
    ]
    remains, discounts = AutopilotHourlyService._load_unit_metrics(tabs)
    assert remains == {123: 6772}
    assert discounts == {123: 0.32, 456: 0}
    tabs.sheet_title.row_values.assert_called_once_with(1)
    tabs.sheet_title.batch_get.assert_called_once()
    assert tabs.sheet_title.batch_get.call_args.args[0] == ["B2:B20", "C2:C20", "A2:A20"]


def test_hourly_saves_despite_pu_and_unit_failures(monkeypatch, archive):
    """Срез сохраняется до UNIT и независимо от неуспешных записей трёх метрик ПУ."""
    sheet = connect_memory(archive)
    clock = Mock()
    clock.now.return_value = moment(8, 31)
    monkeypatch.setattr(service_module, "datetime", clock)
    monkeypatch.setattr(service_module, "MonitoringSnapshotWriter", Mock(return_value=archive))
    monkeypatch.setattr(
        service_module, "GoogleTabs", Mock(side_effect=[Mock(), requests.Timeout()])
    )
    monkeypatch.setattr(service_module, "load_api_tokens", Mock(return_value={}))
    pu = Mock()
    pu.read_articles.return_value = [1]
    pu.write_metric.return_value = MetricWriteResult("test", False)
    monkeypatch.setattr(service_module, "AutopilotSheetsWriter", Mock(return_value=pu))
    repository = Mock()
    repository.fetch_article_accounts.return_value = {}
    repository.fetch_today_orders_metrics.return_value = {
        "orders_sum_rub": {1: 100},
        "orders_count": {1: 1},
        "orders_avg_price": {1: 100},
    }
    repository.fetch_profit_calculation_inputs.return_value = {}
    repository.fetch_today_advert_activity.side_effect = RuntimeError(
        "Дополнительный источник недоступен"
    )
    service = AutopilotHourlyService(
        repository=repository,
        cometa_client=Mock(
            fetch_today_spend=AsyncMock(return_value={1: 10}),
        ),
    )
    monkeypatch.setattr(service, "_collect_funnel_data", AsyncMock(return_value=pd.DataFrame()))
    summary = asyncio.run(service.run())
    assert summary.monitoring_capture_status == "saved"
    assert summary.monitoring_synced == 1
    assert not summary.monitoring_pu_written
    assert len(sheet.rows) == 2
    assert db_rows(archive)[-1]["snapshot_at"] == moment(8, 31)


def test_order_day_is_moscow_and_null_prices_are_rejected(archive):
    """Граница дня одинакова при UTC-сессии БД; отсутствующая цена не занижает сумму."""
    with archive.engine.begin() as connection:
        connection.execute(
            text("""
            CREATE TABLE wb_order_feed (nm_id BIGINT, created_at TIMESTAMPTZ,
                status TEXT, seller_price NUMERIC);
            INSERT INTO wb_order_feed VALUES
                (1, '2026-10-18 20:59:59+00', 'created', 5),
                (1, '2026-10-18 21:00:00+00', 'created', 10),
                (1, '2026-10-19 20:59:59+00', 'created', 20),
                (1, '2026-10-19 21:00:00+00', 'created', 40)
        """)
        )
    database = Mock()

    def read_frame(query, params=None):
        """Выполняет реальный SQL заказов только в отдельной тестовой БД."""
        with archive.engine.connect() as connection:
            return pd.read_sql(query, connection, params=params)

    database.read_sql_to_dataframe.side_effect = read_frame
    repository = AutopilotRepository(database)
    assert repository.fetch_today_orders_metrics(date(2026, 10, 19), [1])["orders_sum_rub"] == {
        1: 30
    }
    with archive.engine.begin() as connection:
        connection.execute(
            text("INSERT INTO wb_order_feed VALUES (1, '2026-10-19 10:00:00+00', 'created', NULL)")
        )
    with pytest.raises(ValueError):
        repository.fetch_today_orders_metrics(date(2026, 10, 19), [1])
