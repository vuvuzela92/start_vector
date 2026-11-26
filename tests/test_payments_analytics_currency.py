from io import BytesIO
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from src_oop.jobs.calculation_of_purchases_china import run, ved_balance_analytics as ved


def test_conversion_preserves_existing_columns():
    """Проверяет адреса прежних полей и рублёвые суммы для Китая и логистики."""
    frame = pd.DataFrame({
        "Этап платежа": ["Аванс", "Баланс 1", "Простой", "Страхование груза"],
        "Поставщик": ["Завод", "ZILOL", "Перевозчик", "Страховщик"],
        "Дата_платеж_календарь": [""] * 4, "Дата_план": [""] * 4,
        "Сумма_оплаты": [10] * 4, "Оплачено": [3] * 4, "Не_оплачено": [7] * 4,
        "_currency": ["", "", "EUR", ""],
    })
    with patch.object(ved, "load_cbr_rates", return_value={"CNY": 12, "EUR": 90, "USD": 80}) as rates:
        result = ved.VedBalanceAnalyticsService.prepare_dataframe_for_upload(frame)
    rates.assert_called_once_with()
    for amount, expected in zip(frame.columns[4:7], ([120, 10, 900, ""], [36, 3, 270, ""], [84, 7, 630, ""])):
        position = result.columns.get_loc(amount)
        assert result.columns[position + 1:position + 3].tolist() == ["валюта", "сумма в рублях"]
        assert result.iloc[:, position + 2].tolist() == expected
    assert result.columns[-2:].tolist() == ["updated_at", "Валюта из логистики"]


def test_single_destination():
    """Проверяет единственную запись и расширение сетки только целевого листа."""
    frame = pd.DataFrame([[1] * 33])
    service = MagicMock()
    service.prepare_dataframe_for_upload.return_value = frame
    with patch.object(run, "_build_combined_balance_with_ved", return_value=(frame, service)), patch.object(run, "GoogleTabs") as target:
        target.return_value.sheet_title.col_count = 26
        run.update_payments_analyze_with_ved()
    target.assert_called_once_with(table_title="Платежный календарь", sheet_title="Аналитика_платежей",
                                   spreadsheet_id="1lL9LSIBE5IxXIrx-R5aBu-DUib6wHuK2TU9fUpS9xOs")
    target.return_value.sheet_title.add_cols.assert_called_once_with(7)
    target.return_value.set_df_to_google.assert_called_once_with(frame)
    service.upload_to_sheet.assert_not_called()


def test_incomplete_bulletin():
    """Прерывает расчёт при неполном ответе ЦБ до подключения к листу выгрузки."""
    with patch.object(ved, "urlopen", return_value=BytesIO(b"<ValCurs/>")), pytest.raises(ValueError):
        ved.load_cbr_rates()
