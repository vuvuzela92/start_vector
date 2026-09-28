import pandas as pd

from src_oop.jobs.assembly_info.repository import AssemblyInfoRepository


def test_copy_serializer_uses_postgresql_null_marker_and_preserves_text() -> None:
    """Проверяет безопасное преобразование значений для COPY без SQL-параметров."""
    assert AssemblyInfoRepository._serialize_value(None) == r"\N"
    assert AssemblyInfoRepository._serialize_value(float("nan")) == r"\N"
    assert AssemblyInfoRepository._serialize_value(pd.Timestamp("2026-09-23T12:00:00")) == "2026-09-23T12:00:00"
    assert AssemblyInfoRepository._serialize_value("статус, с запятой") == "статус, с запятой"
