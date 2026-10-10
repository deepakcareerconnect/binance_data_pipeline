
from pathlib import Path

import pytest

from src.historical.historical_pipeline import HistoricalPipeline


@pytest.fixture
def pipeline():
    # Key generation does not need a live S3 connection.
    return HistoricalPipeline(s3_client=None)


@pytest.mark.parametrize(
    ("archive_name", "expected_year", "expected_month"),
    [
        ("BTCUSDT-1m-2026-10-01.zip", "2026", "10"),
        ("BTCUSDT-1m-2026-10-02.zip", "2026", "10"),
        ("BTCUSDT-1m-2026-10.zip", "2026", "10"),
        ("ETHUSDT-1m-2025-01.zip", "2025", "01"),
    ],
)
def test_build_s3_key_uses_correct_year_month(
    pipeline, archive_name, expected_year, expected_month
):
    key = pipeline.build_s3_key(
        symbol=archive_name.split("-")[0],
        interval="1m",
        archive_path=Path(archive_name),
    )

    assert f"year={expected_year}/month={expected_month}/" in key
    assert "year=0010/" not in key


@pytest.mark.parametrize(
    "archive_name",
    [
        "BTCUSDT-1m-invalid-10.zip",
        "BTCUSDT-1m-2026-13.zip",
        "BTCUSDT-1m-2026-00.zip",
    ],
)
def test_build_s3_key_rejects_invalid_archive_dates(
    pipeline, archive_name
):
    with pytest.raises(ValueError):
        pipeline.build_s3_key(
            symbol="BTCUSDT",
            interval="1m",
            archive_path=Path(archive_name),
        )
