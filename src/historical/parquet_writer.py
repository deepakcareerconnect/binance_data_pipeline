import io
import logging
from datetime import datetime, timezone
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq


logger = logging.getLogger(__name__)


KLINE_SCHEMA = pa.schema([
    (
        "symbol",
        pa.string()
    ),
    (
        "interval",
        pa.string()
    ),
    (
        "open_time",
        pa.timestamp(
            "us",
            tz="UTC"
        )
    ),
    (
        "open_price",
        pa.float64()
    ),
    (
        "high_price",
        pa.float64()
    ),
    (
        "low_price",
        pa.float64()
    ),
    (
        "close_price",
        pa.float64()
    ),
    (
        "volume",
        pa.float64()
    ),
    (
        "close_time",
        pa.timestamp(
            "us",
            tz="UTC"
        )
    ),
    (
        "quote_asset_volume",
        pa.float64()
    ),
    (
        "number_of_trades",
        pa.int64()
    ),
    (
        "taker_buy_base_volume",
        pa.float64()
    ),
    (
        "taker_buy_quote_volume",
        pa.float64()
    )
])


def timestamp_to_datetime(
    timestamp_us: int
) -> datetime:
    """
    Convert microsecond timestamp to UTC datetime.
    """

    timestamp_us = int(
        timestamp_us
    )

    return datetime.fromtimestamp(
        timestamp_us / 1_000_000,
        tz=timezone.utc
    )


def build_kline_rows(
    records: list,
    symbol: str,
    interval: str
) -> list[dict[str, Any]]:
    """
    Convert normalized records into dictionaries
    matching the Parquet schema.
    """

    rows = []

    for index, record in enumerate(records):

        if len(record) != 11:
            raise ValueError(
                f"Invalid normalized record at row "
                f"{index} | "
                f"expected 11 fields | "
                f"received {len(record)}"
            )

        try:

            row = {
                "symbol": symbol,
                "interval": interval,

                "open_time": timestamp_to_datetime(
                    record[0]
                ),

                "open_price": float(
                    record[1]
                ),

                "high_price": float(
                    record[2]
                ),

                "low_price": float(
                    record[3]
                ),

                "close_price": float(
                    record[4]
                ),

                "volume": float(
                    record[5]
                ),

                "close_time": timestamp_to_datetime(
                    record[6]
                ),

                "quote_asset_volume": float(
                    record[7]
                ),

                "number_of_trades": int(
                    record[8]
                ),

                "taker_buy_base_volume": float(
                    record[9]
                ),

                "taker_buy_quote_volume": float(
                    record[10]
                )
            }

        except (
            ValueError,
            TypeError,
            IndexError
        ) as error:

            raise ValueError(
                f"Failed to build Parquet row | "
                f"row={index} | error={error}"
            ) from error

        rows.append(row)

    return rows


def records_to_parquet(
    records: list,
    symbol: str,
    interval: str,
    compression: str = "snappy"
) -> bytes:
    """
    Convert normalized Binance records into
    Snappy-compressed Parquet bytes.
    """

    if not records:
        raise ValueError(
            f"No records available for Parquet conversion | "
            f"symbol={symbol}"
        )

    rows = build_kline_rows(
        records=records,
        symbol=symbol,
        interval=interval
    )

    table = pa.Table.from_pylist(
        rows,
        schema=KLINE_SCHEMA
    )

    output_buffer = io.BytesIO()

    pq.write_table(
        table,
        output_buffer,
        compression=compression
    )

    parquet_data = (
        output_buffer.getvalue()
    )

    logger.info(
        "Parquet created | "
        "symbol=%s | interval=%s | "
        "records=%s | size=%s bytes | "
        "compression=%s",
        symbol,
        interval,
        len(records),
        len(parquet_data),
        compression
    )

    return parquet_data