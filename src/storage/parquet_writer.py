import io
import logging

import pyarrow as pa
import pyarrow.parquet as pq


logger = logging.getLogger(__name__)


KLINE_SCHEMA = pa.schema([
    ("symbol", pa.string()),
    ("interval", pa.string()),

    ("open_time", pa.timestamp("us")),

    ("open_price", pa.float64()),
    ("high_price", pa.float64()),
    ("low_price", pa.float64()),
    ("close_price", pa.float64()),
    ("volume", pa.float64()),

    ("close_time", pa.timestamp("us")),

    ("quote_asset_volume", pa.float64()),
    ("number_of_trades", pa.int64()),
    ("taker_buy_base_volume", pa.float64()),
    ("taker_buy_quote_volume", pa.float64()),
])


def convert_klines_to_parquet(
    records: list,
    symbol: str,
    interval: str
) -> bytes:

    rows = []

    for record in records:

        rows.append({
            "symbol": symbol,
            "interval": interval,

            "open_time": record[0],

            "open_price": float(record[1]),
            "high_price": float(record[2]),
            "low_price": float(record[3]),
            "close_price": float(record[4]),
            "volume": float(record[5]),

            "close_time": record[6],

            "quote_asset_volume": float(record[7]),
            "number_of_trades": int(record[8]),
            "taker_buy_base_volume": float(record[9]),
            "taker_buy_quote_volume": float(record[10]),
        })

    table = pa.Table.from_pylist(
        rows,
        schema=KLINE_SCHEMA
    )

    buffer = io.BytesIO()

    pq.write_table(
        table,
        buffer,
        compression="snappy"
    )

    return buffer.getvalue()