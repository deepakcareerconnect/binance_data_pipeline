import logging


logger = logging.getLogger(__name__)


def normalize_timestamp(
    timestamp: int
) -> int:
    """
    Normalize Binance timestamps to microseconds.

    Binance historical files normally contain
    timestamps in milliseconds.
    """

    timestamp = int(timestamp)

    # Already microseconds
    if timestamp > 10**14:
        return timestamp

    # Milliseconds → microseconds
    if timestamp > 10**11:
        return timestamp * 1000

    raise ValueError(
        f"Unsupported Binance timestamp: {timestamp}"
    )


def normalize_klines(
    records: list,
    symbol: str,
    interval: str
) -> list:
    """
    Normalize Binance kline records.

    Removes the final Binance 'ignore' field.

    Input:
        12 fields

    Output:
        11 fields
    """

    normalized_records = []

    for index, record in enumerate(records):

        if len(record) != 12:
            raise ValueError(
                f"Invalid Binance kline record | "
                f"row={index} | "
                f"expected 12 fields | "
                f"received {len(record)}"
            )

        try:

            open_time = normalize_timestamp(
                record[0]
            )

            close_time = normalize_timestamp(
                record[6]
            )

            normalized_record = [
                open_time,
                float(record[1]),
                float(record[2]),
                float(record[3]),
                float(record[4]),
                float(record[5]),
                close_time,
                float(record[7]),
                int(record[8]),
                float(record[9]),
                float(record[10])
            ]

            normalized_records.append(
                normalized_record
            )

        except (
            ValueError,
            TypeError,
            IndexError
        ) as error:

            raise ValueError(
                f"Normalization failed | "
                f"row={index} | "
                f"error={error}"
            ) from error

    logger.info(
        "Normalization completed | "
        "symbol=%s | interval=%s | records=%s",
        symbol,
        interval,
        len(normalized_records)
    )

    return normalized_records