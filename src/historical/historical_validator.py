import logging
import math


logger = logging.getLogger(__name__)


EXPECTED_FIELDS = 12


def _normalize_timestamp_to_ms(timestamp: int) -> int:
    """
    Normalize timestamp to milliseconds.

    Supported timestamp magnitudes:

    milliseconds:
        ~1.7e12

    microseconds:
        ~1.7e15

    nanoseconds:
        ~1.7e18
    """

    timestamp = int(timestamp)

    # Milliseconds
    if timestamp < 10**14:
        return timestamp

    # Microseconds
    if timestamp < 10**17:
        return timestamp // 1_000

    # Nanoseconds
    if timestamp < 10**20:
        return timestamp // 1_000_000

    raise ValueError(
        f"Unsupported timestamp magnitude: {timestamp}"
    )


def validate_historical_records(
    records: list,
    symbol: str,
    interval: str
) -> bool:
    """
    Validate Binance historical kline records.

    Binance kline structure:

    0  open_time
    1  open
    2  high
    3  low
    4  close
    5  volume
    6  close_time
    7  quote_asset_volume
    8  number_of_trades
    9  taker_buy_base_volume
    10 taker_buy_quote_volume
    11 ignore
    """

    if not records:
        raise ValueError(
            f"No historical records found | "
            f"symbol={symbol}"
        )

    previous_open_time_ms = None

    expected_interval_ms = None

    # =========================================================
    # Calculate expected interval
    # =========================================================

    if interval.endswith("m"):

        minutes = int(
            interval[:-1]
        )

        expected_interval_ms = (
            minutes * 60 * 1000
        )

    elif interval.endswith("h"):

        hours = int(
            interval[:-1]
        )

        expected_interval_ms = (
            hours * 60 * 60 * 1000
        )

    elif interval.endswith("d"):

        days = int(
            interval[:-1]
        )

        expected_interval_ms = (
            days * 24 * 60 * 60 * 1000
        )

    # =========================================================
    # Validate records
    # =========================================================

    for index, record in enumerate(records):

        # -----------------------------------------------------
        # Field count
        # -----------------------------------------------------

        if len(record) != EXPECTED_FIELDS:

            raise ValueError(
                f"Invalid record at row {index} | "
                f"expected {EXPECTED_FIELDS} fields | "
                f"received {len(record)}"
            )

        try:

            raw_open_time = int(
                record[0]
            )

            open_price = float(
                record[1]
            )

            high_price = float(
                record[2]
            )

            low_price = float(
                record[3]
            )

            close_price = float(
                record[4]
            )

            volume = float(
                record[5]
            )

            raw_close_time = int(
                record[6]
            )

            quote_asset_volume = float(
                record[7]
            )

            number_of_trades = int(
                record[8]
            )

            taker_buy_base_volume = float(
                record[9]
            )

            taker_buy_quote_volume = float(
                record[10]
            )

        except (
            ValueError,
            TypeError
        ) as error:

            raise ValueError(
                f"Invalid data at row {index} | "
                f"error={error}"
            ) from error

        # -----------------------------------------------------
        # Normalize timestamps for validation
        # -----------------------------------------------------

        open_time_ms = _normalize_timestamp_to_ms(
            raw_open_time
        )

        close_time_ms = _normalize_timestamp_to_ms(
            raw_close_time
        )

        # -----------------------------------------------------
        # Numeric validity
        # -----------------------------------------------------

        numeric_values = [
            open_price,
            high_price,
            low_price,
            close_price,
            volume,
            quote_asset_volume,
            taker_buy_base_volume,
            taker_buy_quote_volume
        ]

        if not all(
            math.isfinite(value)
            for value in numeric_values
        ):

            raise ValueError(
                f"Non-finite numeric value at row {index}"
            )

        # -----------------------------------------------------
        # Timestamp validation
        # -----------------------------------------------------

        if open_time_ms <= 0:

            raise ValueError(
                f"Invalid open_time at row {index}"
            )

        if close_time_ms <= open_time_ms:

            raise ValueError(
                f"Invalid timestamp range at row {index}"
            )

        # -----------------------------------------------------
        # Price validation
        # -----------------------------------------------------

        if min(
            open_price,
            high_price,
            low_price,
            close_price
        ) < 0:

            raise ValueError(
                f"Negative price at row {index}"
            )

        if high_price < max(
            open_price,
            close_price,
            low_price
        ):

            raise ValueError(
                f"Invalid high price at row {index}"
            )

        if low_price > min(
            open_price,
            close_price,
            high_price
        ):

            raise ValueError(
                f"Invalid low price at row {index}"
            )

        # -----------------------------------------------------
        # Volume validation
        # -----------------------------------------------------

        if volume < 0:

            raise ValueError(
                f"Negative volume at row {index}"
            )

        if quote_asset_volume < 0:

            raise ValueError(
                f"Negative quote volume at row {index}"
            )

        if taker_buy_base_volume < 0:

            raise ValueError(
                f"Negative taker-buy base volume "
                f"at row {index}"
            )

        if taker_buy_quote_volume < 0:

            raise ValueError(
                f"Negative taker-buy quote volume "
                f"at row {index}"
            )

        # -----------------------------------------------------
        # Trade count validation
        # -----------------------------------------------------

        if number_of_trades < 0:

            raise ValueError(
                f"Negative number_of_trades "
                f"at row {index}"
            )

        # -----------------------------------------------------
        # Duplicate / ordering validation
        # -----------------------------------------------------

        if previous_open_time_ms is not None:

            if open_time_ms <= previous_open_time_ms:

                raise ValueError(
                    f"Duplicate or out-of-order "
                    f"open_time at row {index}"
                )

            if expected_interval_ms is not None:

                actual_difference = (
                    open_time_ms
                    - previous_open_time_ms
                )

                if actual_difference != expected_interval_ms:

                    raise ValueError(
                        f"Unexpected timestamp gap at "
                        f"row {index} | "
                        f"expected={expected_interval_ms} ms | "
                        f"actual={actual_difference} ms"
                    )

        previous_open_time_ms = open_time_ms

    # =========================================================
    # Validation successful
    # =========================================================

    logger.info(
        "Historical validation successful | "
        "symbol=%s | interval=%s | records=%s",
        symbol,
        interval,
        len(records)
    )

    return True