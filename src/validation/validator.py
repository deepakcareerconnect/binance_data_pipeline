import logging


logger = logging.getLogger(__name__)


EXPECTED_KLINE_FIELDS = 12


def validate_kline_response(
    data,
    symbol: str
) -> bool:

    # --------------------------------------------------
    # 1. Response must be a list
    # --------------------------------------------------

    if not isinstance(data, list):

        raise ValueError(
            f"Invalid response for {symbol}: "
            f"expected list"
        )

    # --------------------------------------------------
    # 2. Response cannot be empty
    # --------------------------------------------------

    if not data:

        raise ValueError(
            f"Empty kline response for {symbol}"
        )

    # --------------------------------------------------
    # 3. Validate each candle
    # --------------------------------------------------

    previous_open_time = None

    for index, record in enumerate(data):

        if not isinstance(record, list):

            raise ValueError(
                f"Record {index} is not a list"
            )

        # Binance returns 12 fields
        if len(record) != EXPECTED_KLINE_FIELDS:

            raise ValueError(
                f"Record {index}: expected "
                f"{EXPECTED_KLINE_FIELDS} fields, "
                f"received {len(record)}"
            )

        open_time = record[0]
        close_time = record[6]

        open_price = record[1]
        high_price = record[2]
        low_price = record[3]
        close_price = record[4]
        volume = record[5]

        # --------------------------------------------------
        # Timestamp validation
        # --------------------------------------------------

        if not isinstance(open_time, int):

            raise ValueError(
                f"Record {index}: invalid open_time"
            )

        if not isinstance(close_time, int):

            raise ValueError(
                f"Record {index}: invalid close_time"
            )

        if close_time < open_time:

            raise ValueError(
                f"Record {index}: "
                f"close_time < open_time"
            )

        # --------------------------------------------------
        # Ensure chronological order
        # --------------------------------------------------

        if (
            previous_open_time is not None
            and open_time <= previous_open_time
        ):

            raise ValueError(
                f"Record {index}: "
                f"timestamps are not increasing"
            )

        previous_open_time = open_time

        # --------------------------------------------------
        # Price validation
        # --------------------------------------------------

        try:

            open_price = float(open_price)
            high_price = float(high_price)
            low_price = float(low_price)
            close_price = float(close_price)
            volume = float(volume)

        except (TypeError, ValueError):

            raise ValueError(
                f"Record {index}: "
                f"invalid numeric values"
            )

        if min(
            open_price,
            high_price,
            low_price,
            close_price
        ) < 0:

            raise ValueError(
                f"Record {index}: "
                f"negative price detected"
            )

        if volume < 0:

            raise ValueError(
                f"Record {index}: "
                f"negative volume detected"
            )

    logger.info(
        "Validation successful | "
        "symbol=%s | records=%s",
        symbol,
        len(data)
    )

    return True