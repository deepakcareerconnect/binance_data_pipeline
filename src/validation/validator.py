import logging


logger = logging.getLogger(__name__)


EXPECTED_KLINE_FIELDS = 12


def validate_kline_response(
    data,
    symbol: str
) -> bool:

    # Check that response is a list
    if not isinstance(data, list):
        raise ValueError(
            f"Invalid response for {symbol}: "
            f"expected list, got {type(data).__name__}"
        )

    # Check that response contains data
    if len(data) == 0:
        raise ValueError(
            f"No kline data returned for {symbol}"
        )

    # Validate every kline record
    for index, record in enumerate(data):

        # Each record should be a list
        if not isinstance(record, list):
            raise ValueError(
                f"Invalid kline record at index {index}"
            )

        # Binance kline contains 12 fields
        if len(record) != EXPECTED_KLINE_FIELDS:
            raise ValueError(
                f"Invalid kline record at index {index}: "
                f"expected {EXPECTED_KLINE_FIELDS} fields, "
                f"received {len(record)}"
            )

        open_time = record[0]
        close_time = record[6]

        # Validate timestamps
        if not isinstance(open_time, int):
            raise ValueError(
                f"Invalid open time at index {index}: "
                f"{open_time}"
            )

        if not isinstance(close_time, int):
            raise ValueError(
                f"Invalid close time at index {index}: "
                f"{close_time}"
            )

        # Close time shouldn't be before open time
        if close_time < open_time:
            raise ValueError(
                f"Invalid timestamps at index {index}: "
                f"close_time < open_time"
            )

    logger.info(
        "Validation successful: symbol=%s records=%s",
        symbol,
        len(data)
    )

    return True