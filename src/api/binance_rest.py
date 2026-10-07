import logging
import time

import requests


logger = logging.getLogger(__name__)


class BinanceRESTClient:

    def __init__(
        self,
        base_url: str,
        max_retries: int = 3,
        backoff_factor: int = 2
    ):
        self.base_url = base_url.rstrip("/")
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor

    def get_klines(
        self,
        symbol: str,
        interval: str = "1m",
        limit: int = 100
    ):

        endpoint = f"{self.base_url}/api/v3/klines"

        params = {
            "symbol": symbol,
            "interval": interval,
            "limit": limit
        }

        for attempt in range(1, self.max_retries + 1):

            try:

                logger.info(
                    "Binance API request | "
                    "symbol=%s | interval=%s | attempt=%s/%s",
                    symbol,
                    interval,
                    attempt,
                    self.max_retries
                )

                response = requests.get(
                    endpoint,
                    params=params,
                    timeout=30
                )

                # Successful response
                if response.status_code == 200:

                    logger.info(
                        "Binance API request successful | "
                        "symbol=%s",
                        symbol
                    )

                    return response.json()

                # Rate limit
                if response.status_code == 429:

                    logger.warning(
                        "Binance rate limit reached | "
                        "symbol=%s",
                        symbol
                    )

                    if attempt == self.max_retries:
                        response.raise_for_status()

                    sleep_time = self.backoff_factor ** attempt

                    logger.info(
                        "Retrying after %s seconds",
                        sleep_time
                    )

                    time.sleep(sleep_time)

                    continue

                # Server-side errors
                if response.status_code >= 500:

                    logger.warning(
                        "Binance server error | "
                        "status=%s | attempt=%s",
                        response.status_code,
                        attempt
                    )

                    if attempt == self.max_retries:
                        response.raise_for_status()

                    sleep_time = self.backoff_factor ** attempt

                    time.sleep(sleep_time)

                    continue

                # Other HTTP errors
                response.raise_for_status()

            except requests.exceptions.Timeout:

                logger.warning(
                    "Binance request timeout | "
                    "symbol=%s | attempt=%s",
                    symbol,
                    attempt
                )

                if attempt == self.max_retries:
                    raise

                sleep_time = self.backoff_factor ** attempt

                time.sleep(sleep_time)

            except requests.exceptions.ConnectionError:

                logger.warning(
                    "Binance connection error | "
                    "symbol=%s | attempt=%s",
                    symbol,
                    attempt
                )

                if attempt == self.max_retries:
                    raise

                sleep_time = self.backoff_factor ** attempt

                time.sleep(sleep_time)

        raise RuntimeError(
            f"Binance API failed after "
            f"{self.max_retries} attempts"
        )