import requests
import logging


logger = logging.getLogger(__name__)


class BinanceRESTClient:

    def __init__(self, base_url: str):
        self.base_url = base_url.rstrip("/")

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

        logger.info(
            "Requesting Binance klines: "
            "symbol=%s interval=%s limit=%s",
            symbol,
            interval,
            limit
        )

        response = requests.get(
            endpoint,
            params=params,
            timeout=30
        )

        response.raise_for_status()

        return response.json()