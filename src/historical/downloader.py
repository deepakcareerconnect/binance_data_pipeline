import logging
from pathlib import Path


import requests

from src.historical.checksum import calculate_sha256


logger = logging.getLogger(__name__)


class HistoricalDownloader:

    def __init__(
        self,
        base_url: str,
        download_directory: str = "data/historical",
        timeout: int = 300
    ):
        self.base_url = base_url.rstrip("/")
        self.download_directory = Path(download_directory)
        self.timeout = timeout

        self.download_directory.mkdir(
            parents=True,
            exist_ok=True
        )

    def build_monthly_url(
        self,
        symbol: str,
        interval: str,
        year: int,
        month: int
    ) -> str:

        filename = (
            f"{symbol}-{interval}-"
            f"{year:04d}-{month:02d}.zip"
        )

        return (
            f"{self.base_url}/"
            f"data/spot/monthly/klines/"
            f"{symbol}/"
            f"{interval}/"
            f"{filename}"
        )

    def build_daily_url(
        self,
        symbol: str,
        interval: str,
        year: int,
        month: int,
        day: int
    ) -> str:

        filename = (
            f"{symbol}-{interval}-"
            f"{year:04d}-{month:02d}-{day:02d}.zip"
        )

        return (
            f"{self.base_url}/"
            f"data/spot/daily/klines/"
            f"{symbol}/"
            f"{interval}/"
            f"{filename}"
        )

    def download(
        self,
        url: str,
        symbol: str,
        filename: str
    ) -> Path:

        symbol_directory = (
            self.download_directory / symbol
        )

        symbol_directory.mkdir(
            parents=True,
            exist_ok=True
        )

        output_path = (
            symbol_directory / filename
        )

        # Don't download the same archive twice.
        if output_path.exists():
            logger.info(
                "File already exists | path=%s",
                output_path
            )
            return output_path

        logger.info(
            "Downloading | url=%s",
            url
        )

        response = requests.get(
            url,
            stream=True,
            timeout=self.timeout
        )

        if response.status_code == 404:
            raise FileNotFoundError(
                f"Historical file not found: {url}"
            )

        response.raise_for_status()

        with output_path.open("wb") as file:

            for chunk in response.iter_content(
                chunk_size=1024 * 1024
            ):
                if chunk:
                    file.write(chunk)

        checksum = calculate_sha256(
            file_path=str(output_path)
        )

        logger.info(
            "Download completed | "
            "file=%s | size=%s bytes | sha256=%s",
            output_path,
            output_path.stat().st_size,
            checksum
        )

        return output_path

    def download_monthly(
        self,
        symbol: str,
        interval: str,
        year: int,
        month: int
    ) -> Path:

        filename = (
            f"{symbol}-{interval}-"
            f"{year:04d}-{month:02d}.zip"
        )

        url = self.build_monthly_url(
            symbol=symbol,
            interval=interval,
            year=year,
            month=month
        )

        return self.download(
            url=url,
            symbol=symbol,
            filename=filename
        )

    def download_daily(
        self,
        symbol: str,
        interval: str,
        year: int,
        month: int,
        day: int
    ) -> Path:

        filename = (
            f"{symbol}-{interval}-"
            f"{year:04d}-{month:02d}-{day:02d}.zip"
        )

        url = self.build_daily_url(
            symbol=symbol,
            interval=interval,
            year=year,
            month=month,
            day=day
        )

        return self.download(
            url=url,
            symbol=symbol,
            filename=filename
        )