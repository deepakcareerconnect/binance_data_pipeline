import csv
import json
import logging
from pathlib import Path


logger = logging.getLogger(__name__)


def read_csv_file(
    file_path: str
) -> list:
    """
    Read a CSV file and return rows as lists.
    """

    path = Path(file_path)

    if not path.exists():
        raise FileNotFoundError(
            f"Historical CSV not found: {path}"
        )

    records = []

    with path.open(
        mode="r",
        encoding="utf-8-sig",
        newline=""
    ) as file:

        reader = csv.reader(file)

        for row in reader:

            if not row:
                continue

            if all(
                not str(value).strip()
                for value in row
            ):
                continue

            records.append(row)

    logger.info(
        "CSV file read successfully | "
        "file=%s | records=%s",
        path,
        len(records)
    )

    return records


def read_json_file(
    file_path: str
) -> list:
    """
    Read a JSON file containing a list of records.
    """

    path = Path(file_path)

    if not path.exists():
        raise FileNotFoundError(
            f"Historical JSON not found: {path}"
        )

    with path.open(
        mode="r",
        encoding="utf-8"
    ) as file:

        data = json.load(file)

    if not isinstance(data, list):
        raise ValueError(
            "Historical JSON must contain a list"
        )

    logger.info(
        "JSON file read successfully | "
        "file=%s | records=%s",
        path,
        len(data)
    )

    return data


def read_historical_file(
    file_path: str
) -> list:
    """
    Read historical CSV or JSON file based on extension.
    """

    extension = Path(
        file_path
    ).suffix.lower()

    if extension == ".csv":
        return read_csv_file(
            file_path
        )

    if extension == ".json":
        return read_json_file(
            file_path
        )

    raise ValueError(
        f"Unsupported historical file format: "
        f"{extension}"
    )