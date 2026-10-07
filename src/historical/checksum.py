import hashlib
import logging
from pathlib import Path


logger = logging.getLogger(__name__)


def calculate_sha256(
    file_path: str,
    chunk_size: int = 1024 * 1024
) -> str:
    """
    Calculate SHA-256 checksum for a file.

    The file is read in chunks so large historical archives
    do not need to be loaded entirely into memory.
    """

    path = Path(file_path)

    if not path.exists():
        raise FileNotFoundError(
            f"File not found for checksum calculation: {path}"
        )

    if not path.is_file():
        raise ValueError(
            f"Checksum target is not a file: {path}"
        )

    sha256 = hashlib.sha256()

    with path.open("rb") as file:

        while True:

            chunk = file.read(chunk_size)

            if not chunk:
                break

            sha256.update(chunk)

    checksum = sha256.hexdigest()

    logger.info(
        "SHA-256 calculated | "
        "file=%s | sha256=%s",
        path.name,
        checksum
    )

    return checksum