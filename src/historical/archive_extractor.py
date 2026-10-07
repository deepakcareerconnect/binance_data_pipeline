import logging
import zipfile
from pathlib import Path


logger = logging.getLogger(__name__)


def extract_archive(
    archive_path: str
) -> list[Path]:
    """
    Safely extract a ZIP archive.

    Prevents ZIP path traversal attacks by ensuring that
    every extracted file remains inside the extraction directory.
    """

    archive = Path(archive_path)

    if not archive.exists():
        raise FileNotFoundError(
            f"Archive not found: {archive}"
        )

    if not archive.is_file():
        raise ValueError(
            f"Archive path is not a file: {archive}"
        )

    if archive.suffix.lower() != ".zip":
        raise ValueError(
            f"Expected ZIP archive: {archive}"
        )

    extraction_directory = (
        archive.parent / archive.stem
    )

    extraction_directory.mkdir(
        parents=True,
        exist_ok=True
    )

    extracted_files = []

    with zipfile.ZipFile(
        archive,
        "r"
    ) as zip_file:

        for member in zip_file.infolist():

            member_path = (
                extraction_directory
                / member.filename
            ).resolve()

            extraction_root = (
                extraction_directory.resolve()
            )

            if (
                member_path != extraction_root
                and extraction_root
                not in member_path.parents
            ):
                raise ValueError(
                    "Unsafe ZIP archive detected. "
                    f"Attempted path: {member.filename}"
                )

        for member in zip_file.infolist():

            zip_file.extract(
                member,
                extraction_directory
            )

            extracted_path = (
                extraction_directory
                / member.filename
            )

            if extracted_path.is_file():
                extracted_files.append(
                    extracted_path
                )

    logger.info(
        "Archive extracted | "
        "archive=%s | files=%s",
        archive,
        len(extracted_files)
    )

    return extracted_files