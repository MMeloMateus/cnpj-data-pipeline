import zipfile
import os
import sys
from pathlib import Path
from zipfile import ZipFile
import logging
# from dotenv import load_dotenv
from datetime import datetime
from dateutil.relativedelta import relativedelta

# load_dotenv(verbose=False)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter(
    "%(asctime)s - %(levelname)s - %(name)s - %(message)s"
)
handler.setFormatter(formatter)

if not logger.handlers:
    logger.addHandler(handler)


def check_path(path: str) -> None:
    """
    Check if the path exists and is a directory.
    """
    path = Path(path)

    if not path.exists():
        logger.error("Path does not exist: %s", path)
        raise FileNotFoundError(f"The path '{path}' does not exist")

    if not path.is_dir():
        logger.error("Path is not a directory: %s", path)
        raise NotADirectoryError(f"The path '{path}' is not a directory")


def list_archives(path: str,  **context) -> list[Path]:
    """
    Return a list of .zip files found in the given directory.
    """
    check_path(path)

    path = Path(path)
    zip_files = [
        p for p in path.iterdir()
        if p.is_file() and p.suffix.lower() == ".zip"
    ]

    if not zip_files:
        logger.warning("No .zip files found in directory: %s", path)
        raise FileNotFoundError("No .zip file found in the directory")

    logger.info("Found %d zip file(s) in %s", len(zip_files), path)
    return zip_files


def uncompress_zip_file(origin_path: str, output_dir: str, **context) -> None:
    """
    Extract all .zip files from the source directory into the output directory.
    Corrupted ZIP files are skipped.
    """
    output_dir = Path(output_dir)
    os.makedirs(output_dir, exist_ok=True)

    zip_files = list_archives(origin_path)

    for file in zip_files:
        try:
            logger.info("Extracting zip file: %s", file)
            with ZipFile(file, "r") as zip_obj:
                zip_obj.extractall(path=output_dir)

        except zipfile.BadZipFile:
            logger.exception("Corrupted ZIP file skipped: %s", file)


def uncompress_zip_file_range(
    origin_base_path: str,
    output_dir: str,
    start_date: str,
    end_date: str,
    **context,
) -> None:
    """
    Uncompress zip files month by month within a date range.
    Supports folders in the format YYYY-MM.
    """

    start = datetime.strptime(start_date, "%Y-%m")
    end = datetime.strptime(end_date, "%Y-%m")
    current = start

    processed_months = 0
    while current <= end:
        # Pasta de origem no formato YYYY-MM
        origin_month_path = Path(origin_base_path) / f"{current.year}-{current.month:02d}"

        # Pasta de destino no formato YYYY-MM
        output_month_path = Path(output_dir) / f"{current.year}-{current.month:02d}"

        logger.info(
            "Processing month %s",
            current.strftime("%Y-%m")
        )

        try:
            uncompress_zip_file(
                origin_path=str(origin_month_path),
                output_dir=str(output_month_path),
            )
            processed_months += 1

        except FileNotFoundError:
            logger.warning(
                "No files found for %s",
                current.strftime("%Y-%m")
            )

        current += relativedelta(months=1)

    if processed_months == 0:
        raise RuntimeWarning("No zip files were processed in the given date range")