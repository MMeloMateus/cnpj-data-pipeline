# airflow/src/extract/extract.py
import os
import sys
import logging
from urllib.parse import urljoin
from dotenv import load_dotenv
import requests
from bs4 import BeautifulSoup
import pendulum

load_dotenv(verbose=False)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(name)s - %(message)s")
handler.setFormatter(formatter)

if not logger.handlers:
    logger.addHandler(handler)


def download_files(url_file: str, path_destiny: str, **context):
    """
    Download all .zip files from a given URL into path_destiny.
    """
    os.makedirs(path_destiny, exist_ok=True)
    logger.info("Starting HTML request: %s", url_file)

    try:
        response_request = requests.get(url_file, timeout=30)
        response_request.raise_for_status()

        soup = BeautifulSoup(response_request.text, "html.parser")
        archive_names = [a['href'] for a in soup.find_all('a') if a.get('href', '').endswith(".zip")]
        logger.info("Files found on page: %d", len(archive_names))

        archive_to_download = [name for name in archive_names if not os.path.exists(os.path.join(path_destiny, name))]
        logger.info("Files that will be downloaded: %s", archive_to_download)

        for archive in archive_to_download:
            file_url = urljoin(url_file, archive)
            local_path = os.path.join(path_destiny, archive)

            logger.info("Downloading file: %s -> %s", file_url, local_path)
            try:
                with requests.get(file_url, stream=True, timeout=60) as r:
                    r.raise_for_status()
                    with open(local_path, "wb") as f:
                        for chunk in r.iter_content(chunk_size=8192):
                            if chunk:
                                f.write(chunk)
                logger.info("Download completed: %s", archive)
            except Exception:
                logger.exception("Failed to download file: %s", archive)

        logger.info("Process completed successfully for URL: %s", url_file)

    except Exception:
        logger.exception("Failed to perform base HTML request: %s", url_file)
        raise


def download_files_for_period(forced_date=None, **context):
    """
    Airflow PythonOperator callable.
    Uses forced_date if provided, otherwise takes data_interval_start from the context.
    """
    data_dir_bronze = os.getenv("DATA_DIR_BRONZE", "/opt/project/data/bronze")
    base_url = os.getenv("RFB_BASE_URL")
    if not base_url:
        raise RuntimeError("RFB_BASE_URL not set in environment")

    # usa forced_date se passado, senão pega do contexto
    di_start = forced_date or context.get("data_interval_start") or context.get("execution_date")
    if di_start is None:
        raise RuntimeError("No execution date found in context")

    year = di_start.year
    month = di_start.month

    local_path = os.path.join(data_dir_bronze, f"{year}-{month:02d}")
    url = urljoin(base_url.rstrip("/") + "/", f"{year}-{month:02d}/")

    logger.info("Downloading period year=%s month=%s to %s from %s", year, month, local_path, url)
    download_files(url, local_path, **context)


def download_files_for_range(start_date, end_date, **context):
    """
    Download files for all months between start_date and end_date.
    start_date and end_date devem ser objetos pendulum.DateTime
    """
    current = start_date.start_of("month")
    end = end_date.start_of("month")

    while current <= end:
        logger.info("Processing month: %s-%02d", current.year, current.month)
        # Passando forced_date para evitar conflito com o context
        download_files_for_period(forced_date=current, **context)
        current = current.add(months=1)
