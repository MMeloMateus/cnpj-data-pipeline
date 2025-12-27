# airflow/src/extract/extract.py
import os
import sys
import logging
from pathlib import Path
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

def download_files(url_file: str, path_destiny: str):
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

def download_files_for_period(**context):
    """
    Airflow PythonOperator callable.
    Uses data_interval_start from the context to decide year/month.
    """
    data_dir_bronze = os.getenv("DATA_DIR_BRONZE", "/opt/project/data/bronze")
    base_url = os.getenv("RFB_BASE_URL")
    if not base_url:
        raise RuntimeError("RFB_BASE_URL not set in environment")

    # data_interval_start is a pendulum datetime
    di_start = context.get("data_interval_start") or context.get("execution_date")
    if di_start is None:
        raise RuntimeError("No execution date found in context")

    year = di_start.year
    month = di_start.month

    local_path = os.path.join(data_dir_bronze, f"{year}-{month:02d}")
    url = urljoin(base_url.rstrip("/") + "/", f"{year}-{month:02d}/")

    logger.info("Downloading period year=%s month=%s to %s from %s", year, month, local_path, url)
    download_files(url, local_path)

def download_files_for_range(start_date, end_date):
    """
    Download files for all months between start_date and end_date.
    """
    current = start_date.start_of("month")
    end = end_date.start_of("month")

    while current <= end:
        logger.info("Processando mês: %s-%02d", current.year, current.month)
        download_files_for_period(data_interval_start=current)
        current = current.add(months=1)
