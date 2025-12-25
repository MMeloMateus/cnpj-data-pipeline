import os
import sys
import logging
from pathlib import Path
from urllib.parse import urljoin
from dotenv import load_dotenv
import requests
from bs4 import BeautifulSoup


def download_files(url_file, path_destiny):
    """
    Download all .zip files from a given URL into path_destiny.
    """

    logger = logging.getLogger(__name__)
    
    # Test if the path exist...
    if not os.path.exists(path_destiny):
        logger.info("The directory did not exist. Creating it: %s", path_destiny)
        os.makedirs(path_destiny, exist_ok=True)
    else:
        logger.info("Directory already exists %s", path_destiny)
    
    logger.info("Starting HTML request: %s", url_file)

    try:
        response_request = requests.get(url_file, timeout=15)
        response_request.raise_for_status()

        soup = BeautifulSoup(response_request.text, "html.parser")
        
        archive_names = [a['href'] for a in soup.find_all('a') if a['href'].endswith(".zip")]
        logger.info("Files found: %d", len(archive_names))
            
        # Verify if the archive is alredy in the past
        archive_to_download = [name for name in archive_names if not os.path.exists(os.path.join(path_destiny, name))]
        logger.debug("Files that will be downloaded: %s", archive_to_download)
        
        for archive in archive_to_download:
            # request
            file_url = urljoin(url_file, archive)
            local_path = os.path.join(path_destiny, archive)  

            logger.info("Downloading file: %s",archive)

            try:
                r = requests.get(file_url, stream=True,timeout=15)
                r.raise_for_status()
                
                # Write the archive
                with open(local_path, "wb") as f:
                    for chunk in r.iter_content(chunk_size=8192): # 8KB
                        if chunk: # Test if chunk is not null
                            f.write(chunk) 
                logger.info("Download completed: %s",archive)
            
            except Exception:
                logger.exception("Failed to download file: %s", archive)
    
        logger.info("Process completed successfully")
    
    except Exception:
        logger.critical(
            "Failed to perform base HTML request: %s",
            url_file,
            exc_info=True
        )
        return


def main():
           
    log_file_path = "/app/logs/downloader.log"
    
    logging.basicConfig(
        filename=log_file_path,
        level=logging.DEBUG,
        format="%(asctime)s - %(levelname)s - %(name)s - %(levelname)s - %(message)s",
    )

    logger = logging.getLogger(__name__)
    logger.info("Initializing application")

    load_dotenv()

    DATA_DIR_BRONZE = Path(os.getenv("DATA_DIR_BRONZE"))
    BASE_URL = os.getenv("RFB_BASE_URL")

    args = sys.argv[1:]
    if len(args) % 2 != 0:
        logger.error("Invalid number of arguments. Please provide pairs of year and month.")
        raise ValueError("Invalid number of arguments")
    
    for i in range(0, len(args), 2):
        year = args[i]
        month = args[i+1]
        
        local_path = os.path.join(DATA_DIR_BRONZE,f"{year}-{month}/")
        url = urljoin(BASE_URL.rstrip("/") + "/", f"{year}-{month}/")
    
        logger.info("PATH destiny: %s", local_path)
        logger.info("URL base: %s", url)
        print(local_path)
        print(url)
    
        # Chama a função que faz o download
        download_files(url, local_path)

if __name__ == "__main__":
    main()