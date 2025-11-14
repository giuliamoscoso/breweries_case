import json
import time
import requests

from typing import List, Dict
from datetime import datetime

from config.logging_config import LoggingConfig
from config.az_storage import AzureStorageConfig


class BronzeExtractionTask:
    """Bronze Layer Task for Bees Breweries Case pipeline."""

    def __init__(self):
        logger = LoggingConfig()
        self.log = logger.configure_logging()

        self.execution_date = datetime.today().strftime("%Y-%m-%d")

        self.BASE_URL = "https://api.openbrewerydb.org/v1/breweries"

        self.azure_storage = AzureStorageConfig()

        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "BreweriesPipeline/1.0"})

    def execute(self):
        self.log.info("Starting Bronze Extraction Task.")

        json_data = self._fetch_all_data()

        self._save_to_bronze_layer(json_data)

        self.log.info("Bronze Extraction Task completed.")

    def _fetch_all_data(
        self, per_page: int = 200, max_retries: int = 3
    ) -> List[Dict]:
        """Method responsible for fetching all breweries data from the API
        Open Brewery DB with pagination.

        Args:
            per_page: Number of records per page (default: 200).
            max_retries: Maximum number of retries for failed requests
                (default: 3).

        Returns:
            List[Dict]: List of dictionaries with all breweries data.
        """
        self.log.info("Fetching data from Open Brewery DB API.")

        all_breweries = []
        page = 1

        while True:
            self.log.info(f"Fetching page {page}...")
            breweries = self._fetch_page(page, per_page, max_retries)

            if not breweries:
                break

            all_breweries.extend(breweries)
            page += 1

            time.sleep(0.1)

        self.log.info(f"Total breweries extracted: {len(all_breweries)}")

        return all_breweries

    def _fetch_page(
        self, page: int, per_page: int, max_retries: int
    ) -> List[Dict]:
        """Method responsible for fetching a single page with retry logic.

        Args:
            page: Page number to fetch.
            per_page: Number of records per page.
            max_retries: Maximum number of retries for failed requests.

        Returns:
            List[Dict]: List of brewery records from the page.
        """
        url = f"{self.BASE_URL}?page={page}&per_page={per_page}"

        for attempt in range(max_retries):
            try:
                response = self.session.get(url, timeout=30)
                response.raise_for_status()
                return response.json()

            except requests.exceptions.RequestException as e:
                if attempt == max_retries - 1:
                    self.log.error(
                        f"Failed to fetch page {page} after {max_retries} attempts: {e}"
                    )
                    return []

                wait_time = 2**attempt
                self.log.warning(
                    f"Retry {attempt + 1}/{max_retries} after {wait_time}s..."
                )
                time.sleep(wait_time)

        return []

    def _save_to_bronze_layer(self, data: List[Dict]):
        """Method responsible for checking if the container exists and creating
        it if not, then saving the fetched data to the Bronze layer.

        Args:
            data: List of dictionaries with breweries data.
        """
        self.azure_storage._ensure_container_exists()

        self.log.info("Saving data to Bronze layer.")

        # Convert list to JSON bytes
        json_bytes = json.dumps(data).encode("utf-8")

        self.azure_storage.upload_to_layer(
            json_bytes, "breweries_list.json", "bronze"
        )

        self.log.info("Data successfully saved to Bronze layer.")
