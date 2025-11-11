import os
import json
import logging

from typing import List, Dict
from azure.storage.blob import BlobServiceClient, ContentSettings

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


class AzureStorage:

    def __init__(self, container_name: str = "datalake"):
        """
        Initialize Azure Storage client.

        Args:
            container_name: Name of the blob container

        Raises:
            AzureStorageError: If connection string is missing or invalid
        """
        self.container_name = container_name
        connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")

        if not connection_string:
            raise Exception(
                "AZURE_STORAGE_CONNECTION_STRING environment variable not set"
            )

        try:
            self.blob_service_client = (
                BlobServiceClient.from_connection_string(connection_string)
            )
            self._ensure_container_exists()
            logger.info(
                f"Successfully connected to Azure Storage container: {container_name}"
            )
        except Exception as e:
            raise Exception(f"Failed to initialize Azure Storage: {str(e)}")

    def _ensure_container_exists(self):
        """Create container if the container doesn't exist"""
        try:
            container_client = self.blob_service_client.get_container_client(
                self.container_name
            )

            if not container_client.exists():
                container_client.create_container()
                logger.info(f"Created container: {self.container_name}")
        except Exception as e:
            logger.error(f"Error checking/creating container: {str(e)}")
            raise

    def upload_to_bronze(self, data: List[Dict], filename: str) -> str:
        """Upload raw data to Bronze layer in JSON Lines format.

        Args:
            data: List of dictionaries containing raw brewery data;
            filename: Name of the file to be created in blob storage.
        Returns:
            str: Path to uploaded blob
        """
        if not data:
            raise ValueError("Data cannot be empty")

        # Convert to JSON Lines format
        json_lines = "\n".join(json.dumps(record) for record in data)

        blob_path = f"bronze/breweries/raw_data/{filename}"
        return self.upload_blob(json_lines.encode("utf-8"), blob_path)

    def upload_to_silver(self, parquet_bytes: bytes, blob_path: str) -> str:
        """Upload processed data to Silver layer.

        Args:
            parquet_bytes: Parquet data as bytes;
            blob_path: Path including partitions.
        Returns:
            str: Path to uploaded blob.
        """
        if not parquet_bytes:
            raise ValueError("Parquet data cannot be empty")

        if not blob_path.startswith("silver/"):
            blob_path = f"silver/{blob_path}"

        return self.upload_blob(parquet_bytes, blob_path)

    def upload_to_gold(self, parquet_bytes: bytes, blob_path: str) -> str:
        """Upload aggregated data to Gold layer.

        Args:
            parquet_bytes: Parquet data as bytes;
            blob_path: Path for the aggregated data.
        Returns:
            str: Path to uploaded blob.
        """
        if not parquet_bytes:
            raise ValueError("Parquet data cannot be empty")

        if not blob_path.startswith("gold/"):
            blob_path = f"gold/{blob_path}"

        return self.upload_blob(parquet_bytes, blob_path)
