import os
import io
import json
import pandas as pd

from azure.storage.blob import BlobServiceClient
from config.logging_config import LoggingConfig


class AzureStorageConfig:
    """Azure Storage Configuration for Data Lake operations."""

    def __init__(self, container_name: str = "datalake"):
        """
        Initialize Azure Storage client.

        Args:
            container_name: Name of the blob container

        Raises:
            AzureStorageError: If connection string is missing or invalid
        """
        # Initialize logging
        logger = LoggingConfig()
        self.log = logger.configure_logging()

        self.container_name = container_name
        self.connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")

        if not self.connection_string:
            raise Exception(
                "AZURE_STORAGE_CONNECTION_STRING environment variable not set"
            )

        try:
            self.blob_service_client = (
                BlobServiceClient.from_connection_string(
                    self.connection_string
                )
            )
            self._ensure_container_exists()
            self.log.info(
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
                self.log.info(f"Created container: {self.container_name}")
        except Exception as e:
            raise Exception(f"Error checking/creating container: {str(e)}")

    def _get_blob_path(self, layer: str, blob_path: str = None) -> str:
        """Construct full path for the specified layer.

        Args:
            layer: Medallion architecture layer ('bronze', 'silver', 'gold');
            blob_path (Optional[str]): Path within the layer.
        Returns:
            Full path including storage account, container, layer, and
            blob path.
        Raises:
            ValueError: If layer is invalid.
        """
        valid_layers = {"bronze", "silver", "gold"}
        if layer not in valid_layers:
            raise ValueError(f"Layer must be one of {valid_layers}")

        # Extract storage account name from connection string
        account_name = None
        for part in self.connection_string.split(";"):
            if part.startswith("AccountName="):
                account_name = part.split("=", 1)[1]
                break

        if not account_name:
            raise ValueError(
                "Could not extract account name from connection string"
            )

        if blob_path:
            # Ensure blob_path doesn't start with layer prefix
            if blob_path.startswith(f"{layer}/"):
                blob_path = blob_path[len(f"{layer}/") :]

            # Use wasbs protocol for better compatibility
            wasbs_path = f"wasbs://{self.container_name}@{account_name}.blob.core.windows.net/{layer}/{blob_path}"
        else:
            wasbs_path = f"wasbs://{self.container_name}@{account_name}.blob.core.windows.net/{layer}"

        return wasbs_path

    def _read_from_layer(
        self,
        layer: str,
        blob_path: str = None,
        file_format: str = None,
        spark=None,
    ):
        """Read data from specified medallion architecture layer.

        Args:
            layer: Source layer ('bronze', 'silver', or 'gold');
            blob_path (Optional[str]): Path within the layer to read the blob.
                If None, reads all files from the layer;
            file_format (Optional[str]): File format ('json', 'parquet', 'csv').
                If None, auto-detects from file extension;
            spark (Optional): SparkSession for reading parquet files from
                entire layer.
        Returns:
            Data in appropriate format (list for JSON, DataFrame for
                parquet/csv, bytes for unknown).
        Raises:
            ValueError: If layer is invalid or spark is required but not
                provided.
        """
        valid_layers = {"bronze", "silver", "gold"}
        if layer not in valid_layers:
            raise ValueError(f"Layer must be one of {valid_layers}")

        # If no blob_path, read entire layer with Spark
        if blob_path is None:
            if spark is None:
                raise ValueError(
                    "SparkSession is required to read entire layer"
                )

            full_path = self._get_blob_path(layer)

            # Auto-detect format or default to parquet
            fmt = file_format or "parquet"

            df = spark.read.format(fmt).load(full_path)
            self.log.info(f"Read {fmt} files from layer {layer}")
            return df

        # Auto-detect format from extension if not provided
        if file_format is None:
            file_format = blob_path.rstrip("/").split(".")[-1].lower()

        # Handle single file with blob storage
        if not blob_path.startswith(f"{layer}/"):
            blob_path = f"{layer}/{blob_path}"

        data = self._read_blob(blob_path)

        # Parse based on format
        if file_format == "json":
            return json.loads(data.decode("utf-8"))
        elif file_format == "parquet":
            return pd.read_parquet(io.BytesIO(data))
        elif file_format == "csv":
            return pd.read_csv(io.BytesIO(data))
        else:
            # Return raw bytes for unknown formats
            return data

    def _read_blob(self, blob_path: str) -> bytes:
        """Read a blob from the specified path in the container.

        Args:
            blob_path: Path within the container to read the blob.
        Returns:
            bytes: Content of the blob.
        """
        try:
            blob_client = self.blob_service_client.get_blob_client(
                container=self.container_name, blob=blob_path
            )

            download_stream = blob_client.download_blob()
            data = download_stream.readall()

            self.log.info(f"Read blob from {blob_path}")

            return data
        except Exception as e:
            raise Exception(f"Failed to read blob from {blob_path}: {str(e)}")

    def _upload_blob(self, data: bytes, blob_path: str) -> str:
        """Upload a blob to the specified path in the container.

        Args:
            data: Data to upload as bytes;
            blob_path: Path within the container to upload the blob.
        Returns:
            str: Path to uploaded blob.
        """
        try:
            blob_client = self.blob_service_client.get_blob_client(
                container=self.container_name, blob=blob_path
            )

            blob_client.upload_blob(data, overwrite=True)

            self.log.info(f"Uploaded blob to {blob_path}")

            return blob_path
        except Exception as e:
            raise Exception(f"Failed to upload blob to {blob_path}: {str(e)}")

    def upload_to_layer(self, data: bytes, blob_path: str, layer: str) -> str:
        """Upload data to the specified medallion architecture layer.

        Args:
            data: Data to upload as bytes;
            blob_path: Path within the layer to upload the blob;
            layer: Target layer ('bronze', 'silver', or 'gold').
        Returns:
            str: Path to uploaded blob.
        Raises:
            ValueError: If data is empty or layer is invalid.
        """
        if not data:
            raise ValueError("Data cannot be empty")

        valid_layers = {"bronze", "silver", "gold"}
        if layer not in valid_layers:
            raise ValueError(f"Layer must be one of {valid_layers}")

        if not blob_path.startswith(f"{layer}/"):
            blob_path = f"{layer}/{blob_path}"

        return self._upload_blob(data, blob_path)
