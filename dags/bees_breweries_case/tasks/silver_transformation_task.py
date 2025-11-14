import os

from typing import List
from pyspark.sql import SparkSession, DataFrame

from config.logging_config import LoggingConfig
from config.az_storage import AzureStorageConfig

from bees_breweries_case.tools.schema_tables import SchemaTables


class SilverTransformationTask:
    """Silver Layer Task for Bees Breweries Case pipeline."""

    def __init__(self):
        logger = LoggingConfig()
        self.log = logger.configure_logging()

        self.azure_storage = AzureStorageConfig()

        # Extract account name from connection string
        connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
        account_name = None
        account_key = None

        for part in connection_string.split(";"):
            if part.startswith("AccountName="):
                account_name = part.split("=", 1)[1]
            elif part.startswith("AccountKey="):
                account_key = part.split("=", 1)[1]

        # Initialize Spark session with Azure configuration
        self.spark = SparkSession.builder.appName(
            "SilverTransformationTask"
        ).getOrCreate()

    def execute(self):
        self.log.info("Starting Silver Transformation Task.")

        bronze_data = self.azure_storage._read_from_layer(
            "bronze", "breweries_list.json"
        )

        # Cast latitude and longitude to float
        bronze_data_normalized = self._normalize_bronze_data_types(bronze_data)

        transformed_data = self._process_bronze_data(bronze_data_normalized)

        self._save_to_silver_layer(transformed_data)

        self.log.info("Silver Transformation Task completed.")

    def _normalize_bronze_data_types(self, data: List) -> List:
        """Normalizes 'latitude' and 'longitude' columns casting it to float
        type.

        Args:
            data (List): List of dictionaries with Bronze layer data.

        Returns:
            bronze_data_normalized (List): List of dictionaries with
                normalized Bronze layer data.
        """
        self.log.info("Normalizing Bronze data types.")

        for record in data:
            record["latitude"] = (
                float(record["latitude"])
                if record["latitude"] is not None
                else None
            )
            record["longitude"] = (
                float(record["longitude"])
                if record["longitude"] is not None
                else None
            )

        return data

    def _process_bronze_data(self, data) -> DataFrame:
        """
        Convert bronze layer JSON data into a Spark DataFrame with cleaned
        state_province values.

        Args:
            data: List of dictionaries with bronze layer data.
        Returns:
            df (DataFrame): Transformed data as a Pyspark DataFrame.
        """
        self.log.info("Transforming data for Silver layer.")

        schema = SchemaTables.BRONZE_SCHEMA_BREWERIES

        df = self.spark.createDataFrame(data, schema=schema)

        # Handle null values in partition column
        df = df.fillna({"state_province": "UNKNOWN"})

        self.log.info(f"Transformed {df.count()} records for Silver layer.")

        return df

    def _save_to_silver_layer(self, df: DataFrame):
        """
        Save transformed DataFrame to Silver layer in parquet format,
        partitioned by state_province column.

        Args:
            df: Transformed Pyspark DataFrame.
        """
        self.log.info("Saving data to Silver layer.")

        # Write to local temporary directory first
        temp_path = "/tmp/silver_breweries"

        df.write.format("parquet").mode("overwrite").partitionBy(
            "state_province"
        ).save(temp_path)

        self.log.info(f"Data written to temporary path: {temp_path}")

        # Upload partitioned files to Azure
        self._upload_partitioned_data(temp_path)

        self.log.info("Data successfully saved to Silver layer.")

    def _upload_partitioned_data(self, temp_path: str):
        """
        Upload partitioned parquet files from local temp to Azure Silver layer.

        Args:
            temp_path: Local temporary directory path.
        """
        for root, dirs, files in os.walk(temp_path):
            for file in files:
                if file.endswith(".parquet"):
                    local_file_path = os.path.join(root, file)

                    # Get path from temp directory
                    path = os.path.relpath(local_file_path, temp_path)

                    with open(local_file_path, "rb") as f:
                        data = f.read()

                    self.azure_storage.upload_to_layer(data, path, "silver")

                    self.log.info(f"Uploaded {path} to silver layer")
