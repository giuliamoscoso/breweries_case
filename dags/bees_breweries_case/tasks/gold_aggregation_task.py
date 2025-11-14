import os

from pyspark.sql import SparkSession, DataFrame

from config.logging_config import LoggingConfig
from config.az_storage import AzureStorageConfig


class GoldAggregationTask:
    """Gold Layer Task for Bees Breweries Case pipeline."""

    def __init__(self):
        logger = LoggingConfig()
        self.log = logger.configure_logging()

        self.azure_storage = AzureStorageConfig()

        # Initialize Spark session
        self.spark = SparkSession.builder.appName(
            "GoldAggregationTask"
        ).getOrCreate()

    def execute(self):
        self.log.info("Starting Gold Aggregation Task.")

        silver_data = self._read_silver_layer()

        aggregated_data = self._aggregate_silver_data(silver_data)

        self._save_to_gold_layer(aggregated_data)

        self.log.info("Gold Aggregation Task completed.")

    def _read_silver_layer(self) -> DataFrame:
        """
        Download silver layer parquet files from Azure and read with Spark.

        Returns:
            DataFrame: Silver layer data as Spark DataFrame.
        """
        self.log.info("Downloading silver layer data from Azure.")

        temp_path = "/tmp/silver_breweries_read"
        os.makedirs(temp_path, exist_ok=True)

        # List all blobs in silver layer
        container_client = (
            self.azure_storage.blob_service_client.get_container_client(
                self.azure_storage.container_name
            )
        )

        blob_list = container_client.list_blobs(name_starts_with="silver/")

        # Download all parquet files
        for blob in blob_list:
            if blob.name.endswith(".parquet"):
                # Preserve directory structure
                local_file_path = os.path.join(
                    temp_path, blob.name.replace("silver/", "")
                )
                local_dir = os.path.dirname(local_file_path)
                os.makedirs(local_dir, exist_ok=True)

                # Download blob
                blob_client = container_client.get_blob_client(blob.name)
                with open(local_file_path, "wb") as f:
                    f.write(blob_client.download_blob().readall())

                self.log.info(f"Downloaded {blob.name} to {local_file_path}")

        # Read all parquet files with Spark
        df = self.spark.read.format("parquet").load(temp_path)
        self.log.info(f"Read {df.count()} records from silver layer")

        return df

    def _aggregate_silver_data(self, df: DataFrame) -> DataFrame:
        """
        Aggregate a view with the quantity of breweries per type (brewery_type
        column) and location.

        Args:
            df (DataFrame): Silver layer DataFrame.
        Returns:
            aggregated_df (DataFrame): Aggregated DataFrame for Gold layer.
        """
        self.log.info("Aggregating data for Gold layer.")

        aggregated_df = (
            df.groupBy("brewery_type", "city", "state_province", "country")
            .agg({"id": "count"})
            .withColumnRenamed("count(id)", "brewery_count")
        )

        aggregated_df = aggregated_df.orderBy("brewery_count", ascending=False)

        self.log.info(
            f"Aggregation complete. Generated {aggregated_df.count()} brewery type/location groups."
        )

        return aggregated_df

    def _save_to_gold_layer(self, df: DataFrame):
        """
        Save aggregated DataFrame to Gold layer in parquet format.

        Args:
            df (DataFrame): Aggregated DataFrame for Gold layer.
        """
        self.log.info("Saving aggregated data to Gold layer.")

        # Write to local temporary directory first
        temp_path = "/tmp/gold_breweries"

        df.write.format("parquet").mode("overwrite").save(temp_path)

        self.log.info(f"Data written to temporary path: {temp_path}")

        # Upload files to Azure
        self._upload_gold_data(temp_path)

        self.log.info("Aggregated data successfully saved to Gold layer.")

    def _upload_gold_data(self, temp_path: str):
        """
        Upload parquet files from local temp to Azure Gold layer.

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

                    self.azure_storage.upload_to_layer(data, path, "gold")

                    self.log.info(f"Uploaded {path} to gold layer")
