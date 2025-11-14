import os
import pytest

from unittest.mock import Mock, patch, MagicMock, mock_open, call
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

from tasks.gold_aggregation_task import GoldAggregationTask


@pytest.fixture(scope="session")
def spark():
    """Create a Spark session for testing."""
    spark = (
        SparkSession.builder.appName("TestGoldAggregation")
        .master("local[1]")
        .getOrCreate()
    )
    yield spark
    spark.stop()


@pytest.fixture
def mock_dependencies():
    """Fixture to mock all dependencies."""
    with patch(
        "tasks.gold_aggregation_task.LoggingConfig"
    ) as mock_logger, patch(
        "tasks.gold_aggregation_task.AzureStorageConfig"
    ) as mock_storage, patch(
        "tasks.gold_aggregation_task.SparkSession"
    ) as mock_spark_session:

        mock_logger_instance = Mock()
        mock_logger.return_value = mock_logger_instance
        mock_logger_instance.configure_logging.return_value = Mock()

        mock_storage_instance = Mock()
        mock_storage.return_value = mock_storage_instance

        # Mock blob service client
        mock_blob_service = Mock()
        mock_storage_instance.blob_service_client = mock_blob_service
        mock_storage_instance.container_name = "test-container"

        yield {
            "logger": mock_logger_instance,
            "storage": mock_storage_instance,
            "spark_session": mock_spark_session,
        }


@pytest.fixture
def sample_silver_data(spark):
    """Fixture providing sample silver layer data as Spark DataFrame."""
    data = [
        (
            "1",
            "Test Brewery 1",
            "micro",
            "San Francisco",
            "California",
            "United States",
        ),
        (
            "2",
            "Test Brewery 2",
            "micro",
            "San Francisco",
            "California",
            "United States",
        ),
        (
            "3",
            "Test Brewery 3",
            "regional",
            "Portland",
            "Oregon",
            "United States",
        ),
        (
            "4",
            "Test Brewery 4",
            "brewpub",
            "Portland",
            "Oregon",
            "United States",
        ),
        (
            "5",
            "Test Brewery 5",
            "micro",
            "Seattle",
            "Washington",
            "United States",
        ),
        (
            "6",
            "Test Brewery 6",
            "large",
            "Denver",
            "Colorado",
            "United States",
        ),
        (
            "7",
            "Test Brewery 7",
            "micro",
            "Denver",
            "Colorado",
            "United States",
        ),
        (
            "8",
            "Test Brewery 8",
            "micro",
            "Denver",
            "Colorado",
            "United States",
        ),
    ]

    schema = StructType(
        [
            StructField("id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("brewery_type", StringType(), True),
            StructField("city", StringType(), True),
            StructField("state_province", StringType(), True),
            StructField("country", StringType(), True),
        ]
    )

    return spark.createDataFrame(data, schema)


@pytest.fixture
def mock_blob_list():
    """Fixture providing mock blob list from Azure."""
    blob1 = Mock()
    blob1.name = "silver/state_province=California/part-00000.parquet"

    blob2 = Mock()
    blob2.name = "silver/state_province=Oregon/part-00000.parquet"

    blob3 = Mock()
    blob3.name = "silver/state_province=Washington/part-00000.parquet"

    blob4 = Mock()
    blob4.name = "silver/_SUCCESS"  # Non-parquet file

    return [blob1, blob2, blob3, blob4]


class TestGoldAggregationTask:
    """Test suite for GoldAggregationTask class."""

    def test_init(self, mock_dependencies):
        """Test task initialization."""
        task = GoldAggregationTask()

        assert task.azure_storage is not None
        assert task.log is not None
        assert task.spark is not None

    def test_execute_success(self, mock_dependencies, sample_silver_data):
        """Test successful execution of the task."""
        task = GoldAggregationTask()
        mock_aggregated_df = Mock()

        with patch.object(
            task, "_read_silver_layer", return_value=sample_silver_data
        ) as mock_read, patch.object(
            task, "_aggregate_silver_data", return_value=mock_aggregated_df
        ) as mock_aggregate, patch.object(
            task, "_save_to_gold_layer"
        ) as mock_save:

            task.execute()

            mock_read.assert_called_once()
            mock_aggregate.assert_called_once_with(sample_silver_data)
            mock_save.assert_called_once_with(mock_aggregated_df)
            assert task.log.info.call_count >= 2

    def test_read_silver_layer(self, mock_dependencies, mock_blob_list):
        """Test reading data from silver layer."""
        task = GoldAggregationTask()

        # Mock container client
        mock_container_client = Mock()
        task.azure_storage.blob_service_client.get_container_client.return_value = (
            mock_container_client
        )
        mock_container_client.list_blobs.return_value = mock_blob_list

        # Mock blob download
        mock_blob_client = Mock()
        mock_blob_data = Mock()
        mock_blob_data.readall.return_value = b"parquet_data"
        mock_blob_client.download_blob.return_value = mock_blob_data
        mock_container_client.get_blob_client.return_value = mock_blob_client

        # Mock Spark read
        mock_spark_reader = Mock()
        mock_format = Mock()
        mock_df = Mock()
        mock_df.count.return_value = 100

        task.spark.read = mock_spark_reader
        mock_spark_reader.format.return_value = mock_format
        mock_format.load.return_value = mock_df

        with patch("os.makedirs"), patch("builtins.open", mock_open()):

            result = task._read_silver_layer()

            # Verify container client was called
            task.azure_storage.blob_service_client.get_container_client.assert_called_once_with(
                "test-container"
            )

            # Verify blobs were listed
            mock_container_client.list_blobs.assert_called_once_with(
                name_starts_with="silver/"
            )

            # Verify only parquet files were downloaded (3 out of 4 blobs)
            assert mock_container_client.get_blob_client.call_count == 3

            # Verify Spark read was called
            mock_spark_reader.format.assert_called_once_with("parquet")
            assert result == mock_df

    def test_aggregate_silver_data(
        self, mock_dependencies, spark, sample_silver_data
    ):
        """Test aggregation of silver data."""
        task = GoldAggregationTask()
        task.spark = spark

        result_df = task._aggregate_silver_data(sample_silver_data)

        # Check that aggregation was performed
        assert result_df.count() > 0

        # Check that result has expected columns
        expected_columns = [
            "brewery_type",
            "city",
            "state_province",
            "country",
            "brewery_count",
        ]
        assert set(result_df.columns) == set(expected_columns)

        # Check brewery_count column exists and has correct type
        assert "brewery_count" in result_df.columns

        # Verify specific aggregations
        denver_micro = result_df.filter(
            (result_df.brewery_type == "micro") & (result_df.city == "Denver")
        ).collect()

        assert len(denver_micro) == 1
        assert (
            denver_micro[0]["brewery_count"] == 2
        )  # 2 micro breweries in Denver

    def test_aggregate_silver_data_counts_correctly(
        self, mock_dependencies, spark
    ):
        """Test that aggregation counts breweries correctly per group."""
        task = GoldAggregationTask()
        task.spark = spark

        # Create test data with known counts
        data = [
            ("1", "Brewery 1", "micro", "City1", "State1", "Country1"),
            ("2", "Brewery 2", "micro", "City1", "State1", "Country1"),
            ("3", "Brewery 3", "micro", "City1", "State1", "Country1"),
            ("4", "Brewery 4", "regional", "City1", "State1", "Country1"),
            ("5", "Brewery 5", "regional", "City1", "State1", "Country1"),
        ]

        schema = StructType(
            [
                StructField("id", StringType(), True),
                StructField("name", StringType(), True),
                StructField("brewery_type", StringType(), True),
                StructField("city", StringType(), True),
                StructField("state_province", StringType(), True),
                StructField("country", StringType(), True),
            ]
        )

        df = spark.createDataFrame(data, schema)
        result_df = task._aggregate_silver_data(df)

        # Should have 2 groups: micro and regional for City1
        assert result_df.count() == 2

        # Check micro count
        micro_row = result_df.filter(
            result_df.brewery_type == "micro"
        ).collect()[0]
        assert micro_row["brewery_count"] == 3

        # Check regional count
        regional_row = result_df.filter(
            result_df.brewery_type == "regional"
        ).collect()[0]
        assert regional_row["brewery_count"] == 2

    def test_aggregate_silver_data_orders_by_count(
        self, mock_dependencies, spark
    ):
        """Test that aggregated data is ordered by brewery_count descending."""
        task = GoldAggregationTask()
        task.spark = spark

        data = [
            ("1", "Brewery 1", "micro", "City1", "State1", "Country1"),
            ("2", "Brewery 2", "regional", "City2", "State1", "Country1"),
            ("3", "Brewery 3", "regional", "City2", "State1", "Country1"),
            ("4", "Brewery 4", "regional", "City2", "State1", "Country1"),
        ]

        schema = StructType(
            [
                StructField("id", StringType(), True),
                StructField("name", StringType(), True),
                StructField("brewery_type", StringType(), True),
                StructField("city", StringType(), True),
                StructField("state_province", StringType(), True),
                StructField("country", StringType(), True),
            ]
        )

        df = spark.createDataFrame(data, schema)
        result_df = task._aggregate_silver_data(df)

        # Collect results and verify order
        results = result_df.collect()
        assert results[0]["brewery_count"] >= results[1]["brewery_count"]

    def test_save_to_gold_layer(self, mock_dependencies, spark):
        """Test saving aggregated data to gold layer."""
        task = GoldAggregationTask()
        task.spark = spark

        # Create a mock DataFrame
        mock_df = Mock()
        mock_writer = Mock()
        mock_format = Mock()
        mock_mode = Mock()

        mock_df.write = mock_writer
        mock_writer.format.return_value = mock_format
        mock_format.mode.return_value = mock_mode
        mock_mode.save = Mock()

        with patch.object(task, "_upload_gold_data") as mock_upload:
            task._save_to_gold_layer(mock_df)

            mock_writer.format.assert_called_once_with("parquet")
            mock_format.mode.assert_called_once_with("overwrite")
            mock_mode.save.assert_called_once_with("/tmp/gold_breweries")
            mock_upload.assert_called_once_with("/tmp/gold_breweries")

    def test_upload_gold_data(self, mock_dependencies):
        """Test uploading parquet files to Azure gold layer."""
        task = GoldAggregationTask()

        # Mock os.walk to simulate files
        mock_walk_result = [
            (
                "/tmp/gold_breweries",
                [],
                ["part-00000.parquet", "part-00001.parquet", "_SUCCESS"],
            )
        ]

        with patch("os.walk", return_value=mock_walk_result), patch(
            "builtins.open", mock_open(read_data=b"parquet_data")
        ), patch(
            "os.path.relpath", side_effect=lambda x, y: x.replace(y + "/", "")
        ):

            task._upload_gold_data("/tmp/gold_breweries")

            # Should upload only 2 parquet files, not _SUCCESS
            assert task.azure_storage.upload_to_layer.call_count == 2

            # Verify upload calls contain correct layer
            for call_args in task.azure_storage.upload_to_layer.call_args_list:
                assert call_args[0][2] == "gold"

    def test_upload_gold_data_with_subdirectories(self, mock_dependencies):
        """Test uploading parquet files from subdirectories."""
        task = GoldAggregationTask()

        mock_walk_result = [
            ("/tmp/gold_breweries", ["subdir"], ["part-00000.parquet"]),
            ("/tmp/gold_breweries/subdir", [], ["part-00001.parquet"]),
        ]

        with patch("os.walk", return_value=mock_walk_result), patch(
            "builtins.open", mock_open(read_data=b"data")
        ), patch(
            "os.path.relpath", side_effect=lambda x, y: x.replace(y + "/", "")
        ):

            task._upload_gold_data("/tmp/gold_breweries")

            # Should upload 2 parquet files from different directories
            assert task.azure_storage.upload_to_layer.call_count == 2

    def test_upload_gold_data_skips_non_parquet(self, mock_dependencies):
        """Test that only .parquet files are uploaded to gold layer."""
        task = GoldAggregationTask()

        mock_walk_result = [
            (
                "/tmp/gold_breweries",
                [],
                [
                    "part-00000.parquet",
                    "_SUCCESS",
                    "metadata.json",
                    ".crc",
                    "part-00001.parquet",
                ],
            )
        ]

        with patch("os.walk", return_value=mock_walk_result), patch(
            "builtins.open", mock_open(read_data=b"data")
        ), patch(
            "os.path.relpath", side_effect=lambda x, y: x.replace(y + "/", "")
        ):

            task._upload_gold_data("/tmp/gold_breweries")

            # Only 2 parquet files should be uploaded
            assert task.azure_storage.upload_to_layer.call_count == 2

    def test_read_silver_layer_creates_temp_directory(
        self, mock_dependencies, mock_blob_list
    ):
        """Test that temp directory is created for downloading silver data."""
        task = GoldAggregationTask()

        mock_container_client = Mock()
        task.azure_storage.blob_service_client.get_container_client.return_value = (
            mock_container_client
        )
        mock_container_client.list_blobs.return_value = mock_blob_list

        mock_blob_client = Mock()
        mock_blob_data = Mock()
        mock_blob_data.readall.return_value = b"data"
        mock_blob_client.download_blob.return_value = mock_blob_data
        mock_container_client.get_blob_client.return_value = mock_blob_client

        mock_spark_reader = Mock()
        mock_format = Mock()
        mock_df = Mock()
        mock_df.count.return_value = 10

        task.spark.read = mock_spark_reader
        mock_spark_reader.format.return_value = mock_format
        mock_format.load.return_value = mock_df

        with patch("os.makedirs") as mock_makedirs, patch(
            "builtins.open", mock_open()
        ):

            task._read_silver_layer()

            # Verify temp directory creation
            assert any(
                call_args[0][0] == "/tmp/silver_breweries_read"
                for call_args in mock_makedirs.call_args_list
            )

    def test_aggregate_silver_data_handles_empty_dataframe(
        self, mock_dependencies, spark
    ):
        """Test aggregation handles empty DataFrame gracefully."""
        task = GoldAggregationTask()
        task.spark = spark

        # Create empty DataFrame with schema
        schema = StructType(
            [
                StructField("id", StringType(), True),
                StructField("brewery_type", StringType(), True),
                StructField("city", StringType(), True),
                StructField("state_province", StringType(), True),
                StructField("country", StringType(), True),
            ]
        )

        empty_df = spark.createDataFrame([], schema)
        result_df = task._aggregate_silver_data(empty_df)

        assert result_df.count() == 0
        assert "brewery_count" in result_df.columns
