import os
import pytest

from unittest.mock import Mock, patch, MagicMock, mock_open, call
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType

from tasks.silver_transformation_task import SilverTransformationTask


@pytest.fixture(scope="session")
def spark():
    """Create a Spark session for testing."""
    spark = (
        SparkSession.builder.appName("TestSilverTransformation")
        .master("local[1]")
        .getOrCreate()
    )
    yield spark
    spark.stop()


@pytest.fixture
def mock_dependencies():
    """Fixture to mock all dependencies."""
    with patch(
        "tasks.silver_transformation_task.LoggingConfig"
    ) as mock_logger, patch(
        "tasks.silver_transformation_task.AzureStorageConfig"
    ) as mock_storage, patch(
        "tasks.silver_transformation_task.SparkSession"
    ) as mock_spark_session, patch.dict(
        os.environ,
        {
            "AZURE_STORAGE_CONNECTION_STRING": "AccountName=testaccount;AccountKey=testkey==;"
        },
    ):

        mock_logger_instance = Mock()
        mock_logger.return_value = mock_logger_instance
        mock_logger_instance.configure_logging.return_value = Mock()

        mock_storage_instance = Mock()
        mock_storage.return_value = mock_storage_instance

        yield {
            "logger": mock_logger_instance,
            "storage": mock_storage_instance,
            "spark_session": mock_spark_session,
        }


@pytest.fixture
def sample_bronze_data():
    """Fixture providing sample bronze layer data."""
    return [
        {
            "id": "1",
            "name": "Test Brewery 1",
            "brewery_type": "micro",
            "address_1": "123 Main St",
            "address_2": None,
            "address_3": None,
            "city": "San Francisco",
            "state_province": "California",
            "postal_code": "94102",
            "country": "United States",
            "longitude": "-122.4194",
            "latitude": "37.7749",
            "phone": "1234567890",
            "website_url": "https://testbrewery1.com",
            "state": "California",
            "street": "123 Main St",
        },
        {
            "id": "2",
            "name": "Test Brewery 2",
            "brewery_type": "regional",
            "address_1": "456 Oak Ave",
            "address_2": None,
            "address_3": None,
            "city": "Portland",
            "state_province": "Oregon",
            "postal_code": "97201",
            "country": "United States",
            "longitude": "-122.6750",
            "latitude": "45.5152",
            "phone": "9876543210",
            "website_url": "https://testbrewery2.com",
            "state": "Oregon",
            "street": "456 Oak Ave",
        },
        {
            "id": "3",
            "name": "Test Brewery 3",
            "brewery_type": "brewpub",
            "address_1": "789 Pine Rd",
            "address_2": None,
            "address_3": None,
            "city": "Seattle",
            "state_province": None,
            "postal_code": "98101",
            "country": "United States",
            "longitude": None,
            "latitude": None,
            "phone": None,
            "website_url": None,
            "state": None,
            "street": "789 Pine Rd",
        },
    ]


@pytest.fixture
def sample_normalized_data():
    """Fixture providing normalized bronze data with float types."""
    return [
        {
            "id": "1",
            "name": "Test Brewery 1",
            "brewery_type": "micro",
            "address_1": "123 Main St",
            "address_2": None,
            "address_3": None,
            "city": "San Francisco",
            "state_province": "California",
            "postal_code": "94102",
            "country": "United States",
            "longitude": -122.4194,
            "latitude": 37.7749,
            "phone": "1234567890",
            "website_url": "https://testbrewery1.com",
            "state": "California",
            "street": "123 Main St",
        },
        {
            "id": "2",
            "name": "Test Brewery 2",
            "brewery_type": "regional",
            "address_1": "456 Oak Ave",
            "address_2": None,
            "address_3": None,
            "city": "Portland",
            "state_province": "Oregon",
            "postal_code": "97201",
            "country": "United States",
            "longitude": -122.6750,
            "latitude": 45.5152,
            "phone": "9876543210",
            "website_url": "https://testbrewery2.com",
            "state": "Oregon",
            "street": "456 Oak Ave",
        },
    ]


class TestSilverTransformationTask:
    """Test suite for SilverTransformationTask class."""

    def test_init(self, mock_dependencies):
        """Test task initialization."""
        task = SilverTransformationTask()

        assert task.azure_storage is not None
        assert task.log is not None
        assert task.spark is not None

    def test_init_extracts_account_info_from_connection_string(
        self, mock_dependencies
    ):
        """Test that account name and key are extracted from connection string."""
        with patch.dict(
            os.environ,
            {
                "AZURE_STORAGE_CONNECTION_STRING": "AccountName=myaccount;AccountKey=mykey123;"
            },
        ):
            task = SilverTransformationTask()
            # Task should initialize without errors
            assert task is not None

    def test_execute_success(
        self, mock_dependencies, sample_bronze_data, sample_normalized_data
    ):
        """Test successful execution of the task."""
        task = SilverTransformationTask()
        mock_df = Mock()

        task.azure_storage._read_from_layer.return_value = sample_bronze_data

        with patch.object(
            task,
            "_normalize_bronze_data_types",
            return_value=sample_normalized_data,
        ) as mock_normalize, patch.object(
            task, "_process_bronze_data", return_value=mock_df
        ) as mock_process, patch.object(
            task, "_save_to_silver_layer"
        ) as mock_save:

            task.execute()

            task.azure_storage._read_from_layer.assert_called_once_with(
                "bronze", "breweries_list.json"
            )
            mock_normalize.assert_called_once_with(sample_bronze_data)
            mock_process.assert_called_once_with(sample_normalized_data)
            mock_save.assert_called_once_with(mock_df)
            assert task.log.info.call_count >= 2

    def test_normalize_bronze_data_types_with_valid_coordinates(
        self, mock_dependencies
    ):
        """Test normalization of latitude and longitude from strings to floats."""
        task = SilverTransformationTask()

        input_data = [
            {"id": "1", "latitude": "37.7749", "longitude": "-122.4194"},
            {"id": "2", "latitude": "45.5152", "longitude": "-122.6750"},
        ]

        result = task._normalize_bronze_data_types(input_data)

        assert result[0]["latitude"] == 37.7749
        assert result[0]["longitude"] == -122.4194
        assert result[1]["latitude"] == 45.5152
        assert result[1]["longitude"] == -122.6750
        assert isinstance(result[0]["latitude"], float)
        assert isinstance(result[0]["longitude"], float)

    def test_normalize_bronze_data_types_with_null_coordinates(
        self, mock_dependencies
    ):
        """Test normalization handles null latitude and longitude."""
        task = SilverTransformationTask()

        input_data = [
            {"id": "1", "latitude": None, "longitude": None},
            {"id": "2", "latitude": "45.5152", "longitude": None},
        ]

        result = task._normalize_bronze_data_types(input_data)

        assert result[0]["latitude"] is None
        assert result[0]["longitude"] is None
        assert result[1]["latitude"] == 45.5152
        assert result[1]["longitude"] is None

    def test_process_bronze_data_creates_dataframe(
        self, mock_dependencies, spark, sample_normalized_data
    ):
        """Test processing bronze data creates a Spark DataFrame."""
        task = SilverTransformationTask()
        task.spark = spark

        with patch(
            "tasks.silver_transformation_task.SchemaTables"
        ) as mock_schema:
            # Create a simple schema for testing
            mock_schema.BRONZE_SCHEMA_BREWERIES = StructType(
                [
                    StructField("id", StringType(), True),
                    StructField("name", StringType(), True),
                    StructField("state_province", StringType(), True),
                    StructField("latitude", FloatType(), True),
                    StructField("longitude", FloatType(), True),
                ]
            )

            simple_data = [
                {
                    "id": "1",
                    "name": "Brewery 1",
                    "state_province": "CA",
                    "latitude": 37.77,
                    "longitude": -122.42,
                },
                {
                    "id": "2",
                    "name": "Brewery 2",
                    "state_province": None,
                    "latitude": 45.52,
                    "longitude": -122.68,
                },
            ]

            result_df = task._process_bronze_data(simple_data)

            assert result_df.count() == 2
            # Check that null state_province is filled with "UNKNOWN"
            unknown_records = result_df.filter(
                result_df.state_province == "UNKNOWN"
            )
            assert unknown_records.count() == 1

    def test_process_bronze_data_handles_null_state_province(
        self, mock_dependencies, spark
    ):
        """Test that null state_province values are replaced with 'UNKNOWN'."""
        task = SilverTransformationTask()
        task.spark = spark

        with patch(
            "tasks.silver_transformation_task.SchemaTables"
        ) as mock_schema:
            mock_schema.BRONZE_SCHEMA_BREWERIES = StructType(
                [
                    StructField("id", StringType(), True),
                    StructField("state_province", StringType(), True),
                ]
            )

            data_with_nulls = [
                {"id": "1", "state_province": None},
                {"id": "2", "state_province": "California"},
                {"id": "3", "state_province": None},
            ]

            result_df = task._process_bronze_data(data_with_nulls)

            unknown_count = result_df.filter(
                result_df.state_province == "UNKNOWN"
            ).count()
            assert unknown_count == 2
            california_count = result_df.filter(
                result_df.state_province == "California"
            ).count()
            assert california_count == 1

    def test_save_to_silver_layer(self, mock_dependencies, spark):
        """Test saving DataFrame to silver layer."""
        task = SilverTransformationTask()
        task.spark = spark

        # Create a mock DataFrame
        mock_df = Mock()
        mock_writer = Mock()
        mock_format = Mock()
        mock_mode = Mock()
        mock_partition = Mock()

        mock_df.write = mock_writer
        mock_writer.format.return_value = mock_format
        mock_format.mode.return_value = mock_mode
        mock_mode.partitionBy.return_value = mock_partition
        mock_partition.save = Mock()

        with patch.object(task, "_upload_partitioned_data") as mock_upload:
            task._save_to_silver_layer(mock_df)

            mock_writer.format.assert_called_once_with("parquet")
            mock_format.mode.assert_called_once_with("overwrite")
            mock_mode.partitionBy.assert_called_once_with("state_province")
            mock_partition.save.assert_called_once_with(
                "/tmp/silver_breweries"
            )
            mock_upload.assert_called_once_with("/tmp/silver_breweries")

    def test_upload_partitioned_data(self, mock_dependencies):
        """Test uploading partitioned parquet files to Azure."""
        task = SilverTransformationTask()

        # Mock os.walk to simulate directory structure
        mock_walk_result = [
            ("/tmp/silver_breweries", ["state_province=CA"], []),
            (
                "/tmp/silver_breweries/state_province=CA",
                [],
                ["part-00000.parquet", "part-00001.parquet"],
            ),
            ("/tmp/silver_breweries", ["state_province=OR"], []),
            (
                "/tmp/silver_breweries/state_province=OR",
                [],
                ["part-00000.parquet"],
            ),
        ]

        with patch("os.walk", return_value=mock_walk_result), patch(
            "builtins.open", mock_open(read_data=b"parquet_data")
        ):

            task._upload_partitioned_data("/tmp/silver_breweries")

            # Should upload 3 parquet files
            assert task.azure_storage.upload_to_layer.call_count == 3

            # Verify upload calls contain correct layer
            for call_args in task.azure_storage.upload_to_layer.call_args_list:
                assert call_args[0][2] == "silver"

    def test_upload_partitioned_data_handles_nested_structure(
        self, mock_dependencies
    ):
        """Test uploading handles nested partition directory structure."""
        task = SilverTransformationTask()

        mock_walk_result = [
            (
                "/tmp/silver_breweries/state_province=CA",
                [],
                ["part-00000.parquet"],
            )
        ]

        with patch("os.walk", return_value=mock_walk_result), patch(
            "builtins.open", mock_open(read_data=b"test_data")
        ) as m_open, patch(
            "os.path.relpath",
            return_value="state_province=CA/part-00000.parquet",
        ):

            task._upload_partitioned_data("/tmp/silver_breweries")

            task.azure_storage.upload_to_layer.assert_called_once_with(
                b"test_data", "state_province=CA/part-00000.parquet", "silver"
            )

    def test_upload_partitioned_data_skips_non_parquet_files(
        self, mock_dependencies
    ):
        """Test that only .parquet files are uploaded."""
        task = SilverTransformationTask()

        mock_walk_result = [
            (
                "/tmp/silver_breweries",
                [],
                ["part-00000.parquet", "_SUCCESS", "metadata.json"],
            )
        ]

        with patch("os.walk", return_value=mock_walk_result), patch(
            "builtins.open", mock_open(read_data=b"data")
        ):

            task._upload_partitioned_data("/tmp/silver_breweries")

            # Only the .parquet file should be uploaded
            assert task.azure_storage.upload_to_layer.call_count == 1

    def test_normalize_bronze_data_types_preserves_other_fields(
        self, mock_dependencies
    ):
        """Test that normalization only affects latitude/longitude."""
        task = SilverTransformationTask()

        input_data = [
            {
                "id": "1",
                "name": "Test Brewery",
                "latitude": "37.7749",
                "longitude": "-122.4194",
                "city": "San Francisco",
            }
        ]

        result = task._normalize_bronze_data_types(input_data)

        assert result[0]["id"] == "1"
        assert result[0]["name"] == "Test Brewery"
        assert result[0]["city"] == "San Francisco"
        assert result[0]["latitude"] == 37.7749
        assert result[0]["longitude"] == -122.4194
