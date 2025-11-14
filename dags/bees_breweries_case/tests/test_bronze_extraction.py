import json
import pytest
from unittest.mock import Mock, patch, MagicMock, call
from requests.exceptions import RequestException, Timeout

from tasks.bronze_extraction_task import BronzeExtractionTask


@pytest.fixture
def mock_dependencies():
    """Fixture to mock all dependencies."""
    with patch(
        "tasks.bronze_extraction_task.LoggingConfig"
    ) as mock_logger, patch(
        "tasks.bronze_extraction_task.AzureStorageConfig"
    ) as mock_storage:

        mock_logger_instance = Mock()
        mock_logger.return_value = mock_logger_instance
        mock_logger_instance.configure_logging.return_value = Mock()

        mock_storage_instance = Mock()
        mock_storage.return_value = mock_storage_instance

        yield {
            "logger": mock_logger_instance,
            "storage": mock_storage_instance,
        }


@pytest.fixture
def sample_brewery_data():
    """Fixture providing sample brewery data."""
    return [
        {
            "id": "1",
            "name": "Test Brewery 1",
            "brewery_type": "micro",
            "city": "San Francisco",
            "state": "California",
        },
        {
            "id": "2",
            "name": "Test Brewery 2",
            "brewery_type": "regional",
            "city": "Portland",
            "state": "Oregon",
        },
    ]


class TestBronzeExtractionTask:
    """Test suite for BronzeExtractionTask class."""

    def test_init(self, mock_dependencies):
        """Test task initialization."""
        task = BronzeExtractionTask()

        assert task.BASE_URL == "https://api.openbrewerydb.org/v1/breweries"
        assert task.session is not None
        assert (
            task.session.headers.get("User-Agent") == "BreweriesPipeline/1.0"
        )
        assert task.execution_date is not None

    def test_execute_success(self, mock_dependencies, sample_brewery_data):
        """Test successful execution of the task."""
        task = BronzeExtractionTask()

        with patch.object(
            task, "_fetch_all_data", return_value=sample_brewery_data
        ) as mock_fetch, patch.object(
            task, "_save_to_bronze_layer"
        ) as mock_save:

            task.execute()

            mock_fetch.assert_called_once()
            mock_save.assert_called_once_with(sample_brewery_data)
            assert task.log.info.call_count >= 2

    def test_fetch_all_data_single_page(
        self, mock_dependencies, sample_brewery_data
    ):
        """Test fetching data when all results fit in one page."""
        task = BronzeExtractionTask()

        with patch.object(
            task, "_fetch_page", side_effect=[sample_brewery_data, []]
        ):
            result = task._fetch_all_data(per_page=200)

            assert result == sample_brewery_data
            assert len(result) == 2

    def test_fetch_all_data_multiple_pages(self, mock_dependencies):
        """Test fetching data across multiple pages."""
        task = BronzeExtractionTask()

        page1_data = [{"id": "1", "name": "Brewery 1"}]
        page2_data = [{"id": "2", "name": "Brewery 2"}]
        page3_data = [{"id": "3", "name": "Brewery 3"}]

        with patch.object(
            task,
            "_fetch_page",
            side_effect=[page1_data, page2_data, page3_data, []],
        ), patch("tasks.bronze_extraction_task.time.sleep"):

            result = task._fetch_all_data(per_page=1)

            assert len(result) == 3
            assert result == page1_data + page2_data + page3_data

    def test_fetch_page_success(self, mock_dependencies, sample_brewery_data):
        """Test successful page fetch."""
        task = BronzeExtractionTask()

        mock_response = Mock()
        mock_response.json.return_value = sample_brewery_data
        mock_response.raise_for_status = Mock()

        with patch.object(task.session, "get", return_value=mock_response):
            result = task._fetch_page(page=1, per_page=200, max_retries=3)

            assert result == sample_brewery_data
            task.session.get.assert_called_once_with(
                "https://api.openbrewerydb.org/v1/breweries?page=1&per_page=200",
                timeout=30,
            )

    def test_fetch_page_retry_then_success(
        self, mock_dependencies, sample_brewery_data
    ):
        """Test retry logic - fails once then succeeds."""
        task = BronzeExtractionTask()

        mock_response = Mock()
        mock_response.json.return_value = sample_brewery_data
        mock_response.raise_for_status = Mock()

        with patch.object(
            task.session, "get", side_effect=[Timeout(), mock_response]
        ), patch("tasks.bronze_extraction_task.time.sleep"):

            result = task._fetch_page(page=1, per_page=200, max_retries=3)

            assert result == sample_brewery_data
            assert task.session.get.call_count == 2

    def test_fetch_page_max_retries_exceeded(self, mock_dependencies):
        """Test that empty list is returned after max retries."""
        task = BronzeExtractionTask()

        with patch.object(
            task.session,
            "get",
            side_effect=RequestException("Connection error"),
        ), patch("tasks.bronze_extraction_task.time.sleep"):

            result = task._fetch_page(page=1, per_page=200, max_retries=3)

            assert result == []
            assert task.session.get.call_count == 3
            task.log.error.assert_called_once()

    def test_fetch_page_http_error(self, mock_dependencies):
        """Test handling of HTTP errors."""
        task = BronzeExtractionTask()

        mock_response = Mock()
        mock_response.raise_for_status.side_effect = RequestException(
            "404 Not Found"
        )

        with patch.object(
            task.session, "get", return_value=mock_response
        ), patch("tasks.bronze_extraction_task.time.sleep"):

            result = task._fetch_page(page=1, per_page=200, max_retries=2)

            assert result == []

    def test_save_to_bronze_layer(
        self, mock_dependencies, sample_brewery_data
    ):
        """Test saving data to bronze layer."""
        task = BronzeExtractionTask()

        task._save_to_bronze_layer(sample_brewery_data)

        task.azure_storage._ensure_container_exists.assert_called_once()

        expected_json_bytes = json.dumps(sample_brewery_data).encode("utf-8")
        task.azure_storage.upload_to_layer.assert_called_once_with(
            expected_json_bytes, "breweries_list.json", "bronze"
        )

        assert task.log.info.call_count >= 2

    def test_save_to_bronze_layer_empty_data(self, mock_dependencies):
        """Test saving empty data to bronze layer."""
        task = BronzeExtractionTask()

        task._save_to_bronze_layer([])

        task.azure_storage._ensure_container_exists.assert_called_once()

        expected_json_bytes = json.dumps([]).encode("utf-8")
        task.azure_storage.upload_to_layer.assert_called_once_with(
            expected_json_bytes, "breweries_list.json", "bronze"
        )

    def test_exponential_backoff_timing(self, mock_dependencies):
        """Test that retry delays follow exponential backoff."""
        task = BronzeExtractionTask()

        with patch.object(
            task.session, "get", side_effect=RequestException("Error")
        ), patch("tasks.bronze_extraction_task.time.sleep") as mock_sleep:

            task._fetch_page(page=1, per_page=200, max_retries=3)

            # Verify exponential backoff: 2^0=1, 2^1=2
            sleep_calls = [call(1), call(2)]
            mock_sleep.assert_has_calls(sleep_calls, any_order=False)
