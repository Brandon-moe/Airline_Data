from data_load_repository import BigQueryDataLoadingRepository
import unittest
from unittest.mock import Mock, call
from main import *


class TestMainPipeline(unittest.TestCase):
    def setup(self):
        load_dotenv()

    def test_extract_function_success(self):
        # Create a mock for requests.get and response
        get_mock = Mock(name="get_mock")
        response_mock = Mock()
        response_mock.status_code = 200
        response_mock.text = '{"response": "some data"}'
        get_mock.return_value = response_mock

        # Call the function with the mock
        api_key = os.getenv("API_KEY")
        result = extract(api_key=api_key, base_url="http://example.com/",
                         make_get_request=get_mock)

        # Assertions
        get_mock.assert_called_once_with(f"http://example.com/flights?api_key={api_key}")
        self.assertEqual(result, "some data")

    def test_extract_function_failure(self):
        # Create a mock for requests.get and response
        get_mock = Mock(name="get_mock")
        response_mock = Mock()
        response_mock.status_code = 400
        response_mock.text = '{"response": "some data"}'
        get_mock.return_value = response_mock

        # Call the function with the mock
        api_key = os.getenv("API_KEY")
        result = extract(api_key=api_key, base_url="http://example.com/",
                         make_get_request=get_mock)

        # Assertions
        get_mock.assert_called_once_with(f"http://example.com/flights?api_key={api_key}")
        self.assertEqual(result, {"message": "Invalid response received from airlabs API"})

    def test_load_function_success(self):
        # Create a mock for the BigQuery client
        mock_client = Mock()

        # Initialize the repository with the mock client
        DATASET_ID = "test_dataset"
        repository = BigQueryDataLoadingRepository(mock_client, DATASET_ID)

        # Mock the repository's load_data method
        repository.load_data = Mock(side_effect=[
            None,  # First call for NEW_TABLE_ID overwrite
            None  # Second call for OLD_TABLE_ID append
        ])

        # Mock data
        df = pd.DataFrame
        new_data = pd.DataFrame

        # Call the function with the mock repository
        result = load(df, new_data, DATASET_ID, "old_table",
                      "new_table", repository)

        # Assertions
        self.assertEqual(result, {"message": "Database updated successfully"})

        # Check the calls to the load_data method
        repository.load_data.assert_has_calls([
            call(df, "new_table", "WRITE_TRUNCATE"),
            call(new_data, "old_table", "WRITE_APPEND")
        ])

        # Ensure the load_data was called exactly twice
        self.assertEqual(repository.load_data.call_count, 2)

    def test_load_function_table_not_found(self):
        # Create a mock for the BigQuery client
        mock_client = Mock()

        # Mock the load_table_from_dataframe method
        mock_client.load_table_from_dataframe.side_effect = [
            None,  # First call for NEW_TABLE_ID overwrite
            Exception("Not found: Table"),  # Second call for OLD_TABLE_ID append
            None  # Third call for OLD_TABLE_ID create
        ]

        # Initialize the repository with the mock client
        DATASET_ID = "test_dataset"
        repository = BigQueryDataLoadingRepository(mock_client, DATASET_ID)

        # Mock the repository's load_data method
        repository.load_data = Mock(side_effect=[
            None,  # First call for NEW_TABLE_ID overwrite
            Exception("Not found: Table"),  # Second call for OLD_TABLE_ID append
            None  # Third call for OLD_TABLE_ID create
        ])

        # Mock data
        df = pd.DataFrame
        new_data = pd.DataFrame

        # Call the function with the mock repository
        result = load(df, new_data, DATASET_ID, "old_table", "new_table", repository)

        # Assertions
        self.assertEqual(result, {"message": "Database updated successfully"})

        # Check the calls to the load_data method
        repository.load_data.assert_has_calls([
            call(df, "new_table", "WRITE_TRUNCATE"),
            call(new_data, "old_table", "WRITE_APPEND"),
            call(new_data, "old_table", "WRITE_EMPTY")
        ])

        # Ensure the load_data was called exactly three times
        self.assertEqual(repository.load_data.call_count, 3)

    def test_load_function_error(self):
        # Create a mock for the BigQuery client
        mock_client = Mock()

        # Initialize the repository with the mock client
        DATASET_ID = "test_dataset"
        repository = BigQueryDataLoadingRepository(mock_client, DATASET_ID)

        # Mock the repository's load_data method
        repository.load_data = Mock(side_effect=[
            None,  # First call for NEW_TABLE_ID overwrite
            Exception("Unexpected error")  # Second call for OLD_TABLE_ID append
        ])

        # Mock data
        df = pd.DataFrame
        new_data = pd.DataFrame

        # Call the function with the mock repository
        result = load(df, new_data, DATASET_ID, "old_table", "new_table", repository)

        # Assertions
        self.assertEqual(result, {"message": "An unexpected error occurred"})

        # Check the calls to the load_data method
        repository.load_data.assert_has_calls([
            call(df, "new_table", "WRITE_TRUNCATE"),
            call(new_data, "old_table", "WRITE_APPEND")
        ])

        # Ensure the load_data was called exactly twice
        self.assertEqual(repository.load_data.call_count, 2)
