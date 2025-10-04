import unittest
from unittest import mock
import os
import json
import shutil 
from pathlib import Path

# Adjust the import to the correct path where the ExtractTask is defined
# **CORRECTION**: The path is now set to src.pipeline.luigi_pipeline based on the user's project structure.
from src.pipeline.luigi_pipeline import ExtractTask, DATA_DIR 

# Define test paths to ensure data is saved in the correct location
TEST_BRONZE_DIR = Path(DATA_DIR) / "bronze"
TEST_FILE_PATH = TEST_BRONZE_DIR / "breweries_raw.json"

class TestExtractTask(unittest.TestCase):
    """
    Tests for the ExtractTask (Bronze Layer) of the Luigi pipeline.
    The focus is on simulating the API call and verifying the correct storage 
    of the raw data in the JSON file.
    """

    def setUp(self):
        """Setup: Ensures the test directory is clean before each test."""
        if TEST_BRONZE_DIR.exists():
            # Safely remove the folder for cleanup
            shutil.rmtree(TEST_BRONZE_DIR, ignore_errors=True)
        os.makedirs(TEST_BRONZE_DIR, exist_ok=True)
        
    def tearDown(self):
        """Teardown: Removes the test directory after each test."""
        if TEST_BRONZE_DIR.exists():
            shutil.rmtree(TEST_BRONZE_DIR, ignore_errors=True)
            
    # Uses mock.patch to simulate the return of the external function fetch_breweries
    @mock.patch('src.pipeline.luigi_pipeline.fetch_breweries')
    def test_extract_task_success(self, mock_fetch):
        """
        Tests the successful extraction scenario.
        Verifies that the extraction function is called and the raw JSON is saved correctly.
        """
        
        # 1. Simulation Data (Mock Data)
        # Defines the simulated data that 'fetch_breweries' should return
        mock_data = [
            {"id": "test_1", "name": "Mock Brewery A", "city": "Springfield", "state": "IL", "brewery_type": "micro"},
            {"id": "test_2", "name": "Mock Brewery B", "city": "Shelbyville", "state": "IL", "brewery_type": "planning"},
        ]
        
        # Configures the mocked function to return the test data
        mock_fetch.return_value = mock_data
        
        # 2. Execution
        task = ExtractTask()
        task.run()
        
        # 3. Assertions
        
        # Confirms that the external extraction function was called
        mock_fetch.assert_called_once()
        
        # Confirms that the output file exists
        self.assertTrue(TEST_FILE_PATH.is_file(), 
                        "The raw JSON file was not created in the Bronze directory.")
        
        # Confirms that the file content is as expected
        with open(TEST_FILE_PATH, "r", encoding="utf-8") as f:
            saved_data = json.load(f)
        
        self.assertEqual(saved_data, mock_data, 
                         "The content of the saved JSON file does not match the mocked data.")
        self.assertEqual(len(saved_data), 2, 
                         "The number of records in the saved file is incorrect.")

    @mock.patch('src.pipeline.luigi_pipeline.fetch_breweries')
    def test_extract_task_failure_api_error(self, mock_fetch):
        """
        Tests the API failure scenario.
        Verifies that the task propagates (re-raises) the exception and does not create the output file.
        """
        
        # Configures the mocked function to raise an exception (simulating network/API failure)
        error_message = "API connection timed out or returned 500."
        mock_fetch.side_effect = Exception(error_message)
        
        task = ExtractTask()
        
        # Asserts that running the task raises the exception
        with self.assertRaisesRegex(Exception, error_message):
            task.run()
            
        # Asserts that NO output file was created after the failure
        self.assertFalse(TEST_FILE_PATH.is_file(), 
                         "The raw JSON file was created despite the API failure.")