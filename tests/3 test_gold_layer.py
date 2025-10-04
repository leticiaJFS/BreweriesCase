"""
Unit Test — Gold Layer Creation

This script validates the correct generation of the Gold Layer in the
Medallion architecture pipeline.

It performs the following checks:
1. Ensures that the Gold Layer Parquet file is successfully created.
2. Confirms that the resulting DataFrame is not empty.
3. Verifies that the expected columns exist in the aggregated dataset.

This test helps guarantee the integrity of the aggregation process 
from the Silver Layer to the Gold Layer.
"""

import os
import pandas as pd
from src.transformations.silver_to_gold import silver_to_gold, GOLD_PATH

def test_gold_layer_creation():
    """
    Unit test to validate the Gold Layer creation and structure.
    
    Steps:
    1. Execute the `silver_to_gold()` transformation function.
    2. Check whether the output Parquet file exists at the expected location.
    3. Load the Parquet file into a DataFrame.
    4. Assert that the DataFrame:
       - Is not empty.
       - Contains all required columns.
    
    Raises:
        AssertionError: If any validation step fails.
    """

    # Step 1: Run the transformation from Silver to Gold Layer
    print("Starting Silver to Gold transformation...")
    silver_to_gold()
    print("Transformation completed.")
    
    # Step 2: Define the expected output Parquet file path
    gold_file = os.path.join(GOLD_PATH, "breweries_aggregated.parquet")
    
    # Step 3: Validate that the file was created
    assert os.path.isfile(gold_file), "Gold Layer Parquet file not found"
    
    # Step 4: Load the Gold Layer data into a DataFrame
    df = pd.read_parquet(gold_file)
    
    # Step 5: Check that the DataFrame is not empty
    assert not df.empty, "Gold Layer DataFrame is empty"
    
    # Step 6: Ensure required columns exist in the DataFrame
    expected_columns = ["state", "city", "brewery_type", "brewery_count"]
    assert all(col in df.columns for col in expected_columns), \
        f"Missing columns in Gold Layer: Expected {expected_columns}, found {df.columns.tolist()}"

    print("Gold Layer test passed successfully!")


# Optional: Allow running this test directly (not only via pytest)
if __name__ == "__main__":
    test_gold_layer_creation()