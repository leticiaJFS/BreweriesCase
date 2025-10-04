import os
import pandas as pd
from src.transformations.bronze_to_silver import bronze_to_silver, BRONZE_PATH, SILVER_PATH

def test_silver_layer_creation():
    """
    
    Tests if the Silver Layer Parquet files are created correctly 
    and partitioned as expected (state/city).
    
    This test verifies:
    1. The successful execution of the Bronze-to-Silver transformation.
    2. The creation of at least one state folder (Level 1 partition).
    3. The creation of at least one city folder within that state (Level 2 partition).
    4. The existence of the 'data.parquet' file inside the city directory.
    5. That the created Parquet file is not empty (content validation).
    """

    # Step 1: Run the transformation from Bronze to Silver Layer
    print("Starting Bronze to Silver transformation...")
    bronze_to_silver(BRONZE_PATH, SILVER_PATH)
    print("Transformation completed.")
    
    # Step 2: List all state folders in Silver Layer
    states = [
        d for d in os.listdir(SILVER_PATH) 
        if os.path.isdir(os.path.join(SILVER_PATH, d))
    ]
    
    # 3. Select the first state found for deeper verification
    first_state = states[0]
    first_state_path = os.path.join(SILVER_PATH, first_state)
    
    # List the directories (which represent cities) within the first state
    cities = [
        d for d in os.listdir(first_state_path) 
        if os.path.isdir(os.path.join(first_state_path, d))
    ]
    
    # Assert that at least one city directory was created
    assert len(cities) > 0, f"No city folders created inside '{first_state}'. Level 2 partition failure."
    print(f"Success: {len(cities)} cities found in state '{first_state}'.")

    # 4. Check for the existence of the Parquet file in the first city
    first_city = cities[0]
    parquet_file = os.path.join(first_state_path, first_city, "data.parquet")
    
    # Assert that the final data file exists
    assert os.path.isfile(parquet_file), f"Parquet file not found at path: {parquet_file}"
    print(f"Success: 'data.parquet' file found in city '{first_city}'.")

   # 5. Content Validation: Check if the file can be read and is not empty
    try:
        df = pd.read_parquet(parquet_file)
        # Assert that the read DataFrame contains data (is not empty)
        assert not df.empty, f"The Parquet file '{parquet_file}' is empty."
        print(f"Success: Parquet file read. It has {len(df)} records.")
    except Exception as e:
        assert False, f"Error reading the Parquet file: {e}"

    print("\nSilver Layer Creation Test completed SUCCESSFULLY.")