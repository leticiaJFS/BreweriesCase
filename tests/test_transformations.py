import os
import pandas as pd
from src.transformations.bronze_to_silver import bronze_to_silver, SILVER_PATH

def test_silver_layer_creation():
    """
    Test if the Silver Layer Parquet files are created correctly.
    This test checks if at least one state/city partition exists after transformation.
    """
    # Run the transformation
    bronze_to_silver()
    
    # List all state folders in Silver Layer
    states = [d for d in os.listdir(SILVER_PATH) if os.path.isdir(os.path.join(SILVER_PATH, d))]
    
    # Ensure at least one state folder exists
    assert len(states) > 0, "No state folders created in Silver Layer"
    
    # Pick first state and check a city folder
    first_state_path = os.path.join(SILVER_PATH, states[0])
    cities = [d for d in os.listdir(first_state_path) if os.path.isdir(os.path.join(first_state_path, d))]
    
    assert len(cities) > 0, "No city folders created in Silver Layer"
    
    # Check if Parquet file exists in first city
    parquet_file = os.path.join(first_state_path, cities[0], "data.parquet")
    assert os.path.isfile(parquet_file), f"Parquet file not found: {parquet_file}"
    
    # Optional: check if Parquet can be read
    df = pd.read_parquet(parquet_file)
    assert not df.empty, "Parquet file is empty"
