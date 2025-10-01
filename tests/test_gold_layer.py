import os
import pandas as pd
from src.transformations.silver_to_gold import silver_to_gold, GOLD_PATH

def test_gold_layer_creation():
    """
    Test if Gold Layer Parquet file is created and not empty.
    """
    silver_to_gold()
    
    gold_file = os.path.join(GOLD_PATH, "breweries_aggregated.parquet")
    assert os.path.isfile(gold_file), "Gold Layer Parquet file not found"
    
    df = pd.read_parquet(gold_file)
    assert not df.empty, "Gold Layer DataFrame is empty"
    assert all(col in df.columns for col in ["state", "city", "brewery_type", "brewery_count"]), "Missing columns in Gold Layer"
