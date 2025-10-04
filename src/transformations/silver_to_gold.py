"""
Gold Layer Aggregation Script

Purpose:
--------
This script reads the cleaned and partitioned Parquet files from the Silver Layer,
aggregates them by brewery type and location, and produces the Gold Layer dataset.
The resulting dataset provides a summarized view suitable for analytics and dashboards.

Overview of Processing Steps:
-----------------------------
1. Read all Parquet files from the Silver Layer (partitioned by state and city).
2. Concatenate all partitions into a single DataFrame.
3. Aggregate the number of breweries by 'brewery_type', 'state', and 'city'.
4. Save the aggregated results into the Gold Layer as a Parquet file.

"""

import pandas as pd
import os
import glob

# ---------------------------------------------------------------------
# PATH CONFIGURATION
# ---------------------------------------------------------------------

# Define paths for Silver input and Gold output
SILVER_PATH = os.path.join(os.getcwd(), "data", "silver")
GOLD_PATH = os.path.join(os.getcwd(), "data", "gold")

def silver_to_gold():
    """
    Aggregates data from the Silver Layer to create the Gold Layer.

    This function:
    - Reads all Parquet files stored in partitioned directories (by state and city).
    - Combines them into a unified dataset.
    - Performs aggregation to count the number of breweries by type and location.
    - Saves the resulting dataset into the Gold Layer as a Parquet file.
    """
    
    # Ensure Gold directory exists
    os.makedirs(GOLD_PATH, exist_ok=True)
    
    # -----------------------------------------------------------------
    # 1. Locate all Parquet files in the Silver Layer
    # -----------------------------------------------------------------
    parquet_files = glob.glob(os.path.join(SILVER_PATH, "state=*", "city=*", "data.parquet"))
    
    if not parquet_files:
        print("[Gold] No Silver Layer Parquet files found — aggregation skipped.")
        return
    
    # -----------------------------------------------------------------
    # 2. Read and combine all Silver Layer partitions
    # -----------------------------------------------------------------
    df_list = [pd.read_parquet(f) for f in parquet_files]
    df = pd.concat(df_list, ignore_index=True)
    print(f"[Gold] Loaded {len(df)} records from {len(parquet_files)} Silver partitions.")

    # -----------------------------------------------------------------
    # 3. Aggregate: count breweries by type and location
    # -----------------------------------------------------------------
    agg_df = (
        df.groupby(["state", "city", "brewery_type"])
            .size()
            .reset_index(name="brewery_count")
    )
    
    # -----------------------------------------------------------------
    # 4. Save aggregated data as Parquet (Gold Layer)
    # -----------------------------------------------------------------
    
    gold_file = os.path.join(GOLD_PATH, "breweries_aggregated.parquet")
    agg_df.to_parquet(gold_file, index=False)

    print(f"[Gold] Aggregated data saved successfully → {gold_file}")
    print("[Gold] Preview of aggregated data:")
    print(agg_df.head())

# ---------------------------------------------------------------------
# SCRIPT ENTRY POINT
# ---------------------------------------------------------------------

if __name__ == "__main__":
    silver_to_gold()
