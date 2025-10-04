"""
Silver Layer Transformation Script

Purpose:
--------
This script performs the transformation process from the Bronze Layer
(raw JSON data extracted from the Open Brewery DB API) into the Silver Layer
(a cleaned, structured, and partitioned dataset in Parquet format).

Overview of Processing Steps:
-----------------------------
1. Load raw JSON data from the Bronze Layer.
2. Select relevant columns for analytical use.
3. Clean and sanitize string and numeric values.
4. Partition and save the dataset by 'state' and 'city' into Parquet format.

"""

import pandas as pd
import os
import json
import numpy as np
import re

# ---------------------------------------------------------------------
# PATH CONFIGURATION
# ---------------------------------------------------------------------

# Absolute paths for Bronze input and Silver output directories

BRONZE_PATH = os.path.join(os.getcwd(), "data", "bronze", "breweries_raw.json")
SILVER_PATH = os.path.join(os.getcwd(), "data", "silver")

# ---------------------------------------------------------------------
# HELPER FUNCTIONS
# ---------------------------------------------------------------------

def sanitize_folder_name(name):
    """
    Remove spaces at the start/end and replace problematic characters for Windows and Unix systems..
    
    This function:
    - Strips leading/trailing spaces.
    - Replaces characters not allowed in Windows folder names (< > : " / \ | ? *) with underscores.

    Parameters
    ----------
    name : str
        The original folder name.

    Returns
    -------
    str
        A cleaned and safe folder name.
    """

    name = name.strip() 
    name = re.sub(r'[<>:"/\\|?*]', '_', name)  
    return name

# ---------------------------------------------------------------------
# MAIN TRANSFORMATION FUNCTION
# ---------------------------------------------------------------------

def bronze_to_silver(bronze_path, silver_dir):
    """    
    Transforms Bronze Layer data into the Silver Layer.

    This function:
    1. Loads the raw JSON data from the Bronze Layer.
    2. Filters only the relevant columns for analytical processing.
    3. Cleans missing values to ensure Parquet compatibility.
    4. Saves the transformed dataset partitioned by state and city.

    Parameters
    ----------
    bronze_path : str
        Path to the input Bronze Layer JSON file.
    silver_dir : str
        Path to the Silver Layer output directory.
    """

    # -----------------------------------------------------------------
    # 1. LOAD RAW DATA
    # -----------------------------------------------------------------
    
    with open(bronze_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    df = pd.DataFrame(data)
    
    # Ensure Silver output directory exists
    os.makedirs(silver_dir, exist_ok=True)

    # -----------------------------------------------------------------
    # 2. SELECT RELEVANT COLUMNS
    # -----------------------------------------------------------------
    columns = [
        "id", "name", "brewery_type", "street", "city", "state",
        "postal_code", "country", "longitude", "latitude", "phone", "website_url"
    ]
    df = df[columns]
    
    # -----------------------------------------------------------------
    # 3. HANDLE MISSING VALUES AND DATA TYPES
    # -----------------------------------------------------------------
    string_cols = ["id", "name", "brewery_type", "street", "city", "state", "postal_code", "country", "phone", "website_url"]
    numeric_cols = ["longitude", "latitude"]

    # Replace missing string values with empty strings
    df[string_cols] = df[string_cols].fillna("")      

    # Ensure numeric columns are properly typed (convert errors to NaN) 
    df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors="coerce")  

    # -----------------------------------------------------------------
    # 4. PARTITION AND SAVE DATA
    # -----------------------------------------------------------------   
    for state, state_df in df.groupby("state"):
        
        state_clean = sanitize_folder_name(state)
        for city, city_df in state_df.groupby("city"):
            
            city_clean = sanitize_folder_name(city)

            # Construct the partition directory path
            city_path = os.path.join(SILVER_PATH, f"state={state_clean}", f"city={city_clean}")
            os.makedirs(city_path, exist_ok=True)
            
            # Save the city-level data as a Parquet file
            city_df.to_parquet(os.path.join(city_path, "data.parquet"), index=False)
            print(f"Saved Silver Layer: state={state} / city={city_clean}")

# ---------------------------------------------------------------------
# SCRIPT ENTRY POINT
# ---------------------------------------------------------------------

if __name__ == "__main__":
    bronze_to_silver(BRONZE_PATH, SILVER_PATH)
