"""
Silver Layer Transformation

This script reads the raw data (Bronze Layer) from the Open Brewery DB API,
transforms it into a clean tabular format, and saves it in a columnar storage
format (Parquet). The data is partitioned by brewery location: state and city.

Transformations performed:
1. Filtered relevant columns for analytics.
2. Replaced missing values (NaN) with empty strings to avoid issues in Parquet.
3. Partitioned data by 'state' and 'city' for optimized querying.
"""

import pandas as pd
import os
import json
import numpy as np
import re

# Paths for Bronze input and Silver output
BRONZE_PATH = os.path.join(os.getcwd(), "data", "bronze", "breweries_raw.json")
SILVER_PATH = os.path.join(os.getcwd(), "data", "silver")

def sanitize_folder_name(name):
    """
    Remove spaces at the start/end and replace problematic characters for Windows.
    """
    name = name.strip()  # remove espaços extras
    name = re.sub(r'[<>:"/\\|?*]', '_', name)  # substitui caracteres inválidos
    return name

def bronze_to_silver(bronze_path, silver_dir):
    """
    Reads raw JSON data from the Bronze Layer, transforms it into a clean
    DataFrame, and writes Parquet files partitioned by state and city.
    """
    # Load JSON data
    with open(bronze_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    
    # Convert to DataFrame
    df = pd.DataFrame(data)
    
    # Ensure Silver folder exists
    os.makedirs(silver_dir, exist_ok=True)

    # Filtragem de colunas: Mantemos apenas colunas relevantes para análise (id, name, type, localização, etc.).    
    # Select relevant columns for analytics
    columns = [
        "id", "name", "brewery_type", "street", "city", "state",
        "postal_code", "country", "longitude", "latitude", "phone", "website_url"
    ]
    df = df[columns]
    
    # Separate numeric and string columns
    string_cols = ["id", "name", "brewery_type", "street", "city", "state", "postal_code", "country", "phone", "website_url"]
    numeric_cols = ["longitude", "latitude"]

    #Tratamento de valores ausentes: NaN é substituído por "" para evitar problemas na leitura do Parquet.
    # Replace missing values with empty string to ensure consistency in Parquet
    df[string_cols] = df[string_cols].fillna("")          # strings: empty string
    df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors="coerce")  # numbers: NaN
    
    # Save Parquet files partitioned by state and city
    for state, state_df in df.groupby("state"):
        # remove espaços no início/fim e substitui caracteres inválidos (<>:"/\|?*) por _.
        state_clean = sanitize_folder_name(state)
        for city, city_df in state_df.groupby("city"):
            # remove espaços no início/fim e substitui caracteres inválidos (<>:"/\|?*) por _.
            city_clean = sanitize_folder_name(city)

            # Build folder path for partition
            city_path = os.path.join(SILVER_PATH, f"state={state_clean}", f"city={city_clean}")
            os.makedirs(city_path, exist_ok=True)
            
            # Save Parquet file (columnar format)
            city_df.to_parquet(os.path.join(city_path, "data.parquet"), index=False)
            print(f"Saved Silver Layer: state={state} / city={city_clean}")

if __name__ == "__main__":
    bronze_to_silver(BRONZE_PATH, SILVER_PATH)
