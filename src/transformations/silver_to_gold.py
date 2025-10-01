"""
Gold Layer Aggregation

This script reads the Silver Layer (Parquet files partitioned by state and city),
aggregates the number of breweries by brewery_type, state, and city, and saves
the results in Parquet format for analytics.
"""

import pandas as pd
import os
import glob

SILVER_PATH = os.path.join(os.getcwd(), "data", "silver")
GOLD_PATH = os.path.join(os.getcwd(), "data", "gold")

def silver_to_gold():
    """
    Aggregate Silver Layer data to produce Gold Layer: number of breweries
    by brewery_type, state, and city.
    """
    os.makedirs(GOLD_PATH, exist_ok=True)
    
    # Encontrar todos os arquivos Parquet na Silver Layer
    parquet_files = glob.glob(os.path.join(SILVER_PATH, "state=*", "city=*", "data.parquet"))
    
    if not parquet_files:
        print("No Silver Layer files found.")
        return
    
    # Ler todos os arquivos Silver em um único DataFrame
    df_list = [pd.read_parquet(f) for f in parquet_files]
    df = pd.concat(df_list, ignore_index=True)
    
    # Agregar: contar breweries por tipo e localização
    agg_df = df.groupby(["state", "city", "brewery_type"]).size().reset_index(name="brewery_count")
    
    # Salvar Gold Layer como Parquet
    gold_file = os.path.join(GOLD_PATH, "breweries_aggregated.parquet")
    agg_df.to_parquet(gold_file, index=False)
    print(f"Gold Layer saved: {gold_file}")
    print(agg_df.head())

if __name__ == "__main__":
    silver_to_gold()
