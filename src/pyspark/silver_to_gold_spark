"""
PySpark equivalent of the pipeline transformation.
Requires Spark environment to run. Provided as example to demonstrate PySpark knowledge.

Purpose:
--------
This script reads the cleaned and partitioned Parquet files from the Silver Layer,
aggregates them by brewery type and location, and produces the Gold Layer dataset.
The resulting dataset provides a summarized view suitable for analytics and dashboards.

Note:
-----
This script requires a Spark environment. It is provided as an example
to demonstrate PySpark knowledge and pipeline implementation.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count
import os

# ---------------------------------------------------------------------
# PATH CONFIGURATION
# ---------------------------------------------------------------------

SILVER_PATH = os.path.join(os.getcwd(), "data", "silver")
GOLD_PATH = os.path.join(os.getcwd(), "data", "gold")

# ---------------------------------------------------------------------
# MAIN AGGREGATION FUNCTION
# ---------------------------------------------------------------------

def silver_to_gold_spark(silver_dir, gold_dir):
    """
    Aggregates data from the Silver Layer to create the Gold Layer using PySpark.

    Steps:
    1. Read all partitioned Parquet files from the Silver Layer.
    2. Combine all partitions into a single DataFrame.
    3. Aggregate the number of breweries by 'brewery_type', 'state', and 'city'.
    4. Save the aggregated dataset into the Gold Layer as a Parquet file.
    """

    # -----------------------------------------------------------------
    # 1. CREATE SPARK SESSION
    # -----------------------------------------------------------------
    spark = SparkSession.builder.appName("SilverToGold").getOrCreate()

    # -----------------------------------------------------------------
    # 2. LOAD SILVER LAYER PARQUET FILES
    # -----------------------------------------------------------------
    if not os.path.exists(silver_dir):
        print(f"[Gold] Silver directory {silver_dir} does not exist. Exiting.")
        return

    # Read all partitioned Parquet files using Spark
    df = spark.read.option("mergeSchema", "true").parquet(silver_dir)

    print(f"[Gold] Loaded {df.count()} records from Silver Layer partitions.")

    # -----------------------------------------------------------------
    # 3. AGGREGATE DATA
    # -----------------------------------------------------------------
    agg_df = (
        df.groupBy("state", "city", "brewery_type")
          .agg(count("*").alias("brewery_count"))
    )

    # -----------------------------------------------------------------
    # 4. SAVE GOLD LAYER PARQUET
    # -----------------------------------------------------------------
    os.makedirs(gold_dir, exist_ok=True)
    output_path = os.path.join(gold_dir, "breweries_aggregated.parquet")

    agg_df.write.mode("overwrite").parquet(output_path)
    print(f"[Gold] Aggregated Gold Layer saved → {output_path}")

    print("[Gold] Preview of aggregated data:")
    agg_df.show(5, truncate=False)

    spark.stop()

# ---------------------------------------------------------------------
# SCRIPT ENTRY POINT
# ---------------------------------------------------------------------

if __name__ == "__main__":
    silver_to_gold_spark(SILVER_PATH, GOLD_PATH)
