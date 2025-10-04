"""
PySpark Bronze to Silver Transformation Script, equivalent of the pipeline transformation.
Requires Spark environment to run. Provided as example to demonstrate PySpark knowledge.

Purpose:
--------
This script performs the transformation from the Bronze Layer
(raw JSON data extracted from the Open Brewery DB API) into the Silver Layer
(a cleaned, structured, and partitioned dataset in Parquet format) using PySpark.

Note:
-----
This script requires a Spark environment. It is provided as an example
to demonstrate PySpark knowledge and pipeline implementation.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
import re
import os

# ---------------------------------------------------------------------
# PATH CONFIGURATION
# ---------------------------------------------------------------------

BRONZE_PATH = os.path.join(os.getcwd(), "data", "bronze", "breweries_raw.json")
SILVER_PATH = os.path.join(os.getcwd(), "data", "silver")

# ---------------------------------------------------------------------
# HELPER FUNCTIONS
# ---------------------------------------------------------------------

def sanitize_folder_name(name):
    """
    Remove leading/trailing spaces and replace problematic characters for folder names.
    """
    name = name.strip()
    name = re.sub(r'[<>:"/\\|?*]', '_', name)
    return name

# ---------------------------------------------------------------------
# MAIN TRANSFORMATION FUNCTION
# ---------------------------------------------------------------------

def bronze_to_silver_spark(bronze_path, silver_dir):
    """
    Transforms Bronze Layer JSON data into the Silver Layer using PySpark.

    Steps:
    1. Load raw JSON data into a Spark DataFrame.
    2. Select relevant columns for analytical processing.
    3. Clean missing values (strings → empty, numeric → null).
    4. Partition and save the dataset by 'state' and 'city' in Parquet format.
    """

    # -----------------------------------------------------------------
    # 1. CREATE SPARK SESSION
    # -----------------------------------------------------------------
    spark = SparkSession.builder.appName("BronzeToSilver").getOrCreate()

    # -----------------------------------------------------------------
    # 2. LOAD RAW JSON DATA
    # -----------------------------------------------------------------
    df = spark.read.json(bronze_path)

    # -----------------------------------------------------------------
    # 3. SELECT RELEVANT COLUMNS
    # -----------------------------------------------------------------
    columns = [
        "id", "name", "brewery_type", "street", "city", "state",
        "postal_code", "country", "longitude", "latitude", "phone", "website_url"
    ]
    df = df.select(*columns)

    # -----------------------------------------------------------------
    # 4. HANDLE MISSING VALUES AND DATA TYPES
    # -----------------------------------------------------------------
    string_cols = ["id", "name", "brewery_type", "street", "city", "state", "postal_code", "country", "phone", "website_url"]
    numeric_cols = ["longitude", "latitude"]

    for col_name in string_cols:
        df = df.withColumn(col_name, col(col_name).cast("string"))
    
    for col_name in numeric_cols:
        df = df.withColumn(col_name, col(col_name).cast("double"))

    # Replace null strings with empty
    for col_name in string_cols:
        df = df.na.fill({col_name: ""})

    # -----------------------------------------------------------------
    # 5. PARTITION AND SAVE DATA
    # -----------------------------------------------------------------
    # Create the base Silver directory if it doesn't exist
    os.makedirs(silver_dir, exist_ok=True)

    # Apply folder name sanitization via Spark
    # Note: Spark automatically handles partition folders using column values
    # We can create temporary sanitized columns for partitioning
    from pyspark.sql.functions import udf
    from pyspark.sql.types import StringType

    sanitize_udf = udf(sanitize_folder_name, StringType())
    df = df.withColumn("state_clean", sanitize_udf(col("state")))
    df = df.withColumn("city_clean", sanitize_udf(col("city")))

    output_path = os.path.join(silver_dir)

    df.write.partitionBy("state_clean", "city_clean").mode("overwrite").parquet(output_path)
    print(f"Saved Silver Layer to {output_path}")

    spark.stop()

# ---------------------------------------------------------------------
# SCRIPT ENTRY POINT
# ---------------------------------------------------------------------

if __name__ == "__main__":
    bronze_to_silver_spark(BRONZE_PATH, SILVER_PATH)
