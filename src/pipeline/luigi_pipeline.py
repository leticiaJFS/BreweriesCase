# src/luigi_pipeline.py  

"""
Luigi Pipeline – Medallion Architecture (Bronze → Silver → Gold)

Overview:
---------
This Luigi pipeline automates the end-to-end data flow across the three Medallion
architecture layers:

1. **Bronze Layer** – Extracts raw brewery data from the Open Brewery DB API.
2. **Silver Layer** – Transforms raw JSON into partitioned Parquet files by state and city.
3. **Gold Layer** – Aggregates analytics-ready data (e.g., count of breweries by type and location).

Key Features:
--------------
- **Automated orchestration** using Luigi.
- **Retry logic** for fault-tolerance (3 retries, 60s delay).
- **Centralized logging** and global failure handling.
- **Idempotent execution**: safely re-runnable without data corruption.
- **Ready for scheduling** via Task Scheduler, cron, or Docker.

Author: Leticia Soares
Date: 2025-10-04
"""

import luigi
import os
import json
import logging
from luigi import Event

from api.breweries_api import fetch_breweries
from transformations.bronze_to_silver import bronze_to_silver
from transformations.silver_to_gold import silver_to_gold

# ---------------------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------------------

# Base directory for all data layers
DATA_DIR = "data"

# Configure global logging (with file + console handlers)
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)

log_file = os.path.join(LOG_DIR, "pipeline.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),  # exibe no console
        logging.FileHandler("logs/pipeline.log", mode="a", encoding="utf-8")  # grava em arquivo
    ]
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------
# GLOBAL EVENT HANDLER
# ---------------------------------------------------------------------

@luigi.Task.event_handler(Event.FAILURE)
def on_failure(task, exception):
    """
    Global Luigi failure handler.

    Triggered whenever any task fails during pipeline execution.
    Logs the failure event with task name and error details.

    Parameters
    ----------
    task : luigi.Task
        The Luigi task that failed.
    exception : Exception
        The raised exception instance.
    """
    logger.error(f"[ALERTA] Tarefa {task.__class__.__name__} falhou: {exception}")

# ---------------------------------------------------------------------
# BRONZE TASK – Extract from API
# ---------------------------------------------------------------------

class ExtractTask(luigi.Task):
    """
    Extract data from the Open Brewery API (Bronze Layer).

    Responsibilities:
    -----------------
    - Calls `fetch_breweries()` to retrieve all pages from the API.
    - Saves the raw JSON response into the Bronze folder.
    - Ensures idempotency by overwriting existing files.
    - Includes retry logic for transient API/network issues.
    """
    retry_count = 3
    retry_delay = 60

    def output(self):
        """
        Defines the Bronze output file path.
        """
        return luigi.LocalTarget(os.path.join(DATA_DIR, "bronze", "breweries_raw.json"))

    def run(self):
        """
        Executes the API extraction step.
        """
        try:
            logger.info("[ExtractTask] Fetching data from Open Brewery API...")
            data = fetch_breweries()
            os.makedirs(os.path.dirname(self.output().path), exist_ok=True)
            with open(self.output().path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=4)
            logger.info("[ExtractTask] Bronze layer data saved successfully.")
        except Exception as e:
            logger.exception("[ExtractTask] Error during API extraction.")
            raise e
        
# ---------------------------------------------------------------------
# SILVER TASK – Transform Bronze to Silver
# ---------------------------------------------------------------------

class SilverTask(luigi.Task):
    """
    Transform raw Bronze JSON data into a structured Parquet format (Silver Layer).

    Responsibilities:
    -----------------
    - Reads the Bronze JSON file.
    - Cleans and normalizes the data using `bronze_to_silver()`.
    - Writes Parquet files partitioned by state and city.
    - Creates a `_SUCCESS` marker file for Luigi dependency tracking.
    """

    retry_count = 3
    retry_delay = 60

    def requires(self):
        # Specifies the dependency on the Bronze extraction step.        
        return ExtractTask()

    def output(self):
        # Defines the Silver layer completion marker file.
        return luigi.LocalTarget(os.path.join(DATA_DIR, "silver", "_SUCCESS"))

    def run(self):
        # Executes the transformation from Bronze to Silver.
        try:
            logger.info("[SilverTask] Starting Bronze → Silver transformation...")
            bronze_path = self.input().path
            silver_dir = os.path.dirname(self.output().path)
            os.makedirs(silver_dir, exist_ok=True)

            bronze_to_silver(bronze_path, silver_dir)
            
            # Create success marker file for Luigi tracking
            with open(self.output().path, "w") as f:
                f.write("")
            logger.info("[SilverTask] Silver layer successfully generated.")
        except Exception as e:
            logger.exception("[SilverTask] Error during Bronze → Silver transformation.")
            raise e
        
# ---------------------------------------------------------------------
# GOLD TASK – Aggregate Silver to Gold
# ---------------------------------------------------------------------

class GoldTask(luigi.Task):
    """
    Aggregate transformed Silver data into the analytical Gold Layer.

    Responsibilities:
    -----------------
    - Reads partitioned Silver Parquet files.
    - Aggregates the number of breweries by state, city, and type.
    - Saves the result as a single Parquet file in the Gold folder.
    - Ensures idempotency by removing any previous Gold file before writing.
    """

    retry_count = 3
    retry_delay = 60

    def requires(self) -> luigi.Task:
        #Specifies the dependency on the Silver transformation step.
        return SilverTask()

    def output(self) -> luigi.LocalTarget:
        #Defines the final Gold layer output file.
        return luigi.LocalTarget(os.path.join(DATA_DIR, "gold", "breweries_aggregated.parquet"))

    def run(self):
        #Executes the aggregation from Silver to Gold.
        try:
            logger.info("[GoldTask] Starting Silver → Gold aggregation...")
            gold_file = self.output().path
            os.makedirs(os.path.dirname(gold_file), exist_ok=True)

            # Remove old Gold file to ensure idempotency            
            if os.path.exists(gold_file):
                os.remove(gold_file)

            silver_to_gold()
            logger.info("[GoldTask] Gold layer successfully created.")
        except Exception as e:
            logger.exception("[GoldTask] Error during Gold layer aggregation.")
            raise e

# ---------------------------------------------------------------------
# PIPELINE EXECUTION
# ---------------------------------------------------------------------

if __name__ == "__main__":
    """
    Pipeline entry point.

    Running this module will:
    - Trigger the GoldTask.
    - Luigi will automatically resolve dependencies:
        GoldTask → SilverTask → ExtractTask.
    - Scheduler connection:
        - `local_scheduler=False` connects to an external Luigi Scheduler
          (useful for Docker or distributed execution).
    """

    # Example for local testing (standalone mode):
    luigi.build([GoldTask()], local_scheduler=True)

    # Production-like execution (Docker-compatible):
    '''luigi.build(
        [GoldTask()], 
        local_scheduler=False, 
        scheduler_host='luigi-scheduler', 
        scheduler_port=8082,
    )'''
