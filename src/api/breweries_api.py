"""
Script: breweries_api.py
----------------------------------------------------------
Purpose:
    Extract raw brewery data from the Open Brewery DB API
    and save it into the Bronze layer of the Medallion Architecture.

Architecture:
    - Bronze Layer: Raw data ingestion from external sources (API)
    - Silver Layer: Cleaned and transformed data
    - Gold Layer: Aggregated and analytical data

This script corresponds to the Bronze step:
    API → JSON file → data/bronze/breweries_raw.json

Main Features:
    - Fetches all available brewery data using pagination
    - Handles API errors gracefully
    - Saves data in JSON format (human-readable, indented)
    - Ensures idempotency by overwriting existing files

Author: Leticia Soares
Date: 2025-10-04
----------------------------------------------------------
"""

import requests
import json
import os

# ----------------------------------------------------------
# CONFIGURATION
# ----------------------------------------------------------

# Absolute path for the Bronze layer directory
BRONZE_PATH = os.path.join(os.getcwd(), "data", "bronze")

# File name for the Bronze layer output
FILENAME = "breweries_raw.json"


# ----------------------------------------------------------
# FUNCTION: Fetch data from API
# ----------------------------------------------------------
def fetch_breweries():
    """
    Fetch brewery data from the Open Brewery DB API.

    The API is paginated, returning up to 50 results per page.
    This function continues to request data until no more results
    are returned, ensuring that all available breweries are collected.

    Returns:
        list: A list of dictionaries containing all brewery records.

    Raises:
        Exception: If the API request fails with a non-200 status code.
    """
    all_data = []
    per_page = 50
    page = 1

    print("Fetching data from the API...")

    # Loop through all pages until API returns an empty response
    while True:
        url = f"https://api.openbrewerydb.org/v1/breweries?per_page={per_page}&page={page}"
        response = requests.get(url)

        # Check if the API responded successfully
        if response.status_code != 200:
            raise Exception(f"Error accessing API: {response.status_code}")

        data = response.json()

        # Stop the loop if no more data is returned
        if not data:
            break

        # Extend accumulated list with current page data
        all_data.extend(data)
        print(f"Page {page} downloaded, total records so far: {len(all_data)}")
        page += 1

    print(f"Total records retrieved: {len(all_data)}")
    return all_data


# ----------------------------------------------------------
# FUNCTION: Save data to Bronze layer
# ----------------------------------------------------------
def save_bronze(data):
    """
    Save the raw brewery data into the Bronze layer.

    Creates the directory if it does not exist and saves the JSON file
    in a human-readable format with indentation.

    Args:
        data (list): List of brewery records (dictionaries)

    Returns:
        None
    """
    # Ensure Bronze directory exists
    os.makedirs(BRONZE_PATH, exist_ok=True)

    # Build the full file path
    filepath = os.path.join(BRONZE_PATH, FILENAME)

    print(f"Attempting to save data to: {filepath}")

    # Write the data to a JSON file
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4)

    print(f"Data successfully saved to {filepath}")


# ----------------------------------------------------------
# MAIN EXECUTION
# ----------------------------------------------------------
if __name__ == "__main__":
    """
    Main entry point for the Bronze layer pipeline.

    Steps:
        1. Extract data from the Open Brewery DB API.
        2. Save the extracted data into the Bronze layer directory.

    Output:
        data/bronze/breweries_raw.json
    """
    # Fetch data from API
    data = fetch_breweries()

    # Save data into Bronze layer
    save_bronze(data)
