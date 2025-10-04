# Breweries Data Pipeline

This project demonstrates a **data pipeline** that consumes data from the Open Brewery DB API, transforms it, and persists it into a **data lake** following the **medallion architecture**: Bronze → Silver → Gold.

---

## **Project Objective**

- Fetch brewery data from the [Open Brewery DB API](https://www.openbrewerydb.org/)  
- Persist raw data in **Bronze Layer**  
- Transform and store data in **Silver Layer** as **Parquet**, partitioned by location (state & city)  
- Aggregate data in **Gold Layer** for analytics: count of breweries by type and location  
- Include automated **tests** for each layer  

---

## **Project Structure**

BreweriesCase/

├─ data/

│ ├─ bronze/ # Raw JSON from API

│ ├─ silver/ # Parquet partitioned by state/city

│ └─ gold/ # Aggregated Parquet

├─ src/

│ ├─ api/

│ │ └─ breweries_api.py # Bronze Layer extraction

│ └─ transformations/

│ ├─ bronze_to_silver.py # Bronze → Silver

│ └─ silver_to_gold.py # Silver → Gold

├─ tests/

│ ├─ test_transformations.py # Silver Layer tests

│ └─ test_gold_layer.py # Gold Layer tests

├─ README.md

└─ requirements.txt


---

## **Setup & Dependencies**

1. Clone the repository:
git clone https://github.com/leticiaJFS/BreweriesCase.git
cd BreweriesCase

2. Create a virtual environment (optional but recommended):
python -m venv venv
.\venv\Scripts\activate    # Windows
source venv/bin/activate   # Linux/Mac

3. Install dependencies:
python -m pip install -r requirements.txt
# Or manually:
python -m pip install requests pandas pyarrow pytest

## **Pipeline Execution** 
1. Bronze Layer
Fetch raw data from API:
python src/api/breweries_api.py
Output: data/bronze/breweries_raw.json

2.Silver Layer
Transform raw JSON into Parquet, partitioned by state and city:
python src/transformations/bronze_to_silver.py
Output: data/silver/state=.../city=.../data.parquet

- Transformations Applied:
- Filter relevant columns for analytics
- Handle missing values: strings → empty "", numeric → NaN
- Partitioned by state and city for optimized querying

3. Gold Layer
Aggregate Silver Layer to count breweries by brewery_type, state and city:
python src/transformations/silver_to_gold.py
- Output: data/gold/breweries_aggregated.parquet

Aggregation Applied:
- brewery_count = number of breweries per brewery_type per location
- Columnar storage for analytics

## **Testing** 
Automated tests are included for Silver and Gold layers:
python -m pytest tests/
- tests/test_bronze_layer.py → checks Bronze Layer extraction
- tests/test_silver_layer.py → checks Silver Layer creation
- tests/test_gold_layer.py → checks Gold Layer aggregation

## **Testing**
- All scripts are modular (src/api for extraction, src/transformations for ETL)
- Data layers separated in data/bronze, data/silver, data/gold
- Paths and folder names are sanitized to avoid special character issues
