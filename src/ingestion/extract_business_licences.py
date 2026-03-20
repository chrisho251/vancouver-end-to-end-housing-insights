"""
extract_business_licences.py

Full-load ingestion script for the Vancouver Business Licences dataset.
Fetches ALL business licence records (from 1900 onward) via the Vancouver
Open Data Explore API v2.1, using offset-based pagination, and saves
the result as a date-stamped JSON file in the landing directory.

Source: https://opendata.vancouver.ca/explore/dataset/business-licences
"""

import requests
import pandas as pd
from datetime import datetime
import os
import logging

from src.ingestion.ingestion_utils import save_as_split_json

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Landing directory for raw ingested data
LANDING_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "data", "landing", "business_licences")

# Base URL for the Vancouver Open Data Explore API v2.1
BASE_URL = "https://opendata.vancouver.ca/api/explore/v2.1/catalog/datasets"

# Dataset configuration
DATASET_ID = "business-licences"
DATE_FIELD = "issueddate"


def create_landing_directory():
    """Create the landing directory if it does not already exist."""
    if not os.path.exists(LANDING_DIR):
        os.makedirs(LANDING_DIR)
        logging.info(f"Created landing directory: {LANDING_DIR}")


def fetch_all_business_licences():
    """
    Fetch ALL business licence records from the Vancouver Open Data API.

    Returns:
        pd.DataFrame or None: The ingested DataFrame, or None if no data.
    """
    logging.info("Starting full load of Business Licences dataset...")

    all_records = []
    limit = 100  # API v2.1 max records per page
    offset = 0

    while True:
        url = f"{BASE_URL}/{DATASET_ID}/records"

        # Set query parameters: full load from 1900 onward
        params = {
            "limit": limit,
            "offset": offset,
            "where": f"{DATE_FIELD} >= date'1900-01-01'"
        }

        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            data = response.json()

            # Extract the records list from the API response
            records = data.get("results", [])
            if not records:
                break  # No more records to fetch

            all_records.extend(records)
            offset += limit
            logging.info(f"Fetched {len(all_records)} records so far (offset={offset})...")

            # If fewer records returned than the limit, we've reached the last page
            if len(records) < limit:
                break

        except requests.exceptions.RequestException as e:
            logging.error(f"API error while fetching Business Licences at offset {offset}: {e}")
            break

    if all_records:
        df = pd.DataFrame(all_records)

        # Drop complex GeoJSON geometry column if present
        if 'geom' in df.columns:
            df = df.drop(columns=['geom'])

        # Add audit columns for data lineage
        df['created_by'] = 'python_full_load_job'
        df['ingested_dt'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Save as JSON file(s), auto-splitting if record count exceeds threshold
        save_as_split_json(df, LANDING_DIR, "business_licences")
        return df
    else:
        logging.warning("No Business Licences records were fetched.")
        return None


def run():
    """Entry point for the business licences ingestion pipeline."""
    create_landing_directory()
    fetch_all_business_licences()


if __name__ == "__main__":
    run()