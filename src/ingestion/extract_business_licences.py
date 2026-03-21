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

# CSV export URL for the Business Licences dataset
API_EXPORT_URL = "https://opendata.vancouver.ca/api/explore/v2.1/catalog/datasets/business-licences/exports/csv?delimiter=%2C"

# Dataset configuration
DATASET_ID = "business-licences"
DATE_FIELD = "issueddate"


def create_landing_directory():
    """Create the landing directory if it does not already exist"""
    if not os.path.exists(LANDING_DIR):
        os.makedirs(LANDING_DIR)
        logging.info(f"Created landing directory: {LANDING_DIR}")


def fetch_all_business_licences():
    """
    Fetch ALL business licence records from the Vancouver Open Data API

    Returns:
        pd.DataFrame or None: The ingested DataFrame, or None if no data
    """
    logging.info("Starting full load of Business Licences dataset...")

    logging.info("Downloading full Business Licences dataset from API...")

    try:
        # Pandas can read directly from a URL that returns CSV
        df = pd.read_csv(API_EXPORT_URL, sep=',')
        logging.info(f"Downloaded {len(df)} business licences records")

        # Ensure we only keep records from 1900 onward
        # 'issueddate' is an ISO date string, we can convert and filter
        if 'issueddate' in df.columns:
            df['parsed_date'] = pd.to_datetime(df['issueddate'], errors='coerce')
            df = df[df['parsed_date'].dt.year >= 1900]
            df = df.drop(columns=['parsed_date'])
            logging.info(f"Filtered to {len(df)} records (issueddate >= 1900).")

        # Drop complex GeoJSON geometry column if present
        if 'geom' in df.columns:
            df = df.drop(columns=['geom'])

        # Add audit columns for data lineage
        df['created_by'] = 'system'
        df['ingested_dt'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Save as JSON file(s), auto-splitting if record count exceeds threshold
        save_as_split_json(df, LANDING_DIR, "business_licences")
        return df

    except Exception as e:
        logging.error(f"Error during Business Licences ingestion: {e}")
        return None


def run():
    """Entry point for the business licences ingestion pipeline"""
    create_landing_directory()
    df = fetch_all_business_licences()
    if df is None:
        raise Exception("Business Licences ingestion failed")
    return df


if __name__ == "__main__":
    run()