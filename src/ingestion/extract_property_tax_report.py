"""
extract_property_tax_report.py

Full-load ingestion script for the Vancouver Property Tax Report dataset.
Fetches ALL property tax records via the Vancouver
Open Data Explore API v2.1, and saves the result as a date-stamped JSON file 
in the landing directory.

Source: https://opendata.vancouver.ca/explore/dataset/property-tax-report
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
LANDING_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "data", "landing", "property_tax_report")

# CSV export URL for the Property Tax Report dataset
API_EXPORT_URL = "https://opendata.vancouver.ca/api/explore/v2.1/catalog/datasets/property-tax-report/exports/csv?delimiter=%2C"

# Dataset configuration
DATASET_ID = "property-tax-report"


def create_landing_directory():
    """Create the landing directory if it does not already exist"""
    if not os.path.exists(LANDING_DIR):
        os.makedirs(LANDING_DIR)
        logging.info(f"Created landing directory: {LANDING_DIR}")


def fetch_all_property_tax():
    """
    Fetch ALL property tax report records from the Vancouver Open Data API

    Returns:
        pd.DataFrame or None: The ingested DataFrame, or None if no data
    """
    logging.info("Starting full load of Property Tax Report dataset...")

    logging.info("Downloading full Property Tax Report dataset from API...")

    try:
        # Pandas can read directly from a URL that returns CSV
        df = pd.read_csv(API_EXPORT_URL, sep=',')
        logging.info(f"Downloaded {len(df)} property tax records")

        # Ensure we only keep records from 1900 onward, though the dataset usually starts later
        if 'report_year' in df.columns:
            df = df[df['report_year'] >= 1900]
            logging.info(f"Filtered to {len(df)} records (report_year >= 1900)")

        # Add audit columns for data lineage
        df['created_by'] = 'system'
        df['ingested_dt'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Save as JSON file(s), auto-splitting if record count exceeds threshold
        save_as_split_json(df, LANDING_DIR, "property_tax_report")
        return df

    except Exception as e:
        logging.error(f"Error during Property Tax ingestion: {e}")
        return None

def run():
    """Entry point for the property tax report ingestion pipeline"""
    create_landing_directory()
    df = fetch_all_property_tax()
    if df is None:
        raise Exception("Property Tax ingestion failed")
    return df

if __name__ == "__main__":
    run()