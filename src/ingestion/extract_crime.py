"""
extract_crime.py

Full-load ingestion script for the Vancouver Police Department (VPD) crime dataset.
Downloads the complete crime data CSV from VPD GeoDASH (all years from 2003 onward),
normalizes column names, adds audit columns, and saves the result as a date-stamped
JSON file in the landing directory.

Source: https://geodash.vpd.ca/opendata/crimedata_download/crimedata_csv_all_years.csv
"""

import pandas as pd
from datetime import datetime
import os
import logging

from src.ingestion.ingestion_utils import save_as_split_json

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Landing directory for raw ingested data
LANDING_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "data", "landing", "crime")

# Direct CSV download URL from VPD GeoDASH (contains all years from 2003 to present)
VPD_CSV_URL = "https://geodash.vpd.ca/opendata/crimedata_download/crimedata_csv_all_years.csv"


def create_landing_directory():
    """Create the landing directory if it does not already exist."""
    if not os.path.exists(LANDING_DIR):
        os.makedirs(LANDING_DIR)
        logging.info(f"Created landing directory: {LANDING_DIR}")


def fetch_all_crime_data():
    """
    Fetch ALL crime records from the VPD GeoDASH CSV endpoint.

    Returns:
        pd.DataFrame or None: The ingested DataFrame, or None on error.
    """
    logging.info("Connecting to VPD GeoDASH to download full crime dataset...")

    try:
        # Download the complete CSV directly into a Pandas DataFrame
        # This file contains data from 2003 to present and may take a few seconds
        df = pd.read_csv(VPD_CSV_URL)
        logging.info(f"Downloaded {len(df)} crime records from VPD GeoDASH.")

        # Normalize column names to lowercase for PostgreSQL compatibility
        df.columns = [col.lower() for col in df.columns]

        # Add audit columns for data lineage
        df['created_by'] = 'python_full_load_job'
        df['ingested_dt'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Save as JSON file(s), auto-splitting if record count exceeds threshold
        save_as_split_json(df, LANDING_DIR, "crime")
        return df

    except Exception as e:
        logging.error(f"Error during crime data ingestion: {e}")
        return None


def run():
    """Entry point for the crime data ingestion pipeline."""
    create_landing_directory()
    fetch_all_crime_data()


if __name__ == "__main__":
    run()