"""
extract_local_areas.py

Full-load ingestion script for the Vancouver Local Area Boundaries dataset.
Fetches all local area boundary records via the Vancouver Open Data API CSV
export endpoint, adds audit columns, and saves the result as a date-stamped
JSON file in the landing directory.

Source: https://opendata.vancouver.ca/explore/dataset/local-area-boundary
"""

import pandas as pd
from datetime import datetime
import os
import logging

from src.ingestion.ingestion_utils import save_as_split_json

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Landing directory for raw ingested data
LANDING_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "data", "landing", "local_areas")

# CSV export URL for the Local Area Boundary dataset
API_EXPORT_URL = "https://opendata.vancouver.ca/api/explore/v2.1/catalog/datasets/local-area-boundary/exports/csv?delimiter=%2C"


def create_landing_directory():
    """Create the landing directory if it does not already exist"""
    if not os.path.exists(LANDING_DIR):
        os.makedirs(LANDING_DIR)
        logging.info(f"Created landing directory: {LANDING_DIR}")


def fetch_all_local_areas():
    """
    Fetch ALL local area boundary records from the Vancouver Open Data API

    Returns:
        pd.DataFrame or None: The ingested DataFrame, or None on error
    """
    logging.info("Downloading full Local Area Boundaries dataset from API...")

    try:
        # Pandas can read directly from a URL that returns CSV
        df = pd.read_csv(API_EXPORT_URL, sep=',')
        logging.info(f"Downloaded {len(df)} local area records")

        # Add audit columns for data lineage
        df['created_by'] = 'system'
        df['ingested_dt'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Save as JSON file(s), auto-splitting if record count exceeds threshold
        save_as_split_json(df, LANDING_DIR, "local_areas")
        return df

    except Exception as e:
        logging.error(f"Error during Local Areas ingestion: {e}")
        return None


def run():
    """Entry point for the local areas ingestion pipeline"""
    create_landing_directory()
    df = fetch_all_local_areas()
    if df is None:
        raise Exception("Local Areas ingestion failed")
    return df


if __name__ == "__main__":
    run()