"""
transform_crime.py

This module cleans and transforms the Vancouver Police Department (VPD) Crime dataset.
"""

import pandas as pd
import os
import logging
from src.transformation.transform_utils import load_landing_data, save_staging_data

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

from datetime import datetime

LANDING_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "data", "landing", "crime")
current_date = datetime.now().strftime('%Y%m%d')
STAGING_FILE = os.path.join(os.path.dirname(__file__), "..", "..", "data", "staging", f"crime_transformed_{current_date}.csv")

from pyproj import Transformer

def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """Apply data cleaning transformations to the Crime DataFrame"""
    initial_len = len(df)
    
    # Normalize column names to lower for consistency
    df.columns = [str(c).lower() for c in df.columns]
    
    # Drop audit columns
    df = df.drop(columns=['created_by', 'ingested_dt'], errors='ignore')
    
    # Eliminate duplicate entries
    df = df.drop_duplicates()
    logging.info(f"Dropped {initial_len - len(df)} duplicate crime records")
    
    # Address missing/anonymized values
    # For 'Offence Against a Person', coordinates are often 0.0, 0.0
    if 'x' in df.columns and 'y' in df.columns:
        # Replace exactly 0.0 or closely 0.0 with NaN
        mask_zero = (df['x'] == 0.0) & (df['y'] == 0.0)
        df.loc[mask_zero, ['x', 'y']] = pd.NA
        logging.info(f"Replaced {mask_zero.sum()} anonymized 0.0 coordinates with NaN")
        
    # Datetime aggregation
    # Combine YEAR, MONTH, DAY, HOUR, MINUTE into a single timestamp
    time_cols = ['year', 'month', 'day', 'hour', 'minute']
    if all(col in df.columns for col in time_cols):
        # Fill missing hours/minutes with 0 for aggregation
        temp_df = df[time_cols].fillna(0).astype('Int64')
        
        # Create a datetime series using pandas
        temp_df.columns = [c.lower() for c in temp_df.columns]
        df['timestamp'] = pd.to_datetime(temp_df, errors='coerce')
        
        # Optionally drop the original time columns
        df = df.drop(columns=time_cols)
        logging.info("Merged separate time columns into single TIMESTAMP")
        
    # Coordinate Reprojection
    if 'x' in df.columns and 'y' in df.columns:
        transformer = Transformer.from_crs("EPSG:26910", "EPSG:4326", always_xy=True)
        
        def reproject(row):
            if pd.isna(row['x']) or pd.isna(row['y']):
                return pd.NA, pd.NA
            lon, lat = transformer.transform(row['x'], row['y'])
            return lat, lon

        coords = df.apply(reproject, axis=1, result_type='expand')
        df['latitude'] = coords[0]
        df['longitude'] = coords[1]
        df = df.drop(columns=['x', 'y'])
        logging.info("Reprojected x/y (UTM) to latitude/longitude (WGS84) and dropped original columns")

    return df

def run() -> pd.DataFrame:
    """Execute the transformation pipeline for Crime Data"""
    logging.info("Starting transformation for Crime Data...")
    
    # Load all historical parts from landing zone
    try:
        df = load_landing_data(LANDING_DIR)
    except FileNotFoundError:
        logging.warning("No landing crime data found. Returning empty DataFrame")
        return pd.DataFrame()
        
    # Clean the dataset
    df_cleaned = clean_data(df)
    
    # Save the cleaned data to the staging area for next pipeline steps
    save_staging_data(df_cleaned, STAGING_FILE)
    logging.info(f" Crime Data transformation completed. Output shape: {df_cleaned.shape}")
    
    return df_cleaned

if __name__ == "__main__":
    run()
