"""
transform_business_licences.py

This module performs data cleaning and transformation for the Vancouver Business Licences dataset
"""

import pandas as pd
import os
import logging
from src.transformation.transform_utils import load_landing_data, save_staging_data

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

from datetime import datetime

LANDING_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "data", "landing", "business_licences")
current_date = datetime.now().strftime('%Y%m%d')
STAGING_FILE = os.path.join(os.path.dirname(__file__), "..", "..", "data", "staging", f"business_licences_transformed_{current_date}.csv")

import json
import ast

def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """Apply data cleaning transformations to the DataFrame."""
    initial_len = len(df)
    
    # Normalize column names to lower
    df.columns = [str(c).lower() for c in df.columns]
    
    # Drop audit columns
    df = df.drop(columns=['created_by', 'ingested_dt'], errors='ignore')

    # Rename glued words
    rename_dict = {
        'folderyear': 'folder_year',
        'licencersn': 'licence_rsn',
        'licencerevisionnumber': 'licence_revision_number',
        'businessname': 'business_name',
        'businesstradename': 'business_trade_name',
        'businesssubtype': 'business_sub_type',
        'unittype': 'unit_type',
        'localareamodel': 'local_area_model',
        'postalcode': 'postal_code',
        'localarea': 'local_area',
        'issueddate': 'issued_date',
        'expireddate': 'expired_date',
        'feeclass': 'fee_class',
        'feepaid': 'fee_paid'
    }
    df = df.rename(columns=rename_dict)

    # Eliminate duplicate entries
    # Clean string columns that might contain newline characters causing CSV breakages
    string_cols = df.select_dtypes(include=['object', 'string']).columns
    for col in string_cols:
        df[col] = df[col].astype(str).str.replace(r'\n', ' ', regex=True).replace('nan', pd.NA).replace('None', pd.NA).replace('', pd.NA)

    df = df.drop_duplicates()
    logging.info(f"Dropped {initial_len - len(df)} duplicate records.")
    
    # Address missing values
    # Specific columns mentioned: businesstradename, businesssubtype, unit, unittype, house, street, postalcode
    target_na_cols = ['business_trade_name', 'business_sub_type', 'unit', 'unit_type', 'house', 'street', 'postal_code']
    for col in target_na_cols:
        if col in df.columns:
            # fillna(pd.NA) to standardize nulls
            df[col] = df[col].replace(r'^\s*$', pd.NA, regex=True)
    
    # Geo point JSON transformation
    # Extract latitude and longitude from geo_point_2d
    if 'geo_point_2d' in df.columns:
        # Some datasets output geo_point as "Lat, Lon" string, not JSON, contradicting docs
        # Handle both JSON literal and comma-separated string formats
        def parse_geo(val):
            if pd.isna(val) or str(val).lower() == 'nan':
                return pd.NA, pd.NA
            # If dictionary
            if isinstance(val, dict):
                return val.get('lat', pd.NA), val.get('lon', pd.NA)
            val_str = str(val).strip()
            # If "lat, lon" string
            if val_str.count(',') == 1 and not val_str.startswith('{'):
                parts = val_str.split(',')
                try:
                    return float(parts[0]), float(parts[1])
                except ValueError:
                    return pd.NA, pd.NA
            # If JSON string
            try:
                d = ast.literal_eval(val_str) if "'" in val_str else json.loads(val_str)
                return d.get('lat', pd.NA), d.get('lon', pd.NA)
            except Exception:
                return pd.NA, pd.NA

        df['latitude'], df['longitude'] = zip(*df['geo_point_2d'].apply(parse_geo))
        df = df.drop(columns=['geo_point_2d'])
        logging.info("Parsed geo_point_2d into latitude and longitude and dropped original column.")

    # Synchronize datetimes to UTC
    for col in ['issued_date', 'expired_date']:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce', utc=True)
            
    logging.info("Completed cleaning according to cleaning.md rules")
    return df

def run() -> pd.DataFrame:
    """Execute the transformation pipeline for Business Licences"""
    logging.info("Starting transformation for Business Licences...")
    
    # Load all historical parts from landing zone
    try:
        df = load_landing_data(LANDING_DIR)
    except FileNotFoundError:
        logging.warning("No landing data found. Returning empty DataFrame")
        return pd.DataFrame()
        
    # Clean the dataset
    df_cleaned = clean_data(df)
    
    # Save the cleaned data to the staging area for next pipeline steps
    save_staging_data(df_cleaned, STAGING_FILE)
    logging.info(f"Business Licences transformation completed. Output shape: {df_cleaned.shape}")
    
    return df_cleaned

if __name__ == "__main__":
    run()
