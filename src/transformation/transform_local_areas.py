"""
transform_local_areas.py

This module cleans and transforms the Vancouver Local Area Boundaries dataset
"""

import pandas as pd
import os
import logging
from src.transformation.transform_utils import load_landing_data, save_staging_data

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

from datetime import datetime

LANDING_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "data", "landing", "local_areas")
current_date = datetime.now().strftime('%Y%m%d')
STAGING_FILE = os.path.join(os.path.dirname(__file__), "..", "..", "data", "staging", f"local_areas_transformed_{current_date}.csv")

import geopandas as gpd
from shapely.geometry import shape
import json

def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """Apply data cleaning transformations to the Local Areas DataFrame"""
    initial_len = len(df)
    
    # Normalize column names to lower
    df.columns = [str(c).lower() for c in df.columns]
    
    # Drop audit columns
    df = df.drop(columns=['created_by', 'ingested_dt'], errors='ignore')

    # Eliminate duplicate entries
    df = df.drop_duplicates()
    logging.info(f"Dropped {initial_len - len(df)} duplicate records")
    
    # Clean extra quotes in 'geom' GeoJSON
    if 'geom' in df.columns:
        # Some CSV representations escape double quotes with another double quote (e.g. ""type"")
        df['geom'] = df['geom'].astype(str).str.replace('""', '"')
        
        # Geospatial Parsing -> Shapely geometry
        def parse_geom(g_str):
            try:
                # If the string is 'nan', don't parse it
                if g_str.lower() == 'nan' or not g_str.strip():
                    return None
                
                # Replace any single quotes wrapping json keys to double quotes just in case
                valid_json = g_str.replace("'", '"')
                geom_dict = json.loads(valid_json)
                return shape(geom_dict)
            except Exception as e:
                return None
                
        df['geometry'] = df['geom'].apply(parse_geom)
        df = gpd.GeoDataFrame(df, geometry='geometry')
        df = df.drop(columns=['geom'])
        logging.info("Parsed 'geom' column into Shapely Geometry objects and dropped original column")
        
    # Extract center point coordinates from geo_point_2d
    if 'geo_point_2d' in df.columns:
        # Expected format: "49.24476647864864, -123.10314680625231"
        split_coords = df['geo_point_2d'].astype(str).str.split(',', expand=True)
        if split_coords.shape[1] == 2:
            df['center_lat'] = pd.to_numeric(split_coords[0], errors='coerce')
            df['center_lon'] = pd.to_numeric(split_coords[1], errors='coerce')
            df = df.drop(columns=['geo_point_2d'])
            logging.info("Extracted center_lat and center_lon from geo_point_2d and dropped original column")
            
    logging.info("Completed cleaning for Local Areas Boundaries")
    return df

def run() -> pd.DataFrame:
    """Execute the transformation pipeline for Local Areas Data"""
    logging.info("Starting transformation for Local Area Boundaries...")
    
    # Load all historical parts from landing zone
    try:
        df = load_landing_data(LANDING_DIR)
    except FileNotFoundError:
        logging.warning("No landing local areas data found. Returning empty DataFrame")
        return pd.DataFrame()
        
    # Clean the dataset
    df_cleaned = clean_data(df)
    
    # Save the cleaned data to the staging area for next pipeline steps
    save_staging_data(df_cleaned, STAGING_FILE)
    logging.info(f"Local Areas transformation completed. Output shape: {df_cleaned.shape}")
    
    return df_cleaned

if __name__ == "__main__":
    run()
