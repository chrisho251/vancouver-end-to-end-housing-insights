"""
transform_utils.py

Shared utility functions for data transformation step.
"""

import pandas as pd
import glob
import os
import logging

def load_landing_data(source_dir: str) -> pd.DataFrame:
    """
    Load all JSON parts from a landing folder into a single DataFrame
    """
    success_file = os.path.join(source_dir, "_SUCCESS")
    if not os.path.exists(success_file):
        raise FileNotFoundError(f"Landing data incomplete: Missing _SUCCESS flag in {source_dir}. Ingestion may not be finished.")
        
    json_files = glob.glob(os.path.join(source_dir, "*.json"))
    if not json_files:
        raise FileNotFoundError(f"No JSON files found in {source_dir}")
    
    logging.info(f"Found {len(json_files)} JSON file(s) in {source_dir}. Loading into DataFrame...")
    
    dfs = []
    for file_path in json_files:
        df_part = pd.read_json(file_path)
        dfs.append(df_part)
        
    unified_df = pd.concat(dfs, ignore_index=True)
    logging.info(f"Successfully loaded {len(unified_df)} total records from {source_dir}.")
    
    return unified_df


def save_staging_data(df: pd.DataFrame, output_path: str):
    """
    Save transformed DataFrame to a CSV file in the staging directory
    """
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_csv(output_path, index=False)
    logging.info(f"Saved transformed data ({len(df)} records) to {output_path}")
