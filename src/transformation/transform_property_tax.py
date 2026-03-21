"""
transform_property_tax.py

This module cleans and transforms the Vancouver Property Tax Report dataset
"""

import pandas as pd
import os   
import logging
from src.transformation.transform_utils import load_landing_data, save_staging_data

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

from datetime import datetime

LANDING_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "data", "landing", "property_tax_report")
current_date = datetime.now().strftime('%Y%m%d')
STAGING_FILE = os.path.join(os.path.dirname(__file__), "..", "..", "data", "staging", f"property_tax_transformed_{current_date}.csv")

def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """Apply data cleaning transformations to the Property Tax DataFrame"""
    initial_len = len(df)
    
    # Normalize column names for safety
    df.columns = [str(c).lower() for c in df.columns]
    
    # Drop audit columns
    df = df.drop(columns=['created_by', 'ingested_dt'], errors='ignore')
    
    # Eliminate duplicate entries
    df = df.drop_duplicates()
    logging.info(f"Dropped {initial_len - len(df)} duplicate records")
    
    # PID Standardization
    if 'pid' in df.columns:
        df['pid'] = df['pid'].astype(str).str.replace('-', '', regex=False)
        logging.info("Normalized 'pid' by removing dashes.")
        
    # Empty strings to Null
    empty_cols = ['block', 'plan', 'district_lot', 'from_civic_number']
    for col in empty_cols:
        if col in df.columns:
            df[col] = df[col].replace(r'^\s*$', pd.NA, regex=True)
            
    # Concatenate narrative legal lines
    legal_cols = [c for c in df.columns if 'narrative_legal_line' in c]
    if legal_cols:
        # Sort to ensure line1, line2, ... line5 are in correct order
        legal_cols = sorted(legal_cols)
        # Fill NA with empty string so concatenation doesn't produce NaN
        df['full_legal_description'] = df[legal_cols].fillna('').agg(' '.join, axis=1)
        # Collapse multiple spaces into one
        df['full_legal_description'] = df['full_legal_description'].replace(r'\s+', ' ', regex=True).str.strip()
        # Optionally drop the original legal lines
        df = df.drop(columns=legal_cols)
        logging.info("Concatenated narrative legal lines into 'full_legal_description'.")

    # Type Casting (Finance to Numeric)
    finance_cols = ['current_land_value', 'current_improvement_value', 
                    'previous_land_value', 'previous_improvement_value']
    for col in finance_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
            
    if 'tax_assessment_year' in df.columns:
        df['tax_assessment_year'] = pd.to_numeric(df['tax_assessment_year'], errors='coerce').astype('Int64')
        
    logging.info("Completed cleaning according to cleaning.md rules for Property Tax")
    return df

def run() -> pd.DataFrame:
    """Execute the transformation pipeline for Property Tax Data"""
    logging.info("Starting transformation for Property Tax Data...")
    
    # Load all historical parts from landing zone
    try:
        df = load_landing_data(LANDING_DIR)
    except FileNotFoundError:
        logging.warning("No landing property tax data found. Returning empty DataFrame.")
        return pd.DataFrame()
        
    # Clean the dataset
    df_cleaned = clean_data(df)
    
    # Save the cleaned data to the staging area for next pipeline steps
    save_staging_data(df_cleaned, STAGING_FILE)
    logging.info(f"Property Tax transformation completed. Output shape: {df_cleaned.shape}")
    
    return df_cleaned

if __name__ == "__main__":
    run()
