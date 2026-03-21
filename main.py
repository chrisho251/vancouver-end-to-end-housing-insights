import logging
import sys
from datetime import datetime

# Configure logging for the main pipeline
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Import ingestion modules
from src.ingestion import extract_business_licences
from src.ingestion import extract_crime
from src.ingestion import extract_local_areas
from src.ingestion import extract_property_tax_report

# Import transformation modules
from src.transformation import transform_business_licences
from src.transformation import transform_crime
from src.transformation import transform_local_areas
from src.transformation import transform_property_tax


def main():
    """
    Execute the full end-to-end pipeline

    Step1: Ingestion
    Step2: Transformation 
    Step3: EDA
    Step4: Machine Learning
    Step5: Visualization
    """
    logging.info("=" * 60)
    logging.info("STARTING Vancouver Housing Insights Pipeline")
    logging.info("Step 1: Data Ingestion")
    logging.info(f"Run timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logging.info("=" * 60)

    # Step 1: Data Ingestion
    ingestion_steps = [
        ("Business Licences", extract_business_licences),
        ("Crime Data", extract_crime),
        ("Local Area Boundaries", extract_local_areas),
        ("Property Tax Report", extract_property_tax_report),
    ]

    ingestion_success = 0
    ingestion_fail = 0

    for step_name, module in ingestion_steps:
        logging.info("-" * 40)
        logging.info(f"[START] Ingesting: {step_name}")
        logging.info("-" * 40)

        try:
            module.run()
            logging.info(f"[DONE] {step_name} ingestion completed successfully.")
            ingestion_success += 1
        except Exception as e:
            logging.error(f"[FAIL] {step_name} ingestion failed: {e}")
            ingestion_fail += 1

    if ingestion_fail > 0:
        logging.error("Ingestion phase failed. Aborting pipeline.")
        sys.exit(1)

    logging.info("--- INGESTION SUMMARY ---")
    logging.info(f"  Succeeded: {ingestion_success}/{len(ingestion_steps)}")
    logging.info(f"  Failed:    {ingestion_fail}/{len(ingestion_steps)}")

    # Step 2: Data Transformation
    logging.info("=" * 60)
    logging.info("Step 2: Data Transformation")
    logging.info(f"Run timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logging.info("=" * 60)

    transformation_steps = [
        ("Business Licences", transform_business_licences),
        ("Crime Data", transform_crime),
        ("Local Area Boundaries", transform_local_areas),
        ("Property Tax Report", transform_property_tax),
    ]

    transformation_success = 0
    transformation_fail = 0
    transformed_dataframes = {}

    for step_name, module in transformation_steps:
        logging.info("-" * 40)
        logging.info(f"[START] Transforming: {step_name}")
        logging.info("-" * 40)

        try:
            # Each transformation module runs and returns a cleaned pandas DataFrame
            df = module.run()
            transformed_dataframes[step_name] = df
            logging.info(f"[DONE] {step_name} transformation completed successfully. Final shape: {df.shape}")
            transformation_success += 1
        except Exception as e:
            logging.error(f"[FAIL] {step_name} transformation failed: {e}")
            transformation_fail += 1

    # Print pipeline summary
    logging.info("=" * 60)
    logging.info("PIPELINE EXECUTION COMPLETE")
    logging.info("--- TRANSFORMATION SUMMARY ---")
    logging.info(f"  Succeeded: {transformation_success}/{len(transformation_steps)}")
    logging.info(f"  Failed:    {transformation_fail}/{len(transformation_steps)}")
    logging.info("=" * 60)

    if transformation_fail > 0:
        sys.exit(1)

    # At this point, `transformed_dataframes` holds all the necessary DataFrames 
    # to proceed to Step 3 (EDA) and Step 4 (ML Models).
    
    df_business = transformed_dataframes.get("Business Licences")
    df_crime = transformed_dataframes.get("Crime Data")
    df_local_areas = transformed_dataframes.get("Local Area Boundaries")
    df_property_tax = transformed_dataframes.get("Property Tax Report")

    return df_business, df_crime, df_local_areas, df_property_tax


if __name__ == "__main__":
    main()
