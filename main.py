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


def main():
    """
    Execute the full end-to-end pipeline.

    Step1: Runs all 4 ingestion scripts sequentially:
      1. Business Licences
      2. Crime Data 
      3. Local Area Boundaries
      4. Property Tax Report
    
    Step2: Transformation

    Step3: EDA

    Step4: Machine Learning

    Step5: Visualization
    """
    logging.info("=" * 60)
    logging.info("STARTING Vancouver Housing Insights Ingestion Pipeline")
    logging.info(f"Run timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logging.info("=" * 60)

    ingestion_steps = [
        #("Business Licences", extract_business_licences),
        #("Crime Data", extract_crime),
        #("Local Area Boundaries", extract_local_areas),
        ("Property Tax Report", extract_property_tax_report),
    ]

    success_count = 0
    fail_count = 0

    for step_name, module in ingestion_steps:
        logging.info("-" * 40)
        logging.info(f"[START] Ingesting: {step_name}")
        logging.info("-" * 40)

        try:
            module.run()
            logging.info(f"[DONE] {step_name} ingestion completed successfully.")
            success_count += 1
        except Exception as e:
            logging.error(f"[FAIL] {step_name} ingestion failed: {e}")
            fail_count += 1

    # Print summary
    logging.info("=" * 60)
    logging.info("INGESTION PIPELINE COMPLETE")
    logging.info(f"Succeeded: {success_count}/{len(ingestion_steps)}")
    logging.info(f"Failed: {fail_count}/{len(ingestion_steps)}")
    logging.info("=" * 60)

    if fail_count > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
