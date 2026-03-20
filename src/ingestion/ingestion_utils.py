"""
ingestion_utils.py

Shared utility functions for all ingestion scripts.
Provides a helper to save a DataFrame as one or more JSON files,
automatically splitting by record count when a file exceeds the threshold.
"""

import math
import os
import logging
from datetime import datetime

import pandas as pd


def save_as_split_json(
    df: pd.DataFrame,
    output_dir: str,
    source_name: str,
    max_records_per_file: int = 100_000,
) -> list[str]:
    """
    Save a DataFrame to one or more JSON files, splitting automatically
    when the number of records exceeds the threshold.

    Naming convention:
        {source_name}_{YYYYMMDD}_part_001.json
        {source_name}_{YYYYMMDD}_part_002.json
        ...

    Returns:
        A list of absolute paths to the saved JSON files.
    """
    # Ensure the output directory exists
    os.makedirs(output_dir, exist_ok=True)

    current_date = datetime.now().strftime("%Y%m%d")
    total_records = len(df)
    num_parts = math.ceil(total_records / max_records_per_file)

    logging.info(
        f"Splitting {total_records} records into {num_parts} part(s) "
        f"(max {max_records_per_file} records per file)."
    )

    saved_files: list[str] = []

    for part_idx in range(num_parts):
        start = part_idx * max_records_per_file
        end = min(start + max_records_per_file, total_records)
        chunk = df.iloc[start:end]

        # Part number is 1-indexed and zero-padded to 3 digits
        part_number = f"{part_idx + 1:03d}"
        file_name = f"{source_name}_{current_date}_part_{part_number}.json"
        file_path = os.path.join(output_dir, file_name)

        chunk.to_json(file_path, orient="records", indent=2, force_ascii=False)
        logging.info(
            f"  Saved part {part_number}: {len(chunk)} records → {file_path}"
        )
        saved_files.append(file_path)

    logging.info(
        f"Done: saved {total_records} records across {len(saved_files)} file(s)."
    )
    return saved_files
