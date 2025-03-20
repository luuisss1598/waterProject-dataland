from utils.logging_config import logger
import pandas as pd
import os
import uuid
from typing import Dict

"""Function to split csv file into batches"""

def split_csv_into_batches(raw_data_source_file_path: str, temp_storage_path: str, batch_size: int) -> Dict:
    logger.info('Create unique temp folder for data...')
    run_id = str(uuid.uuid4())
    run_id_dir = os.path.join(temp_storage_path, f'run_{run_id}')

    os.makedirs(run_id_dir)

    logger.info(f'Splitting CSV files in batches of size: {batch_size} rows')
    chunk_iterator = pd.read_csv(raw_data_source_file_path, chunksize=batch_size)
    batch_files = [] # track all files created after splitting csv file

    for i, chunk in enumerate(chunk_iterator):
        batch_file = os.path.join(run_id_dir, f'batch_{i:04d}.csv')
        chunk.to_csv(batch_file, index=False)
        batch_files.append(batch_file)
        logger.info(f'Saved batch {i} with {len(chunk)} rows to {batch_file}')

    metadata = {
        'run_id': run_id,
        'batch_files': batch_files,
        'total_batches': len(batch_files)
    }

    return metadata
