import os 
from utils.logging_config import logger
from typing import Dict

"""Funciton to clean up files after partionitioning csv files into multiple batches"""
def clean_up(meta_data: dict, temp_storage_path: str) -> Dict:
    try: 
        if not meta_data:
            msg_exception = f'Metadata is empty {meta_data}'
            logger.info(msg_exception)
            raise ValueError(msg_exception)
    except Exception as err:
        logger.info(f'Metadata is empty: {err}')
        raise

    load_run_id = meta_data['run_id']
    batch_files_removed = []
    total_files_removed = 0
    # make sure run id exist in dict
    if load_run_id:
        root_dir = os.path.join(temp_storage_path, f'run_{load_run_id}')

        logger.info(f'Initializing removal of temp files...')
        for root, dirs, files in os.walk(root_dir):
            # go file by file in the dir
            for file in files:
                os.remove(os.path.join(root, file))
                logger.info(f'File {file} removed from {root}')
                total_files_removed += 1
                batch_files_removed.append(file)
            
            os.rmdir(root)
            logger.info(f'Root with run_id run_{load_run_id} removed...')

    metadata_clean = {
        'root dir': f'{temp_storage_path}/run_{load_run_id}',
        'files_removed': sorted(batch_files_removed),
        'total_files_removed': total_files_removed
    }

    return metadata_clean