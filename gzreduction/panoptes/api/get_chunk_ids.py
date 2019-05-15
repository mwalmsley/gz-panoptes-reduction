import os
import logging
import json
import subprocess
from typing import Any, List, Tuple

import numpy as np


def read_last_id(save_dir) -> Any:
    id_pairs = get_id_pairs_of_chunks(save_dir)
    if id_pairs:
        latest_id = np.max(id_pairs)
        logging.warning('Using {} as latest id'.format(latest_id))
    else: 
        # latest_id = '91178981'   # first decals classification
        latest_id = '159526481'  # most recent full reduction
        logging.warning('No previous chunks found, falling back to {} latest id'.format(latest_id))
    return latest_id


def get_id_pairs_of_chunks(chunk_dir, derived=False) -> List[Tuple[int, int]]:
    """
    Get first and last ids of all chunks in chunk_dir
    
    Args:
        chunk_dir (str): directory to search for chunks
    
    Returns:
        list: of form [(first_id, last_id), ...]
    """
    locs = get_chunk_files(chunk_dir, derived=derived)
    id_pairs = []
    for loc in locs:
        first_line = subprocess.check_output(['head', '-1', loc])
        last_line = subprocess.check_output(['tail', '-1', loc])
        first_id = int(json.loads(first_line)['id'])
        last_id = int(json.loads(last_line)['id'])
        id_pairs.append([first_id, last_id])
        logging.warning('Found {} id pairs'.format(id_pairs))
    return id_pairs


def get_chunk_files(chunk_dir, derived) -> list:
    candidate_filenames = sorted(os.listdir(chunk_dir))
    # messy coupling with names, but it's quick and effective
    chunk_filenames = [x for x in candidate_filenames if 'panoptes_api' in x]
    if derived:
         selected_filenames = [x for x in chunk_filenames if 'derived' in x]
    else:
        selected_filenames = [x for x in chunk_filenames if not 'derived' in x]
    chunk_locs = [os.path.join(chunk_dir, name) for name in selected_filenames]
    return chunk_locs
