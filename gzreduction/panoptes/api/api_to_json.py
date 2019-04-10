import ast
import json
import os
import shutil
import logging
import time
from datetime import datetime, timedelta
from typing import Any, List, Tuple, TypeVar, Dict
from functools import lru_cache

import numpy as np
from tqdm import tqdm
from panoptes_client import Panoptes, Classification, Subject

from gzreduction import settings

ZOONIVERSE_LOGIN_LOC = 'zooniverse_login.json'
# https://panoptes.docs.apiary.io/#reference/classification/classification-collection/list-all-classifications


def get_latest_classifications(save_dir, max_classifications, previous_dir=None, manual_last_id=None) -> None:
    """Download classifications from the Panoptes API

    Downloaded classifications will be saved as a new 'chunk'
    .txt file with first and last id in the filename and data as json line-seperated rows

    If previous_dir is provided and has chunks, continue from the latest id in previous_dir
    Otherwise, start from scratch.
    
    Args:
        save_dir (str): directory into which to save new chunk
        max_classifications (int): stop at this many classifications (useful for debugging)
        previous_dir (str): if previous chunks are in this directory, will continue from them
        manual_last_id (int): (Optional) if not None, start from this id. Incompatible with previous_dir.
    """
    assert save_dir
    if manual_last_id is None:
        last_id = read_last_id(save_dir)
    else:
        assert previous_dir is None
        last_id = manual_last_id

    save_classifications(
        save_dir=save_dir, 
        max_classifications=max_classifications, 
        previous_dir=previous_dir,
        last_id=last_id
    )


def read_last_id(save_dir) -> Any:
    id_pairs = get_id_pairs_of_chunks(save_dir)
    if id_pairs:
        latest_id = np.max(id_pairs)
    else:
        latest_id = 0
    return latest_id


def get_id_pairs_of_chunks(chunk_dir, derived=False) -> List[Tuple[int, int]]:
    """
    Get first and last ids of all chunks in chunk_dir
    
    Args:
        chunk_dir (str): directory to search for chunks
    
    Returns:
        list: of form [(first_id, last_id), ...]
    """
    files = get_chunk_files(chunk_dir, derived=derived)
    first_ids = [int(f.split('_')[-3]) for f in files]
    last_ids = [int(f.split('_')[-1].strip('.txt')) for f in files]
    id_pairs = list(zip(first_ids, last_ids))
    return id_pairs


def get_chunk_files(chunk_dir, derived) -> list:
    candidate_filenames = sorted(os.listdir(chunk_dir))
    # messy coupling with names, but it's quick and effective
    chunk_filenames = [x for x in candidate_filenames]
    if derived:
         selected_filenames = [x for x in chunk_filenames if 'derived' in x]
    else:
        selected_filenames = [x for x in chunk_filenames if not 'derived' in x]
    chunk_locs = [os.path.join(chunk_dir, name) for name in selected_filenames]
    return chunk_locs


def save_classifications(save_dir, previous_dir=None, max_classifications=None, last_id=0) -> None:
    """
    Request responses from the API, starting from last_id (default 0).
    Initially save responses to a temporary file, and then rename once the download is complete and last_id is known.
    
    Args:
        save_dir (str): directory into which to save new chunk
        previous_dir (str, optional): Defaults to None. [description]
        max_classifications (int, optional): Defaults to None. [description]
        last_id (int, optional): Defaults to 0. [description]
    """
    # download here until download is complete, then rename according to last id in download
    temp_loc = os.path.join(save_dir, 'panoptes_api_download_in_progress.txt')
    new_last_id = get_classifications(temp_loc, max_classifications, last_id)
    if os.path.exists(temp_loc):
        save_name = 'panoptes_api_first_{}_last_{}.txt'.format(last_id, new_last_id)
        save_loc = os.path.join(save_dir, save_name)
        # overwrite any existing file!
        if os.path.exists(save_loc):
            os.remove(save_loc)
        shutil.move(temp_loc, save_loc)
    else:
        logging.warning("No new classifications downloaded!")
        print("No new classifications downloaded!")


def get_classifications(save_loc, max_classifications=None, last_id=None, project_id='5733', max_rate_per_sec=15) -> int:
    """Save as we download line-by-line, to avoid memory issues and ensure results are saved.
    
    Args:
        save_loc ([type]): [description]
        max_classifications ([type], optional): Defaults to None. [description]
        last_id ([type], optional): Defaults to None. [description]
    
    Returns:
        int: last id downloaded
    """
    assert save_loc

    with open(ZOONIVERSE_LOGIN_LOC, 'r') as f:
        zooniverse_login = json.load(f)
    Panoptes.connect(**zooniverse_login)

    # TODO specify workflow if possible?
    classifications = Classification.where(
        scope='project',
        project_id=project_id,
        last_id=last_id
    )

    classification_n = 0
    latest_id = 0
    pbar = tqdm(total=max_classifications)
    
    min_time = timedelta(seconds=(1./max_rate_per_sec))
    while classification_n < max_classifications:
        try:  # may possibly be requesting the very first classification twice, not clear how - TODO test
            initial_time = datetime.now()
            classification = classifications.next().raw  # raw is the actual data

            # replace subject id with subject information from API
            subject_id = classification['links']['subjects'][0]  # only works for single-subject projects
            del classification['links']['subjects']
            subject = get_subject(subject_id) # assume id is unique, and hence only one match is possible
            classification['links']['subject'] = subject.raw

            save_classification_to_file(classification, save_loc)

            time_elapsed = datetime.now() - initial_time
            if time_elapsed < min_time:
                sleep_seconds = (min_time - time_elapsed).total_seconds() 
                logging.debug('Sleeping {} seconds'.format(sleep_seconds))
                time.sleep(sleep_seconds)

        except StopIteration:  # all retrieved
            logging.info('All classifications retrieved')
            break

        if int(classification['id']) > latest_id:
            latest_id = int(classification['id'])

        pbar.update()
        classification_n += 1

    pbar.close()
    return latest_id

@lru_cache(maxsize=2**18)
def get_subject(subject_id):
    subject =  Subject.find(subject_id)
    return subject


def save_classification_to_file(classification, save_loc) -> None:
    """Save the API response to a file of json's seperated by newlines.
    If no such file exists, start one.
    
    Args:
        classification ([type]): [description]
        save_loc ([type]): [description]
    """
    assert classification
    assert save_loc
    if os.path.exists(save_loc):
        append_write = 'a' # append if already exists
    else:
        append_write = 'w' # make a new file if not

    with open(save_loc, append_write) as f:
        json.dump(classification, f)
        f.write('\n')    


if __name__ == '__main__':

    # save_dir = 'tests/test_examples'
    # save_dir = settings.panoptes_api_json_dir
    save_dir = '/data/repos/zoobot/data/decals/classifications/raw'
    max_classifications = 10000000

    previous_dir = save_dir
    # OR
    # manual_last_id = '91178981'  # first DECALS classification

    get_latest_classifications(
        save_dir=save_dir,
        previous_dir=previous_dir,
        max_classifications=max_classifications,
        manual_last_id=None
    )


    # with open(ZOONIVERSE_LOGIN_LOC, 'r') as f:
    #     zooniverse_login = json.load(f)
    # Panoptes.connect(**zooniverse_login)

    # TODO specify workflow if possible?
    # classifications = Classification.where(
    #     scope='project',
    #     project_id=galaxy_zoo_id,
    #     last_id=last_id
    # )

    # from panoptes_client import Subject
    # subjects = Subject.where(
    #     scope='project',
    #     project_id='5733'
    #     # id='15069378'
    #     # silently does not support last_id
    # )
    # for n in range(10):
    #     s = subjects.next()

# for each (saved to disk) classification:
# is the subject saved to indexed jsons? (to ponder)?
# if yes, match up (to ponder)
# if not, download to indexed jsons and then match up