import ast
import json
import os
import shutil
import logging
import time
from datetime import datetime, timedelta
from typing import Any, List, Tuple, TypeVar, Dict
from functools import lru_cache
from itertools import count

import numpy as np
from tqdm import tqdm
from panoptes_client import Panoptes, Classification, Subject

from gzreduction import settings
from gzreduction.panoptes.api import get_chunk_ids


# expects auth file in this directory
ZOONIVERSE_LOGIN_LOC = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'zooniverse_login.json')
if not os.path.exists(ZOONIVERSE_LOGIN_LOC):
    raise ValueError('No zooniverse auth details at {}'.format(ZOONIVERSE_LOGIN_LOC))

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
        last_id = get_chunk_ids.read_last_id(save_dir)
    else:
        assert previous_dir is None
        last_id = manual_last_id

    get_classifications(
        save_dir=save_dir, 
        max_classifications=max_classifications,
        last_id=last_id
    )

def get_classifications(save_dir, max_classifications=None, last_id=None, project_id='5733', max_rate_per_sec=40, per_file=5000) -> int:
    """Save as we download line-by-line, to avoid memory issues and ensure results are saved.
    
    Args:
        save_dir ([type]): [description]
        max_classifications ([type], optional): Defaults to None. [description]
        last_id ([type], optional): Defaults to None. [description]
    
    Returns:
        int: last id downloaded
    """
    assert save_dir
    save_loc = get_save_loc(save_dir)  # will be updated every 10k classifications, to trigger Spark

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
    classifications_in_file = 0
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

            classifications_in_file += 1
            if classifications_in_file >= per_file:
                save_loc = get_save_loc(save_dir)
                classifications_in_file = 0

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
    return Subject.find(subject_id)

def save_classification_to_file(classification, save_loc) -> None:
    """Save the API response to a file of json's seperated by newlines.
    If no such file exists, start one.
    Atomic for Spark (saves each classification one at a time)
    
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


def get_save_loc(save_dir):
    # decimal in filename causes problems, this is accurate enough (in seconds)
    return os.path.join(save_dir, 'panoptes_api_{}'.format(int(time.time()))) 

if __name__ == '__main__':

    # save_dir = 'tests/test_examples'
    # save_dir = settings.panoptes_api_json_dir
    # save_dir = '/data/repos/zoobot/data/decals/classifications/raw'
    save_dir = 'data/streaming/input'
    max_classifications = 500

    previous_dir = save_dir
    manual_last_id = None
    # OR
    # previous_dir = None
    # manual_last_id = '91178981'  # first DECALS classification

    get_latest_classifications(
        save_dir=save_dir,
        previous_dir=previous_dir,
        max_classifications=max_classifications,
        manual_last_id=manual_last_id
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