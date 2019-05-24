import ast
import json
import os
import shutil
import logging
import time
import tempfile
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
    """Download classifications from the Panoptes API (from all workflows)

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
    atomic_file = AtomicFile(save_dir)

    while classification_n < max_classifications:
        try:  # may possibly be requesting the very first classification twice, not clear how - TODO test
            initial_time = datetime.now()
            classification = classifications.next().raw  # raw is the actual data

            # replace subject id with subject information from API
            subject_id = classification['links']['subjects'][0]  # only works for single-subject projects
            del classification['links']['subjects']
            subject = get_subject(subject_id) # assume id is unique, and hence only one match is possible
            classification['links']['subject'] = subject.raw

            atomic_file.add(classification)

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

    atomic_file.end_file()  # write anything left
    pbar.close()
    return latest_id

class AtomicFile():
    """Atomic for Spark (saves each batch of n classifications in one operation)"""

    def __init__(self, save_dir, per_file=2500):
        self.save_dir = save_dir
        self.temp_file = self.new_temp_file()
        self.per_file = per_file
        self.classifications_in_file = 0

    def new_temp_file(self):
        self.temp_loc = os.path.join('temp_download_folder', 'temp_file')
        if not os.path.isdir('temp_download_folder'):
            os.mkdir('temp_download_folder')
        if os.path.isfile(self.temp_loc):
            os.remove(self.temp_loc)
        return open(self.temp_loc, 'a+')

    def add(self, classification):
        self.write_to_temp(classification)
        self.classifications_in_file += 1
        if self.classifications_in_file >= self.per_file:
            logging.info('Max per file {} reached, saving'.format(self.per_file))
            self.end_file()
            self.temp_file = self.new_temp_file()

    def write_to_temp(self, classification) -> None:
        """Save the API response to a file of json's seperated by newlines.
        If no such file exists, start one.
        
        Args:
            classification ([type]): [description]
        """
        assert classification
        json.dump(classification, self.temp_file)
        self.temp_file.write('\n')

    def end_file(self):
        save_loc = get_save_loc(self.save_dir)
        logging.info('Atomic save to {}'.format(save_loc))
        assert os.path.exists(self.temp_loc)
        os.rename(self.temp_loc, save_loc)
        self.classifications_in_file = 0



@lru_cache(maxsize=2**18)
def get_subject(subject_id):
    return Subject.find(subject_id)



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