import ast
import json
import os
import shutil
import logging

import numpy as np
from tqdm import tqdm
from panoptes_client import Panoptes, Classification

from gzreduction import settings

# https://panoptes.docs.apiary.io/#reference/classification/classification-collection/list-all-classifications


def get_latest_classifications(json_dir, save_loc, max_classifications):
    _, last_ids = get_saved_ids(json_dir)
    if last_ids:
        latest_id = np.max(last_ids)
    else:
        latest_id = 0
    save_classifications(json_dir, max_classifications, latest_id)
    join_classifications(json_dir, save_loc)


def join_classifications(json_dir, save_loc):
    first_ids, last_ids = get_saved_ids(json_dir)
    assert len(first_ids) > 0
    assert len(last_ids) > 0
    # by definition, expect:
    assert np.min(first_ids) < np.min(last_ids)

    # apart from final last id, all last ids should match a first id
    last_ids.sort()  # inplace!
    for last_id in last_ids[:-1]:
        assert last_id in first_ids

    # save to concatenate
    with open(save_loc, 'w') as save_f:
        for file_loc in tqdm(get_chunk_files(json_dir)):
            logging.info('Adding {}'.format(file_loc))
            if file_loc != save_loc:
                with open(file_loc, 'r') as read_f:
                    line = read_f.readline()
                    while line:
                        save_f.write(line)
                        line = read_f.readline()


def get_saved_ids(json_dir):
    """[summary]
    
    Args:
        json_dir ([type]): [description]
    
    Returns:
        [type]: [description]
    """

    files = get_chunk_files(json_dir)
    first_ids = [int(f.split('_')[-3]) for f in files]
    last_ids = [int(f.split('_')[-1].strip('.txt')) for f in files]
    return first_ids, last_ids


def get_chunk_files(json_dir):
    candidate_filenames = os.listdir(json_dir)
    candidate_locs = [os.path.join(json_dir, name) for name in candidate_filenames]
    return list(filter(lambda x: 'panoptes_api_first_' in x, candidate_locs))



def save_classifications(save_dir, max_classifications=None, last_id=0):
    latest_id = get_classifications(settings.panoptes_api_json_temp_loc, max_classifications, last_id)
    save_name = 'panoptes_api_first_{}_last_{}.txt'.format(last_id, latest_id)
    save_loc = os.path.join(save_dir, save_name)
    shutil.copy(settings.panoptes_api_json_temp_loc, save_loc)


def get_classifications(save_loc, max_classifications=None, last_id=None):

    zooniverse_login_loc = 'zooniverse_login.txt'
    galaxy_zoo_id = 5733  # hardcode for now

    zooniverse_login = read_data_from_txt(zooniverse_login_loc)
    Panoptes.connect(**zooniverse_login)

    classifications = Classification.where(
        scope='project',
        project_id=galaxy_zoo_id,
        last_id=last_id
    )

    classification_n = 0
    latest_id = 0
    pbar = tqdm(total=max_classifications)
    while classification_n < max_classifications:
        try:
            classification = classifications.next().raw  # raw is the actual data
            save_classification_to_file(classification, save_loc)
        except StopIteration:  # all retrieved
            logging.info('All classifications retrieved')
            break

        if int(classification['id']) > latest_id:
            latest_id = int(classification['id'])

        pbar.update()
        classification_n += 1

    pbar.close()
    return latest_id


def save_classification_to_file(classification, save_loc):
    """General
    
    Args:
        classification ([type]): [description]
        save_loc ([type]): [description]
    """

    if os.path.exists(save_loc):
        append_write = 'a' # append if already exists
    else:
        append_write = 'w' # make a new file if not

    with open(save_loc, append_write) as f:
        json.dump(classification, f)
        f.write('\n')    


def read_data_from_txt(file_loc):
    """
    Read and evaluate a python data structure saved as a txt file.
    Args:
        file_loc (str): location of file to read
    Returns:
        data structure contained in file
    """
    with open(file_loc, 'r') as f:
        s = f.read()
        return ast.literal_eval(s)



if __name__ == '__main__':

    save_loc = 'api_results.txt'
    if os.path.isfile(save_loc):
        os.remove(save_loc)

    max_classifications = 50
    # save_classifications(settings.panoptes_api_json_dir, max_classifications)

    get_latest_classifications(
        json_dir=settings.panoptes_api_json_dir,
        save_loc=settings.panoptes_api_json_store,
        max_classifications=max_classifications
    )
