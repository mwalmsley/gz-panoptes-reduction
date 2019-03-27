import ast
import json
import os
import shutil
import logging
from typing import Any, List, Tuple, TypeVar, Dict

import numpy as np
from tqdm import tqdm
from panoptes_client import Panoptes, Classification

from gzreduction import settings

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
    # join_classifications(save_dir, save_loc) # TODO see below


# TODO move this outside, perhaps to before Spark reduction
# def join_classifications(json_dir, save_loc):
#     """Concatenate all saved chunks into one .txt file
    
#     Args:
#         json_dir (str): directory for chunks
#         save_loc (str): path to save all classifications (from all chunks)
#     """
#     first_ids, last_ids = get_ids_of_chunks(json_dir)
#     assert len(first_ids) > 0
#     assert len(last_ids) > 0
#     # by definition, expect:
#     assert np.min(first_ids) < np.min(last_ids)

#     # apart from final last id, all last ids should match a first id
#     last_ids.sort()  # inplace!
#     for last_id in last_ids[:-1]:
#         assert last_id in first_ids

#     # save to concatenate
#     with open(save_loc, 'w') as save_f:
#         for file_loc in tqdm(get_chunk_files(json_dir)):
#             logging.info('Adding {}'.format(file_loc))
#             if file_loc != save_loc:
#                 with open(file_loc, 'r') as read_f:
#                     line = read_f.readline()
#                     while line:
#                         save_f.write(line)
#                         line = read_f.readline()



def get_id_pairs_of_chunks(chunk_dir) -> List[Tuple[int, int]]:
    """
    Get first and last ids of all chunks in chunk_dir
    
    Args:
        chunk_dir (str): directory to search for chunks
    
    Returns:
        list: of form [(first_id, last_id), ...]
    """
    files = get_chunk_files(chunk_dir)
    first_ids = [int(f.split('_')[-3]) for f in files]
    last_ids = [int(f.split('_')[-1].strip('.txt')) for f in files]
    id_pairs = list(zip(first_ids, last_ids))
    return id_pairs


def get_chunk_files(chunk_dir) -> list:
    candidate_filenames = sorted(os.listdir(chunk_dir))
    chunk_filenames = [x for x in candidate_filenames if 'panoptes_api_first_' in x]
    chunk_locs = [os.path.join(chunk_dir, name) for name in chunk_filenames]
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
    save_name = 'panoptes_api_first_{}_last_{}.txt'.format(last_id, new_last_id)
    save_loc = os.path.join(save_dir, save_name)
    # overwrite any existing file!
    if os.path.exists(save_loc):
        os.remove(save_loc)
    shutil.move(temp_loc, save_loc)


def get_classifications(save_loc, max_classifications=None, last_id=None) -> int:
    """Save as we download line-by-line, to avoid memory issues and ensure results are saved.
    
    Args:
        save_loc ([type]): [description]
        max_classifications ([type], optional): Defaults to None. [description]
        last_id ([type], optional): Defaults to None. [description]
    
    Returns:
        int: last id downloaded
    """

    assert save_loc
    zooniverse_login_loc = 'zooniverse_login.txt'
    galaxy_zoo_id = 5733  # hardcode for now

    zooniverse_login = read_data_from_txt(zooniverse_login_loc)
    Panoptes.connect(**zooniverse_login)

    # TODO specify workflow if possible?
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
            save_classification_to_file(rename_to_match_exports(classification), save_loc)
        except StopIteration:  # all retrieved
            logging.info('All classifications retrieved')
            break

        if int(classification['classification_id']) > latest_id:
            latest_id = int(classification['classification_id'])

        pbar.update()
        classification_n += 1

    pbar.close()
    return latest_id


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


def read_data_from_txt(file_loc) -> Any:
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


def rename_to_match_exports(classification: Dict) -> Dict:
    classification['classification_id'] = classification['id']
    del classification['id']
    classification['project_id'] = classification['links']['project']
    classification['user_id'] = classification['links']['user']
    classification['workflow_id'] = classification['links']['workflow']
    classification['subject_id'] = classification['links']['subjects'][0]
    del classification['links']
    return classification


if __name__ == '__main__':

    # save_dir = 'tests/test_examples'
    save_dir = settings.panoptes_api_json_dir
    max_classifications = 10000

    get_latest_classifications(
        save_dir=save_dir,
        previous_dir=None,
        max_classifications=max_classifications,
        manual_last_id='91178981'  # first DECALS classification
    )
