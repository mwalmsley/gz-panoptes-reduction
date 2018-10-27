import ast
import json
import os
import logging

from panoptes_client import Panoptes, Classification


def get_classifications():

    zooniverse_login_loc = 'zooniverse_login.txt'
    galaxy_zoo_id = 5733  # hardcode for now

    zooniverse_login = read_data_from_txt(zooniverse_login_loc)
    Panoptes.connect(**zooniverse_login)

    classifications = Classification.where(
        # scope='project',
        project_id=galaxy_zoo_id,
    )

    save_loc = 'api_results.txt'
    for n in range(5):
        classification = classifications.next()
        save_classification_to_file(classification, save_loc)


def save_classification_to_file(classification, save_loc):
    if os.path.exists(save_loc):
        append_write = 'a' # append if already exists
    else:
        append_write = 'w' # make a new file if not

    with open(save_loc, append_write) as f:
        json.dump(classification.raw, f)
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
    get_classifications()