import json
import os

import pandas as pd
from tqdm import tqdm

from gzreduction.panoptes.api import api_to_json
from gzreduction import settings


def load_extract_row(row_str):
    """Load Panoptes extract row into memory.
    Sensitive to export schema changes - should only be used on existing data.
    All new classifications should be downloaded via Panoptes API.
    
    Args:
        row_str (str): single line of Panoptes workflow classifications export
    
    Returns:
        dict: all data from the string, with nested dicts for annotations, metadata, subject_data
    """
    result = {}
    opening_str, metadata_and_annotations, subject_str_and_closing = row_str.split('"{"')
    opening_data = opening_str.split(',')
    # if these ever change, we'll be in trouble, hence this should only be used for past exports
    result['classification_id'] = opening_data[0]
    result['user_name'] = opening_data[1]
    result['user_id'] = opening_data[2]
    result['user_ip'] = opening_data[3]
    result['workflow_id'] = opening_data[4]
    result['workflow_name'] = opening_data[5]
    result['workflow_version'] = opening_data[6]
    result['created_at'] = opening_data[7]
    result['gold_standard'] = opening_data[8]
    result['expert'] = opening_data[9]

    metadata_str, annotations_str = metadata_and_annotations.split('"[{"')
    metadata_str_to_load = '{' + metadata_str[:-2].replace('""', '"').strip("'")
    result['metadata'] = json.loads(metadata_str_to_load)

    annotations_str_to_load = '[{' + annotations_str[:-2].replace('""', '"')
    result['annotations'] = json.loads(annotations_str_to_load)

    subject_str, closing_str = subject_str_and_closing.split('}",')
    subject_str_to_load = ('{"' + subject_str + '}').replace('""', '"')
    result['subject_data'] = json.loads(subject_str_to_load)

    result['subject_ids'] = closing_str

    return result


def convert_extract_to_json(extract_loc, save_loc, max_lines=1e12):
    # could do this with Spark, but only runs once and loads line-by-line so minor benefit
    with open(extract_loc, 'r') as f:
        _ = f.readline()  # skip header
        line = f.readline()
        line_n = 0
        pbar = tqdm(total=max_lines)  # we don't know how many lines total, without reading...
        while line and line_n < max_lines:
            row_data = load_extract_row(line.strip('\n'))
            api_to_json.save_classification_to_file(row_data, save_loc)
            line_n += 1
            line = f.readline()
            pbar.update()
            # about 2500 lines per second, or about 12h for 1 million lines
        pbar.close()


if __name__ == '__main__':
    save_loc = settings.panoptes_extract_json_loc
    if os.path.isfile(save_loc):
        os.remove(save_loc)

    convert_extract_to_json(
        extract_loc=settings.panoptes_old_style_classifications_loc,
        save_loc=save_loc,
        # max_lines=50000
        )
