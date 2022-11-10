import json
import os
import argparse

import pandas as pd
from tqdm import tqdm

from gzreduction.panoptes_api.api import api_to_json
from gzreduction.deprecated import settings


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


def format_row_like_api(row):

    # actually, I format api like export
    # see reformat_api_like_exports line 140ish
    # only one tweak to both

    row['subject_id'] = row['subject_ids']
    del row['subject_ids']

    row['project_id'] = '5733'  # TODO hardcoded for now as not in export

    # API 'created_at' looks like this "2019-07-18T20:25:12.394Z"
    # but Panoptes 'created_at' looks like this "2019-10-23 13:30:24 UTC"
    # lets match the API, as that works nicely with Spark
    row['created_at'] = pd.to_datetime(row['created_at']).strftime("%Y-%m-%dT%H:%M:%S") + 'Z'  # is always UTC (Good job Cam)

    # once derived workflow data is added, api questions look like this
    # {"task":"T0", "task_label": "Is the galaxy simply smooth and rounded, with no sign of a disk?", "value":"![features_or_disk_new.png](https://panoptes-uploads.zooniverse.org/production/project_attached_image/1ec52a74-9e49-4579-91ff-0140eb5371e6.png =60x) Features or Disk","task_id":"T0","value_index":"1","multiple_choice":false}
    # but panoptes looks like
    # {"task": "T0", "task_label": "Is the galaxy simply smooth and rounded, with no sign of a disk?", "value": "![smooth_triple_flat_new.png](https://panoptes-uploads.zooniverse.org/production/project_attached_image/75cec1ed-f50a-4151-8877-f6e74f6748eb.png =60x) Smooth"
    # add the 'multiple choice' flag that flatten.py uses to filter
    # annotations = json.loads(row['annotations'])
    for annotation in row['annotations']:
        annotation['task_id'] = annotation['task']  # flatten uses this convention for T0 etc
        # annotation['value_index'] = 
        annotation['multiple_choice'] = annotation["task_label"] == "Do you see any of these rare features?"

    
    # row['id'] = row['classification_id']
    # del row['classification_id']
    
    # row['links'] = {
    #     'project': '5733',  
    #     'user': row['user_id'],
    #     'user_group': None,
    #     'workflow': row['workflow_id'],
    #     'subject': {
    #         'links': {
    #             'id': row['subject_ids']
    #         }
    #     }
    # }
    # del row['user_id']
    # del row['workflow_id']
    # del row['subject_ids']

    return row


def convert_extract_to_json(extract_loc, save_dir, max_lines=1e12):
    # could do this with Spark, but only runs once and loads line-by-line so minor benefit
    with open(extract_loc, 'r') as f:
        _ = f.readline()  # skip header
        line = f.readline()
        line_n = 0

        atomic_file = api_to_json.AtomicFile(save_dir)

        pbar = tqdm(total=max_lines)  # we don't know how many lines total, without reading...
        while line and line_n < max_lines:
            row = load_extract_row(line.strip('\n'))
            row = format_row_like_api(row)
            atomic_file.add(row)
            line_n += 1
            line = f.readline()
            pbar.update()
            # about 2500 lines per second, or about 12h for 1 million lines
        pbar.close()

        atomic_file.end_file()  # be sure to write the last few


if __name__ == '__main__':

     # os.environ['PYSPARK_DRIVER_PYTHON'] = hardcoded_python
    """
    python gzreduction/panoptes/extract/panoptes_extract_to_json.py
    """

    parser = argparse.ArgumentParser('Panoptes Extract to JSON')
    parser.add_argument(
        '--export',
        dest='export_loc',
        type=str,
        default='data/expert_classifications_export.csv'
    )
    parser.add_argument(
        '--save-dir',
        dest='save_dir',
        type=str,
        default='data/expert_classifications_export'
    )
    args = parser.parse_args()

    export_loc = args.export_loc
    save_dir = args.save_dir
    # if os.path.isdir(save_dir):
    #     shutil.rmtree(save_dir)
    if not os.path.exists(save_dir):
        os.mkdir(save_dir)

    convert_extract_to_json(
        extract_loc=export_loc,
        save_dir=save_dir,
        # max_lines=50000
        )
