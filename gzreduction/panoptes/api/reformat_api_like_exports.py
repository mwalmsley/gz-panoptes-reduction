import os
from collections import namedtuple
from typing import List, Dict
from datetime import datetime
import json

import requests
import pandas as pd

from gzreduction.panoptes.api import api_to_json

with open('auth_token.txt', 'r') as f:
    AUTH_TOKEN = f.readline()

WorkflowVersion = namedtuple('WorkflowVersion', ['major', 'minor'])

# /api/workflow_versions?workflow_id=X

# assume that calls to get workflow versions, if cached, are negligable vs. calls for classifications

def read_workflow_versions_from_decimal_str(major_minor_decimal_str):
    assert isinstance(major_minor_decimal_str, str)
    return WorkflowVersion(*major_minor_decimal_str.split('.'))


def get_workflow_contents(workflow_id, version_id):
    headers =  {
    'Accept': 'application/vnd.api+json; version=1',
    'Content-Type': 'application/json'
    }
    url = r'https://panoptes.zooniverse.org/api/workflow_versions/{}'.format(version_id)
    return requests.get(url, headers=headers)


def get_all_workflow_versions(workflow_id):
    """Get the contents of all workflow versions under workflow_id
    Form:
        meta.versions: count (over all pages), previous_href, next_href).
        versions: list of [changeset->task contents, created_at, href, id]

    Can pick the most recent workflow version, as workflows are always up-to-date (if active)

    
    Args:
        workflow_id ([type]): [description]
    
    Returns:
        [type]: [description]
    """
    all_versions = []
    page = 1
    total_pages = 999  # will be updated within while loop
    while page <= total_pages:
        response_page = get_page_of_versions(workflow_id, page)
        total_pages = response_page['meta']['workflow_versions']['page_count']
        total_version_count = response_page['meta']['workflow_versions']['count']
        all_versions.extend(response_page['workflow_versions'])
        page += 1
    assert len(all_versions) == total_version_count
    return all_versions

def get_page_of_versions(workflow_id, page):
    # it's possible that the auth token I take from notifications.zooniverse expires, causing 401 errors
    url = 'https://panoptes.zooniverse.org/api/workflow_versions?page={}&workflow_id={}'.format(page, workflow_id)
    headers =  {
    'Accept': 'application/vnd.api+json; version=1',
    'Content-Type': 'application/json',
    "Authorization": "Bearer {}".format(AUTH_TOKEN)
    }
    response = requests.get(url, headers=headers)
    return response.json()

def make_workflow_version_df(workflow_versions):
    # pandas handles json format easily :)
    df = pd.DataFrame(data=workflow_versions)
    df['major_number'] = df['major_number'].astype(str)
    df['minor_number'] = df['minor_number'].astype(str)
    return df

def find_matching_version(raw_classification, workflow_df):
    version_id_pair = get_version_id_pair(raw_classification)
    matching_workflows = workflow_df[
        (workflow_df['major_number'] == version_id_pair.major) & (workflow_df['minor_number'] == version_id_pair.minor)
        ]
    assert len(matching_workflows) == 1
    return matching_workflows.squeeze().to_dict()

def get_version_id_pair(raw_classification: Dict):
    decimal_version_id = raw_classification['metadata']['workflow_version']
    version_id_pair = read_workflow_versions_from_decimal_str(decimal_version_id)
    return version_id_pair

def insert_workflow_contents(raw_classification: Dict, workflow: Dict) -> Dict:
    classification = raw_classification.copy()  # will modify by reference below
    annotations = classification['annotations'] # list of task/answer (index) dict pairs
    workflow_strings = workflow['strings'] # dict of informative strings for each task or answer
    for annotation_n in range(len(annotations)):  # indexing to avoid modifying the iterator
        task_value_pair = annotations[annotation_n]
        # get the indices
        # task e.g. T0, value e.g. 1 (indices)
        task_id = task_value_pair['task']
        value_index = task_value_pair['value']
        # get the strings
        #e.g.T0.question, T0.answers.0.label, 
        task_label = workflow_strings['{}.question'.format(task_id)]
        if isinstance(value_index, int):
            multiple_choice = False
            value_string = workflow_strings['{}.answers.{}.label'.format(task_id, value_index)]
        elif isinstance(value_index, list):
            multiple_choice = True
            value_string = [workflow_strings['{}.answers.{}.label'.format(task_id, index)] for index in value_index]
        elif not value_index:  # i.e. None
            multiple_choice = None
            value_string = None
        else:
            print(task_id, task_label)
            raise ValueError('value index not recognised: {}'.format(value_index))
        # modify task/value pairs to include both indices and strings
        task_value_pair['task_label'] = task_label
        task_value_pair['task_id'] = task_id
        task_value_pair['value'] = value_string
        task_value_pair['value_index'] = value_index
        task_value_pair['multiple_choice'] = multiple_choice
    return classification  # modified


def rename_metadata_like_exports(classification: Dict) -> Dict:
    classification = clarify_workflow_version(classification)  # requires links attribute
    classification['classification_id'] = classification['id']
    del classification['id']
    classification['project_id'] = classification['links']['project']
    classification['user_id'] = classification['links']['user']
    classification['workflow_id'] = classification['links']['workflow']
    classification['subject_id'] = classification['links']['subjects'][0]  # assumes single subject
    del classification['links']
    return classification


def clarify_workflow_version(input_classification: Dict):
    classification = input_classification.copy()  # avoid modify-by-reference
    version_id_pair = get_version_id_pair(classification)
    classification['workflow_major_version'] = version_id_pair.major
    classification['workflow_minor_version'] = version_id_pair.minor
    return classification


def derive_classification(classification, workflows_df):
    classification = clarify_workflow_version(classification)
    classification = rename_metadata_like_exports(classification)
    workflow = find_matching_version(classification, workflows_df)
    classification = insert_workflow_contents(classification, workflow)
    return classification


if __name__ == '__main__':

    workflow_id = '6122'  # GZ decals workflow

    workflow_versions = get_all_workflow_versions(workflow_id)
    workflows_df = make_workflow_version_df(workflow_versions)

    raw_classifications_dirs = ['data/raw/classifications/api']
    raw_classification_locs = []
    for raw_dir in raw_classifications_dirs:
        raw_classification_locs.extend(api_to_json.get_chunk_files(raw_dir))

    save_dir = 'data/raw/classifications/api/derived'

    for raw_loc in raw_classification_locs:
        raw_name = os.path.split(raw_loc)[-1]
        save_loc = os.path.join(save_dir, raw_name)
        with open(raw_loc, 'r') as read_f:
            with open(save_loc, 'w') as write_f:
                while True:
                    line = read_f.readline()
                    if not line:
                        break
                    classification = json.loads(line)
                    if classification['links']['workflow'] == workflow_id:
                        derived_classification = derive_classification(classification, workflows_df)
                        json.dump(derived_classification, write_f)
                        write_f.write('\n')
    
        # with open(raw_loc, 'r') as read_f:
        #     # verify all classifications are recorded
        #     all_raw_classifications = [json.loads(x) for x in read_f.readlines()]
        #     all_matching_classifications = [x for x in all_raw_classifications if x['links']['workflow'] == '6122']
        #     classification_ids = [x['id'] for x in all_matching_classifications]

        # with open(save_loc, 'r') as save_f:
        #     all_derived_classifications = [json.loads(x) for x in save_f.readlines()]
        #     derived_ids = [x['classification_id'] for x in all_derived_classifications]
        
        # print(len(classification_ids), len(derived_ids))
        
        # from collections import Counter

        # raw_counter = Counter(classification_ids)
        # derived_counter = Counter(derived_ids)

        # print(raw_counter - derived_counter)

                




    # with open('tests/test_examples/workflow_versions.txt', 'w') as f:
    #     json.dump(workflow_versions, f)
    # print(workflow_versions)
    # changesets = [x['changeset'] for x in workflow_versions]
    # print([x.keys() for x in changesets])
    # exit(0)
    # version_ids = [version['id'] for version in workflow_versions]
    # print(version_ids)

    # print('\n')
    # exit(0)

    # version_id = '20722553'
    # version_id = '19752322'
    # response = get_workflow_contents(workflow_id='6122', version_id=version_id).json()
    # print(response)
    # response = get_workflow_contents(WorkflowVersion(major='3', minor='1'))
    # 
    # temp()

    # response = requests.get('https://panoptes-staging.zooniverse.org/api/workflows/2/versions')
    # print(response.text)