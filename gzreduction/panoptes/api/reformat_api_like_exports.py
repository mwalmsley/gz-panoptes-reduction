from collections import namedtuple

from datetime import datetime
import json

import requests
import pandas as pd

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
    decimal_version_id = raw_classification['metadata']['workflow_version']
    version_id_pair = read_workflow_versions_from_decimal_str(decimal_version_id)
    print(version_id_pair)
    print(workflow_df['major_number'])
    print(workflow_df['minor_number'])
    matching_workflows = workflow_df[
        (workflow_df['major_number'] == version_id_pair.major) & (workflow_df['minor_number'] == version_id_pair.minor)
        ]
    assert len(matching_workflows) == 1
    return matching_workflows.squeeze()


# def find_matching_version(raw_classification, all_workflow_versions):
    # O(n_versions * n_classifications), maybe slow
    # assumes workflow versions are sorted by time
    # classification_time = datetime(raw_classification['metadata']['started_at'])
    # for version_n, version in enumerate(all_workflow_versions):  
    #     version_time = datetime(version['created_at'])
    #     if version_time > classification_time:
    #         matched_version_index = version_n - 1  # the previous version was used for this classification
    #         return all_workflow_versions[matched_version_index].copy()

def insert_workflow_contents(raw_classification, workflow_version):
    raise NotImplementedError

# def temp():
#     from urllib import request

#     headers = {
#     'Accept': 'application/vnd.api+json; version=1',
#     'Content-Type': 'application/json'
#     }
#     # r = request.Request('https://panoptes-staging.zooniverse.org/api/workflows/2/versions', headers=headers)
#     r = request.Request('https://panoptes.zooniverse.org/api/workflows/6122/versions', headers=headers)

#     response_body = request.urlopen(r).read()
#     print(response_body)    

if __name__ == '__main__':

    workflow_versions = get_all_workflow_versions('6122')
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
