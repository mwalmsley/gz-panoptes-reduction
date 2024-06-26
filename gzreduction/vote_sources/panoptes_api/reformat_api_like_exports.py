import os
import logging
import time
from functools import lru_cache
from collections import namedtuple
from typing import List, Dict
from datetime import datetime
import json
import ast
from typing import List

import requests
import pandas as pd
from panoptes_client import Panoptes
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, split, to_json, from_json
from pyspark.sql.types import TimestampType, BooleanType, StringType, IntegerType, ArrayType, StructType, StructField

from gzreduction.panoptes_api.api import api_to_json

def get_panoptes_auth_token():
    # This token is only valid for ~2 hours. Don't use for long-running downloads. 
    # Here, we only need a few calls to get the workflow versions
    # Will ask the devs to expose this nicely with the already-built expiry check.
    with open(api_to_json.ZOONIVERSE_LOGIN_LOC, 'r') as f:  # beware sneaky shared global state
        zooniverse_login = json.load(f)
    Panoptes.connect(**zooniverse_login)
    return Panoptes._local.panoptes_client.get_bearer_token()

def get_panoptes_headers(auth_token):
    return {
    'Accept': 'application/vnd.api+json; version=1',
    'Content-Type': 'application/json',
    "Authorization": "Bearer {}".format(auth_token)
    }

WorkflowVersion = namedtuple('WorkflowVersion', ['major', 'minor'])

# assume that calls to get workflow versions, if cached, are negligable vs. calls for classifications

def read_workflow_versions_from_decimal_str(major_minor_decimal_str):
    assert isinstance(major_minor_decimal_str, str)
    return WorkflowVersion(*major_minor_decimal_str.split('.'))
read_workflow_versions_from_decimal_str_udf = udf(read_workflow_versions_from_decimal_str)


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
    assert isinstance(workflow_id, str)
    auth_token = get_panoptes_auth_token()
    all_versions = []
    page = 1
    total_pages = 999  # will be updated within while loop
    while page <= total_pages:
        response_page = get_page_of_versions(workflow_id, page, auth_token)
        total_pages = response_page['meta']['workflow_versions']['page_count']
        total_version_count = response_page['meta']['workflow_versions']['count']
        all_versions.extend(response_page['workflow_versions'])
        page += 1
    assert len(all_versions) == total_version_count
    return all_versions

def get_page_of_versions(workflow_id, page, auth_token):
    # it's possible that the auth token I take from notifications.zooniverse expires, causing 401 errors
    url = 'https://panoptes.zooniverse.org/api/workflow_versions?page={}&workflow_id={}'.format(page, workflow_id)
    response = requests.get(url, headers=get_panoptes_headers(auth_token))
    return response.json()

def make_workflow_version_df(workflow_versions):
    # pandas handles json format easily :)
    df = pd.DataFrame(data=workflow_versions)
    df['workflow_major_version'] = df['major_number'].astype(str)
    df['workflow_minor_version'] = df['minor_number'].astype(str)
    del df['major_number']
    del df['minor_number']
    return df


def find_matching_version(major_version, minor_version, workflow_df):
    # version_id_pair = get_version_id_pair(raw_classification)
    matching_workflows = workflow_df[
        (workflow_df['workflow_major_version'] == major_version) & (workflow_df['workflow_minor_version'] == minor_version)
        ]
    assert len(matching_workflows) == 1
    return matching_workflows.squeeze().to_dict()


def insert_workflow_contents(annotations, workflow):
    workflow_strings = workflow['strings'] # dict of informative strings for each task or answer
    for annotation_n in range(len(annotations)):  # indexing to avoid modifying the iterator
        task_value_pair = annotations[annotation_n]
        # get the indices
        # task e.g. T0, value e.g. 1 (indices)
        try:
            task_id = task_value_pair['task']
            value_index = task_value_pair['value']
        except KeyError:
            return None  # anything missing these is a bad classification
            # raise KeyError(task_value_pair, annotations)
        # get the strings
        #e.g.T0.question, T0.answers.0.label, 
        task_label = workflow_strings['{}.question'.format(task_id)]
        if isinstance(value_index, str):
            try:
                value_index = int(value_index)
            except ValueError:
                value_index = ast.literal_eval(value_index)

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
            # raise ValueError(task_id, task_label)
            raise ValueError('value index not recognised: {} {}'.format(type(value_index), value_index))
        # modify task/value pairs to include both indices and strings
        task_value_pair['task_label'] = task_label
        task_value_pair['task_id'] = task_id
        task_value_pair['value'] = value_string
        task_value_pair['value_index'] = value_index
        task_value_pair['multiple_choice'] = multiple_choice
    return annotations  # modified by reference



def rename_metadata_like_exports(df):
    df = df.withColumnRenamed('id', 'classification_id')  # existing, new
    df = df.withColumn('project_id', df['links']['project'])
    df = df.withColumn('user_id', df['links']['user'])
    df = df.withColumn('workflow_id', df['links']['workflow'])
    df = df.withColumn('subject_id', df['links']['subject']['id'])
    df = df.drop('links')
    return df


def clarify_workflow_version(df):
    version_id_pair = split(df['metadata']['workflow_version'], '\\.')  # need to escape the period
    df = df.withColumn('workflow_major_version', version_id_pair.getItem(0))
    df = df.withColumn('workflow_minor_version', version_id_pair.getItem(1))
    return df


def derive_directories_with_spark(
    input_dir: str, 
    output_dir: str, 
    workflows: dict, 
    mode='batch', 
    print_output=False,
    spark=None):

    if not spark:
        spark = SparkSession \
            .builder \
            .appName("derive_directories") \
            .getOrCreate()



    if mode == 'stream':
            # infer schema from existing file
        tiny_loc = os.path.dirname(os.path.realpath(__file__)) + "/../../../data/examples/panoptes_raw.txt"
        assert os.path.exists(tiny_loc)
        schema = spark.read.json(tiny_loc).schema
        logging.warning('Attempting to stream derived files to {}'.format(input_dir))
        df = spark.readStream.json(input_dir, schema=schema)
    else:
        df = spark.read.json(input_dir)

    df = df.filter(df['links']['workflow'].isin(list(workflows.keys())))  # include only allowed workflows

    df = clarify_workflow_version(df)
    df = rename_metadata_like_exports(df)

    workflows_str = json.dumps(workflows)
    def match_and_insert_workflow(annotations, workflow_id, major_version, minor_version, workflows_str):
        workflow_versions = pd.DataFrame(data=json.loads(workflows_str)[workflow_id])
        workflow = find_matching_version(major_version, minor_version, workflow_versions)
        annotations_dict = json.loads(annotations)
        updated_annotations_dict = insert_workflow_contents(annotations_dict, workflow)
        return json.dumps(updated_annotations_dict)
    match_and_insert_workflow_udf = udf(
        lambda a, b, c, d: match_and_insert_workflow(a, b, c, d, workflows_str),
        StringType()
    )

    # apparently can't pass struct as udf argument, need to use as string
    df = df.withColumn('annotations', to_json(df['annotations']))  
    df = df.withColumn(
        'annotations',
        match_and_insert_workflow_udf(
            df['annotations'],
            df['workflow_id'],  # added by rename_metadata_like_exports
            df['workflow_major_version'],  # added by clarify_workflow_version
            df['workflow_minor_version']  # added by clarify_workflow_version
        )
    )

   # parse annotations back out again (using the new schema, of course)
    annotations_schema = ArrayType(StructType([
        StructField("task", StringType(), False),
        StructField("value", StringType(), False),
        StructField("task_label", StringType(), False),
        StructField("task_id", StringType(), False),
        StructField("value_index", StringType(), False),
        StructField("multiple_choice", BooleanType(), False)
    ]))
    df = df.withColumn('annotations', from_json(df['annotations'], schema=annotations_schema))


    if mode == 'stream':
        query = df.writeStream \
            .outputMode('append') \
            .option('checkpointLocation', os.path.join(output_dir, 'checkpoints')) \
            .trigger(processingTime='3 seconds') \
            .start(path=output_dir, format='json')
        print('Derived data ready to stream')
        if print_output:
            while True:
                time.sleep(0.1)
                if query.status['isDataAvailable']:
                    print(datetime.now(), query.status['message'])
    else:
        df.show()
        df.write.save(output_dir, format='json', mode='overwrite')

def derive_chunks(workflow_ids: list, raw_classification_dir: str, output_dir: str, mode: str, print_status: bool, spark):
    assert isinstance(workflow_ids, list)
    assert isinstance(raw_classification_dir, str)

    workflows = {}
    for workflow_id in workflow_ids:
        workflow_versions = get_all_workflow_versions(workflow_id)
        workflows[workflow_id] = make_workflow_version_df(workflow_versions).to_dict()

    derive_directories_with_spark(raw_classification_dir, output_dir, workflows, mode, print_status)


if __name__ == '__main__':

    workflow_id = ['12410']  # GZ decals workflow
    raw_classification_dirs = 'temp/raw'

    spark = SparkSession \
        .builder \
        .appName("shared") \
        .getOrCreate()
    derive_chunks(workflow_id, raw_classification_dirs, 'temp/derived', mode='batch', print_status=True, spark=spark)
