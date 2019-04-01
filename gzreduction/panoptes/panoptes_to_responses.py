
import logging
import os
import io
import sys
import json
from datetime import datetime, timezone
import shutil

import pandas as pd
from pyspark import SparkContext, SparkConf

from gzreduction import settings
from gzreduction.schemas.dr5_schema import dr5_schema
from gzreduction.panoptes.api import api_to_json


def preprocess_classifications(
        classifications_locs,
        schema,
        start_date=None,
        save_loc=None):
    """
    Convert a raw Panoptes classification extract into flattened and cleaned responses

    Args:
        classification_locs (list): of text files with json rows, where each row is a classification
        schema (Schema):  definition object for questions and answers
        start_date (pd.datetime): (optional) if not None, filter out Panoptes classifications before this date
        save_loc (str): (optional) if not None, save text file of responses to this location

    Returns:
        (Spark RDD) rows=responses, columns=question/answer/user/time
    """
    os.environ['PYSPARK_PYTHON'] = sys.executable  # path to current interpreter
    appName = 'preprocess_classifications'  # name to display
    master = 'local[3]'  # don't run on remote cluster. [*] indicates use all cores.
    conf = SparkConf().setAppName(appName).setMaster(master) 
    sc = SparkContext(conf=conf)  # this tells spark how to access a cluster

    # load all classifications as RDD
    assert isinstance(classifications_locs, list)
    classification_chunks = [sc.textFile(loc) for loc in classifications_locs]
    lines = sc.union(classification_chunks)

    # convert to dicts (still held in RDD)
    classifications = lines.map(lambda x: load_classification_line(x))

    if start_date:  # e.g. live date of public GZ DR5
        classifications = classifications.filter(lambda x: x['created_at'] >= start_date)

    # separate out to one response per line
    responses = classifications.flatMap(explode_classification)

    # remove bad responses
    filtered_responses = responses.filter(lambda x: not is_multiple_choice(x) and not is_null_classification(x))
    # rename, remove markdown and None's
    cleaned_responses = filtered_responses.map(lambda x: clean_response(x, schema))
    # slightly awkwardly coupled
    final_responses = cleaned_responses.filter(lambda x: x is not None)

    if save_loc is not None:
        if os.path.isdir(save_loc):
            shutil.rmtree(save_loc)
        final_responses.map(lambda x: response_to_line(x)).saveAsTextFile(save_loc)
        logging.info('Saved Panoptes responses to {}'.format(save_loc))

    return final_responses  # still an rdd, not collected


def response_to_line_header():
    return [
        'classification_id',
        'created_at',
        'user_id',
        'subject_id',
        'task',
        'value'
    ]


def response_to_line(r, header=response_to_line_header()):
    """Convert response to csv-style format, for later translation to pandas
    
    Args:
        r ([type]): [description]
    
    Returns:
        [type]: [description]
    """
    data = ['{}'.format(r[col]) for col in header]
    return ','.join(data)


def load_classification_line(line):
    """Classifications are saved to disk as one json per line.
    Load this json to dict, and handle minor type corrections
    
    Args:
        line (str): json of classification (including annotations)
    
    Returns:
        dict: classification (including annotations)
    """
    classification = json.loads(line)
    assert isinstance(classification, dict)
    assert isinstance(classification['annotations'], list)
    classification['created_at'] = pd.to_datetime(classification['created_at'])
    # TODO any further type updates
    return classification


def explode_classification(classification):
    """Convert a single line in Panoptes export to list of responses
    
    Args:
        classification (dict): single Panoptes classification e.g. single API response

    Returns:
        list: of dicts of form {time, user, subject, question, answer}
    """
    annotation_data = classification['annotations']
    flat_df = pd.DataFrame(annotation_data)  # put each [task, value] pair on a row
    for col in ['user_id', 'classification_id', 'created_at']:
        flat_df[col] = classification[col]  # record the (same) user/subject/metadata on every row
    flat_df['subject_id'] = classification['subject_id']  # WARNING export needs subject_id -> subject_ids
    return [row.to_dict() for _, row in flat_df.iterrows()]


def is_multiple_choice(response):
    try:
        return response['multiple_choice'] == True  # also handles = None -> False
    except KeyError:  # for exports where this field doesn't yet exist
        multiple_choice = {
            'Do you see any of these rare features in the image?',
            'Do you see any of these rare features?',
        }
        return response['task_label'] in multiple_choice


def is_null_classification(response):
    return response['value'] is None


def clean_response(response, schema):
    """Clean a single response
    For response values: remove trailing spaces, set lowercase and remove markdown, remove null responses
    Rename question.raw_name to question.name and answer.raw_name to answer.name, for all questions and answers
    Note: the order of operation is important for response values to match the schema.
    
    Args:
        response (pd.Series): of form {time, user, subject, question, answer}
        schema (Schema):  definition object for questions and answers

    Returns:
        response (pd.Series): as above
    """
    if not_null_or_space_string(response['value']):
             # remove trailing spaces, lowercase, remove markdown
        response['value'] = sanitise_string(response['value'])

    # check again, because it might now be null
    if not_null_or_space_string(response['value']):
        response = rename_response(response, schema)
        return response

    # if we made the response null, return a None (filter outside, slightly awkward)
    return None


def not_null_or_space_string(x):
    if x is None:
        return False
    return bool(x.strip(' '))  # True if string has any characters


def rename_response(response, schema):
    """
    Rename all questions ('task') and answers ('value') according to schema
    If question or task is not found in schema, log critical failure but rely on external checks to raise Exception
    Response should have already had markdown removed, else will log critical failure

    Args:
        response (pd.Series): of form {'task': raw_question_name, 'value': raw_answer_value}
        schema (Schema): definition to use for renaming. Must include Question and Answer objects.

    Returns:
        (pd.Series) of form {'task': new_question_name, 'value': new_answer_value}
    """
    try:
        del response['task_label']
    except KeyError:
        pass

    try:
        question = schema.get_question_from_raw_name(response['task'])
    except IndexError:
        logging.critical('Question "{}" not found in schema'.format(response['task']))
        return None
    response['task'] = question.name

    try:
        answer = question.get_answer_from_raw_name(response['value'])
    except IndexError:
        logging.critical('Answer {} of type {} not found in schema for question {} '.format(
            response['value'], type(response['value']), response['task']))
        return None
    response['value'] = answer.name

    return response


def sanitise_string(string):
    """
    Remove ' and leading/trailing space, then remove any leading image markdown, make lowercase
    TODO ' removal is currently broken/untested
    Args:
        string (str): string to be edited

    Returns:
        (str) original string without leading/trailing space or leading markdown
    """
    if type(string) == str:
        clean_string = string.strip("'").lstrip(' ').rstrip(' ').lower()
        return remove_image_markdown(clean_string)
    else:
        logging.warning('{} is not string'.format(string))
        return string


def remove_image_markdown(string):
    """
    Remove leading image markdown by bracket detection
    Assumes string is in one of two forms:
        1) [name](url.png) some text after a space
        2) some text without markdown
    Args:
        string (str): string which may include leading image markdown

    Returns:
        (str) string with any leading image markdown removed
    """
    if string is None:
        return ''
    else:
        if '[' in string:  # might have markdown beginning string
            if '[' in string.split()[0]:  # first block is markdown image
                return string[list(string).index(')') + 1:].lstrip()  # first closing parenthesis ends md
        else:
            return string


def find_last_id_in_responses(responses_loc):
    os.environ['PYSPARK_PYTHON'] = sys.executable  # path to current interpreter
    appName = 'get_last_id'  # name to display
    master = 'local[*]'  # don't run on remote cluster. [*] indicates use all cores, [3] for 3 cores
    conf = SparkConf().setAppName(appName).setMaster(master) 
    sc = SparkContext(conf=conf)  # this tells spark how to access a cluster
    lines = sc.textFile(responses_loc)
    lines.max(lambda x: json.loads(x)['classification_id'])


if __name__ == '__main__':

    logging.basicConfig(
        filename='panoptes_to_responses.log',
        format='%(asctime)s %(message)s',
        filemode='w',
        level=logging.DEBUG)
    
    classification_dir = 'data/raw/classifications/api/derived'
    classification_locs = api_to_json.get_chunk_files(classification_dir)

    save_loc = settings.panoptes_flat_classifications
    # save_loc = 'data/temp'
    if os.path.isfile(save_loc):
        os.remove(save_loc)
    
    # make flat table of classifications. Basic use-agnostic view. Fast to append. Slow to run!
    flat_classifications = preprocess_classifications(
        classification_locs,
        dr5_schema,
        start_date=datetime(year=2018, month=3, day=15, tzinfo=timezone.utc),  # public launch
        save_loc=save_loc
    )
