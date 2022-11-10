
import logging
import os
import io
import sys
import json
from datetime import datetime, timezone
import shutil

import pandas as pd
from pyspark import SparkContext, SparkConf

from gzreduction.deprecated import settings
from gzreduction.schemas.dr5_schema import dr5_schema
from gzreduction.panoptes_api.api import api_to_json


def start_spark(app_name):
    os.environ['PYSPARK_PYTHON'] = sys.executable  # path to current interpreter
    appName = app_name  # name to display
    if os.path.isdir('/data/repos'):
        master = 'local[3]'  # local desktop, 3 cores so I can still work
    else:
        master = 'local[*]' # ec2, [*] indicates use all cores.
    conf = SparkConf().setAppName(appName).setMaster(master) 
    return SparkContext(conf=conf)  # this tells spark how to access a cluster


def preprocess_classifications(
        classifications_locs,
        schema,
        start_date=None,
        save_dir=None):
    """
    Convert a raw Panoptes classification extract into flattened and cleaned responses

    Args:
        classification_locs (list): of text files with json rows, where each row is a classification
        schema (Schema):  definition object for questions and answers
        start_date (pd.datetime): (optional) if not None, filter out Panoptes classifications before this date
        save_dir (str): (optional) if not None, save text file of responses to this location

    Returns:
        (Spark RDD) rows=responses, columns=question/answer/user/time
    """
    sc = start_spark('preprocess_classifications')

    # load all classifications as RDD
    assert isinstance(classifications_locs, list)
    classification_chunks = [sc.textFile(loc) for loc in classifications_locs]
    if not classification_chunks:
        raise ValueError('No chunks found in {}'.format(classifications_locs))
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

    if save_dir is not None:
        if os.path.isdir(save_dir):
            shutil.rmtree(save_dir)
        final_responses.map(lambda x: response_to_line(x)).saveAsTextFile(save_dir)
        logging.info('Saved Panoptes responses to {}'.format(save_dir))

    sc.stop()

    return final_responses  # still an rdd, not collected


def response_to_line_header():
    return [
        'classification_id',
        'created_at',
        'user_id',
        'subject_id',
        # 'iauname',
        # 'subject_url',
        'task',
        'value'
    ]
# also need to update columns_to_copy, if passing these values through

def response_to_line(r, header=response_to_line_header()):
    """Convert response to csv-style format, for later translation to pandas
    
    Args:
        r ([type]): [description]
    
    Returns:
        [type]: [description]
    """
    data = ['{}'.format(r[col]) for col in header]
    return ','.join(data)




if __name__ == '__main__':

    logging.basicConfig(
        filename='panoptes_to_responses.log',
        format='%(asctime)s %(message)s',
        filemode='w',
        level=logging.DEBUG)
    
    classification_dir = 'data/raw/classifications/api'
    classification_locs = api_to_json.get_chunk_files(classification_dir, derived=True)
    assert classification_locs  # must not be empty

    save_dir = settings.panoptes_flat_classifications
    # save_dir = 'data/temp'
    if os.path.isfile(save_dir):
        os.remove(save_dir)
    
    # make flat table of classifications. Basic use-agnostic view. Fast to append. Slow to run!
    flat_classifications = preprocess_classifications(
        classification_locs,
        dr5_schema,
        start_date=datetime(year=2018, month=3, day=15),  # public launch  tzinfo=timezone.utc
        save_dir=save_dir
    )
