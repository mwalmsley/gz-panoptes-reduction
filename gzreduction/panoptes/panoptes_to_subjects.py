import logging
import os
import shutil
import json
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, to_date, lit, explode, udf, count
from pyspark.sql.types import TimestampType, BooleanType, StringType, IntegerType

from gzreduction.panoptes.api import api_to_json
from gzreduction.panoptes import panoptes_to_responses, responses_to_votes


def run(raw_dir, workflow_id, spark=None):
    if not spark:
        spark = SparkSession \
        .builder \
        .appName("subjects") \
        .getOrCreate()

    # if not [x for x in os.listdir(raw_dir) if x.endswith('json')]:
    #     raise ValueError('No JSON files found to be read - check derived pipeline output?')
    raw_df = spark.read.json(raw_dir)
    return extract_subjects(raw_df, workflow_id)

def extract_subjects(df, workflow_id):
    df = df.filter(df['links']['workflow'] == workflow_id)
    # classifications = lines.map(lambda x: panoptes_to_responses.load_classification_line(x))
    # classifications_after_start = classifications.filter(lambda x: x['created_at'] >= start_date)
    # raw_subjects = classifications_after_start.map(lambda x: x['links']['subject'])
    # # may not be necessary with date filter
    # not_manga = raw_subjects.filter(lambda x: '!MANGAID' not in x['metadata'].keys())
    # subjects = not_manga.map(lambda x: get_subject(x))
    # output_lines = subjects.map(lambda x: panoptes_to_responses.response_to_line(x, header=header()))
    # output_lines.saveAsTextFile(save_dir)

    # df.printSchema()

    subject_df = df.select(
        df['links']['subject']['locations'][0]['image/png'].alias('subject_url'),
        df['links']['subject']['metadata']['!iauname'].alias('iauname'),
        df['links']['subject']['id'].alias('subject_id'))

    # subjects will repeat
    return subject_df.toPandas().drop_duplicates(subset=['subject_id'], keep='first')  # spark has no keep=first option :( due to sharding



# def get_subjects(df):

#     # metadata_keys = raw_subject['metadata'].keys()
#     # iauname_keys = ['iauname', '!iauname']
#     # if not any(key in metadata_keys for key in iauname_keys):
#     #     raise ValueError(raw_subject)
#     # for key in iauname_keys:
#     #     if key in metadata_keys:
#     #         iauname = raw_subject['metadata'][key]
#     #         break

#     df = df.select(
#         df['subject']['locations'][0]['image/png'].alias('subject_url'),
#         df['subject']['metadata']['iauname'].alias('iauname'),
#         df['subject']['id'].alias('subject_id'))

#     df.show()

#     return df


def header():
    return ['subject_id', 'iauname', 'subject_url']
    # return ['subject_id', 'iauname', 'subject_url', 'metadata']

if __name__ == '__main__':

    logging.basicConfig(
        filename='panoptes_to_subjects.log',
        format='%(asctime)s %(message)s',
        filemode='w',
        level=logging.DEBUG)

    # raw_dir = 'data/streaming/derived_output'
    # save_dir = 'data/streaming/subjects'
    raw_dir = 'temp/raw'
    save_dir = 'temp/subjects'
    if os.path.isdir(save_dir):
        shutil.rmtree(save_dir)

    df = run(raw_dir, save_dir, workflow_id='6122')
    print(df.head())

