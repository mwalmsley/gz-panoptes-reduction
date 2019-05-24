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


def run(raw_dir, output_dir, workflow_ids, spark=None, mode='stream'):
    assert isinstance(workflow_ids, list)
    if not spark:
        spark = SparkSession \
        .builder \
        .appName("subjects") \
        .getOrCreate()

    if mode == 'stream':
        # infer schema from existing file (copied from derived)
        tiny_loc = os.path.dirname(os.path.realpath(__file__)) + "/../../data/examples/panoptes_raw.txt"
        print(tiny_loc)
        assert os.path.exists(tiny_loc)
        schema = spark.read.json(tiny_loc).schema
        raw_df = spark.readStream.json(raw_dir, schema=schema)
    else:
        raw_df = spark.read.json(raw_dir)
    
    df = extract_subjects(raw_df, workflow_ids)

    if mode == 'stream':
        df.writeStream.outputMode('append') \
        .option('checkpointLocation', os.path.join(output_dir, 'checkpoints')) \
        .trigger(processingTime='3 seconds') \
        .start(path=output_dir, format='json')
    else:
        df.write.json(output_dir, mode='overwrite')

def extract_subjects(df, workflow_ids):
    df = df.filter(df['links']['workflow'].isin(workflow_ids))

    subject_df = df.select(
        df['links']['subject']['locations'][0]['image/png'].alias('subject_url'),
        df['links']['subject']['metadata']['!iauname'].alias('iauname'),
        df['links']['subject']['id'].alias('subject_id'))

    # subjects will repeat
    subject_df = subject_df.drop_duplicates(subset=['subject_id'])  # unlike pandas, will keep one row only

    return subject_df


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

    run(raw_dir, save_dir, workflow_ids=['6122'])
