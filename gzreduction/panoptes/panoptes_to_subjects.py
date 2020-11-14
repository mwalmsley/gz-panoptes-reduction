import logging
import os
import shutil

from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, to_date, lit, explode, udf, count
from pyspark.sql.types import TimestampType, BooleanType, StringType, IntegerType

# this is designed to work on the raw API data
# use panoptes_export_to_subjects.py for batch-mode export analysis
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
    """Get key subject details from df of raw classification responses.
    Remember that if you want to change columns here, 
    You need to re-run in batch mode to regenerate all subjects according to the new columns.
    (see __main__ to do so)
    
    Args:
        df ([type]): raw classification responses
        workflow_ids ([type]): filter to these workflow ids
    
    Returns:
        [type]: subject data read from raw responses, only including only non-astro columns and keys
    """
    df = df.filter(df['links']['workflow'].isin(workflow_ids))

    subject_df = df.select(
        # external keys
        df['links']['subject']['metadata']['!iauname'].alias('iauname'),  # to link to astro. catalogs
        df['links']['subject']['id'].alias('subject_id'),  # to link to classifications
        # panoptes-only metadata (may be null)
        df['links']['subject']['locations'][0]['image/png'].alias('subject_url'),  # to view easily
        df['links']['subject']['metadata']['#uploader'].alias('uploader'),
        df['links']['subject']['metadata']['#upload_date'].alias('upload_date'),
        df['links']['subject']['metadata']['#retirement_limit'].alias('retirement_limit')
    )

    # subjects will repeat
    subject_df = subject_df.drop_duplicates(subset=['subject_id'])  # unlike pandas, will keep one row only

    return subject_df


if __name__ == '__main__':

    logging.basicConfig(
        filename='panoptes_to_subjects.log',
        format='%(asctime)s %(message)s',
        filemode='w',
        level=logging.DEBUG)


    raw_dir = '../zoobot/data/decals/classifications/streaming/raw'
    save_dir = '../zoobot/data/decals/classifications/streaming/subjects'
    # raw_dir = 'temp/raw'
    # save_dir = 'temp/subjects'
    if os.path.isdir(save_dir):
        shutil.rmtree(save_dir)

    # mode='stream'
    mode='not_stream'
    run(raw_dir, save_dir, workflow_ids=['6122', '10581', '10582'], mode=mode)
