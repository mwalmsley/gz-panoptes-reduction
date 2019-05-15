import logging
import os
import time
import datetime

from pyspark.sql import SparkSession

from pyspark.sql.functions import to_timestamp, to_date, lit, explode, udf
from pyspark.sql.types import TimestampType, StringType

from gzreduction.panoptes.panoptes_to_responses import sanitise_string
from gzreduction.schemas.dr5_schema import dr5_schema


def get_question(task_id, schema):
    try:
        question = schema.get_question_from_raw_name(task_id)
    except IndexError:
        logging.critical('Question "{}" not found in schema'.format(task_id))
        return None
    return question.name

def get_answer(task_id, cleaned_response, schema):
    try:
        question = schema.get_question_from_raw_name(task_id)
        answer = question.get_answer_from_raw_name(cleaned_response)
    except IndexError:
        logging.critical('Answer {} of type {} not found in schema for question {} '.format(
            cleaned_response, type(cleaned_response), task_id))
        return None
    return answer.name


sanitise_string_udf = udf(lambda x: sanitise_string(x), returnType=StringType())  
get_question_udf = udf(lambda x: get_question(x, dr5_schema), returnType=StringType())
get_answer_udf = udf(lambda x, y: get_answer(x, y, dr5_schema), returnType=StringType())


def api_df_to_responses(df):

    df = df.withColumn('created_at', to_timestamp(df['created_at']))

    start_date = to_date(lit('2018-03-20')).cast(TimestampType())  # lit means 'column of literal value' i.e. dummy column of that everywhere
    df = df.filter(df['created_at'] > start_date)

    exploded = df.select(
        explode('annotations').alias('annotations_struct'),
        'created_at',
        'user_id',
        'subject_id',
        'classification_id'
    )
    flattened = exploded.select(
        'annotations_struct.*',
        'created_at',
        'user_id',
        'subject_id',
        'classification_id'
        )

    # filter for multiple choice and None
    flattened = flattened.filter(flattened['multiple_choice'] == False) # can only compare without lit() when calling directly e.g. df[x]    
    flattened = flattened.filter(flattened['value'] != 'No')

    # https://docs.databricks.com/spark/latest/spark-sql/udf-python.html
    flattened = flattened.withColumn('cleaned_response', sanitise_string_udf(flattened['value']))
    # TODO filter again to avoid nulls?
    flattened = flattened.withColumn(
        'question',
        get_question_udf(
            flattened['task_id']
        )
    )
    flattened = flattened.withColumn(
        'response',
        get_answer_udf(
            flattened['task_id'],
            flattened['cleaned_response']
        )
    )

    flat_view = flattened.select(
        'created_at', 'user_id', 'subject_id', 'classification_id', 'question', 'response'
    )
    # flat_view.show()
    # https://spark.apache.org/docs/latest/api/python/pyspark.sql.html?highlight=save#pyspark.sql.DataFrameWriter
    # flat_view.write.save(
    #     'temp_flat.csv', 
    #     mode='overwrite',
    #     format='csv')
    return flat_view


def stream(input_dir, output_dir, print_status=False, spark=None):
    if not spark:
        spark = SparkSession \
        .builder \
        .appName("shared") \
        .getOrCreate()

    # infer schema from existing file
    example_loc = "data/raw/classifications/api/derived_panoptes_api_first_156988066_last_157128147.txt"
    schema = spark.read.json(example_loc).schema

    df = spark.readStream.json(input_dir, schema=schema)
    flat_df = api_df_to_responses(df)

    query = flat_df.writeStream \
        .outputMode('append') \
        .option('checkpointLocation', os.path.join(output_dir, 'checkpoints')) \
        .start(path=output_dir, format='json')
    
    if print_status:
        while True:
            time.sleep(0.1)
            if query.status['isDataAvailable']:
                print(datetime.datetime.now(), query.status['message'])

    return flat_df  # streaming dataframe - this one is not started! TODO split save/ontinue?

if __name__ == '__main__':

    stream(
        input_dir='data/streaming/derived_output', 
        output_dir='data/streaming/flat_output'
    )
