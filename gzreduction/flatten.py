import logging
# import itertools
import os
import time
import datetime
import uuid

from pyspark.sql import SparkSession

from pyspark.sql.functions import to_timestamp, to_date, lit, explode, udf, lit
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
    df = df.filter(df['created_at'] > start_date)  # requires pyspark > 2.2 or fails silently, removing all rows...

    cols_to_preserve = [
        'created_at',
        'workflow_id',
        'user_id',
        'subject_id',
        'classification_id'
    ]

    exploded = df.select(
        explode('annotations').alias('annotations_struct'),
        *cols_to_preserve
    )
    flattened = exploded.select(
        'annotations_struct.*',
        *cols_to_preserve
        )

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

    flat_view = flattened.select(*cols_to_preserve, 'question', 'response')

    # TODO this doesn't work because it wasn't run at first, so state is not saved - needs full re-run
    # flat_view = flat_view.drop_duplicates()  # on all columns

    # define unique id for each question-response event

    # flat_view = flat_view.withColumn('hello', lit('world'))
    # flat_view = flat_view.withColumn('hello_again', lit('world'))
    # flat_view = flat_view.withColumn(
    #     'hello_thrice',
    #     sum(
    #         [flat_view[col] for col in ['hello', 'hello_again']]
    #         )
    #     )
    # flat_view = flat_view.withColumn('hello_twice', flat_view['hello'] + flat_view['hello_again'])

    # flat_view = flat_view.withColumn(
    #     'response_id_str',
    #     flat_view['classification_id'] + flat_view['question'] + flat_view['response']
    # )
    # flat_view = flat_view.withColumn(
    #     'response_id',
    #     udf(get_uuid_from_str)(df['response_id_str'])
    # )
    # flat_view.drop('response_id_str')

    return flat_view
# 34126

def get_uuid_from_str(x: str):
    if not isinstance(x, str):
        raise TypeError('Expected str, got {} ({})'.format(x, type(x)))
    return uuid.uuid5(uuid.NAMESPACE_DNS, x)
# zlib.adler32('hello'.encode())

# def get_uuid_from_columns(*cols):
#     return get_uuid_from_str(reduce(lambda x, y: x + y, cols))

def stream(input_dir, output_dir, print_status=False, spark=None):
    if not spark:
        spark = SparkSession \
        .builder \
        .appName("shared") \
        .getOrCreate()

    this_dir = os.path.dirname(os.path.realpath(__file__))
    # infer schema from existing file
    example_loc = os.path.join(this_dir, '../data/examples/panoptes_derived.json')
    assert os.path.exists(example_loc)
    schema = spark.read.json(example_loc).schema

    df = spark.readStream.json(input_dir, schema=schema)
    flat_df = api_df_to_responses(df)

    query = flat_df.writeStream \
        .outputMode('append') \
        .option('checkpointLocation', os.path.join(output_dir, 'checkpoints')) \
        .trigger(processingTime='3 seconds') \
        .start(path=output_dir, format='json')
    
    if print_status:
        while True:
            time.sleep(0.2)
            if query.status['isDataAvailable']:
                print(datetime.datetime.now(), query.status['message'])

if __name__ == '__main__':

    stream(
        input_dir='temp/derived', 
        output_dir='temp/flat',
        print_status=True
    )
