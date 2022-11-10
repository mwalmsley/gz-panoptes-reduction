import logging
# import itertools
import os
import time
import datetime
# import uuid
import zlib
import argparse

from pyspark.sql import SparkSession

from pyspark.sql.functions import to_timestamp, to_date, lit, explode, udf, lit
from pyspark.sql.types import TimestampType, StringType

from gzreduction.panoptes_api.extract.spark.panoptes_to_responses import sanitise_string
from gzreduction.schemas.dr5_schema import dr5_schema


def get_question(task_id, schema):
    try:
        question = schema.get_question_from_raw_name(task_id)
    except IndexError:
        raise ValueError('Question "{}" not found in schema'.format(task_id))
        return None
    return question.name


def get_answer(task_id, cleaned_response, schema):
    try:
        question = schema.get_question_from_raw_name(task_id)
        answer = question.get_answer_from_raw_name(cleaned_response)
    except IndexError:
        raise ValueError('Answer {} of type {} not found in schema for question {} '.format(
            cleaned_response, type(cleaned_response), task_id))
        return None
    return answer.name


sanitise_string_udf = udf(lambda x: sanitise_string(x), returnType=StringType())  
get_question_udf = udf(lambda x: get_question(x, dr5_schema), returnType=StringType())
get_answer_udf = udf(lambda x, y: get_answer(x, y, dr5_schema), returnType=StringType())


def api_df_to_responses(df):
    
    df.printSchema()

    df = df.withColumn('created_at', to_timestamp(df['created_at']))
    start_date_str = '2018-03-20'
    start_date = to_date(lit(start_date_str)).cast(TimestampType())  # lit means 'column of literal value' i.e. dummy column of that everywhere

    print(f'Before applying filters: {df.count()}')
    df = df.filter(df['created_at'] > start_date)  # requires pyspark 3.0+ or fails silently, removing all rows...
    print(f'Classifications after {start_date_str}: {df.count()}')

    cols_to_preserve = [
        'created_at',
        'workflow_id',
        'workflow_version',
        'user_id',
        'person_id',
        'subject_id',
        'iauname',
        'classification_id'
    ]

    # make sure these are cast to String, not ints
    for col in cols_to_preserve:
        if col is not 'created_at':
            df = df.withColumn(
                col,
                df[col].cast(StringType())
            )

    # print('Exploding annotations')
    exploded = df.select(
        explode('annotations').alias('annotations_struct'),
        *cols_to_preserve
    )
    # df.printSchema()
    print('Selecting exploded cols + other cols to preserve')
    flattened = exploded.select(
        'annotations_struct.*',
        *cols_to_preserve
        )

    # print(f'Total responses: {flattened.count()}')

    flattened = flattened.dropna(how='any', subset=['task_id', 'value'])
    # print(f'Excluding blank question/answers: {flattened.count()}')

    flattened = flattened.filter(flattened['multiple_choice'] == False) # can only compare without lit() when calling directly e.g. df[x]    
    print(f'Excluding multiple choice answers: {flattened.count()}')

    # https://docs.databricks.com/spark/latest/spark-sql/udf-python.html
    flattened = flattened.withColumn('cleaned_response', sanitise_string_udf(flattened['value']))

    flattened = flattened.withColumn(
        'question',
        get_question_udf(
            flattened['task_id']
        )
    )

    print(flattened['workflow_version'])

    workflow_version_with_latest_merging_q = 66.425
    flattened = flattened.filter(
        # keep if: not merging q, or not old version
        (~(flattened['question'] == 'merging')) | (flattened['workflow_version'] >= workflow_version_with_latest_merging_q)  
    )
    print(f'Excluding responses to old (before {workflow_version_with_latest_merging_q}) merging question: {flattened.count()}')


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
    flat_view = flat_view.withColumn(
        'response_id',
        get_unique_int_from_three_strings_udf(flat_view['classification_id'], flat_view['question'], flat_view['response'])
    )

    return flat_view


def get_unique_int_from_three_strings(x: str, y: str, z: str):
    if x is None:
        x = 'error_x'
    if y is None:
        y = 'error_y'
    if z is None:
        z = 'error_z'
    concat = x + y + z
    return zlib.adler32(concat.encode())
get_unique_int_from_three_strings_udf = udf(get_unique_int_from_three_strings)

# def get_uuid_from_str(x: str):
#     if not isinstance(x, str):
#         raise TypeError('Expected str, got {} ({})'.format(x, type(x)))
#     return uuid.uuid5(uuid.NAMESPACE_DNS, x)
# 

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


def batch(input_dir, output_dir, spark=None):
    if not spark:
        spark = SparkSession \
        .builder \
        .appName("shared") \
        .getOrCreate()

    # if os.path.isdir(output_dir)
    #     shutil.rmtree(output_dir)
    this_dir = os.path.dirname(os.path.realpath(__file__))
    # infer schema from existing file
    example_loc = os.path.join(this_dir, '../data/examples/panoptes_derived.json')
    assert os.path.exists(example_loc)

    
    schema = spark.read.json(example_loc).schema
    
    df = spark.read.json(input_dir, schema=schema)
    df.show()
    # print(df.count())
    flat_df = api_df_to_responses(df)
    flat_df.write.json(output_dir)


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input',
        dest='input_dir',
        type=str,
        default='temp/raw'
    )
    parser.add_argument(
        '--save-dir',
        dest='save_dir',
        type=str,
        default='temp/flat/latest_batch_export'
    )
    args = parser.parse_args()

    # Spark requires Java 8, not 11. Update JAVA_HOME accordingly (will vary by system)
    os.environ['JAVA_HOME'] = '/usr/lib/jvm/java-8-openjdk-amd64'

    # stream(
    #     input_dir='temp/derived', 
    #     output_dir='temp/flat',
    #     print_status=True
    # )
    
    batch(
        input_dir=args.input_dir, 
        output_dir=args.save_dir
    )