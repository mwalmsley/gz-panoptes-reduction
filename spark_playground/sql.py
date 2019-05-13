import logging
import time
import datetime
import functools

from pyspark.sql import SparkSession

from pyspark.sql.functions import to_timestamp, to_date, lit, explode, udf, count
from pyspark.sql.types import TimestampType, BooleanType, StringType, IntegerType

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


def responses_to_reduced_votes(flat_df):

    # aggregate by creating question_response pairs, grouping, pivoting, and summing
    join_string_udf = udf(lambda x, y: x + '_' + y)
    flat_df = flat_df.withColumn('question_response', join_string_udf('question', 'response'))
    df = flat_df.groupBy('subject_id').pivot('question_response').agg(count('question_response'))
    df = df.na.fill(0)

    # add any missing question_response
    initial_cols = df.schema.names
    for col in dr5_schema.get_count_columns():
        if col not in initial_cols:  # not created in pivot as no examples given
            df = df.withColumn(col, lit(0))  # create, fill with 0's
    
    for col in dr5_schema.get_count_columns():
        assert col in df.schema.names

    # calculate total responses per question
    for question in dr5_schema.questions:
        df = df.withColumn(
            question.total_votes, 
            functools.reduce(lambda x, y: x + y, [df[col] for col in question.get_count_columns()])
        )

    # df_loc = 'temp_agg.csv'

    # if fits in memory: 
    # df.repartition(1).write.csv(path=df_loc, mode="append", header="true")
    # df.toPandas().to_csv(df_loc)

    # if doesn't fit in memory:
    # df.write.save(
    #     df_loc,
    #     mode='overwrite',
    #     format='csv')

    return df




if __name__ == '__main__':

    spark = SparkSession \
        .builder \
        .appName("hello") \
        .getOrCreate()

    # infer schema from existing file
    tiny_loc = "data/raw/classifications/api/derived_panoptes_api_first_156988066_last_157128147.txt"
    schema = spark.read.json(tiny_loc).schema
    # df = spark.read.json("data/raw/classifications/api/derived_panoptes_api_first_157128147_last_157128227.txt")


    # streaming mode

    df = spark.readStream.json('data/streaming/derived_output', schema=schema)
    flat_df = api_df_to_responses(df)

    query = flat_df.writeStream \
        .outputMode('append') \
        .option('checkpointLocation', 'data/streaming/flat_checkpoints') \
        .start(path='data/streaming/flat_output', format='json')
    
    while True:
        time.sleep(0.1)
        if query.status['isDataAvailable']:
            print(datetime.datetime.now(), query.status['message'])

    spark.streams.awaitAnyTermination()

    # batch mode


    # aggregated_df = responses_to_reduced_votes(flat_df)
    # flat_df.awaitTermination()