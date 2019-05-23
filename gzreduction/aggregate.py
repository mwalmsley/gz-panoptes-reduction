import logging
import os
import time
import datetime
import functools

from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, to_date, lit, explode, udf, count
from pyspark.sql.types import TimestampType, BooleanType, StringType, IntegerType

from gzreduction.panoptes.panoptes_to_responses import sanitise_string
from gzreduction.schemas.dr5_schema import dr5_schema

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

    # calculate fractions per answer
    for question in dr5_schema.questions:
        for answer in question.answers:
            df = df.withColumn(
                question.get_fraction_column(answer),  # TODO should refactor this to answer.fraction etc in schema?
                df[question.get_count_column(answer)] / df[question.total_votes]  # may give nans?
            )
        # df = df.fillna(0)  # some total vote columns will be 0, but this doesn't seem to cause na?

    return df


def run(input_dir, spark=None):

    if not spark:
        spark = SparkSession \
        .builder \
        .appName("aggregate") \
        .getOrCreate()

    if not [x for x in os.listdir(input_dir) if x.endswith('json')]:
        raise ValueError('No JSON files found to be aggregated - check flat pipeline output?')

    print('Reading flat data')
    flat_df = spark.read.json(input_dir)

    print('Reducing votes')
    df = responses_to_reduced_votes(flat_df)

    print('Repartitoning from {}'.format(df.rdd.partitions.size))

    df = df.repartition(1)

    print('sending to pandas')
    return df.toPandas()


