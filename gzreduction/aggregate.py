import logging
import os
import functools
import argparse

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, udf, count

from gzreduction.panoptes.panoptes_to_responses import sanitise_string
from gzreduction.schemas.dr5_schema import dr5_schema


def responses_to_reduced_votes(flat_df):

    # should already be done but just in case
    flat_df = flat_df.drop_duplicates(subset=['classification_id', 'question', 'response'])

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

    print('Reading flattened responses')
    this_dir = os.path.dirname(os.path.realpath(__file__))
    # infer schema from existing file
    example_loc = os.path.join(this_dir, '../data/examples/panoptes_flat.json')
    assert os.path.exists(example_loc)
    schema = spark.read.json(example_loc).schema

    flat_df = spark.read.json(input_dir, schema=schema)
    print('Total responses: {}'.format(flat_df.count()))

    print('Reducing responses')
    df = responses_to_reduced_votes(flat_df)
    print('Classified galaxies: {}'.format(df.count()))


    print('Repartitoning from {} to 1'.format(df.rdd.getNumPartitions()))
    df = df.repartition(1)
    print('Repartition complete')
    return df


if __name__ == '__main__':

    # Spark requires Java 8, not 11. Update JAVA_HOME accordingly (will vary by system)
    os.environ['JAVA_HOME'] = '/usr/lib/jvm/java-8-openjdk-amd64'

    logging.basicConfig(
        level=logging.INFO
    )

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input',
        dest='input_dir',
        type=str,
        default='temp/flat/latest_batch_export'
    )
    parser.add_argument(
        '--save-loc',
        dest='save_loc',
        type=str,
        default='temp/latest_aggregated.parquet'
    )
    parser.add_argument(
        '--subjects',
        dest='subjects_loc',
        type=str,
        default=''
    )
    args = parser.parse_args()
    
    df = run(input_dir=args.input_dir)
    df = df.toPandas()
    df['subject_id'] = df['subject_id'].astype(int)
    # print(df['subject_id'])
    df.to_parquet(args.save_loc, index=False)
    logging.info(f'Aggregation saved, {len(df)}')

    if not args.subjects_loc == '':
        # # will now switch to pandas in-memory
        subject_df = pd.read_parquet(args.subjects_loc)
        # print(subject_df['subject_id'])
        before = len(df)
        df = pd.merge(df, subject_df, on='subject_id', how='inner')
        logging.info(f'Found {len(df)} of {before} matching subject ids')
        df.to_csv(os.path.join(os.path.dirname(args.save_loc), 'classifications.csv'), index=False)

    