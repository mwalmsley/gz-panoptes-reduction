import logging
import os
import functools
import argparse

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, udf, count
from pyspark.sql.types import ArrayType, MapType, FloatType, StructType, StructField, BooleanType, StringType, IntegerType, TimestampType

from gzreduction.panoptes_api.extract.spark.panoptes_to_responses import sanitise_string
from gzreduction.schemas.dr5_schema import dr5_schema


def responses_to_reduced_votes(flat_df, drop_people=[]):
    """Aggregate flat (subject, classification, question, answer) table into total votes etc by subject_id
    Optionally, drop classifications from person_id in 'drop_people' list

    Args:
        flat_df ([type]): [description]
        drop_people ([type], optional): [description]. Defaults to None.

    Returns:
        [type]: [description]
    """

    # should already be done but just in case
    flat_df = flat_df.drop_duplicates(subset=['classification_id', 'question', 'response'])

    if drop_people:
        print(f'Removing {len(drop_people)} people. Before: {flat_df.count()} responses')
        flat_df = flat_df.filter(~flat_df['person_id'].isin(drop_people))  # important ~
        print(f'After: {flat_df.count()} responses')

    # aggregate by creating question_response pairs, grouping, pivoting, and summing
    join_string_udf = udf(lambda x, y: x + '_' + y)
    flat_df = flat_df.withColumn('question_response', join_string_udf('question', 'response'))
    df = flat_df.groupBy('iauname').pivot('question_response').agg(count('question_response'))  # switched to iauname
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


def run(input_dir, drop_people=[], spark=None):

    if not spark:
        spark = SparkSession \
        .builder \
        .appName("aggregate") \
        .getOrCreate()

    # if not [x for x in os.listdir(input_dir) if x.endswith('json')]:
    #     raise ValueError('No JSON files found to be aggregated - check flat pipeline output?')

    # print('Reading flattened responses')
    # this_dir = os.path.dirname(os.path.realpath(__file__))
    # infer schema from existing file
    # example_loc = os.path.join(this_dir, '../data/examples/panoptes_flat.json')
    # assert os.path.exists(example_loc)
    # # schema = spark.read.json(example_loc).schema

    # flat_df = spark.read.json(input_dir, schema=schema)

    flat_df = spark.read.parquet(input_dir)

    print('Total responses: {}'.format(flat_df.count()))

    print('Reducing responses')
    df = responses_to_reduced_votes(flat_df, drop_people=drop_people)
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
        # default='temp/flat/latest_batch_export'
        default='temp/flat_all' # WARNING needs manual copy!
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
    parser.add_argument(
        '--drop-people',
        dest='drop_people_loc',
        type=str,
        default='~/repos/zoobot/notebooks/catalogs/unlikely_users_dr5.csv'
    )
    args = parser.parse_args()

    if args.drop_people_loc.lower() != 'none':
        drop_people = list(pd.read_csv(args.drop_people_loc)['computer_id'])
    else:
        drop_people = []
    
    df = run(input_dir=args.input_dir, drop_people=drop_people)
    df = df.toPandas()
    # df['subject_id'] = df['subject_id'].astype(int)
    # print(df['subject_id'])
    df.to_parquet(args.save_loc, index=False)
    logging.info(f'Aggregation saved, {len(df)}')

    # if not args.subjects_loc == '':
    #     # # will now switch to pandas in-memory
    #     subject_df = pd.read_csv(args.subjects_loc)
    #     import numpy as np
    #     import json
    #     def get_iauname(metadata):
    #         metadata = json.loads(metadata)
    #         possible_keys = ['iauname', '!iauname', 'IAUNAME', '!IAUNAME']
    #         for key in possible_keys:
    #             if key in metadata.keys():
    #                 return metadata[key]
    #         return np.nan

    #     subject_df['iauname'] = subject_df['metadata'].apply(get_iauname)
        
    #     before = len(df)
    #     df = pd.merge(df, subject_df, on='subject_id', how='inner')
    #     logging.info(f'Found {len(df)} of {before} matching subject ids')
    #     df.to_csv(os.path.join(os.path.dirname(args.save_loc), 'classifications.csv'), index=False)
