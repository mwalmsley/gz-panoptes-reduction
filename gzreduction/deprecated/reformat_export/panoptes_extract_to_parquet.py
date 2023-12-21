import json
import os
import argparse

import pandas as pd
from tqdm import tqdm

from gzreduction.panoptes_api.api import api_to_json
from gzreduction.ouroborous import settings

from pyspark.sql import SparkSession


from pyspark.sql.functions import udf, lit
from pyspark.sql.types import ArrayType, MapType, FloatType, StructType, StructField, BooleanType, StringType, IntegerType, TimestampType

from gzreduction.deprecated.spark import flatten


def get_person_id(user_id, user_ip):
    if not pd.isna(user_id): 
        return str(int(user_id))
    if not pd.isna(user_ip):
        return 'ip_' + user_ip
    else:
        return None  # explicit


def annotation_to_struct(x):
    annotations = json.loads(x)
    for annotation in annotations:  # modify by reference
    # once derived workflow data is added, api questions look like this
    # {"task":"T0", "task_label": "Is the galaxy simply smooth and rounded, with no sign of a disk?", "value":"![features_or_disk_new.png](https://panoptes-uploads.zooniverse.org/production/project_attached_image/1ec52a74-9e49-4579-91ff-0140eb5371e6.png =60x) Features or Disk","task_id":"T0","value_index":"1","multiple_choice":false}
    # but panoptes looks like
    # {"task": "T0", "task_label": "Is the galaxy simply smooth and rounded, with no sign of a disk?", "value": "![smooth_triple_flat_new.png](https://panoptes-uploads.zooniverse.org/production/project_attached_image/75cec1ed-f50a-4151-8877-f6e74f6748eb.png =60x) Smooth"
    # add the 'multiple choice' flag that flatten.py uses to filter
    # annotations = json.loads(row['annotations'])
        annotation['task_id'] = annotation['task']  # flatten uses this convention for T0 etc
        annotation['multiple_choice'] = annotation["task_label"] == "Do you see any of these rare features?"
    return annotations


def subject_data_str_to_iauname(x):
    subject_data = list(json.loads(x).values())[0]
    possible_keys = ['iauname', '!iauname', 'IAUNAME', '!IAUNAME']
    for key in possible_keys:
        if key in subject_data.keys():
            return subject_data[key]
    return None  # explicit


def metadata_str_to_struct(x):
    data = json.loads(x)
    # convert time fields
    for time_field in ['started_at', 'finished_at']:
        data[time_field] = pd.to_datetime(data[time_field])
    return data


def convert_extract_to_parquet(extract_loc, save_dir, spark=None):

    if not spark:
        spark = SparkSession \
        .builder \
        .appName("shared") \
        .getOrCreate()

    # https://spark.apache.org/docs/latest/api/python/pyspark.sql.html?highlight=read%20csv
    # ignore (escape) " if already within quotes to avoid splitting by , within the columnwise jsons

    # NullPointerException if you try to access something you promised would never be null

    # cannot read nested structs straight from CSV, sadly, so will parse columns individually
    metadata_struct = StructType([
        StructField('source', StringType(), False),
        StructField('session', StringType(), False),
        StructField('viewport', StructType([
            StructField('width', StringType(), False),
            StructField('height', StringType(), False)
        ]), False
        ),
        StructField('started_at', TimestampType(), False),
        StructField('user_agent', StringType(), False),
        StructField('utc_offset', StringType(), False),
        StructField('finished_at', TimestampType(), False),
        StructField('live_project', BooleanType(), False),
        StructField('interventions', StringType(), False),  # actually struct
        StructField('user_language', StringType(), False),
        StructField('source', StringType(), False),
        StructField('subject_dimensions', StringType(), False),  # actually struct
        StructField('subject_selection_state', StringType(), False),  # actually struct
        StructField('workflow_translation_id', StringType(), True),  # actually struct, sometimes null
    ])

    # TODO answer (at the very least) is very occasionally null, and this causes either EOF/Null pointer (if not nullable) or raise error like
    # ValueError: Answer None of type <class 'NoneType'> not found in schema for question T0
    # should filter out tasks with missing keys for these
    annotations_struct = ArrayType(StructType([
        StructField('task', StringType(), True),
        StructField('task_id', StringType(), True),
        StructField('task_label', StringType(), True),
        StructField('value', StringType(), True),
        StructField('multiple_choice', BooleanType(), True),
    ]))

    # subject_data_internal_struct = StructType(
    #     # StructField('!iauname', StringType(), True),
    #     # StructField('iauname', StringType(), True)
    # )
    # subject_data_struct = ArrayType(MapType(StringType(), subject_data_internal_struct))

    schema = StructType([
        StructField('classification_id', StringType(), False),
        StructField('user_name', StringType(), True),
        StructField('user_id', StringType(), True),
        StructField('user_ip', StringType(), True),
        StructField('workflow_id', StringType(), False),
        StructField('workflow_name', StringType(),False),
        StructField('workflow_version', FloatType(), False),
        StructField('created_at', StringType(), False),
        StructField('gold_standard' ,StringType(), False),
        StructField('expert', StringType(), False),
        StructField('metadata', StringType(), False),
        StructField('annotations', StringType(), False),
        StructField('subject_data', StringType(), False),
        StructField('subject_ids', StringType(),False)
    ])




    # schema = StructType([
    #     StructField('name', StructType([
    #          StructField('firstname', StringType(), True),
    #          StructField('middlename', StringType(), True),
    #          StructField('lastname', StringType(), True)
    #          ])),
    #      StructField('id', StringType(), True),
    #      StructField('gender', StringType(), True),
    #      StructField('salary', IntegerType(), True)
    #      ])

    ds = spark.read.csv(extract_loc, header=True, quote='"', escape='"', schema=schema, mode='FAILFAST')

    # for debugging
    # ds = ds.sample(withReplacement=False, fraction=.1, seed=42)
    # print(ds.head())

    # need to unpack metadata and subject data
    # print(ds.head()['metadata'])
    # print(ds.head()['annotations'])

    metadata_str_to_struct_udf = udf(metadata_str_to_struct, returnType=metadata_struct) 
    annotations_str_to_struct_udf = udf(annotation_to_struct, returnType=annotations_struct)  
    subject_data_str_to_iauname_udf = udf(subject_data_str_to_iauname, returnType=StringType())  
    get_person_id_udf = udf(get_person_id, returnType=StringType())

    ds = ds.withColumn('metadata', metadata_str_to_struct_udf(ds['metadata']))
    ds = ds.withColumn('annotations', annotations_str_to_struct_udf(ds['annotations']))
    ds = ds.withColumn('iauname', subject_data_str_to_iauname_udf(ds['subject_data']))

    ds = ds.withColumn('person_id', get_person_id_udf(ds['user_id'], ds['user_ip']))

    ds = ds.withColumnRenamed('subject_ids', 'subject_id')
    ds = ds.withColumn('project_id', lit('5733'))  # TODO hardcoded for now as not in export. lit to make it a column, as Spark requires.

    flattened = flatten.api_df_to_responses(ds)

    flattened.write.parquet(save_dir, mode='overwrite')


if __name__ == '__main__':

     # os.environ['PYSPARK_DRIVER_PYTHON'] = hardcoded_python
    """
    python gzreduction/panoptes/extract/panoptes_extract_to_json.py
    """

    parser = argparse.ArgumentParser('Panoptes Extract to JSON')
    parser.add_argument(
        '--export',
        dest='export_loc',
        type=str,
        # default='temp/small_dr5_export.csv'
        # default='/media/walml/beta/galaxy_zoo/decals/dr5/volunteer_reduction/2020_11_10_tobias/data/classic-classifications.csv'
        # default='/media/walml/beta/galaxy_zoo/decals/dr5/volunteer_reduction/2020_11_10_tobias/data/enhanced-classifications.csv'
        # default='/media/walml/beta/galaxy_zoo/decals/dr5/volunteer_reduction/2020_11_10_tobias/data/decals-dr5-classifications.csv'
        default='/home/walml/repos/gz-panoptes-reduction/temp/raw/decals-dr5-classifications.csv'
        # default='/home/walml/repos/gz-panoptes-reduction/temp/raw/enhanced-classifications.csv'  # new export, but identical to above
    )
    parser.add_argument(
        '--save-dir',
        dest='save_dir',
        type=str,
        default='temp/flat_preactive'
    )
    args = parser.parse_args()

    os.environ['JAVA_HOME'] = '/usr/lib/jvm/java-8-openjdk-amd64'

    export_loc = args.export_loc
    save_dir = args.save_dir
    # if os.path.isdir(save_dir):
    #     shutil.rmtree(save_dir)
    if not os.path.exists(save_dir):
        os.mkdir(save_dir)

    convert_extract_to_parquet(
        extract_loc=export_loc,
        save_dir=save_dir,
        # max_lines=50000
        )
