from pyspark.sql import SparkSession

from pyspark.sql.functions import to_timestamp, to_date, lit, explode, udf
from pyspark.sql.types import TimestampType, BooleanType, StringType

from gzreduction.panoptes.panoptes_to_responses import sanitise_string, rename_response
from gzreduction.schemas import dr5_schema

sanitise_string_udf = udf(lambda x: sanitise_string(x), returnType=StringType())  
rename_response_udf = udf(lambda x: rename_response(x, dr5_schema), returnType=StringType())

import datetime

if __name__ == '__main__':

    spark = SparkSession \
        .builder \
        .appName("hello") \
        .getOrCreate()

    df = spark.read.json("data/raw/classifications/api/derived/panoptes_api_first_96716946_last_97209898.txt")

    key_df = df.select(
        'annotations',
        'classification_id',
        'subject_id',
        'user_id',
        to_timestamp(df['created_at']).alias('created_at_dt')
        )

    # key_df.createOrReplaceTempView("key_df")


    # key_df.printSchema()
    start_date = to_date(lit('2017-03-20')).cast(TimestampType())  # lit means 'column of literal value' i.e. dummy column of that everywhere
    key_df = key_df.filter(key_df['created_at_dt'] > start_date)

    exploded = key_df.select(
        explode('annotations').alias('annotations_struct'),
         'classification_id')  # doubly nested, need to explode twice
    flattened = exploded.select(
        'annotations_struct.*',
        'classification_id'
        )

    flattened.printSchema()
    # flattened.withColumn('multiple_choice_bool', flattened['multiple_choice'].cast(BooleanType()))

    # filter for multiple choice and None
    flattened = flattened.filter(flattened['multiple_choice'] == False) # can only compare without lit() when calling directly e.g. df[x]    
    flattened = flattened.filter(flattened['value'] != 'No')
    
    flattened.show()

    # https://docs.databricks.com/spark/latest/spark-sql/udf-python.html
    flattene = flattened.select(sanitise_string_udf(flattened['value'])).alias('cleaned_response')
    # TODO filter again to avoid nulls
    # flattened.select(rename_response_udf(flattened['cleaned_response'])).alias('response')

    flattened.show()  # may need another filter to remove any newly-null values
