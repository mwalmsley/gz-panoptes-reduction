import logging

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

if __name__ == '__main__':

    spark = SparkSession \
        .builder \
        .appName("hello") \
        .getOrCreate()

    df = spark.read.json("data/raw/classifications/api/derived_panoptes_api_first_156988066_last_157128147.txt")
    # df = spark.read.json("data/raw/classifications/api/derived_panoptes_api_first_157128147_last_157128227.txt")

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
    flat_view.show()
    # https://spark.apache.org/docs/latest/api/python/pyspark.sql.html?highlight=save#pyspark.sql.DataFrameWriter
    # flat_view.write.save(
    #     'temp_flat.csv', 
    #     mode='overwrite',
    #     format='csv')

    join_string_udf = udf(lambda x, y: x + '_' + y)
    
    flat_view = flat_view.withColumn('question_response', join_string_udf('question', 'response'))

    aggregated = flat_view.groupBy('subject_id').pivot('question_response').agg(count('question_response'))
    aggregated = aggregated.na.fill(0)
    # aggregated = flat_view.groupBy('subject_id', 'question', 'response').agg(count('response'))

    # join_string_udf = udf(lambda x, y: x + '_' + y)
    # aggregated = aggregated.withColumn('question_response', join_string_udf('question', 'response'))
    aggregated.show()

    aggregated_loc = 'temp_agg.csv'

    # fits in memory: 
    # aggregated.repartition(1).write.csv(path=aggregated_loc, mode="append", header="true")
    aggregated.toPandas().to_csv(aggregated_loc)

    # doesn't fit in memory:
    # aggregated.write.save(
    #     aggregated_loc,
    #     mode='overwrite',
    #     format='csv')
    
    # TODO: calculate total votes by question