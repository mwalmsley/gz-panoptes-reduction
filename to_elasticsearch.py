import logging
import json

from pyspark.sql.types import TimestampType, StringType, StructType, StructField
from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id 
import requests
from elasticsearch import Elasticsearch


def json_to_elasticsearch(json_dir, schema):
    spark = SparkSession.builder.master(master='local[*]').appName('to_elasticsearch').getOrCreate()
    df = spark.read.json(json_dir, schema).repartition(1)
    df = df.withColumn("document_id", monotonically_increasing_id())
    print(df.count())
    rdd = df.toJSON()
    print(rdd.count())
    print(rdd.take(1))
    # https://spark.apache.org/docs/latest/streaming-programming-guide.html#design-patterns-for-using-foreachrdd
    rdd.foreachPartition(send_partition)

def send_to_elasticsearch_manual(x):
    # print(type(x))
    url = 'http://127.0.0.1:9200/temp/_doc/{}?pretty'.format(json.loads(x)['document_id'])
    # print(url)
    # data = json.loads(x)  # must be string!
    # print(data)
    headers = {'Content-Type': 'application/json'}
    return requests.put(url, data=x, headers=headers)


def send_partition(partition):
    es = Elasticsearch()
    for row in partition:
         _ = send_to_elasticsearch_client(es, row)


def send_to_elasticsearch_client(es, x):
    return es.index(index="temp", doc_type='_doc', body=x)  # automatic id


if __name__ == '__main__':

    json_dir = '/Data/repos/gzreduction/temp/flat'
    schema = StructType([
            StructField('created_at', TimestampType(), True),
            StructField('workflow_id', StringType(), True),
            StructField('user_id', StringType(), True),
            StructField('subject_id', StringType(), True),
            StructField('classification_id', StringType(), True),
            StructField('question', StringType(), True),
            StructField('response', StringType(), True)
            ])

    json_to_elasticsearch(json_dir, schema)

    # demo = '{"created_at":"2019-05-23T13:56:08.400+01:00","workflow_id":"10582","user_id":"1900757","subject_id":"32505366","classification_id":"167307256","question":"smooth-or-featured","response":"smooth"}'
    # r = send_to_elasticsearch(demo)
    # print(r.json())

    # demo = '{"created_at":"2019-05-23T13:56:08.400+01:00","workflow_id":"10582","user_id":"1900757","subject_id":"32505366","classification_id":"167307256","question":"smooth-or-featured","response":"smooth"}'
    # r = send_to_elasticsearch_client(demo)
    # print(r)