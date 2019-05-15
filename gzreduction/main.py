import os
import time
import shutil
from multiprocessing import Process

from pyspark.sql import SparkSession

from gzreduction.panoptes.api import api_to_json, reformat_api_like_exports
from gzreduction import flatten, aggregate


class Volunteers():

    def __init__(self, working_dir, max_classifications):
        self.raw_dir = os.path.join(working_dir, 'raw')  
        self.derived_dir = os.path.join(working_dir, 'derived')  # can hopefully put checkpoints in same directory: x/checkpoints
        self.flat_dir = os.path.join(working_dir, 'flat')
        for directory in [self.raw_dir, self.derived_dir, self.flat_dir]:
            if not os.path.isdir(directory):
                os.mkdir(directory)

        self.max_classifications = max_classifications  # for debugging

        self.spark = SparkSession \
            .builder \
            .master('local[3]') \
            .appName("volunteers_shared") \
            .getOrCreate()

    def listen(self, blocking=False):
        # start streams first to be ready for new files
        start_derived_stream(self.raw_dir, self.derived_dir, self.spark)
        start_flat_stream(self.derived_dir, self.flat_dir, self.spark)
        # start making new files
        listener = start_panoptes_listener(self.raw_dir, self.max_classifications)  # not spark  TEMP no new classifications
        if blocking:
            listener.join() # wait for all Panoptes API calls to download before returning
            queries = self.spark.streams.active  # list all the streams
            print([q.name for q in queries])
            while any([q.status['isDataAvailable'] for q in queries]):
                print('{} - waiting to process downloaded data'.format(time.time()))
                time.sleep(10)

    def aggregate(self):
        return aggregate.run(self.flat_dir, self.spark)
        # if fits in memory: 
        # df.repartition(1).write.csv(path=df_loc, mode="append", header="true")
        # return df.toPandas()

        # if doesn't fit in memory:
        # df.write.save(
        #     df_loc,
        #     mode='overwrite',
        #     format='csv')


def start_panoptes_listener(save_dir, max_classifications):
    # needs to run in separate process
    p = Process(
            target=api_to_json.get_latest_classifications, 
            args=(
                save_dir,
                max_classifications,
                save_dir,
                None
            )
        )
    p.start()
    return p  # let it run without joining! 


def start_derived_stream(raw_dir, derived_dir, spark):
    return reformat_api_like_exports.derive_chunks(
        workflow_id='6122',  # GZ decals workflow, TODO will change to priority workflow
        raw_classification_dir=raw_dir,
        output_dir=derived_dir,
        mode='stream',
        print_status=False,
        spark=spark
    )


def start_flat_stream(derived_dir, flat_dir, spark):
    return flatten.stream(derived_dir, flat_dir, print_status=False, spark=spark)


def get_new_reduction_demo():
    working_dir = 'temp'
    if os.path.isdir(working_dir):
        shutil.rmtree(working_dir)
    os.mkdir(working_dir)
    
    volunteers = Volunteers(working_dir, max_classifications=100)
    volunteers.listen(blocking=True)
    df = volunteers.aggregate()
    output_loc = os.path.join(working_dir, 'aggregated.csv')
    df.to_csv(output_loc, index=False)


def get_new_reduction():
    working_dir = '../zoobot/data/decals/classifications/streaming'
    volunteers = Volunteers(working_dir, max_classifications=1e8)  # no max classifications, will be long-running
    volunteers.listen(blocking=True)
    df = volunteers.aggregate()
    output_loc = os.path.join(working_dir, 'aggregated.csv')
    df.to_csv(output_loc, index=False)


if __name__ == '__main__':

    # get_new_reduction_demo()

    get_new_reduction()



    # start getting classifications, but in another process (not blocking)
    # spark streams will catch up
    # spark streams should all be non-blocking, except perhaps the last (could instead block here?)