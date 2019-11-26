import os
import logging
import time
import datetime
import shutil
import json
from typing import List
from multiprocessing import Process
import argparse

import pandas as pd
from pyspark.sql import SparkSession

from gzreduction.panoptes.api import api_to_json, reformat_api_like_exports
from gzreduction.panoptes import panoptes_to_subjects
from gzreduction import flatten, aggregate


class Volunteers():

    def __init__(self, working_dir: str, workflow_ids: List, max_classifications: int):
        """Retrieve and aggregate volunteer responses from Panoptes
        
        Args:
            working_dir (str): Root directory of results. Streaming progress state is here.
            workflow_ids (List): Download and clean responses from these workflows
            max_classifications (int): Download no more than max_classifications responses (e.g. 1e10)
        """
        self.raw_dir = os.path.join(working_dir, 'raw')  
        self.derived_dir = os.path.join(working_dir, 'derived')  # can hopefully put checkpoints in same directory: x/checkpoints
        self.flat_dir = os.path.join(working_dir, 'flat')
        self.subject_dir = os.path.join(working_dir, 'subjects')
        for directory in [self.raw_dir, self.derived_dir, self.flat_dir]:
            if not os.path.isdir(directory):
                os.mkdir(directory)

        self.metadata_loc = os.path.join(working_dir, 'metadata.json')  # e.g. to record last execution time
        self.aggregated_loc = os.path.join(working_dir, 'aggregated.parquet')  # aggregated responses
        self.classification_loc = os.path.join(working_dir, 'classifications.csv')  # aggregated responses joined to subject table

        assert isinstance(workflow_ids, list)
        self.workflow_ids = workflow_ids
        self.max_classifications = max_classifications  # for debugging
        
        # if on EC2, use all cores, else, use 3 (so I can still work while it runs) 
        # TODO hacky, should be an arg?
        n_cores = '*'
        if os.path.isdir('/data/repos'):
            n_cores = 3
        
        self.spark = SparkSession \
            .builder \
            .master('local[{}]'.format(n_cores)) \
            .appName("volunteers_shared") \
            .getOrCreate()

    def listen(self, blocking=False):
        # start streams first to be ready for new files
        start_subject_stream(self.raw_dir, self.subject_dir, self.workflow_ids, self.spark)
        start_derived_stream(self.raw_dir, self.derived_dir, self.workflow_ids, self.spark)
        start_flat_stream(self.derived_dir, self.flat_dir, self.spark)
        # start making new files
        listener = start_panoptes_listener(self.raw_dir, self.max_classifications)  # not spark
        if blocking:
            listener.join() # wait for all Panoptes API calls to download before returning
            time.sleep(5)  # make sure all streams have started any final batch before continuing
            queries = self.spark.streams.active  # list all the streams
            while any([q.status['isDataAvailable'] for q in queries]):
                print('{} - waiting to process downloaded data'.format(time.time()))
                time.sleep(10)
        

    def aggregate(self):
        return aggregate.run(self.flat_dir, self.spark)  # returns pandas df

    # Main API access point, from e.g. Zoobot
    def get_all_classifications(self, max_age=datetime.timedelta(hours=1)):
        if max_age:  # if made None, never read
            if os.path.exists(self.metadata_loc):
                metadata = json.load(open(self.metadata_loc, 'r'))
                classification_age = datetime.datetime.now() - pd.to_datetime(metadata['last_run'])
                if  classification_age < max_age:
                    logging.warning('Classification age {} within allowed max age {}, reading from disk'.format(classification_age, max_age))
                    return pd.read_csv(metadata['classification_loc'])

        logging.warning('Retrieving new classifications')
        
        self.listen(blocking=True)

        # reset spark session (messy, see if it works)
        self.spark.stop()
        time.sleep(1)
        self.spark = SparkSession \
            .builder \
            .master('local[*]') \
            .appName("volunteers_aggregate") \
            .getOrCreate()

        aggregated_df = self.aggregate()
        logging.info('Aggregation complete')
        # logging.info('Aggregation complete, saving {} galaxies'.format(aggregated_df.count()))
        aggregated_df.write.save(self.aggregated_loc, mode='overwrite')
        logging.info('Aggregation saved')

        # will now switch to pandas in-memory
        aggregated_df = pd.read_parquet(self.aggregated_loc)
        subject_df = self.spark.read.json(self.subject_dir).drop_duplicates().toPandas()
        logging.info('Aggregated: {}. Subjects: {}'.format(len(aggregated_df), len(subject_df)))

        classification_df = join_subjects_and_aggregated(subject_df, aggregated_df)
        logging.info('Classification complete')
        classification_df.to_csv(self.classification_loc, index=False)
        with open(self.metadata_loc, 'w') as f:
            json.dump(
                {
                    'last_run': str(datetime.datetime.now()),
                    'classification_loc': self.classification_loc,
                    'aggregated_loc': self.aggregated_loc
                },
                f
            )
        logging.info('Classifications saved')
        
        return classification_df


def join_subjects_and_aggregated(subject_df, aggregated_loc: str):
    logging.info('Aggregated Predictions: {}'.format(len(aggregated_loc)))
    logging.info('Subjects: {}'.format(len(subject_df)))
    df = pd.merge(aggregated_loc, subject_df, on='subject_id', how='inner')
    return df


def start_panoptes_listener(save_dir: str, max_classifications: int):
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


def start_subject_stream(raw_dir: str, output_dir: str, workflow_ids: List, spark):
    panoptes_to_subjects.run(
            raw_dir,
            output_dir,
            workflow_ids,
            spark,
            mode='stream'
        )


def start_derived_stream(raw_dir: str, derived_dir: str, workflow_ids: List, spark):
    return reformat_api_like_exports.derive_chunks(
        workflow_ids=workflow_ids,  # GZ decals workflow, TODO will change to priority workflow
        raw_classification_dir=raw_dir,
        output_dir=derived_dir,
        mode='stream',
        print_status=False,
        spark=spark
    )


def start_flat_stream(derived_dir, flat_dir, spark):
    return flatten.stream(derived_dir, flat_dir, print_status=False, spark=spark)


def get_new_reduction(working_dir: str, workflow_ids=['6122', '10581', '10582'], max_classifications=1e8):
    """By default, pull responses from all GZ workflows and aggregate together.
    
    Args:
        working_dir (str): [description]
        workflow_ids (list, optional): [description]. Defaults to ['6122', '10581', '10582'].
        max_classifications ([type], optional): [description]. Defaults to 1e8.
    """
    volunteers = Volunteers(working_dir, workflow_ids, max_classifications)  # no max classifications, will be long-running
    classification_df = volunteers.get_all_classifications(max_age=None)
    classification_df.to_csv(os.path.join(working_dir, 'classifications.csv'), index=False)


if __name__ == '__main__':

    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser('Reduction')
    parser.add_argument(
        '--working_dir',
        dest='working_dir',
        type=str,
        default='../zoobot/data/decals/classifications/streaming'
    )
    parser.add_argument('--max', dest='max_classifications', type=int, default=None)


    args = parser.parse_args()
    if args.max_classifications:
        max_classifications = args.max_classifications
    else:
        max_classifications = 1e8
        # max_classifications = 100

    # working_dir = 'temp'
    working_dir = args.working_dir

    get_new_reduction(working_dir, max_classifications=max_classifications)
