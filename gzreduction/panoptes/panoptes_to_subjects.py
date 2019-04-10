import logging
import os
import shutil
import json

from gzreduction.panoptes.api import api_to_json
from gzreduction.panoptes import panoptes_to_responses, responses_to_votes


def get_subjects(classification_locs, save_dir):
    extract_subjects(classification_locs, save_dir)
    subject_df = responses_to_votes.join_shards(save_dir, header=header())
    return subject_df.drop_duplicates(subset=['subject_id'], keep='first')  # subjects will repeat


def extract_subjects(classification_locs, save_dir):
    sc = panoptes_to_responses.start_spark('get_subjects')

    # load all classifications as RDD
    assert isinstance(classification_locs, list)
    classification_chunks = [sc.textFile(loc) for loc in classification_locs]
    lines = sc.union(classification_chunks)

    classifications = lines.map(lambda x: panoptes_to_responses.load_classification_line(x))
    subjects = classifications.map(lambda x: get_subject(x))
    output_lines = subjects.map(lambda x: panoptes_to_responses.response_to_line(x, header=header()))
    output_lines.saveAsTextFile(save_dir)
    sc.stop()


def get_subject(classification):
    raw_subject = classification['links']['subject']
    loaded_subject = {
        'subject_id': raw_subject['id'],
        'iauname': raw_subject['metadata']['!iauname'],
        'subject_url': raw_subject['locations'][0]['image/png']
        # 'metadata': '"' + json.dumps(raw_subject['metadata']) + '"'
    }
    return loaded_subject


def header():
    return ['subject_id', 'iauname', 'subject_url']
    # return ['subject_id', 'iauname', 'subject_url', 'metadata']

if __name__ == '__main__':

    logging.basicConfig(
        filename='panoptes_to_subjects.log',
        format='%(asctime)s %(message)s',
        filemode='w',
        level=logging.DEBUG)
    
    # classification_dir = 'data/raw/classifications/api'
    classification_dir = '/tmp/working_dir/raw'
    classification_locs = api_to_json.get_chunk_files(classification_dir, derived=True)

    save_dir = '/tmp/working_dir/panoptes_subjects'
    if os.path.isdir(save_dir):
        shutil.rmtree(save_dir)

    get_subjects(classification_locs, save_dir)
