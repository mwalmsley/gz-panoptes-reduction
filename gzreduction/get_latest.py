import logging
import os
import shutil
from datetime import datetime

import pandas as pd

from gzreduction.panoptes import panoptes_to_responses, panoptes_to_subjects, responses_to_votes
from gzreduction.panoptes.api import api_to_json, reformat_api_like_exports
from gzreduction import schemas
from gzreduction.votes_to_predictions import votes_to_predictions


def execute_reduction(workflow_id, working_dir, last_id, max_classifications=1e8):

    if workflow_id == '6122':
        schema = schemas.dr5_schema.dr5_schema # the dr5 schema object in the dr5 schema file. Oops.
    else:
        raise NotImplementedError

    responses_dir = os.path.join(working_dir, 'raw')
    classification_dir = os.path.join(working_dir, 'classifications')
    subject_dir = os.path.join(working_dir, 'subjects')
    output_dir = os.path.join(working_dir, 'output')
    for directory in [responses_dir, classification_dir, output_dir]:
        if not os.path.isdir(directory):
            os.mkdir(directory)
    votes_loc = os.path.join(output_dir, 'latest_votes.csv')
    predictions_loc = os.path.join(output_dir, 'latest_predictions.csv')

    # get raw API responses, place in responses_dir
    api_to_json.get_latest_classifications(
        responses_dir,  #
        previous_dir=None,
        max_classifications=max_classifications,
        manual_last_id=last_id  # should be the last id used in the last major export (provided to shards)
    )

    # reformat_api_like_exports.derive_chunks(workflow_id, [responses_dir])
    derived_responses_locs = api_to_json.get_chunk_files(responses_dir, derived=True)

    # make flat table of classifications. Basic use-agnostic view. 
    panoptes_to_responses.preprocess_classifications(
        derived_responses_locs,
        schema, 
        start_date=datetime(year=2018, month=3, day=15),  # public launch  tzinfo=timezone.utc
        save_dir=classification_dir
    )
    responses = responses_to_votes.join_shards(classification_dir, header=panoptes_to_responses.response_to_line_header())

    responses_to_votes.get_votes(
        responses,
        question_col='task', 
        answer_col='value', 
        schema=schema,
        save_loc=votes_loc)

    panoptes_votes = pd.read_csv(votes_loc)
    logging.info('Unique subjects with votes: {}'.format(len(panoptes_votes['subject_id'].unique())))

    # # awful naming
    votes_to_predictions.votes_to_predictions(
        panoptes_votes,
        schema,
        reduced_save_loc=None,
        predictions_save_loc=predictions_loc
    )
    del panoptes_votes

    # similarly for subjects
    subjects = panoptes_to_subjects.get_subjects(derived_responses_locs, subject_dir)
    logging.info('Subjects: {}'.format(len(subjects)))
    subjects.to_csv(os.path.join(subject_dir, 'subjects.csv'), index=False)
    # subjects = pd.read_csv(os.path.join(subject_dir, 'subjects.csv'))
    assert not any(subjects.duplicated(subset=['subject_id']))

    # join predictions with subjects
    predictions = pd.read_csv(predictions_loc)
    logging.info('Predictions: {}'.format(len(predictions)))
    
    df = pd.merge(predictions, subjects, on='subject_id', how='inner')

    return df



if __name__ == '__main__':


    workflow_id = '6122'
    # last_id = '91178981'   # first decals
    # last_id = '157128147'   # april
    # last_id = '117640757'  # 750,000 subjects before download timed out
    last_id = '159526481'  # pre-launch full reduction ends here

    # for debugging
    max_classifications = 1000
    working_dir = '/tmp/working_dir'
    if os.path.isdir(working_dir):
        shutil.rmtree(working_dir)
    os.mkdir(working_dir)
    # OR
    # for starting/continuing a fixed download, prior to creating master catalog
    # max_classifications = 1e8
    # working_dir = '../zoobot/data/decals/classifications'

    save_loc = os.path.join(working_dir, 'classifications.csv')
    df = execute_reduction(workflow_id, working_dir, last_id, max_classifications=max_classifications)
    print(df.iloc[0])
    print(df.iloc[0]['subject_url'])

    df.to_csv(save_loc, index=False)
