import pytest
import os

import json

import pandas as pd

from gzreduction.panoptes import panoptes_to_classifications
from tests import TEST_EXAMPLE_DIR



def test_load_classification_line(line, classification):
    # expect to recover classification, which was used to create line.
    # also expect classification 'created_at' to have been changed to datetime
    classification['created_at'] = pd.to_datetime(classification['created_at'])

    loaded_line = panoptes_to_classifications.load_classification_line(line)
    assert loaded_line == classification


def test_explode_classifications(classification):

    flat_classifications = panoptes_to_classifications.explode_classification(
        classification)
    result = pd.DataFrame(flat_classifications)  # list of series to DataFrame
    assert set(result.columns.values) == set(['task','task_label','value','user_id','classification_id','created_at','workflow_version','subject_id'])
    assert len(result) == 3  # splits to 3 individual classifications


def test_is_multiple_choice():
    pass


def test_is_null_classification():
    pass


# TODO use test factory, great case
def test_sanitise_string():
    """For a bunch of common strings, make sure the markdown is stripped correctly
    Also tests remove_image_markdown
    """
    features_md = '![features_or_disk_new.png](https://panoptes-uploads.zooniverse.org/production/project_attached_image/1ec52a74-9e49-4579-91ff-0140eb5371e6.png =60x) Features or Disk'
    cant_tell_md = "![cant_tell_arms.png](https://panoptes-uploads.zooniverse.org/production/project_attached_image/e40e428f-3e73-4d40-9eff-1616a7399819.png =60x) Can't tell"
    merging_both_md = '![merger_and_tidal.png](https://panoptes-uploads.zooniverse.org/production/project_attached_image/516be649-9efc-41f7-aa1c-4dd2dad45ac5.png =60x) Both (Merger & Tidal Debris)'
    merging_neither_md = '![no.png](https://panoptes-uploads.zooniverse.org/production/project_attached_image/096879e1-12ae-4df8-abb8-d4a93bc7797f.png =60x) Neither (No Merger & No Tidal Debris) '
    features_no_resize = "![feature_or_disk.png](https://panoptes-uploads.zooniverse.org/production/project_attached_image/f353f2f1-a47e-439d-b9ca-020199162a79.png) Features or Disk"
    bar = '![bar_none_sidebyside_acwacw_200x100.png](https://panoptes-uploads.zooniverse.org/production/project_attached_image/043f8c64-226c-4ff4-957d-af0b06d634df.png =120x60) no bar'
    no_md = 'hello world'

    markdown_strings = [
        features_md,
        cant_tell_md,
        merging_both_md,
        merging_neither_md,
        features_no_resize,
        bar,
        no_md
    ]
    cleaned_strings = list(map(panoptes_to_classifications.sanitise_string, markdown_strings))
    assert cleaned_strings[0] == 'features or disk'
    assert cleaned_strings[1] == "can't tell"
    assert cleaned_strings[2] == 'both (merger & tidal debris)'
    assert cleaned_strings[3] == 'neither (no merger & no tidal debris)'
    assert cleaned_strings[4] == 'features or disk'
    assert cleaned_strings[5] == 'no bar'
    assert cleaned_strings[6] == 'hello world'


# TODO another good opportunity for test factory
def test_rename_response(response_without_markdown, schema, renamed_response):
    """Take response with markdown removed, and rename task and value. Expect renamed_response"""
    result = panoptes_to_classifications.rename_response(response_without_markdown, schema)
    assert all(result == renamed_response)



def test_clean_response(response, schema, cleaned_response):
    cleaned = panoptes_to_classifications.clean_response(response, schema)
    assert cleaned.equals(cleaned_response)


def test_preprocess_classifications(classification_locs, schema, cleaned_responses):
    responses = panoptes_to_classifications.preprocess_classifications(
        classification_locs, 
        schema,
        save_loc='/data/spark/temp_export'
        )
    response_list = responses.collect()
    for n in range(len(cleaned_responses)):
        response = response_list[n]
        expected_response = cleaned_responses[n]
        assert response == expected_response

# # flattening step
# def test_flatten_raw_classifications(raw_classifications, flat_table):
#     result = panoptes_to_classifications.flatten_raw_classifications(raw_classifications)
#     assert result[['subject_id', 'task', 'user_id', 'value']].equals(
#         flat_table[['subject_id', 'task', 'user_id', 'value']])


# def test_clean_table(flat_table, clean_table, schema):
#     result = panoptes_to_classifications.clean_flat_table(flat_table, schema)
#     for col in ['subject_id', 'task', 'user_id', 'value']:
#         assert result[col].equals(clean_table[col])


# # move the question rows to columns
# def test_subject_question_table_to_votes(clean_table, schema, votes):
#     result = panoptes_to_classifications.subject_question_table_to_votes(
#         clean_table,
#         question_col='task',
#         answer_col='value',
#         schema=schema)

#     # classification c1 - user a says T0: features, T2: no, T9: yes, for subject s1 at at 2001-01-01
#     c1 = result[result['classification_id'] == 'c1']
#     assert c1['question-0_features-or-disk'].sum() == 1
#     assert c1['question-1_round'].sum() == 0
#     assert c1['question-2_no'].sum() == 1
#     assert c1['question-9_no'].sum() == 0
#     assert c1['question-9_yes'].sum() == 1

#     # classification c2 - user b says T1: round, T2: no, T9: no, for subject s1 at at 2001-01-02
#     c2 = result[result['classification_id'] == 'c2']
#     assert c2['question-0_features-or-disk'].sum() == 0
#     assert c2['question-1_round'].sum() == 1
#     assert c1['question-2_no'].sum() == 1
#     assert c2['question-9_no'].sum() == 1
#     assert c2['question-9_yes'].sum() == 0

#     for col in votes:
#         assert result[col].equals(votes[col])


# @pytest.fixture()
# def raw_classifications_loc():
#     return os.path.join(TEST_EXAMPLE_DIR, 'panoptes_classifications.csv')


# @pytest.fixture()
# def raw_classification_header_str(raw_classifications_loc):
#     with open(raw_classifications_loc, 'r') as input_f:
#         return input_f.readline()  # first line is header


# @pytest.fixture()
# def raw_classification_str(raw_classifications_loc):
#     with open(raw_classifications_loc, 'r') as input_f:
#         lines = [input_f.readline() for line_n in range(2)]
#     return lines[1]  # second line is a classification


# # whole pipeline DISABLED, THIS IS NO LONGER ONE STEP
# # def test_raw_classifications_to_votes(raw_classifications, schema, votes):
# #     result = panoptes_to_classifications.raw_classifications_to_votes(raw_classifications, schema)

# #     # classification c1 - user a says T0: features, T2: no, T9: yes, for subject s1 at at 2001-01-01
# #     c1 = result[result['classification_id'] == 'c1']
# #     assert c1['question-0_features-or-disk'].sum() == 1
# #     assert c1['question-1_round'].sum() == 0
# #     assert c1['question-2_no'].sum() == 1
# #     assert c1['question-9_no'].sum() == 0
# #     assert c1['question-9_yes'].sum() == 1

# #     # classification c2 - user b says T1: round, T2: no, T9: no, for subject s1 at at 2001-01-02
# #     c2 = result[result['classification_id'] == 'c2']
# #     assert c2['question-0_features-or-disk'].sum() == 0
# #     assert c2['question-1_round'].sum() == 1
# #     assert c1['question-2_no'].sum() == 1
# #     assert c2['question-9_no'].sum() == 1
# #     assert c2['question-9_yes'].sum() == 0

# #     for col in votes:
#         # assert result[col].equals(votes[col])


# # TODO test to remove apostrophe from cant tell - may currently fail
