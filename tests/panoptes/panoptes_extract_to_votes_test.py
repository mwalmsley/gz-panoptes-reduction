# import pytest
# import os

# import json

# import pandas as pd

# from gzreduction.panoptes import panoptes_extract_to_votes
# from tests import TEST_EXAMPLE_DIR


# def test_explode_classifications(raw_classification, raw_classification_header):

#     flat_classifications = panoptes_extract_to_votes.explode_classification(
#         raw_classification, 
#         raw_classification_header
#     )
#     result = pd.DataFrame(flat_classifications)
#     assert set(result.columns.values) == set(['task','task_label','value','user_id','classification_id','created_at','workflow_version','subject_id'])
#     assert len(result) == 2  # splits to 8 individual classifications


# def test_clean_classification(exploded_classification, schema, cleaned_classification):
#     cleaned = panoptes_extract_to_votes.clean_classification(exploded_classification, schema)
#     assert cleaned.equals(cleaned_classification)


# def test_preprocess_classifications_with_spark(raw_classification, schema):
#     classifications_loc = os.path.join(TEST_EXAMPLE_DIR, 'panoptes_classifications_pair.csv')
#     with open(os.path.join(TEST_EXAMPLE_DIR, 'panoptes_classifications_pair.csv'), 'w') as f:
#         f.write('classification_id,user_name,user_id,user_ip,workflow_id,workflow_name,workflow_version,created_at,gold_standard,expert,metadata,annotations,subject_data,subject_ids' + '\n')
#         f.write(raw_classification + '\n')
#         f.write(raw_classification)
#     cleaned_rdd = panoptes_extract_to_votes.preprocess_classifications_with_spark(classifications_loc, schema, save_loc=None)
#     cleaned_classifications = cleaned_rdd.collect()
#     print(cleaned_classifications)
#     assert False


# # TODO use test factory, great case
# def test_sanitise_string():
#     features_md = '![features_or_disk_new.png](https://panoptes-uploads.zooniverse.org/production/project_attached_image/1ec52a74-9e49-4579-91ff-0140eb5371e6.png =60x) Features or Disk'
#     cant_tell_md = "![cant_tell_arms.png](https://panoptes-uploads.zooniverse.org/production/project_attached_image/e40e428f-3e73-4d40-9eff-1616a7399819.png =60x) Can't tell"
#     merging_both_md = '![merger_and_tidal.png](https://panoptes-uploads.zooniverse.org/production/project_attached_image/516be649-9efc-41f7-aa1c-4dd2dad45ac5.png =60x) Both (Merger & Tidal Debris)'
#     merging_neither_md = '![no.png](https://panoptes-uploads.zooniverse.org/production/project_attached_image/096879e1-12ae-4df8-abb8-d4a93bc7797f.png =60x) Neither (No Merger & No Tidal Debris) '
#     features_no_resize = "![feature_or_disk.png](https://panoptes-uploads.zooniverse.org/production/project_attached_image/f353f2f1-a47e-439d-b9ca-020199162a79.png) Features or Disk"
#     bar = '![bar_none_sidebyside_acwacw_200x100.png](https://panoptes-uploads.zooniverse.org/production/project_attached_image/043f8c64-226c-4ff4-957d-af0b06d634df.png =120x60) no bar'
#     no_md = 'hello world'

#     markdown_strings = [
#         features_md,
#         cant_tell_md,
#         merging_both_md,
#         merging_neither_md,
#         features_no_resize,
#         bar,
#         no_md
#     ]
#     cleaned_strings = list(map(panoptes_extract_to_votes.sanitise_string, markdown_strings))
#     assert cleaned_strings[0] == 'features or disk'
#     assert cleaned_strings[1] == "can't tell"
#     assert cleaned_strings[2] == 'both (merger & tidal debris)'
#     assert cleaned_strings[3] == 'neither (no merger & no tidal debris)'
#     assert cleaned_strings[4] == 'features or disk'
#     assert cleaned_strings[5] == 'no bar'
#     assert cleaned_strings[6] == 'hello world'


# # flattening step
# def test_flatten_raw_classifications(raw_classifications, flat_table):
#     result = panoptes_extract_to_votes.flatten_raw_classifications(raw_classifications)
#     assert result[['subject_id', 'task', 'user_id', 'value']].equals(
#         flat_table[['subject_id', 'task', 'user_id', 'value']])


# def test_clean_table(flat_table, clean_table, schema):
#     result = panoptes_extract_to_votes.clean_flat_table(flat_table, schema)
#     for col in ['subject_id', 'task', 'user_id', 'value']:
#         assert result[col].equals(clean_table[col])


# # move the question rows to columns
# def test_subject_question_table_to_votes(clean_table, schema, votes):
#     result = panoptes_extract_to_votes.subject_question_table_to_votes(
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
# #     result = panoptes_extract_to_votes.raw_classifications_to_votes(raw_classifications, schema)

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
