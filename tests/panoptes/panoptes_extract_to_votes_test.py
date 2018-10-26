import pytest

import json

import pandas as pd

from gzreduction.panoptes import panoptes_extract_to_votes
from gzreduction.schemas.schema import Schema, Question, Answer


@pytest.fixture()
def question_0():
    question_0 = Question(
        name='question-0',
        raw_name='T0'
    )
    question_0.set_answers([
        Answer(
            name='features-or-disk',
            raw_name='features or disk'),
    ])
    return question_0


@pytest.fixture()
def question_1():
    question_1 = Question(
        name='question-1',
        raw_name='T1'
    )
    question_1.set_answers([
        Answer(
            name='round',
            raw_name='completely round'),
    ])
    return question_1


@pytest.fixture()
def question_2():
    question_2 = Question(
        name='question-2',
        raw_name='T2'
    )
    question_2.set_answers([
        Answer(
            name='no',
            raw_name='no'),
    ])
    return question_2


@pytest.fixture()
def question_9():
    question_9 = Question(
        name='question-9',
        raw_name='T9'
    )
    question_9.set_answers([
        Answer(
            name='no',
            raw_name='no'),
        Answer(
            name='yes',
            raw_name='yes'),
    ])
    return question_9


@pytest.fixture()
def schema(question_0, question_1, question_2, question_9):
    schema = Schema()
    schema.add_question(question_0)
    schema.add_question(question_1)
    schema.add_question(question_2)
    schema.add_question(question_9)
    return schema


@pytest.fixture()
def raw_annotations_a():
    return [
        {
            "task": "T0",  # only in annotation a
            "task_label": "Is the galaxy simply smooth and rounded, with no sign of a disk?",
            "value": "![feature_or_disk.png](https://panoptes-uploads.zooniverse.org/production/project_attached_image/f353f2f1-a47e-439d-b9ca-020199162a79.png) Features or Disk"},

        {
            "task": "T2",  # in both a and b, users agree
            "task_label": "Could this be a disk viewed edge-on?",
            "value":"![no.png](https://panoptes-uploads.zooniverse.org/production/project_attached_image/096879e1-12ae-4df8-abb8-d4a93bc7797f.png) No"},

        {
            "task": "T9",  # in both a and b, users disagree
            "task_label": "Is there anything odd?",
            "value": "![yes.png](https://panoptes-uploads.zooniverse.org/production/project_attached_image/503a6354-7f72-4899-b620-4399dbd5cf93.png) Yes"},
    ]


@pytest.fixture()
def raw_annotations_b():
    return [
        {
            "task": "T1",  # only in annotation b
            "task_label": "How rounded is it?",
            "value": "![rounded.png](https://panoptes-uploads.zooniverse.org/production/project_attached_image/94412557-f564-40b9-9423-1d2e47cb1104.png) Completely round"},

        {
            "task": "T2",  # in both a and b, users agree
            "task_label": "Could this be a disk viewed edge-on?",
            "value": "![no.png](https://panoptes-uploads.zooniverse.org/production/project_attached_image/096879e1-12ae-4df8-abb8-d4a93bc7797f.png) No"},

        {
            "task": "T9",  # in both a and b, users disagree
            "task_label": "Is there anything odd?",
            "value": "![no.png](https://panoptes-uploads.zooniverse.org/production/project_attached_image/096879e1-12ae-4df8-abb8-d4a93bc7797f.png) No"},

        {
            "task": "T9",  # in both a and b, users disagree
            "task_label": "Is there anything odd?",
            "value": ""},  # user left side without submitting answer - '' recorded. Filter out.

        {
            "task": "T0",
            "task_label": "Is there anything odd?",
            "value": None}  # user left side without submitting answer - None recorded. Filter out.
    ]


# pack each annotation up as JSON to appear just like the Panoptes classifications export
@pytest.fixture()
def raw_classifications(raw_annotations_a, raw_annotations_b):
    # TODO multiple subject ids
    return pd.DataFrame([
        {
            # classification c1 - user a says T0: features, T2: no, T9: yes, for subject s1 at at 2001-01-01
            'classification_id': 'c1',
            'user_id': 'a',
            'annotations': json.dumps(raw_annotations_a),
            'subject_ids': 's1',
            'created_at': '2001-01-01',
            'workflow_version': 'v1'},
        {
            # classification c2 - user b says T1: round, T2: no, T9: no, for subject s1 at at 2001-01-02
            'classification_id': 'c2',
            'user_id': 'b',
            'annotations': json.dumps(raw_annotations_b),
            'subject_ids': 's1',
            'created_at': '2001-01-02',
            'workflow_version': 'v1'}
    ])


# intermediate step: the annotations should be flattened into a table like this
# of form [user, subject, task, value] for every user, subject, task and value
# this is the subject_question table
@pytest.fixture()
def flat_table():
    # simply rearranged version of raw input, should have markdown and original names
    # TODO multiple subject ids
    return pd.DataFrame([

        # expected responses from user a
        {
            'classification_id': 'c1',
            'created_at': '2001-01-01',
            'task': 'T0',
            'value': '![feature_or_disk.png](https://panoptes-uploads.zooniverse.org/production/project_attached_image/f353f2f1-a47e-439d-b9ca-020199162a79.png) Features or Disk',
            'user_id': 'a',
            'subject_id': 's1',
            'workflow_version': 'v1'},
        {
            'classification_id': 'c1',
            'created_at': '2001-01-01',
            'task': 'T2',
            'value': '![no.png](https://panoptes-uploads.zooniverse.org/production/project_attached_image/096879e1-12ae-4df8-abb8-d4a93bc7797f.png) No',
            'user_id': 'a',
            'subject_id': 's1',
            'workflow_version': 'v1'},
        {
            'classification_id': 'c1',
            'created_at': '2001-01-01',
            'task': 'T9',
            'value': '![yes.png](https://panoptes-uploads.zooniverse.org/production/project_attached_image/503a6354-7f72-4899-b620-4399dbd5cf93.png) Yes',
            'user_id': 'a',
            'subject_id': 's1',
            'workflow_version': 'v1'},

        # expected responses from user b
        {
            'classification_id': 'c2',
            'created_at': '2001-01-02',
            'task': 'T1',
            'value': '![rounded.png](https://panoptes-uploads.zooniverse.org/production/project_attached_image/94412557-f564-40b9-9423-1d2e47cb1104.png) Completely round',
            'user_id': 'b',
            'subject_id': 's1',
            'workflow_version': 'v1'},
        {
            'classification_id': 'c2',
            'created_at': '2001-01-02',
            'task': 'T2',
            'value': '![no.png](https://panoptes-uploads.zooniverse.org/production/project_attached_image/096879e1-12ae-4df8-abb8-d4a93bc7797f.png) No',
            'user_id': 'b',
            'subject_id': 's1',
            'workflow_version': 'v1'},
        {
            'classification_id': 'c2',
            'created_at': '2001-01-02',
            'task': 'T9',
            'value': '![no.png](https://panoptes-uploads.zooniverse.org/production/project_attached_image/096879e1-12ae-4df8-abb8-d4a93bc7797f.png) No',
            'user_id': 'b',
            'subject_id': 's1',
            'workflow_version': 'v1'},

        {
            'classification_id': 'c2',
            'created_at': '2001-01-02',
            'task': 'T9',
            'value': '',  # not yet filtered, removed in clean_table step
            'user_id': 'b',
            'subject_id': 's1',
            'workflow_version': 'v1'}
    ])


@pytest.fixture()
def clean_table(flat_table):
    df = flat_table.copy()
    df.iloc[0]['value'] = 'features-or-disk'  # questions should have been renamed
    df.iloc[1]['value'] = 'no'
    df.iloc[2]['value'] = 'yes'
    df.iloc[3]['value'] = 'round'
    df.iloc[4]['value'] = 'no'
    df.iloc[5]['value'] = 'no'
    df.iloc[0]['task'] = 'question-0'  # answers should have been renamed
    df.iloc[1]['task'] = 'question-2'
    df.iloc[2]['task'] = 'question-9'
    df.iloc[3]['task'] = 'question-1'
    df.iloc[4]['task'] = 'question-2'
    df.iloc[5]['task'] = 'question-9'
    df = df[:6]  # should filter 6th classification, has empty response value
    return df


# final step: the subject question table has the questions moved to columns
@pytest.fixture()
def votes():

    votes = pd.DataFrame([

        {
            'classification_id': 'c1',
            'created_at': '2001-01-01',
            'question-0_features-or-disk': 1.0,
            'question-1_round': 0.0,
            'question-2_no': 0.0,
            'question-9_no': 0.0,
            'question-9_yes': 0.0,
            'subject_id': 's1',
            'user_id': 'a',
        },

        {
            'classification_id': 'c2',
            'created_at': '2001-01-02',
            'question-0_features-or-disk': 0.0,
            'question-1_round': 1.0,
            'question-2_no': 0.0,
            'question-9_no': 0.0,
            'question-9_yes': 0.0,
            'subject_id': 's1',
            'user_id': 'b',
        },

        {
            'classification_id': 'c1',
            'created_at': '2001-01-01',
            'question-0_features-or-disk': 0.0,
            'question-1_round': 0.0,
            'question-2_no': 1.0,
            'question-9_no': 0.0,
            'question-9_yes': 0.0,
            'subject_id': 's1',
            'user_id': 'a',
        },

        {
            'classification_id': 'c2',
            'created_at': '2001-01-02',
            'question-0_features-or-disk': 0.0,
            'question-1_round': 0.0,
            'question-2_no': 1.0,
            'question-9_no': 0.0,
            'question-9_yes': 0.0,
            'subject_id': 's1',
            'user_id': 'b',
        },

        {
            'classification_id': 'c1',
            'created_at': '2001-01-01',
            'question-0_features-or-disk': 0.0,
            'question-1_round': 0.0,
            'question-2_no': 0.0,
            'question-9_no': 0.0,
            'question-9_yes': 1.0,
            'subject_id': 's1',
            'user_id': 'a',
        },

        {
            'classification_id': 'c2',
            'created_at': '2001-01-02',
            'question-0_features-or-disk': 0.0,
            'question-1_round': 0.0,
            'question-2_no': 0.0,
            'question-9_no': 1.0,
            'question-9_yes': 0.0,
            'subject_id': 's1',
            'user_id': 'b',
        }
    ])
    votes['created_at'] = pd.to_datetime(votes['created_at'])  # avoid datetime comparison hassle
    return votes


# TODO use test factory, great case
def test_sanitise_string():
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
    cleaned_strings = list(map(panoptes_extract_to_votes.sanitise_string, markdown_strings))
    assert cleaned_strings[0] == 'features or disk'
    assert cleaned_strings[1] == "can't tell"
    assert cleaned_strings[2] == 'both (merger & tidal debris)'
    assert cleaned_strings[3] == 'neither (no merger & no tidal debris)'
    assert cleaned_strings[4] == 'features or disk'
    assert cleaned_strings[5] == 'no bar'
    assert cleaned_strings[6] == 'hello world'


# flattening step
def test_flatten_raw_classifications(raw_classifications, flat_table):
    result = panoptes_extract_to_votes.flatten_raw_classifications(raw_classifications)
    assert result[['subject_id', 'task', 'user_id', 'value']].equals(
        flat_table[['subject_id', 'task', 'user_id', 'value']])


def test_clean_table(flat_table, clean_table, schema):
    result = panoptes_extract_to_votes.clean_flat_table(flat_table, schema)
    for col in ['subject_id', 'task', 'user_id', 'value']:
        assert result[col].equals(clean_table[col])


# move the question rows to columns
def test_subject_question_table_to_votes(clean_table, schema, votes):
    result = panoptes_extract_to_votes.subject_question_table_to_votes(
        clean_table,
        question_col='task',
        answer_col='value',
        schema=schema)

    # classification c1 - user a says T0: features, T2: no, T9: yes, for subject s1 at at 2001-01-01
    c1 = result[result['classification_id'] == 'c1']
    assert c1['question-0_features-or-disk'].sum() == 1
    assert c1['question-1_round'].sum() == 0
    assert c1['question-2_no'].sum() == 1
    assert c1['question-9_no'].sum() == 0
    assert c1['question-9_yes'].sum() == 1

    # classification c2 - user b says T1: round, T2: no, T9: no, for subject s1 at at 2001-01-02
    c2 = result[result['classification_id'] == 'c2']
    assert c2['question-0_features-or-disk'].sum() == 0
    assert c2['question-1_round'].sum() == 1
    assert c1['question-2_no'].sum() == 1
    assert c2['question-9_no'].sum() == 1
    assert c2['question-9_yes'].sum() == 0

    for col in votes:
        assert result[col].equals(votes[col])


# whole pipeline DISABLED, THIS IS NO LONGER ONE STEP
# def test_raw_classifications_to_votes(raw_classifications, schema, votes):
#     result = panoptes_extract_to_votes.raw_classifications_to_votes(raw_classifications, schema)

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
        # assert result[col].equals(votes[col])


# TODO test to remove apostrophe from cant tell - may currently fail
