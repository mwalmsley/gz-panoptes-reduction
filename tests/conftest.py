import pytest

import json
import os

import pandas as pd

from gzreduction.schemas.schema import Schema, Question, Answer

@pytest.fixture()
def question_0():
    return Question(
        name='question-0',
        raw_name='T0',
        answers=[
            Answer(
                name='features-or-disk',
                raw_name='features or disk')
        ]
    )


@pytest.fixture()
def question_1():
    return Question(
        name='question-1',
        raw_name='T1',
        answers=[
            Answer(
            name='round',
            raw_name='completely round'),
        ]
    )


@pytest.fixture()
def question_2():
    return Question(
        name='question-2',
        raw_name='T2',
        answers=[
            Answer(
            name='no',
            raw_name='no'),
        ]
    )


@pytest.fixture()
def question_9():
    return Question(
        name='question-9',
        raw_name='T9',
        answers=[
            Answer(
                name='no',
                raw_name='no'),
            Answer(
                name='yes',
                raw_name='yes')
        ]
    )

@pytest.fixture()
def schema(question_0, question_1, question_2, question_9):
    return Schema(questions=[question_0, question_1, question_2, question_9])


@pytest.fixture()
def annotations_a():
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
def annotations_b():
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


@pytest.fixture()
def line(classification):
    """classification as it would appear when saved as json and then read from disk"""
    return json.dumps(classification)


@pytest.fixture()
def classification(annotations_a):
    """user a says T0: features, T2: no, T9: yes, for subject s1 at at 2001-01-01"""
    return {
        'classification_id': 'c1',
        'user_id': 'a',
        'annotations': annotations_a,
        'subject_ids': 's1',
        'created_at': '2001-01-01',
        'workflow_version': 'v1'
    }


@pytest.fixture()
def classification_secondary(annotations_b):
    """user b says T1: round, T2: no, T9: no, for subject s1 at at 2001-01-02"""
    return { 
        'classification_id': 'c2',
        'user_id': 'b',
        'annotations': annotations_b,
        'subject_ids': 's1',
        'created_at': '2001-01-02',
        'workflow_version': 'v1'
    }


@pytest.fixture()
def classification_locs(tmpdir, classification, classification_secondary):
    """Save two .txt files with json classifications.
    For simplicity, put only one classification in each file
    Will load these with Spark
    """
    # TODO multiple subject ids
    classifications_dir = tmpdir.mkdir('classifications_dir').strpath
    locs = [os.path.join(classifications_dir, 'classifications.txt')]
    with open(locs[0], 'w') as f:
        f.write(json.dumps(classification) + '\n')
        f.write(json.dumps(classification_secondary) + '\n')
    return locs




@pytest.fixture()
def response():
    # single response to one question (before any processing)
    return pd.Series(
        {
            'classification_id': 'c1',
            'created_at': '2001-01-01',
            'task': 'T0',
            'value': '![feature_or_disk.png](https://panoptes-uploads.zooniverse.org/production/project_attached_image/f353f2f1-a47e-439d-b9ca-020199162a79.png) Features or Disk',
            'user_id': 'a',
            'subject_ids': 's1',
            'workflow_version': 'v1'
        }
    )

@pytest.fixture()
def response_without_markdown():
    return pd.Series(
        {
            'classification_id': 'c1',
            'created_at': '2001-01-01',
            'task': 'T0',
            'value': 'features or disk',
            'user_id': 'a',
            'subject_ids': 's1',
            'workflow_version': 'v1'
        }
    )


@pytest.fixture()
def renamed_response():
    return pd.Series(
        {
            'classification_id': 'c1',
            'created_at': '2001-01-01',
            'task': 'question-0',
            'value': 'features-or-disk',
            'user_id': 'a',
            'subject_ids': 's1',
            'workflow_version': 'v1'
        }
    )


@pytest.fixture()
def cleaned_response():
        return pd.Series(
        {
            'classification_id': 'c1',
            'created_at': '2001-01-01',
            'task': 'question-0',
            'value': 'features-or-disk',
            'user_id': 'a',
            'subject_ids': 's1',
            'workflow_version': 'v1'
        }
    )





# The annotations should be flattened into a table like this
# of form [user, subject, task, value] for every user, subject, task and value
# this is the subject_question table
@pytest.fixture()
def responses():
    # simply rearranged version of raw input, should have markdown and original names
    # TODO multiple subject ids
    return [
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
    ]


@pytest.fixture()
def cleaned_responses(responses):
    clean_responses = responses.copy()  # base on responses (before cleaning)
    clean_responses[0]['value'] = 'features-or-disk'  # questions should have been renamed
    clean_responses[1]['value'] = 'no'
    clean_responses[2]['value'] = 'yes'
    clean_responses[3]['value'] = 'round'
    clean_responses[4]['value'] = 'no'
    clean_responses[5]['value'] = 'no'
    clean_responses[0]['task'] = 'question-0'  # answers should have been renamed
    clean_responses[1]['task'] = 'question-2'
    clean_responses[2]['task'] = 'question-9'
    clean_responses[3]['task'] = 'question-1'
    clean_responses[4]['task'] = 'question-2'
    clean_responses[5]['task'] = 'question-9'
    clean_responses[0]['created_at'] = pd.to_datetime(clean_responses[0]['created_at'])  # answers should have been renamed
    clean_responses[1]['created_at'] = pd.to_datetime(clean_responses[1]['created_at'])
    clean_responses[2]['created_at'] = pd.to_datetime(clean_responses[2]['created_at'])
    clean_responses[3]['created_at'] = pd.to_datetime(clean_responses[3]['created_at'])
    clean_responses[4]['created_at'] = pd.to_datetime(clean_responses[4]['created_at'])
    clean_responses[5]['created_at'] = pd.to_datetime(clean_responses[5]['created_at'])
    clean_responses = clean_responses[:6]  # should filter 6th classification, has empty response value
    return clean_responses


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