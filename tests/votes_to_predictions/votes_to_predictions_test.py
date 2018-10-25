import pytest

import numpy as np
import pandas as pd

from gzreduction.schemas.schema import Schema, Question, Answer
from gzreduction.votes_to_predictions import votes_to_predictions, uncertainty


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
def votes():
    votes = pd.DataFrame([

        {
            'classification_id': 'c1',
            'created_at': '2001-01-01',
            'question-0_features-or-disk': 1,
            'question-1_round': 0,
            'question-2_no': 0,
            'question-9_no': 0,
            'question-9_yes': 0,
            'subject_id': 's1',
            'user_id': 'a',
        },

        {
            'classification_id': 'c2',
            'created_at': '2001-01-02',
            'question-0_features-or-disk': 0,
            'question-1_round': 1,
            'question-2_no': 0,
            'question-9_no': 0,
            'question-9_yes': 0,
            'subject_id': 's1',
            'user_id': 'b',
        },

        {
            'classification_id': 'c1',
            'created_at': '2001-01-01',
            'question-0_features-or-disk': 0,
            'question-1_round': 0,
            'question-2_no': 1,
            'question-9_no': 0,
            'question-9_yes': 0,
            'subject_id': 's1',
            'user_id': 'a',
        },

        {
            'classification_id': 'c2',
            'created_at': '2001-01-02',
            'question-0_features-or-disk': 0,
            'question-1_round': 0,
            'question-2_no': 1,
            'question-9_no': 0,
            'question-9_yes': 0,
            'subject_id': 's1',
            'user_id': 'b',
        },

        {
            'classification_id': 'c1',
            'created_at': '2001-01-01',
            'question-0_features-or-disk': 0,
            'question-1_round': 0,
            'question-2_no': 0,
            'question-9_no': 0,
            'question-9_yes': 1,
            'subject_id': 's1',
            'user_id': 'a',
        },

        {
            'classification_id': 'c2',
            'created_at': '2001-01-02',
            'question-0_features-or-disk': 0,
            'question-1_round': 0,
            'question-2_no': 0,
            'question-9_no': 1,
            'question-9_yes': 0,
            'subject_id': 's1',
            'user_id': 'b',
        }
    ])
    votes['created_at'] = pd.to_datetime(votes['created_at'])  # avoid datetime comparison hassle
    return votes


def test_votes_to_predictions(votes, schema):
    predictions = votes_to_predictions.votes_to_predictions(votes, schema)

    assert len(predictions) == 1
    subject_pred = predictions.squeeze()

    expected_series = pd.Series({
        'question-0': None,
        'question-2': None,
        'question-9': None,
        'subject_id': 's1',
        'question-0_features-or-disk': 1,
        'question-1_round': 1,
        'question-2_no': 2,
        'question-9_no': 1,
        'question-9_yes': 1,
        'question-0_features-or-disk_min': uncertainty.get_min_p_estimate(np.array([1]), interval=0.8),
        'question-0_features-or-disk_max': uncertainty.get_max_p_estimate(np.array([1]), interval=0.8),
        'question-1_round_min': uncertainty.get_min_p_estimate(np.array([1]), interval=0.8),
        'question-1_round_max': uncertainty.get_max_p_estimate(np.array([1]), interval=0.8),
        'question-2_no_min': uncertainty.get_min_p_estimate(np.array([1, 1]), interval=0.8),
        'question-2_no_max': uncertainty.get_max_p_estimate(np.array([1, 1]), interval=0.8),
        'question-9_no_min': uncertainty.get_min_p_estimate(np.array([1, 0]), interval=0.8),
        'question-9_no_max': uncertainty.get_max_p_estimate(np.array([1, 0]), interval=0.8),
        'question-9_yes_min': uncertainty.get_min_p_estimate(np.array([1, 0]), interval=0.8),
        'question-9_yes_max': uncertainty.get_max_p_estimate(np.array([1, 0]), interval=0.8),
        'question-0_total_votes': 1,
        'question-0_features-or-disk_fraction': 1,
        'question-1_total_votes': 1,
        'question-1_round_fraction': 1,
        'question-2_total_votes': 1,
        'question-2_no_fraction': 1,
        'question-9_total_votes': 2,
        'question-9_no_fraction': 0.5,
        'question-9_yes_fraction': 0.5,
        'question-0_prediction': 'features-or-disk',
        'question-0_total-votes': 1,
        'question-0_prediction-conf': 1,
        'question-1_prediction': 'round',
        'question-1_total-votes': 1,
        'question-1_prediction-conf': 1,
        'question-2_prediction': 'no',
        'question-2_total-votes': 2,
        'question-2_prediction-conf': 1,
        'question-9_prediction': 'no',
        'question-9_total-votes': 2,
        'question-9_prediction-conf': 0.5,
        'question-0_prediction-encoded': 0,
        'question-1_prediction-encoded': 0,
        'question-2_prediction-encoded': 0,
        'question-9_prediction-encoded': 0
    })

    for col in subject_pred.keys():
        print(col)
        assert subject_pred[col] == expected_series[col]
