import pytest

import numpy as np
import pandas as pd

from gzreduction.votes_to_predictions import reduced_votes_to_predictions, uncertainty
from gzreduction.schemas.schema import Schema, Question, Answer


@pytest.fixture()
def question_a():
    question_a = Question(
        name='question-12',
        raw_name='T12'
    )
    question_a.set_answers([
        Answer(
            name='answer-0',
            raw_name='a-0'),
        Answer(
            name='answer-1',
            raw_name='a-1'),
    ])
    return question_a


@pytest.fixture()
def question_b():
    question_b = Question(
        name='question-13',
        raw_name='T13'
    )
    question_b.set_answers([
        Answer(
            name='answer-a',
            raw_name='a-0'),
        Answer(
            name='answer-b',
            raw_name='a-1'),
        Answer(
            name='answer-c',
            raw_name='a-2')
    ])
    return question_b


@pytest.fixture()
def schema(question_a, question_b):
    schema = Schema()
    schema.add_question(question_a)
    schema.add_question(question_b)
    return schema


@pytest.fixture()
def subject_row():
    return pd.Series({
            'subject_id': 'subject_one',
            'question-12_answer-0': 1,
            'question-12_answer-1': 1,  # 1 each, tied votes
            'question-13_answer-a': 0,
            'question-13_answer-b': 1,
            'question-12_total-votes': 1,
            'question-13_total-votes': 1,
        })


@pytest.fixture()
def reduced_df():
    return pd.DataFrame([

        {
            'subject_id': 'subject_one',
            'question-12_answer-0': 1,
            'question-12_answer-1': 1,  # 1 each, tied votes
            'question-13_answer-a': 0,
            'question-13_answer-b': 1,
            'question-13_answer-c': 0,
            'question-12_total-votes': 1,
            'question-13_total-votes': 1,
        },


        {
            'subject_id': 'subject_two',
            'question-12_answer-0': 0,
            'question-12_answer-1': 1,
            'question-13_answer-a': 0,
            'question-13_answer-b': 0,
            'question-13_answer-c': 0,
            'question-12_total-votes': 1,
            'question-13_total-votes': 0,
        },

    ])


@pytest.fixture()
def prediction_df():
    return pd.DataFrame([

        {
            'subject_id': 'subject_one',
            'question-12_answer-0': 1,
            'question-12_answer-1': 1,  # 1 each, tied votes
            'question-12_prediction': 'answer-0',
            'question-12_prediction_conf': 0.5,
            'question-12_total-votes': 1,

            'question-13_answer-a': 0,
            'question-13_answer-b': 1,
            'question-13_prediction': 'answer-b',
            'question-13_prediction_conf': 1.,
            'question-13_total-votes': 1,
        },


        {
            'subject_id': 'subject_two',
            'question-12_answer-0': 0,
            'question-12_answer-1': 1,
            'question-12_prediction': 'answer-1',
            'question-12_prediction_conf': 1.,
            'question-12_total-votes': 1,

            'question-13_answer-a': 0,
            'question-13_answer-b': 0,
            'question-13_answer-c': 0,
            'question-13_prediction': 'answer-a',
            'question-13_prediction_conf': 1./3.,  # no votes for any, tied votes-> confidence in 'a' of 0.33
            'question-13_total-votes': 0,
        },

    ])


def test_get_predictions(reduced_df, schema):

    prediction_df = reduced_votes_to_predictions.get_predictions(reduced_df, schema)

    subject_a = prediction_df.iloc[0]
    assert subject_a['question-12_prediction'] == 'answer-0'  # if equal, default to first value. Reasonable?
    assert subject_a['question-12_prediction-conf'] == 0.5  # conf still accurate, equal odds
    assert subject_a['question-13_prediction'] == 'answer-b'
    assert subject_a['question-13_prediction-conf'] == 1.
    _, (min_p, max_p) = uncertainty.binomial_uncertainty(np.array([0]), interval=0.8)  # no votes for subject one, q13.a
    assert subject_a['subject_id'] == 'subject_one'
    assert subject_a['question-13_answer-a_min'] == min_p
    assert subject_a['question-13_answer-a_max'] == max_p

    subject_b = prediction_df.iloc[1]
    assert subject_b['question-12_prediction'] == 'answer-1'  # if equal and no votes, default to first value.
    assert subject_b['question-12_prediction-conf'] == 1.  # conf still accurate, equal odds
    assert subject_b['question-13_prediction'] == 'answer-a'
    assert subject_b['question-13_prediction-conf'] == 1./3.
    _, (min_p, max_p) = uncertainty.binomial_uncertainty(np.array([0]), interval=0.8)  # no votes for subject two, q12.0
    assert subject_b['subject_id'] == 'subject_two'
    assert subject_b['question-12_answer-0_min'] == min_p
    assert subject_b['question-12_answer-0_max'] == max_p


def test_votes_from_subject_row(subject_row, schema):
    question = schema.get_question_from_name('question-12')
    answer = question.get_answer_from_name('answer-0')
    votes = reduced_votes_to_predictions.votes_from_subject_row(subject_row, question, answer)
    assert votes == np.array(1)

    question = schema.get_question_from_name('question-13')
    answer = question.get_answer_from_name('answer-b')
    votes = reduced_votes_to_predictions.votes_from_subject_row(subject_row, question, answer)
    assert votes == np.array(1)


def test_get_encoders(schema):
    encoders = reduced_votes_to_predictions.get_encoders(schema)
    assert encoders['question-12'].inverse_transform([0]) == 'answer-0'
    assert encoders['question-12'].inverse_transform([1]) == 'answer-1'
    assert encoders['question-13'].inverse_transform([0]) == 'answer-a'
    assert encoders['question-13'].inverse_transform([1]) == 'answer-b'
    assert encoders['question-13'].inverse_transform([2]) == 'answer-c'
    with pytest.raises(ValueError):
        encoders['question-12'].inverse_transform([2])
    with pytest.raises(ValueError):
        encoders['question-13'].inverse_transform([3])


def test_encode_answers(prediction_df, schema):
    encoded_df = reduced_votes_to_predictions.encode_answers(prediction_df, schema)
    print(encoded_df)

    subject_a = prediction_df.iloc[0]
    assert subject_a['question-12_prediction-encoded'] == 0
    assert subject_a['question-13_prediction-encoded'] == 1

    subject_b = prediction_df.iloc[1]
    assert subject_b['question-12_prediction-encoded'] == 1
    assert subject_b['question-13_prediction-encoded'] == 0


# classifier_df['decals-0_truth_encoded'].value_counts()
# classifier_df['decals-0_prediction_encoded'].value_counts()
#
# ### Side note - check I got the right question/answers
# classifier_df['decals-8_prediction_encoded'].value_counts()