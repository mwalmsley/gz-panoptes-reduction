import pytest

import pandas as pd
import numpy as np

from gzreduction.ouroborous import ouroborous_extract_to_votes
from gzreduction.schemas.schema import Schema, Question, Answer


# TODO rename to actual decals style: decals-9, etc.

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
            raw_name='a-1')
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
            raw_name='a-1')
    ])
    return question_b


@pytest.fixture()
def some_schema(question_a, question_b):
    some_schema = Schema()
    some_schema.add_question(question_a)
    some_schema.add_question(question_b)
    return some_schema


@pytest.fixture()
def classification_df():
    return pd.DataFrame([

        {
            'subject_id': 'subject_one',
            'T12': 'a-0',
            'T13': np.nan
        },

        {
            'subject_id': 'subject_one',
            'T12': np.nan,
            'T13': 'a-1'
        },

        {
            'subject_id': 'subject_two',
            'T12': np.nan,
            'T13': 'a-0'
        },

    ])


def test_explode_df(classification_df, some_schema):
    exploded_df = ouroborous_extract_to_votes.explode_df(classification_df, some_schema)
    assert all(np.equal(exploded_df['T12_a-0'], [1, 0, 0]))
    assert all(np.equal(exploded_df['T12_a-1'], [0, 0, 0]))
    assert all(np.equal(exploded_df['T13_a-0'], [0, 0, 1]))
    assert all(np.equal(exploded_df['T13_a-1'], [0, 1, 0]))


# should be tested in schema_utilities, but more convenient here
def test_rename_df_using_schema(classification_df, some_schema):
    exploded_df = ouroborous_extract_to_votes.explode_df(classification_df, some_schema)  # re-use, this is not independent
    renamed_df = some_schema.rename_df_using_schema(exploded_df)
    assert all(np.equal(renamed_df['question-12_answer-0'], [1, 0, 0]))
    assert all(np.equal(renamed_df['question-12_answer-1'], [0, 0, 0]))
    assert all(np.equal(renamed_df['question-13_answer-a'], [0, 0, 1]))
    assert all(np.equal(renamed_df['question-13_answer-b'], [0, 1, 0]))

# TODO test case for removing rare answers
