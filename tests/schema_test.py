import pytest

import pandas as pd

from gzreduction.schemas.schema import Schema, Question, Answer


@pytest.fixture()
def question_a():
    return Question(
        name='question-12',
        raw_name='T12',
        answers=[
            Answer(
                name='answer-0',
                raw_name='a-0'),
            Answer(
                name='answer-1',
                raw_name='a-1')
        ]
)

@pytest.fixture()
def question_b():
    return Question(
        name='question-13',
        raw_name='T13',
        answers=[
            Answer(
                name='answer-a',
                raw_name='a-0'),
            Answer(
                name='answer-b',
                raw_name='a-1')
        ]
    )


@pytest.fixture()
def some_schema(question_a, question_b):
    return Schema(questions=[question_a, question_b])


@pytest.fixture()
def df_to_rename():
    return pd.DataFrame([
            {
                'subject_id': 'subject_one',
                'T12_a-0': 1,
                'T12_a-1': 0,
                'T13_a-0': 0,
                'T13_a-1': 0
            },

            {
                'subject_id': 'subject_one',
                'T12_a-0': 0,
                'T12_a-1': 0,
                'T13_a-0': 0,
                'T13_a-1': 1
            },

            {
                'subject_id': 'subject_two',
                'T12_a-0': 1,
                'T12_a-1': 0,
                'T13_a-0': 0,
                'T13_a-1': 0
            }
        ])


def test_get_counts(question_a):
    count_cols = question_a.get_count_columns()
    assert count_cols == [
        'question-12_answer-0',
        'question-12_answer-1',
    ]


def test_get_all_raw_question_columns(some_schema):
    expected_question_columns = [
        'T12',
        'T13'
    ]
    assert some_schema.get_raw_question_names() == expected_question_columns


# # def test_get_raw_count_columns_schema(some_schema):
# #     expected_count_columns = [
# #         'T12_a-0',
# #         'T12_a-1',
# #         'T13_a-0',
# #         'T13_a-1',
# #     ]
# #     assert some_schema.get_raw_count_columns() == expected_count_columns


# # def test_get_count_columns(some_schema):
# #     expected_count_columns = [
# #         'question-12_answer-0',
# #         'question-12_answer-1',
# #         'question-13_answer-a',
# #         'question-13_answer-b',
# #     ]
# #     assert some_schema.get_count_columns() == expected_count_columns


# # def test_rename_df(df_to_rename, some_schema):
# #     new_df = some_schema.rename_df_using_schema(df_to_rename)
# #     expected_cols = {
# #         'question-12_answer-0',
# #         'question-12_answer-1',
# #         'question-13_answer-a',
# #         'question-13_answer-b'
# #     }
# #     for col in expected_cols:
# #         assert col in new_df.columns.values
