# import pytest

# import numpy as np
# import pandas as pd

# from gzreduction.votes_to_predictions import reduce_votes
# from gzreduction.schemas.schema import Schema, Question, Answer


# @pytest.fixture()
# def question_a():
#     question_a = Question(
#         name='question-12',
#         raw_name='T12',
#         answers=[
#             Answer(
#                 name='answer-0',
#                 raw_name='a-0'),
#             Answer(
#                 name='answer-1',
#                 raw_name='a-1')
#             ],
#     )
#     return question_a


# @pytest.fixture()
# def question_b():
#     question_b = Question(
#         name='question-13',
#         raw_name='T13',
#         answers=[
#             Answer(
#                 name='answer-a',
#                 raw_name='a-0'),
#             Answer(
#                 name='answer-b',
#                 raw_name='a-1')
#         ]
#     )
#     return question_b


# @pytest.fixture()
# def some_schema(question_a, question_b):
#     return Schema(questions=[question_a, question_b])



# @pytest.fixture()
# def votes_df():
#     return pd.DataFrame([

#         {
#             'subject_id': 'subject_one',
#             'question-12_answer-0': 1,
#             'question-12_answer-1': 0,
#             'question-13_answer-a': 0,
#             'question-13_answer-b': 1
#         },

#         {
#             'subject_id': 'subject_one',
#             'question-12_answer-0': 0,
#             'question-12_answer-1': 1,
#             'question-13_answer-a': 0,
#             'question-13_answer-b': 1
#         },

#         {
#             'subject_id': 'subject_two',
#             'question-12_answer-0': 1,
#             'question-12_answer-1': 0,
#             'question-13_answer-a': 0,
#             'question-13_answer-b': 1
#         },

#     ])


# def test_reduce_all_questions(votes_df, some_schema):
#     reduced_df = reduce_votes.reduce_all_questions(votes_df.copy(), some_schema)
#     assert all(np.equal(reduced_df['question-12_answer-0'], [1, 1]))
#     assert all(np.equal(reduced_df['question-12_answer-1'], [1, 0]))
#     assert all(np.equal(reduced_df['question-13_answer-a'], [0, 0]))
#     assert all(np.equal(reduced_df['question-13_answer-b'], [2, 1]))
#     assert all(np.equal(reduced_df['question-12_total-votes'], [2, 1]))
#     assert all(np.equal(reduced_df['question-13_total-votes'], [2, 1]))
