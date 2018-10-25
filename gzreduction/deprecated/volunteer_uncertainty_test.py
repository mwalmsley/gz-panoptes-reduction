# import pytest
#
# import pandas as pd
# import numpy as np
# import matplotlib.pyplot as plt
#
# from gzreduction import volunteer_uncertainty, schema_utilities
#
#
# @pytest.fixture()
# def schema():
#     return {
#
#         'T12': {
#             'question': 'question-12',
#             'answers': {
#                 'a-0': 'answer-0',
#                 'a-1': 'answer-1'
#             }
#         },
#
#         'T13': {
#             'question': 'question-13',
#             'answers': {
#                 'a-0': 'answer-a',
#                 'a-1': 'answer-b'
#             }
#         }
#
#     }
#
#
# @pytest.fixture()
# def votes_df():
#     return pd.DataFrame([
#
#         {
#             'subject_id': 'subject_one',
#             'question-12_answer-0': 1,
#             'question-12_answer-1': 0,
#             'question-13_answer-a': 0,
#             'question-13_answer-b': 1
#         },
#
#         {
#             'subject_id': 'subject_one',
#             'question-12_answer-0': 0,
#             'question-12_answer-1': 1,
#             'question-13_answer-a': 0,
#             'question-13_answer-b': 1
#         },
#
#         {
#             'subject_id': 'subject_two',
#             'question-12_answer-0': 1,
#             'question-12_answer-1': 0,
#             'question-13_answer-a': 0,
#             'question-13_answer-b': 1
#         },
#
#     ])
#
#
# @pytest.fixture()
# def subject_votes():
#     return pd.Series({
#             'subject_id': 'subject_one',
#             'question-12_answer-0': 1,
#             'question-12_answer-1': 0,
#             'question-13_answer-a': 0,
#             'question-13_answer-b': 1,
#             'question-12_total-votes': 1,
#             'question-13_total-votes': 1
#         })
#
#
# @pytest.fixture()
# def question():
#     return 'T12'  # should switch to using renamed questions/answers by this point
#
#
# @pytest.fixture()
# def answer():
#     return 'question-12_answer-0'
#
#
# @pytest.fixture()
# def observations():
#     return [0, 0, 1, 0, 0, 0, 1]
#
#
# def test_bernoulli_information(observations):
#     information = volunteer_uncertainty.bernoulli_information(observations)
#
# #
# # def test_visualise_bernoulli_information():
# #     trials = 40
# #     trial_n = range(1, trials)
# #     obs_low = np.random.rand(trials) < 0.1
# #     obs_med = np.random.rand(trials) < 0.5
# #     obs_high = np.random.rand(trials) < 0.9
# #     for obs in [obs_low, obs_med, obs_high]:
# #         plt.plot(list(trial_n), list(map(lambda x: volunteer_uncertainty.bernoulli_information(obs[:x]), trial_n)))
# #     plt.xlabel('Trials')
# #     plt.ylabel('Information after N trials')
# #     plt.legend(['Low', 'Med', 'High'])
# #     plt.savefig('bernoulli_information.png')
# #     # symmetric, should require at least 1 vote for each option else 0, reduces with more observations
# #     # much more sensitive to changes near 0.5
#
#     """
#     Near the maximum likelihood estimate, low Fisher information therefore indicates that the maximum appears "blunt",
#     that is, the maximum is shallow and there are many nearby values with a similar log-likelihood.
#     Conversely, high Fisher information indicates that the maximum is sharp.
#
#     For Bernoulli, this is the reciprocal of the variance of the mean number of successes
#      """
#
#
# def test_get_uncertainty_for_answer(subject_votes, question, answer):
#     subject_uncertainty = volunteer_uncertainty.get_uncertainty_for_answer(subject_votes, question, answer)
#     print(subject_uncertainty)
#
#
# def test_get_uncertainty_for_question(subject_votes, question, schema):
#     for answer in schema_utilities.get_answers_for_question(schema, question):
#         subject_votes[answer + '_info'] = volunteer_uncertainty.get_uncertainty_for_answer(subject_votes, question, answer)
#     print(subject_votes)
