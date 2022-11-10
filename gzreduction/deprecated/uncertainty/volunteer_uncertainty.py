#
# import numpy as np
# import pandas as pd
# import sympy as sp
#
# from gzreduction import dr5_schema, schema_utilities
#
#
# # vote_table_loc = 'reduction/data/votes/panoptes_votes.csv'
# # votes = pd.read_csv(vote_table_loc)
#
#
# def get_uncertainty_for_answer(subject_votes, question, answer):  # TODO really need to update schema
#     total_answers = question + '_total-votes'
#     # model as Bernoulli random variable
#     observations = np.array([1] * subject_votes[question + '_' + answer] + [0] * subject_votes[total_answers])
#     return bernoulli_information(observations)
#     # TODO infer information in the context of full sample distribution, with min. free parameters
#
#
# def bernoulli_p_estimate(observations):
#     return np.mean(observations)  # can be shown analytically
#
#
# def bernoulli_information(observations):
#     # fisher information https://en.wikipedia.org/wiki/Fisher_information
#     # http://www.chadfulton.com/topics/bernoulli_trials_classical.html#analytic_mle
#     p = bernoulli_p_estimate(observations)
#     return p * (1 - p) / len(observations)
#
#
#
#
# # def binomial_likelihood():
# #     # p of success = theta, T draws, s successes
# #
# #     t, T, s = sp.symbols('theta, T, s')
# #
# #     # http: // www.chadfulton.com / topics / bernoulli_trials_classical.html  # analytic_mle
# #     # Create the functions symbolically
# #     likelihood = (t ** s) * (1 - t) ** (T - s)
# #     # loglike = s * sp.log(t) + (T - s) * sp.log(1 - t)
# #     # score = (s / t) - (T - s) / (1 - t)
# #     # information = T / (s * (1 - s))
# #
# #     # Convert them to Numpy-callable functions
# #     _likelihood = sp.lambdify((t, T, s), likelihood, modules='numpy')
# #     # _loglike = sp.lambdify((t, T, s), loglike, modules='numpy')
# #     # _score = sp.lambdify((t, T, s), score, modules='numpy')
# #     # _information = sp.lambdify((t, T, s), information, modules='numpy')
# #
# #     # interpolate likelihood for all theta and return as callable function
# #     _likelihood()
#
#
#     # return