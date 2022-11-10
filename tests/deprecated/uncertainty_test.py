# import pytest

# import numpy as np
# import matplotlib.pyplot as plt

# from gzreduction.votes_to_predictions import uncertainty


# # don't use a fixture, keep as callable function
# def get_votes(n_votes, p):
#     # run a 1-trial binomial test (i.e. bernoulli) of success prob. p,  n_votes times.
#     return np.random.binomial(1, p, n_votes)


# def test_uncertainty():  # get votes is not a fixture, don't pass
#     true_p = 0.5
#     votes = get_votes(20, true_p)
#     print(votes)
#     v_uncertainty, (v_min, v_max) = uncertainty.binomial_uncertainty(votes, interval=0.9)
#     print(v_min, v_max)
#     assert v_uncertainty > 0
#     assert v_min > 0
#     assert v_min < 1
#     assert v_max > 0
#     assert v_max < 1
#     assert v_max - v_min == v_uncertainty


# def test_uncertainty_with_no_successes():
#     votes = np.zeros(40, dtype=int)
#     v_uncertainty, (v_min, v_max) = uncertainty.binomial_uncertainty(votes, interval=0.9)
#     assert v_uncertainty > 0
#     assert v_min >= 0
#     assert v_min < 0.5
#     assert v_max > 0
#     assert v_max < 0.5
#     assert v_max - v_min == v_uncertainty


# def test_uncertainty_with_all_successes():
#     votes = np.ones(40, dtype=int)
#     v_uncertainty, (v_min, v_max) = uncertainty.binomial_uncertainty(votes, interval=0.9)
#     assert v_uncertainty > 0
#     assert v_min > 0.5
#     assert v_min < 1.
#     assert v_max > 0.5
#     assert v_max <= 1.
#     assert v_max - v_min == v_uncertainty


# # not an automated test - only run when relevant
# # def test_uncertainty_visually():
# #     n_trials_per_p = 40
# #     p_values = [0.1, 0.5, 0.9]
# #     p_est = []
# #     p_err = []
# #     all_p_true = []
# #     for p_true in p_values:
# #         for trial in range(n_trials_per_p):
# #             all_p_true.append(p_true)
# #             votes = get_votes(20, p_true)
# #             p = votes.mean()
# #             p_est.append(p)
# #             v_uncertainty, (v_min, v_max) = uncertainty.binomial_uncertainty(votes, interval=0.8)
# #             p_err.append((p - v_min, v_max - p))
# #
# #     p_err = np.array(p_err)
# #     plt.errorbar(x=p_est, y=range(len(p_est)), xerr=np.transpose(p_err), fmt='o')
# #     plt.scatter(x=all_p_true, y=range(len(p_est)), c='r')
# #     plt.legend(['True', 'Estimated (N=20, conf=80%)'])
# #     plt.axhline(y=40, color='k', linestyle='--', linewidth=1.)
# #     plt.axhline(y=80, color='k', linestyle='--', linewidth=1.)
# #     plt.xlabel('Vote Fraction')
# #     plt.ylabel('Fake Subject')
# #     plt.tight_layout()
# #     plt.savefig('uncertainty_test.png')
