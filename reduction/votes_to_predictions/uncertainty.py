
import numpy as np
from statsmodels.stats.proportion import proportion_confint


def get_min_p_estimate(votes, interval=None):
    _, (min_p, _) = binomial_uncertainty(votes, interval)
    return min_p


def get_max_p_estimate(votes, interval=None):
    _, (_, max_p) = binomial_uncertainty(votes, interval)
    return max_p


def binomial_uncertainty(votes, interval=None):
    """
    Use wilson interval to calculate (exactly) binomial uncertainty on votes
    Args:
        votes (np.array): votes of 0 or 1
        interval (float): confidence interval for p+, p- limits e.g. 0.95

    Returns:
        (float) size of uncertainty interval
        (tuple) of form (min p value, max p value)
    """
    if len(votes) == 0:
        return np.nan, (np.nan, np.nan)

    # calculate wilson interval http://www.ucl.ac.uk/english-usage/staff/sean/resources/binomialpoisson.pdf
    # http://www.statsmodels.org/dev/generated/statsmodels.stats.proportion.proportion_confint.html
    p_minus, p_plus = proportion_confint(
        votes.sum(), len(votes), alpha=1 - interval, method='wilson')
    return p_plus - p_minus, (p_minus, p_plus)
