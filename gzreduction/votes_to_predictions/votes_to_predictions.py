
import logging

import pandas as pd

from gzreduction import settings
from gzreduction.votes_to_predictions import reduce_votes, reduced_votes_to_predictions
from gzreduction.schemas.dr2_schema import dr2_schema
from gzreduction.schemas.dr5_schema import dr5_schema


def votes_to_predictions(votes, schema, reduced_save_loc=None, predictions_save_loc=None):
    """
    Reduce volunteer votes by subject and make data science style predictions
    Args:
        votes (pd.DataFrame): rows of classifications, columns of question_answer, value of count [0, 1]
        schema (Schema): question/answer/column definition object
        reduced_save_loc (str): location to save reduced votes (rows=subjects, columns=question_answer, values=sum)
        predictions_save_loc (str): location to save predictions (reduced votes with convenience columns added)

    Returns:
        (pd.DataFrame) reduced votes with data science convenience columns e.g. _truth_encoded, _prediction_conf
    """

    aggregated = reduce_votes.reduce_all_questions(
        votes, schema, ignore_users=True, save_loc=reduced_save_loc)

    logging.debug(aggregated.columns.values)

    predictions = reduced_votes_to_predictions.reduced_votes_to_predictions(
        aggregated, schema, save_loc=predictions_save_loc)

    return predictions


if __name__ == '__main__':

    logging.basicConfig(
        # filename='votes_to_predictions.log',
        format='%(asctime)s %(message)s',
        # filemode='w',
        level=logging.DEBUG)

    new_panoptes = False
    new_ouroborous = True

    if new_panoptes:
        logging.info('Creating new Panoptes predictions from {}'.format(settings.panoptes_votes_loc))
        panoptes_votes = pd.read_csv(settings.panoptes_votes_loc)

        logging.debug(panoptes_votes['value'].value_counts())

        votes_to_predictions(
            panoptes_votes,
            dr5_schema,
            reduced_save_loc=settings.panoptes_aggregated_votes_loc,
            predictions_save_loc=settings.panoptes_predictions_loc
        )

    if new_ouroborous:
        logging.info('Creating new Ourobours predictions from {}'.format(settings.dr2_votes_loc))
        dr2_votes = pd.read_csv(settings.dr2_votes_loc)
        votes_to_predictions(
            dr2_votes,
            dr2_schema,
            reduced_save_loc=settings.dr2_aggregated_votes_loc,
            predictions_save_loc=settings.dr2_predictions_loc
        )
