
import logging

import pandas as pd

from gzreduction.ouroborous import load_subjects
from gzreduction.deprecated import settings
from gzreduction import shared_utilities


def create_dr2_dr5_comparison():
    """
    Create prediction-and-subject tables to compare DR2 and DR5 classifications against NA10 (see Analysis folder)
    - Load the data-science-style predictions from DR2 and DR5
    - Load DR2 subjects, DR5 subjects and NA10 subjects
    - Identify shared subjects
    - Attach shared subjects to DR2 and DR5 predictions
    - Save prediction-and-subject tables

    Returns:
        None
    """

    new_calibration_subjects = False

    # TODO this needs to be refactored to load from raw subject export
    # Ouro. subjects export of dr1/dr2, metadata previously exploded
    dr2_subjects = pd.read_csv(settings.dr1_dr2_subject_loc)
    logging.info('DR2 subjects {}'.format(len(dr2_subjects)))

    # Panoptes subjects export of dr5, metadata exploded in load routine
    calibration_subjects = get_calibration_subjects(new_calibration_subjects)
    logging.info('Calibration subjects: {}'.format(len(calibration_subjects)))

    # match on ra/dec to identify galaxies shared in both subject sets (i.e. DR2/DR5 subjects)
    matched_subjects, unmatched_subjects = shared_utilities.match_galaxies_to_catalog_pandas(
        calibration_subjects,
        dr2_subjects,
        galaxy_suffix='',  # dr5 values are default
        catalog_suffix='_dr2')
    assert len(matched_subjects) == len(calibration_subjects)

    # load expert classifications, join with calibration subjects for DR2/DR5/NA10 subjects
    expert_subjects = pd.read_csv(settings.expert_catalog_loc, encoding='utf-8')
    matched_subjects_with_nair, _ = shared_utilities.match_galaxies_to_catalog_pandas(
        matched_subjects,
        expert_subjects,
        galaxy_suffix='',
        catalog_suffix='_nair')
    logging.info('DR2/DR5 subjects match to NA10: {}'.format(len(matched_subjects_with_nair)))

    logging.info('Reading DR2 predictions from {}'.format(settings.dr2_predictions_loc))
    dr2_predictions = pd.read_csv(settings.dr2_predictions_loc)

    logging.info('Reading DR5 predictions from {}'.format(settings.panoptes_predictions_loc))
    panoptes_predictions = pd.read_csv(settings.panoptes_predictions_loc)

    dr2_predictions = dr2_predictions.rename(columns={'subject_id': 'subject_id_dr2'})
    dr2_predictions_with_subject = pd.merge(
        dr2_predictions,
        matched_subjects_with_nair,
        on='subject_id_dr2',
        how='inner')
    logging.info('DR2 predictions for subjects in DR2/DR5/NA10: {}'.format(len(dr2_predictions_with_subject)))
    dr2_predictions_with_subject.to_csv(settings.dr2_predictions_with_subject_loc)

    calibration_predictions_with_subject = pd.merge(
        panoptes_predictions,
        matched_subjects_with_nair,
        on='subject_id',
        how='inner')
    logging.info('DR5 predictions for subjects in DR2/DR5/NA10: {}'.format(len(calibration_predictions_with_subject)))
    calibration_predictions_with_subject.to_csv(settings.calibration_predictions_with_subject_loc)


def get_calibration_subjects(new):
    if new:
        return load_subjects.load_current_subjects(
            settings.current_subjects_loc,
            workflow=settings.workflow,
            subject_set=settings.subject_set,
            save_loc=settings.calibration_subject_loc)
    else:
        return pd.read_csv(settings.calibration_subject_loc)


if __name__ == '__main__':

    logging.basicConfig(
        filename='is_panoptes_working.log',
        format='%(levelname)s:%(message)s',
        filemode='w',
        level=logging.DEBUG)

    create_dr2_dr5_comparison()
