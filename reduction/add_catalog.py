
import logging

import pandas as pd
from astropy.table import Table

from reduction import settings
from reduction import shared_utilities


def add_catalog_to_predictions():
    """
    Link prediction table created from classifications with previous subject export
    Final step in reduction pipeline
    - Load predictions from panoptes_predictions_loc
    - Filter out artifacts and predictions with N volunteer responses < 36
    - Load latest subject extract
    - Filter subjects to DECALS DR5 workflow and expand metadata
    - Merge subjects to predictions on subject_id
    - Load joint catalog (includes all NSA data)
    - Merge subjects/predictions to catalog on ra/dec of subjects
    - Save prediction/subject/catalog tables

    Returns:
        None
    """

    # previously filtered to be after start of public DR5 release
    all_predictions = pd.read_csv(settings.panoptes_predictions_loc)
    not_artifact = all_predictions['smooth-or-featured_prediction'] != 'artifact'
    fully_classified = all_predictions['smooth-or-featured_total-votes'] > 35  # a few are slightly under 40
    # more filters if required
    predictions_to_save = all_predictions[not_artifact & fully_classified]
    logging.info('Loaded {} predictions'.format(len(predictions_to_save)))

    uploaded_subjects = pd.read_csv(
        settings.panoptes_old_style_subjects_loc,
        dtype={'workflow_id': str})
    # 'subjects_already_added' includes any subject id duplicates: each workflow will have a row for that subject_id
    subjects_already_added = uploaded_subjects[uploaded_subjects['workflow_id'].astype(str) == '6122']
    # expand out ra/dec for later
    subjects_already_added = shared_utilities.load_current_subjects(subjects_already_added, workflow='6122')

    # join subjects to predictions
    subjects_and_predictions = pd.merge(predictions_to_save, subjects_already_added, on='subject_id', how='inner')
    logging.info('Matched {} predictions with subjects'.format(len(subjects_and_predictions)))

    # Panoptes subjects export of dr5, metadata exploded in load routine
    nsa_version = '1_0_0'  # to select which joint catalog
    data_release = '5'
    catalog_loc = '/data/galaxy_zoo/decals/catalogs/dr{}_nsa_v{}_to_upload.fits'.format(data_release, nsa_version)
    catalog = shared_utilities.astropy_table_to_pandas(Table.read(catalog_loc))  # joint catalog loc
    logging.info('Catalog galaxies: {}'.format(len(catalog)))

    # join catalog to subjects_and_predictions
    predictions_with_catalog, _ = shared_utilities.match_galaxies_to_catalog_pandas(
        galaxies=subjects_and_predictions,
        catalog=catalog,
        galaxy_suffix='_reduced'
    )
    logging.info('Identified {} galaxies in catalog with predictions'.format(len(predictions_with_catalog)))

    predictions_with_catalog.to_csv(settings.panoptes_predictions_with_catalog_loc)
    logging.info('Saved {} galaxies with predictions to {}'.format(
        len(predictions_with_catalog),
        settings.panoptes_predictions_with_catalog_loc))


if __name__ == '__main__':

    logging.basicConfig(
        filename='add_catalog.log',
        format='%(levelname)s:%(message)s',
        filemode='w',
        level=logging.DEBUG)

    add_catalog_to_predictions()
