
"""
Classification data
"""
# dr2 voting data to load
dr1_classifications_export_loc = '/data/repos/galaxy-zoo-panoptes/reduction/data/raw/classifications/2017-10-15_galaxy_zoo_decals_classifications.csv'
dr2_classifications_export_loc = '/data/repos/galaxy-zoo-panoptes/reduction/data/raw/classifications/2017-10-15_galaxy_zoo_decals_dr2_classifications.csv'

# dr1/2 data transformed into vote columns
dr2_votes_loc = '/data/repos/galaxy-zoo-panoptes/reduction/data/votes/dr2_votes.csv'
# dr1/2 data transformed into summed vote columns
dr2_aggregated_votes_loc = '/data/repos/galaxy-zoo-panoptes/reduction/data/votes/dr2_aggregated_votes.csv'
# dr1/2 data transformed into prediction table
dr2_predictions_loc = '/data/repos/galaxy-zoo-panoptes/reduction/data/predictions/dr2_predictions.csv'

# panoptes data to load
panoptes_old_style_classifications_loc = '/Data/repos/galaxy-zoo-panoptes/reduction/data/raw/classifications/2018-09-28_panoptes_classifications.csv'
# panoptes data transformed into vote columns
panoptes_votes_loc = '/data/repos/galaxy-zoo-panoptes/reduction/data/votes/2018-09-28_panoptes_votes.csv'
# panoptes data transformed into vote columns and reduced by subject
panoptes_aggregated_votes_loc = '/data/repos/galaxy-zoo-panoptes/reduction/data/votes/2018-09-28_panoptes_aggregated_votes.csv'
# panoptes data transformed into prediction table
panoptes_predictions_loc = '/data/repos/galaxy-zoo-panoptes/reduction/data/predictions/2018-09-28_panoptes_predictions.csv'


"""
Subject data
"""

# expert classifications from Nair 2010, interpreted during decals routine
# expert_catalog_loc = '/data/repos/galaxy-zoo-panoptes/reduction/data/raw/nair_sdss_catalog_interpreted.fits'
expert_catalog_loc = '/data/galaxy_zoo/decals/catalogs/nair_sdss_catalog_interpreted.csv'

# dr1/dr2 subjects, already extracted and with expanded metadata via decals repo
# minor overwrite transforming subject_id to not include 'object wrapper' TODO move to original extraction
dr1_dr2_subject_loc = '/data/repos/galaxy-zoo-panoptes/reduction/data/subjects/decals_dr1_and_dr2.csv'

# raw subjects from panoptes
current_subjects_loc = '/data/repos/galaxy-zoo-panoptes/reduction/data/raw/subjects/new_subjects.csv'  # TODO what is this?? Still relevant?
panoptes_old_style_subjects_loc = '/Data/repos/galaxy-zoo-panoptes/reduction/data/raw/subjects/2018-09-28_panoptes_subjects.csv'
# expand metadata and save calibration subjects
workflow = '6122'
subject_set = '19832'
calibration_subject_loc = '/data/repos/galaxy-zoo-panoptes/reduction/data/subjects/calibration_subjects.csv'

"""
Final output data: prediction tables attached to subject and Nair info
"""
dr2_predictions_with_subject_loc = '/data/repos/galaxy-zoo-panoptes/reduction/data/output/dr2_predictions_with_subject.csv'
calibration_predictions_with_subject_loc = '/data/repos/galaxy-zoo-panoptes/reduction/data/output/calibration_predictions_with_subject.csv'

panoptes_predictions_with_catalog_loc = '/data/repos/galaxy-zoo-panoptes/reduction/data/output/2018-09-28_panoptes_predictions_with_catalog.csv'