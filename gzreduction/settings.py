import os

working_dir = '/media/walml/beta/galaxy_zoo/decals/gzreduction_ouroborous/working_dir'

export_date = '2019-04-02'

"""
Classification data
"""
# dr2 voting data to load
dr1_classifications_export_loc = os.path.join(working_dir, 'raw/classifications/2017-10-15_galaxy_zoo_decals_classifications.csv')
dr2_classifications_export_loc = os.path.join(working_dir, 'raw/classifications/2017-10-15_galaxy_zoo_decals_dr2_classifications.csv')

# dr1/2 data transformed into vote columns
dr2_votes_loc = os.path.join(working_dir, 'votes/dr2_votes.csv')
# dr1/2 data transformed into summed vote columns
dr2_aggregated_votes_loc = os.path.join(working_dir, 'votes/dr2_aggregated_votes.csv')
# dr1/2 data transformed into prediction table
dr2_predictions_loc = os.path.join(working_dir, 'predictions/dr2_predictions.csv')

# panoptes data to load
panoptes_old_style_classifications_loc = os.path.join(working_dir, 'raw/classifications/extracts/{}_panoptes-classifications.csv'.format(export_date))
panoptes_extract_json_loc = os.path.join(working_dir, 'raw/classifications/extracts/{}_panoptes-classifications-json.txt'.format(export_date))

panoptes_api_json_dir = os.path.join(working_dir, 'raw/classifications/api')  # TODO automatic date or lastid?

# panoptes data as clean flat time/user/question/answer csv
panoptes_flat_classifications = os.path.join(working_dir, 'preprocessed/{}_panoptes-classifications-flattened.csv'.format(export_date))
# panoptes data transformed into vote columns
panoptes_votes_loc = os.path.join(working_dir, 'votes/{}_panoptes_votes.csv'.format(export_date))
# panoptes data transformed into vote columns and reduced by subject
panoptes_aggregated_votes_loc = os.path.join(working_dir, 'votes/{}_panoptes_aggregated_votes.csv'.format(export_date))
# panoptes data transformed into prediction table
panoptes_predictions_loc = os.path.join(working_dir, 'predictions/{}_panoptes_predictions.csv'.format(export_date))


"""
Subject data
"""

# expert classifications from Nair 2010, interpreted during decals routine
# expert_catalog_loc = os.path.join(working_dir, 'raw/nair_sdss_catalog_interpreted.fits')
expert_catalog_loc = '/data/galaxy_zoo/decals/catalogs/nair_sdss_catalog_interpreted.csv'

# dr1/dr2 subjects, already extracted and with expanded metadata via decals repo
# minor overwrite transforming subject_id to not include 'object wrapper' TODO move to original extraction
dr1_dr2_subject_loc = os.path.join(working_dir, 'subjects/decals_dr1_and_dr2.csv')

# raw subjects from panoptes
current_subjects_loc = os.path.join(working_dir, 'raw/subjects/new_subjects.csv')  # TODO what is this?? Still relevant?
panoptes_old_style_subjects_loc = os.path.join(working_dir, 'raw/subjects/{}_panoptes-subjects.csv'.format(export_date))

# expand metadata and save calibration subjects
workflow = '6122'
subject_set = '19832'
calibration_subject_loc = os.path.join(working_dir, 'subjects/calibration_subjects.csv')

"""
Final output data: prediction tables attached to subject and Nair info
"""
dr2_predictions_with_subject_loc = os.path.join(working_dir, 'output/dr2_predictions_with_subject.csv')
calibration_predictions_with_subject_loc = os.path.join(working_dir, 'output/calibration_predictions_with_subject.csv')

panoptes_predictions_with_catalog_loc = os.path.join(working_dir, 'output/{}_panoptes_predictions_with_catalog.csv'.format(export_date))