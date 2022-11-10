
import pandas as pd

from shared_astro_utils.matching_utils import match_galaxies_to_catalog_pandas
from gzreduction.deprecated import settings


def filter_subset(catalog, subset_coords):
    # matched, _ = match_galaxies_to_catalog_pandas(subset_coords, catalog, galaxy_suffix='_subset')
    # return matched
    return catalog[catalog['subject_set_id'] == 60258].reset_index()


if __name__ == '__main__':

    catalog_loc = settings.panoptes_predictions_with_catalog_loc
    subset_coords_loc = '/Data/repos/decals/python/z_analysis/gordon_galaxies_sample_2_unique.csv'
    subset_export_loc = '/Data/repos/decals/python/z_analysis/gordon_galaxies_sample_2_export.csv'

    catalog = pd.read_csv(catalog_loc)
    subset_coords = pd.read_csv(subset_coords_loc)

    subset = filter_subset(catalog, subset_coords)
    print(len(subset))
    assert not subset.empty 

    # delete irrelevant columns
    cols_not_for_export = [
        # 'Unnamed: 0_subset',
        'Unnamed: 0',
        'index',
        'fits_ready',
        'fits_filled',
        'png_ready',
        'png_loc',
        'fits_loc',
        'sky_separation',
        'sdss_search'
    ]
    for col in cols_not_for_export:
        del subset[col]

    subset.to_csv(subset_export_loc)
