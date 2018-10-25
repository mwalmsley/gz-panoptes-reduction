import pytest

import pandas as pd

from gzreduction import shared_utilities


@pytest.fixture()
def galaxies():
    return pd.DataFrame([
        {
            'subject': 'a',
            'ra': 116.448,
            'dec': 25.8621
        },

        {
            'subject': 'b',
            'ra': 323.319,
            'dec': 11.6901
        }
    ])


@pytest.fixture()
def catalog():
    return pd.DataFrame([

        {
            'subject': 'b',
            'ra': 323.319,
            'dec': 11.6901
        },

        {
            'subject': 'a',
            'ra': 116.448,
            'dec': 25.8621
        }

    ])


def test_match_galaxies_to_catalog_pandas(galaxies, catalog):
    matched, unmatched = shared_utilities.match_galaxies_to_catalog_pandas(
        galaxies,
        catalog,
        galaxy_suffix='_g',
        catalog_suffix='_c')
    assert 'a' in matched['subject_c'].values
    assert 'b' in matched['subject_c'].values
    assert 'a' in matched['subject_g'].values
    assert 'b' in matched['subject_g'].values
    assert len(unmatched) == 0
