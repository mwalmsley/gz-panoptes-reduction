import pytest

from reduction.ouroborous import load_subjects
from reduction import settings


TEST_DIRECTORY = 'reduction/test_examples'


@pytest.fixture()
def current_subjects_loc():
    return '{}/{}'.format(TEST_DIRECTORY, 'current_subjects_example.csv')


# TODO this should take the subject download and directly read, with no intermediate code from decals repo
def test_load_current_subjects(current_subjects_loc):
    current_subjects = load_subjects.load_current_subjects(current_subjects_loc, workflow='6122')  # public decals work.

    example_subject = current_subjects.iloc[0]
    assert example_subject['iauname'] == 'J013703.16+004648.0'
    assert example_subject['locations'] == 'https://panoptes-uploads.zooniverse.org/production/subject_location/786725be-25fe-4676-abb5-f22c5b071363.png'
    print(example_subject['ra'])
