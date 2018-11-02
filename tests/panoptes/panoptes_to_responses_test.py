import pytest
import os

import json

import pandas as pd

from gzreduction.panoptes import panoptes_to_responses
from tests import TEST_EXAMPLE_DIR



def test_load_classification_line(line, classification):
    # expect to recover classification, which was used to create line.
    # also expect classification 'created_at' to have been changed to datetime
    classification['created_at'] = pd.to_datetime(classification['created_at'])

    loaded_line = panoptes_to_responses.load_classification_line(line)
    assert loaded_line == classification


def test_explode_classifications(classification):

    flat_classifications = panoptes_to_responses.explode_classification(
        classification)
    result = pd.DataFrame(flat_classifications)  # list of series to DataFrame
    assert set(result.columns.values) == set(['task','task_label','value','user_id','classification_id','created_at','workflow_version','subject_id'])
    assert len(result) == 3  # splits to 3 individual classifications


def test_is_multiple_choice():
    pass


def test_is_null_classification():
    pass


# TODO use test factory, great case
def test_sanitise_string():
    """For a bunch of common strings, make sure the markdown is stripped correctly
    Also tests remove_image_markdown
    """
    features_md = '![features_or_disk_new.png](https://panoptes-uploads.zooniverse.org/production/project_attached_image/1ec52a74-9e49-4579-91ff-0140eb5371e6.png =60x) Features or Disk'
    cant_tell_md = "![cant_tell_arms.png](https://panoptes-uploads.zooniverse.org/production/project_attached_image/e40e428f-3e73-4d40-9eff-1616a7399819.png =60x) Can't tell"
    merging_both_md = '![merger_and_tidal.png](https://panoptes-uploads.zooniverse.org/production/project_attached_image/516be649-9efc-41f7-aa1c-4dd2dad45ac5.png =60x) Both (Merger & Tidal Debris)'
    merging_neither_md = '![no.png](https://panoptes-uploads.zooniverse.org/production/project_attached_image/096879e1-12ae-4df8-abb8-d4a93bc7797f.png =60x) Neither (No Merger & No Tidal Debris) '
    features_no_resize = "![feature_or_disk.png](https://panoptes-uploads.zooniverse.org/production/project_attached_image/f353f2f1-a47e-439d-b9ca-020199162a79.png) Features or Disk"
    bar = '![bar_none_sidebyside_acwacw_200x100.png](https://panoptes-uploads.zooniverse.org/production/project_attached_image/043f8c64-226c-4ff4-957d-af0b06d634df.png =120x60) no bar'
    no_md = 'hello world'

    markdown_strings = [
        features_md,
        cant_tell_md,
        merging_both_md,
        merging_neither_md,
        features_no_resize,
        bar,
        no_md
    ]
    cleaned_strings = list(map(panoptes_to_responses.sanitise_string, markdown_strings))
    assert cleaned_strings[0] == 'features or disk'
    assert cleaned_strings[1] == "can't tell"
    assert cleaned_strings[2] == 'both (merger & tidal debris)'
    assert cleaned_strings[3] == 'neither (no merger & no tidal debris)'
    assert cleaned_strings[4] == 'features or disk'
    assert cleaned_strings[5] == 'no bar'
    assert cleaned_strings[6] == 'hello world'


# TODO another good opportunity for test factory
def test_rename_response(response_without_markdown, schema, renamed_response):
    """Take response with markdown removed, and rename task and value. Expect renamed_response"""
    result = panoptes_to_responses.rename_response(response_without_markdown, schema)
    assert all(result == renamed_response)



def test_clean_response(response, schema, cleaned_response):
    cleaned = panoptes_to_responses.clean_response(response, schema)
    assert cleaned.equals(cleaned_response)


def test_preprocess_classifications(classification_locs, schema, cleaned_responses):
    responses = panoptes_to_responses.preprocess_classifications(
        classification_locs, 
        schema,
        save_loc='/data/spark/temp_export'
        )
    response_list = responses.collect()
    for n in range(len(cleaned_responses)):
        response = response_list[n]
        expected_response = cleaned_responses[n]
        assert response == expected_response
