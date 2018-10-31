import pytest


from gzreduction.panoptes.extract import panoptes_extract_to_json


def test_load_extract_row(extract_row, extract_row_loaded):
    expected_result = extract_row_loaded  # rename for clarity
    result = panoptes_extract_to_json.load_extract_row(extract_row)
    assert len(result.keys() - expected_result.keys()) == 0  # same keys
    for key, value in result.items():
        assert expected_result[key] == value
