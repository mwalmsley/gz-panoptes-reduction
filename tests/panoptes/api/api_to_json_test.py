import pytest

import os

from gzreduction.panoptes.api import api_to_json


@pytest.fixture()
def empty_dir(tmpdir):
    """Empty directory with no chunks. Should download from scratch"""
    return tmpdir.mkdir('empty_dir').strpath


@pytest.fixture()
def dir_with_chunks(tmpdir):
    """Directory with a chunk covering (allegedly) 0 to 124710536. Should continue from here"""
    dir_path = tmpdir.mkdir('empty_dir').strpath
    chunk_loc = os.path.join(dir_path, 'panoptes_api_first_0_last_124710536_.txt')
    with open(chunk_loc, 'w') as f:
        f.write(r'{"id": "124710536", "annotations": [{"task": "T0", "value": 1}, {"task": "T2", "value": 1}, {"task": "T4", "value": 1}, {"task": "T5", "value": 0}, {"task": "T6", "value": 1}, {"task": "T7", "value": 3}, {"task": "T8", "value": 1}, {"task": "T11", "value": 3}, {"task": "T10", "value": [6]}], "created_at": "2018-09-24T13:33:22.723Z", "updated_at": "2018-09-24T13:33:22.777Z", "metadata": {"source": "api", "session": "b77c5c2529cbe730b33d42e4ca8372dcfd6eda874b448f86a6eb9d940f9171b7", "viewport": {"width": 1332, "height": 1087}, "started_at": "2018-09-24T13:32:46.685Z", "user_agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.13; rv:61.0) Gecko/20100101 Firefox/61.0", "utc_offset": "-3600", "finished_at": "2018-09-24T13:33:22.473Z", "live_project": true, "user_language": "en", "user_group_ids": [], "subject_dimensions": [{"clientWidth": 424, "clientHeight": 424, "naturalWidth": 424, "naturalHeight": 424}], "workflow_version": "66.425"}, "href": "/classifications/124710536", "links": {"project": "5733", "user": "290475", "workflow": "6122", "subjects": ["25810095"]}}')
    return dir_path


@pytest.fixture()
def save_loc(tmpdir):
    return os.path.join(tmpdir.mkdir('api_results_dir').strpath, 'api_results.txt')


def test_get_latest_classifications_empty_dir(empty_dir, save_loc):
    max_classifications = 5

    api_to_json.get_latest_classifications(
        json_dir=empty_dir,
        save_loc=save_loc,
        max_classifications=max_classifications
    )


def test_get_latest_classifications_continue_from_dir(dir_with_chunks, save_loc):
    max_classifications = 5

    api_to_json.get_latest_classifications(
        json_dir=dir_with_chunks,
        save_loc=save_loc,
        max_classifications=max_classifications
    )
