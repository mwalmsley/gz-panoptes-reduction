import pytest

import os
import json

import numpy as np

from gzreduction.panoptes.api import api_to_json


@pytest.fixture()
def empty_dir(tmpdir):
    """Empty directory with no chunks. Should download from scratch"""
    return tmpdir.mkdir('no_chunks_here').strpath

@pytest.fixture()
def classification():
    return {"id": "124710536", "annotations": [{"task": "T0", "value": 1}, {"task": "T2", "value": 1}, {"task": "T4", "value": 1}, {"task": "T5", "value": 0}, {"task": "T6", "value": 1}, {"task": "T7", "value": 3}, {"task": "T8", "value": 1}, {"task": "T11", "value": 3}, {"task": "T10", "value": [6]}], "created_at": "2018-09-24T13:33:22.723Z", "updated_at": "2018-09-24T13:33:22.777Z", "metadata": {"source": "api", "session": "b77c5c2529cbe730b33d42e4ca8372dcfd6eda874b448f86a6eb9d940f9171b7", "viewport": {"width": 1332, "height": 1087}, "started_at": "2018-09-24T13:32:46.685Z", "user_agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.13; rv:61.0) Gecko/20100101 Firefox/61.0", "utc_offset": "-3600", "finished_at": "2018-09-24T13:33:22.473Z", "live_project": True, "user_language": "en", "user_group_ids": [], "subject_dimensions": [{"clientWidth": 424, "clientHeight": 424, "naturalWidth": 424, "naturalHeight": 424}], "workflow_version": "66.425"}, "href": "/classifications/124710536", "links": {"project": "5733", "user": "290475", "workflow": "6122", "subjects": ["25810095"]}}

@pytest.fixture()
def dir_with_chunks(tmpdir, classification):
    """Directory with a chunk covering (allegedly) 0 to 25810095. Should continue from here"""
    dir_path = tmpdir.mkdir('empty_dir').strpath
    chunk_a_loc = os.path.join(dir_path, 'panoptes_api_first_0_last_25810000.txt')
    chunk_b_loc = os.path.join(dir_path, 'panoptes_api_first_25810001_last_25810095.txt')
    with open(chunk_a_loc, 'w') as f:
        f.write("some classifications are here")
    with open(chunk_b_loc, 'w') as f:
        json.dump(classification, f)
    return dir_path


@pytest.fixture(params=[True, False])
def previous_dir_exists(request):
    return request.param

@pytest.fixture()
def previous_dir(previous_dir_exists, dir_with_chunks, empty_dir):
    if previous_dir_exists:
        return dir_with_chunks
    else:
        return empty_dir


@pytest.fixture()
def save_dir(tmpdir):
    return tmpdir.mkdir('api_results_dir').strpath


def test_get_chunk_files(previous_dir, previous_dir_exists):
    chunk_locs = api_to_json.get_chunk_files(previous_dir)
    if previous_dir_exists:
        assert len(chunk_locs) == 2
    else:
        assert not chunk_locs  # empty list expected


def test_get_id_pairs_of_chunks(previous_dir, previous_dir_exists):
    id_pairs = api_to_json.get_id_pairs_of_chunks(previous_dir)
    if previous_dir_exists:
        assert id_pairs == [(0, 25810000), (25810001, 25810095)]
    else:
        assert not id_pairs  # empty list expected
        

def test_get_latest_classifications(save_dir, previous_dir_exists, previous_dir):
    max_classifications = 5

    api_to_json.get_latest_classifications(
        save_dir=save_dir,
        previous_dir=previous_dir,
        max_classifications=max_classifications
    )

    id_pairs = api_to_json.get_id_pairs_of_chunks(save_dir)
    assert len(id_pairs) == 1
    assert id_pairs[0][1] > 0
    if previous_dir_exists:
        assert np.max(id_pairs) > 25810095
    else:
        assert id_pairs[0][0] == 0


def test_rename_to_match_exports(classification):
    renamed_classification = api_to_json.rename_to_match_exports(classification)
