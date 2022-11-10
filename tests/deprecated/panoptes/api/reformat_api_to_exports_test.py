# import pytest

# import json
# import os

# import pandas as pd

# from tests import TEST_EXAMPLE_DIR
# from gzreduction.panoptes.api import reformat_api_like_exports

# @pytest.fixture(params=['3.1', '5.2', '7.4'])
# def major_minor_decimal_str(request):
#     return request.param

# @pytest.fixture()
# def workflow_id():
#     return '6122'  # Galaxy Zoo DECALS

# @pytest.fixture()
# def workflow_version():
#     with open(os.path.join(TEST_EXAMPLE_DIR, 'workflow_version.txt')) as f:
#         return json.load(f)

# @pytest.fixture()
# def workflow_versions():
#     with open(os.path.join(TEST_EXAMPLE_DIR, 'workflow_versions.txt')) as f:
#         return json.load(f)


# @pytest.fixture()
# def classification():
#         return {"annotations": [{"task": "T0", "value": 1}, {"task": "T2", "value": 1}, {"task": "T4", "value": 2}, {"task": "T5", "value": 1}, {"task": "T8", "value": 1}, {"task": "T11", "value": 3}, {"task": "T10", "value": [0]}], "created_at": "2018-02-20T10:46:22.371Z", "updated_at": "2018-02-20T10:46:23.010Z", "metadata": {"session": "e69d40c94873e2e4e2868226d5567e0e997bf58e8800eef4def679ff3e69f97f", "viewport": {"width": 1081, "height": 1049}, "started_at": "2018-02-20T10:45:44.292Z", "user_agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.12; rv:58.0) Gecko/20100101 Firefox/58.0", "utc_offset": "0", "finished_at": "2018-02-20T10:46:22.059Z", "live_project": True, "user_language": "en", "user_group_ids": [], "subject_dimensions": [{"clientWidth": 424, "clientHeight": 424, "naturalWidth": 424, "naturalHeight": 424}], "workflow_version": "28.30"}, "href": "/classifications/91179088", "classification_id": "91179088", "project_id": "5733", "user_id": "290475", "workflow_id": "6122", "subject_id": "15715895"}


# # not currently used
# @pytest.fixture()
# def all_versions():
#     return [
#         {
#             'id': '12',
#             'created_at': '2019-01-29T23:09:24.680Z',
#             'major_number': '66',
#             'minor_number': '425',
#             'tasks': ['some tasks'],
#             'strings': {
#                 'T0.answers.0.label': 'answer to T0.0',
#                 'T0.question': 'question for T0'
#                 }
#         }
#     ]

# # @pytest.fixture()
# # def workflow_version():
# #     return reformat_api_like_exports.WorkflowVersion(major='3', minor='1')

# # def test_major_minor_workflow_versions(major_minor_decimal_str):
# #     version = reformat_api_like_exports.read_workflow_versions_from_decimal_str(major_minor_decimal_str)
# #     if major_minor_decimal_str == '3.1':
# #         assert version == reformat_api_like_exports.WorkflowVersion('3', '1')
# #     if major_minor_decimal_str == '5.2':
# #         assert version == reformat_api_like_exports.WorkflowVersion('5', '2')
# #     if major_minor_decimal_str == '7.4':
# #         assert version == reformat_api_like_exports.WorkflowVersion('7', '4')


# # def test_get_workflow_contents(workflow_version):
# #     print(reformat_api_like_exports.get_workflow_contents(workflow_version))
# #     raise NotImplementedError


# def test_get_all_workflow_versions(workflow_id):
#     all_versions = reformat_api_like_exports.get_all_workflow_versions(workflow_id)
#     assert len(all_versions) > 20  # GZ workflow 6122 has at least 20 workflow versions, > 1 page

# def test_make_workflow_version_df(workflow_versions):
#     assert len(workflow_versions) > 1
#     df = reformat_api_like_exports.make_workflow_version_df(workflow_versions)
#     print(df.iloc[0])
#     assert len(df) == len(workflow_versions)


# @pytest.fixture()
# def workflows_df(workflow_versions):  # coupled but I'm lazy
#     return reformat_api_like_exports.make_workflow_version_df(workflow_versions)


# def test_find_matching_version(classification, workflows_df):
#     workflow = reformat_api_like_exports.find_matching_version(classification, workflows_df)
#     assert isinstance(workflow, pd.Series)
#     assert workflow['major_number'] == '28'
#     assert workflow['minor_number'] == '30'
#     # will use later as fixture
#     with open(os.path.join(TEST_EXAMPLE_DIR, 'workflow_28_30.txt'), 'w') as f:
#         data = workflow.to_dict()
#         for bool_col in ['grouped', 'pairwise', 'prioritized']:
#                 del data[bool_col]  # for simplicity of saving, I don't care about these columns
#         json.dump(data, f)

# @pytest.fixture()
# def workflow():
#         with open(os.path.join(TEST_EXAMPLE_DIR, 'workflow_28_30.txt'), 'r') as f:
#                 return json.load(f)


# def test_insert_workflow_contents(classification, workflow):
#         result = reformat_api_like_exports.insert_workflow_contents(classification, workflow)
#         print(result['annotations'])
#         for annotation in result['annotations']:
#                 for expected_attr in ['task', 'task_id', 'value', 'value_index', 'multiple_choice']:
#                         assert expected_attr in annotation.keys()

# def test_rename_metadata_like_exports(classification):
#         pass

# """
# Annotations ultimately look like:
# [
#         {
#                 'task': 'Is the galaxy simply smooth and rounded, with no sign of a disk?', 
#                 'value': '![feature_or_disk.png](https://panoptes-uploads.zooniverse.org/production/project_attached_image/f353f2f1-a47e-439d-b9ca-020199162a79.png) Features or Disk', 
#                 'task_id': 'T0', 
#                 'value_index': 1, 
#                 'multiple_choice': False
#         }, 
#         ...
#         {
#                 'task': 'Do you see any of these rare features in the image?', 
#                 'value': ['![ring.png](https://panoptes-uploads.zooniverse.org/production/project_attached_image/aa3e8836-6f38-474c-a643-4dda5f3776f6.png) Ring'], 
#                 'task_id': 'T10', 
#                 'value_index': [0], 
#                 'multiple_choice': True
#         }
# ]

# """
