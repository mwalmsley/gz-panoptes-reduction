import pytest

import json

import pandas as pd

from gzreduction.schemas.schema import Schema, Question, Answer

@pytest.fixture()
def question_0():
    question_0 = Question(
        name='question-0',
        raw_name='T0'
    )
    question_0.set_answers([
        Answer(
            name='features-or-disk',
            raw_name='features or disk'),
    ])
    return question_0


@pytest.fixture()
def question_1():
    question_1 = Question(
        name='question-1',
        raw_name='T1'
    )
    question_1.set_answers([
        Answer(
            name='round',
            raw_name='completely round'),
    ])
    return question_1


@pytest.fixture()
def question_2():
    question_2 = Question(
        name='question-2',
        raw_name='T2'
    )
    question_2.set_answers([
        Answer(
            name='no',
            raw_name='no'),
    ])
    return question_2


@pytest.fixture()
def question_9():
    question_9 = Question(
        name='question-9',
        raw_name='T9'
    )
    question_9.set_answers([
        Answer(
            name='no',
            raw_name='no'),
        Answer(
            name='yes',
            raw_name='yes'),
    ])
    return question_9


@pytest.fixture()
def schema(question_0, question_1, question_2, question_9):
    schema = Schema()
    schema.add_question(question_0)
    schema.add_question(question_1)
    schema.add_question(question_2)
    schema.add_question(question_9)
    return schema



@pytest.fixture()
def extract_header():
    return 'classification_id,user_name,user_id,user_ip,workflow_id,workflow_name,workflow_version,created_at,gold_standard,expert,metadata,annotations,subject_data,subject_ids'


@pytest.fixture()
def extract_row():
    return r'91178981,MikeWalmsley,290475,2c61707e96c97a759840,6122,DECaLS DR5,28.30,2018-02-20 10:44:42 UTC,,,"{""session"":""e69d40c94873e2e4e2868226d5567e0e997bf58e8800eef4def679ff3e69f97f"",""viewport"":{""width"":1081,""height"":1049},""started_at"":""2018-02-20T10:41:13.381Z"",""user_agent"":""Mozilla/5.0 (Macintosh; Intel Mac OS X 10.12; rv:58.0) Gecko/20100101 Firefox/58.0"",""utc_offset"":""0"",""finished_at"":""2018-02-20T10:44:42.480Z"",""live_project"":true,""user_language"":""en"",""user_group_ids"":[],""subject_dimensions"":[{""clientWidth"":424,""clientHeight"":424,""naturalWidth"":424,""naturalHeight"":424}]}","[{""task"":""T0"",""task_label"":""Is the galaxy simply smooth and rounded, with no sign of a disk?"",""value"":""![feature_or_disk.png](https://panoptes-uploads.zooniverse.org/production/project_attached_image/f353f2f1-a47e-439d-b9ca-020199162a79.png) Features or Disk""},{""task"":""T2"",""task_label"":""Could this be a disk viewed edge-on?"",""value"":""![feature_or_disk.png](https://panoptes-uploads.zooniverse.org/production/project_attached_image/f353f2f1-a47e-439d-b9ca-020199162a79.png) No""},{""task"":""T4"",""task_label"":""Is there a bar feature through the centre of the galaxy?"",""value"":""No Bar""},{""task"":""T5"",""task_label"":""Is there any sign of a spiral arm pattern?"",""value"":""Yes""},{""task"":""T6"",""task_label"":""How tightly wound do the spiral arms appear?"",""value"":""![tight_arms.png](https://panoptes-uploads.zooniverse.org/production/project_attached_image/0726dfdd-72fa-49e8-a112-439294937d5e.png) Tight""},{""task"":""T7"",""task_label"":""How many spiral arms are there?"",""value"":""![cant_tell_arms.png](https://panoptes-uploads.zooniverse.org/production/project_attached_image/e40e428f-3e73-4d40-9eff-1616a7399819.png) Cant tell""},{""task"":""T8"",""task_label"":""How prominent is the central bulge, compared with the rest of the galaxy?"",""value"":""![no_bulge.png](https://panoptes-uploads.zooniverse.org/production/project_attached_image/75c872f6-7198-4b15-b663-8b88cb5c4d4b.png) No bulge""},{""task"":""T11"",""task_label"":""Is the galaxy currently merging, or is there any sign of tidal debris?"",""value"":""Neither""},{""task"":""T10"",""task_label"":""Do you see any of these rare features in the image?"",""value"":[]}]","{""15715879"":{""retired"":null,""ra"":319.11521779916546,""dec"":-0.826509379829966,""mag.g"":13.674222230911255,""mag.i"":12.560198307037354,""mag.r"":12.938228249549866,""mag.u"":15.10558009147644,""mag.z"":12.32387661933899,""nsa_id"":189862.0,""redshift"":0.019291512668132782,""mag.abs_r"":-20.916738510131836,""mag.faruv"":16.92647397518158,""petroflux"":5388.59814453125,""petroth50"":13.936717987060547,""mag.nearuv"":16.298240423202515,""petrotheta"":28.682878494262695,""absolute_size"":11.334824080956198}}",15715879'


# single raw classification row to mimic panoptes
# @pytest.fixture()
# def raw_classification():
#     return ','.join([
#         'c1',
#         'a',
#         json.dumps(raw_annotations_a) + '',
#         's1',
#         '2001-01-01',
#         'v1'
#     ])

@pytest.fixture()
def extract_row_loaded():
    result = {}
    result['classification_id'] = '91178981'
    result['user_name'] = 'MikeWalmsley'
    result['user_id'] = '290475'
    result['user_ip'] = '2c61707e96c97a759840'
    result['workflow_id'] = '6122'
    result['workflow_name'] = 'DECaLS DR5'
    result['workflow_version'] = '28.30'
    result['created_at'] = '2018-02-20 10:44:42 UTC'
    result['gold_standard'] = ''
    result['expert'] = ''
    result['metadata'] = {
        'session': 'e69d40c94873e2e4e2868226d5567e0e997bf58e8800eef4def679ff3e69f97f',
        'viewport': {
                'width': 1081,
                'height': 1049
                },
        'started_at':'2018-02-20T10:41:13.381Z',
        'user_agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.12; rv:58.0) Gecko/20100101 Firefox/58.0',
        'utc_offset': '0',
        'finished_at': '2018-02-20T10:44:42.480Z',
        'live_project': True,
        'user_language': 'en', 
        'user_group_ids':[],
        'subject_dimensions': [{
            'clientWidth': 424,
            'clientHeight': 424,
            'naturalWidth': 424,
            'naturalHeight': 424
            }]}
    result['annotations'] = [
        {
            'task': 'T0',
            'task_label': 'Is the galaxy simply smooth and rounded, with no sign of a disk?',
            'value':'![feature_or_disk.png](https://panoptes-uploads.zooniverse.org/production/project_attached_image/f353f2f1-a47e-439d-b9ca-020199162a79.png) Features or Disk'
        },
        {
            'task': 'T2',
            'task_label': 'Could this be a disk viewed edge-on?',
            'value':'![feature_or_disk.png](https://panoptes-uploads.zooniverse.org/production/project_attached_image/f353f2f1-a47e-439d-b9ca-020199162a79.png) No'
        },
        {
            'task': 'T4',
            'task_label': 'Is there a bar feature through the centre of the galaxy?',
            'value': 'No Bar'
        },
        {
            'task': 'T5',
            'task_label': 'Is there any sign of a spiral arm pattern?',
            'value': 'Yes'
        },
        {
            'task': 'T6',
            'task_label': 'How tightly wound do the spiral arms appear?',
            'value': '![tight_arms.png](https://panoptes-uploads.zooniverse.org/production/project_attached_image/0726dfdd-72fa-49e8-a112-439294937d5e.png) Tight'
        },
        {
            'task': 'T7', 
            'task_label':'How many spiral arms are there?',
            'value':'![cant_tell_arms.png](https://panoptes-uploads.zooniverse.org/production/project_attached_image/e40e428f-3e73-4d40-9eff-1616a7399819.png) Cant tell'
        },
        {
            'task':'T8',
            'task_label':'How prominent is the central bulge, compared with the rest of the galaxy?',
            'value':'![no_bulge.png](https://panoptes-uploads.zooniverse.org/production/project_attached_image/75c872f6-7198-4b15-b663-8b88cb5c4d4b.png) No bulge'
        },
        {
            'task':'T11',
            'task_label':'Is the galaxy currently merging, or is there any sign of tidal debris?',
            'value':'Neither'
        },
        {
            'task':'T10',
            'task_label':'Do you see any of these rare features in the image?',
            'value':[]
        }
    ]
    result['subject_data'] = {
        '15715879': {
            'retired': None,
            'ra': 319.11521779916546,
            'dec': -0.826509379829966,
            'mag.g': 13.674222230911255,
            'mag.i': 12.560198307037354,
            'mag.r': 12.938228249549866,
            'mag.u': 15.10558009147644,
            'mag.z':12.32387661933899, 
            'nsa_id':189862.0,
            'redshift':0.019291512668132782,
            'mag.abs_r':-20.916738510131836,
            'mag.faruv':16.92647397518158,
            'petroflux':5388.59814453125,
            'petroth50':13.936717987060547,
            'mag.nearuv':16.298240423202515,
            'petrotheta':28.682878494262695,
            'absolute_size':11.334824080956198
        }
    }
    result['subject_ids'] = '15715879'
    return result

@pytest.fixture()
def raw_annotations_a():
    return [
        {
            "task": "T0",  # only in annotation a
            "task_label": "Is the galaxy simply smooth and rounded, with no sign of a disk?",
            "value": "![feature_or_disk.png](https://panoptes-uploads.zooniverse.org/production/project_attached_image/f353f2f1-a47e-439d-b9ca-020199162a79.png) Features or Disk"},

        {
            "task": "T2",  # in both a and b, users agree
            "task_label": "Could this be a disk viewed edge-on?",
            "value":"![no.png](https://panoptes-uploads.zooniverse.org/production/project_attached_image/096879e1-12ae-4df8-abb8-d4a93bc7797f.png) No"},

        {
            "task": "T9",  # in both a and b, users disagree
            "task_label": "Is there anything odd?",
            "value": "![yes.png](https://panoptes-uploads.zooniverse.org/production/project_attached_image/503a6354-7f72-4899-b620-4399dbd5cf93.png) Yes"},
    ]


@pytest.fixture()
def raw_annotations_b():
    return [
        {
            "task": "T1",  # only in annotation b
            "task_label": "How rounded is it?",
            "value": "![rounded.png](https://panoptes-uploads.zooniverse.org/production/project_attached_image/94412557-f564-40b9-9423-1d2e47cb1104.png) Completely round"},

        {
            "task": "T2",  # in both a and b, users agree
            "task_label": "Could this be a disk viewed edge-on?",
            "value": "![no.png](https://panoptes-uploads.zooniverse.org/production/project_attached_image/096879e1-12ae-4df8-abb8-d4a93bc7797f.png) No"},

        {
            "task": "T9",  # in both a and b, users disagree
            "task_label": "Is there anything odd?",
            "value": "![no.png](https://panoptes-uploads.zooniverse.org/production/project_attached_image/096879e1-12ae-4df8-abb8-d4a93bc7797f.png) No"},

        {
            "task": "T9",  # in both a and b, users disagree
            "task_label": "Is there anything odd?",
            "value": ""},  # user left side without submitting answer - '' recorded. Filter out.

        {
            "task": "T0",
            "task_label": "Is there anything odd?",
            "value": None}  # user left side without submitting answer - None recorded. Filter out.
    ]


@pytest.fixture()
def raw_classification_flattened():
    data = [
        {
            'classification_id': 'c1',
            'created_at': '2001-01-01',
            'task': 'T0',
            'value': '![feature_or_disk.png](https://panoptes-uploads.zooniverse.org/production/project_attached_image/f353f2f1-a47e-439d-b9ca-020199162a79.png) Features or Disk',
            'user_id': 'a',
            'subject_id': 's1',
            'workflow_version': 'v1'},
        {
            'classification_id': 'c1',
            'created_at': '2001-01-01',
            'task': 'T2',
            'value': '![no.png](https://panoptes-uploads.zooniverse.org/production/project_attached_image/096879e1-12ae-4df8-abb8-d4a93bc7797f.png) No',
            'user_id': 'a',
            'subject_id': 's1',
            'workflow_version': 'v1'},
        {
            'classification_id': 'c1',
            'created_at': '2001-01-01',
            'task': 'T9',
            'value': '![yes.png](https://panoptes-uploads.zooniverse.org/production/project_attached_image/503a6354-7f72-4899-b620-4399dbd5cf93.png) Yes',
            'user_id': 'a',
            'subject_id': 's1',
            'workflow_version': 'v1'}
    ]
    return [pd.Series(x) for x in data]


# pack each annotation up as JSON to appear just like the Panoptes classifications export
@pytest.fixture()
def raw_classifications(raw_annotations_a, raw_annotations_b):
    # TODO multiple subject ids
    return pd.DataFrame([
        {
            # classification c1 - user a says T0: features, T2: no, T9: yes, for subject s1 at at 2001-01-01
            'classification_id': 'c1',
            'user_id': 'a',
            'annotations': json.dumps(raw_annotations_a),
            'subject_ids': 's1',
            'created_at': '2001-01-01',
            'workflow_version': 'v1'},
        {
            # classification c2 - user b says T1: round, T2: no, T9: no, for subject s1 at at 2001-01-02
            'classification_id': 'c2',
            'user_id': 'b',
            'annotations': json.dumps(raw_annotations_b),
            'subject_ids': 's1',
            'created_at': '2001-01-02',
            'workflow_version': 'v1'}
    ])


@pytest.fixture()
def exploded_classification():
    # matches first row of flat table
    return pd.Series(
        {
            'classification_id': 'c1',
            'created_at': '2001-01-01',
            'task': 'T0',
            'value': '![feature_or_disk.png](https://panoptes-uploads.zooniverse.org/production/project_attached_image/f353f2f1-a47e-439d-b9ca-020199162a79.png) Features or Disk',
            'user_id': 'a',
            'subject_id': 's1',
            'workflow_version': 'v1'
        }
    )

@pytest.fixture()
def cleaned_classification():
        return pd.Series(
        {
            'classification_id': 'c1',
            'created_at': '2001-01-01',
            'task': 'question-0',
            'value': 'features-or-disk',
            'user_id': 'a',
            'subject_id': 's1',
            'workflow_version': 'v1'
        }
    )


# intermediate step: the annotations should be flattened into a table like this
# of form [user, subject, task, value] for every user, subject, task and value
# this is the subject_question table
@pytest.fixture()
def flat_table():
    # simply rearranged version of raw input, should have markdown and original names
    # TODO multiple subject ids
    return pd.DataFrame([

        # expected responses from user a
        {
            'classification_id': 'c1',
            'created_at': '2001-01-01',
            'task': 'T0',
            'value': '![feature_or_disk.png](https://panoptes-uploads.zooniverse.org/production/project_attached_image/f353f2f1-a47e-439d-b9ca-020199162a79.png) Features or Disk',
            'user_id': 'a',
            'subject_id': 's1',
            'workflow_version': 'v1'},
        {
            'classification_id': 'c1',
            'created_at': '2001-01-01',
            'task': 'T2',
            'value': '![no.png](https://panoptes-uploads.zooniverse.org/production/project_attached_image/096879e1-12ae-4df8-abb8-d4a93bc7797f.png) No',
            'user_id': 'a',
            'subject_id': 's1',
            'workflow_version': 'v1'},
        {
            'classification_id': 'c1',
            'created_at': '2001-01-01',
            'task': 'T9',
            'value': '![yes.png](https://panoptes-uploads.zooniverse.org/production/project_attached_image/503a6354-7f72-4899-b620-4399dbd5cf93.png) Yes',
            'user_id': 'a',
            'subject_id': 's1',
            'workflow_version': 'v1'},

        # expected responses from user b
        {
            'classification_id': 'c2',
            'created_at': '2001-01-02',
            'task': 'T1',
            'value': '![rounded.png](https://panoptes-uploads.zooniverse.org/production/project_attached_image/94412557-f564-40b9-9423-1d2e47cb1104.png) Completely round',
            'user_id': 'b',
            'subject_id': 's1',
            'workflow_version': 'v1'},
        {
            'classification_id': 'c2',
            'created_at': '2001-01-02',
            'task': 'T2',
            'value': '![no.png](https://panoptes-uploads.zooniverse.org/production/project_attached_image/096879e1-12ae-4df8-abb8-d4a93bc7797f.png) No',
            'user_id': 'b',
            'subject_id': 's1',
            'workflow_version': 'v1'},
        {
            'classification_id': 'c2',
            'created_at': '2001-01-02',
            'task': 'T9',
            'value': '![no.png](https://panoptes-uploads.zooniverse.org/production/project_attached_image/096879e1-12ae-4df8-abb8-d4a93bc7797f.png) No',
            'user_id': 'b',
            'subject_id': 's1',
            'workflow_version': 'v1'},

        {
            'classification_id': 'c2',
            'created_at': '2001-01-02',
            'task': 'T9',
            'value': '',  # not yet filtered, removed in clean_table step
            'user_id': 'b',
            'subject_id': 's1',
            'workflow_version': 'v1'}
    ])


@pytest.fixture()
def clean_table(flat_table):
    df = flat_table.copy()
    df.iloc[0]['value'] = 'features-or-disk'  # questions should have been renamed
    df.iloc[1]['value'] = 'no'
    df.iloc[2]['value'] = 'yes'
    df.iloc[3]['value'] = 'round'
    df.iloc[4]['value'] = 'no'
    df.iloc[5]['value'] = 'no'
    df.iloc[0]['task'] = 'question-0'  # answers should have been renamed
    df.iloc[1]['task'] = 'question-2'
    df.iloc[2]['task'] = 'question-9'
    df.iloc[3]['task'] = 'question-1'
    df.iloc[4]['task'] = 'question-2'
    df.iloc[5]['task'] = 'question-9'
    df = df[:6]  # should filter 6th classification, has empty response value
    return df


# final step: the subject question table has the questions moved to columns
@pytest.fixture()
def votes():

    votes = pd.DataFrame([

        {
            'classification_id': 'c1',
            'created_at': '2001-01-01',
            'question-0_features-or-disk': 1.0,
            'question-1_round': 0.0,
            'question-2_no': 0.0,
            'question-9_no': 0.0,
            'question-9_yes': 0.0,
            'subject_id': 's1',
            'user_id': 'a',
        },

        {
            'classification_id': 'c2',
            'created_at': '2001-01-02',
            'question-0_features-or-disk': 0.0,
            'question-1_round': 1.0,
            'question-2_no': 0.0,
            'question-9_no': 0.0,
            'question-9_yes': 0.0,
            'subject_id': 's1',
            'user_id': 'b',
        },

        {
            'classification_id': 'c1',
            'created_at': '2001-01-01',
            'question-0_features-or-disk': 0.0,
            'question-1_round': 0.0,
            'question-2_no': 1.0,
            'question-9_no': 0.0,
            'question-9_yes': 0.0,
            'subject_id': 's1',
            'user_id': 'a',
        },

        {
            'classification_id': 'c2',
            'created_at': '2001-01-02',
            'question-0_features-or-disk': 0.0,
            'question-1_round': 0.0,
            'question-2_no': 1.0,
            'question-9_no': 0.0,
            'question-9_yes': 0.0,
            'subject_id': 's1',
            'user_id': 'b',
        },

        {
            'classification_id': 'c1',
            'created_at': '2001-01-01',
            'question-0_features-or-disk': 0.0,
            'question-1_round': 0.0,
            'question-2_no': 0.0,
            'question-9_no': 0.0,
            'question-9_yes': 1.0,
            'subject_id': 's1',
            'user_id': 'a',
        },

        {
            'classification_id': 'c2',
            'created_at': '2001-01-02',
            'question-0_features-or-disk': 0.0,
            'question-1_round': 0.0,
            'question-2_no': 0.0,
            'question-9_no': 1.0,
            'question-9_yes': 0.0,
            'subject_id': 's1',
            'user_id': 'b',
        }
    ])
    votes['created_at'] = pd.to_datetime(votes['created_at'])  # avoid datetime comparison hassle
    return votes