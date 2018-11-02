import pytest


from gzreduction.panoptes.extract import panoptes_extract_to_json


@pytest.fixture()
def extract_row():
    """Example csv row to be loaded into memory as nested dict"""
    return r'91178981,MikeWalmsley,290475,2c61707e96c97a759840,6122,DECaLS DR5,28.30,2018-02-20 10:44:42 UTC,,,"{""session"":""e69d40c94873e2e4e2868226d5567e0e997bf58e8800eef4def679ff3e69f97f"",""viewport"":{""width"":1081,""height"":1049},""started_at"":""2018-02-20T10:41:13.381Z"",""user_agent"":""Mozilla/5.0 (Macintosh; Intel Mac OS X 10.12; rv:58.0) Gecko/20100101 Firefox/58.0"",""utc_offset"":""0"",""finished_at"":""2018-02-20T10:44:42.480Z"",""live_project"":true,""user_language"":""en"",""user_group_ids"":[],""subject_dimensions"":[{""clientWidth"":424,""clientHeight"":424,""naturalWidth"":424,""naturalHeight"":424}]}","[{""task"":""T0"",""task_label"":""Is the galaxy simply smooth and rounded, with no sign of a disk?"",""value"":""![feature_or_disk.png](https://panoptes-uploads.zooniverse.org/production/project_attached_image/f353f2f1-a47e-439d-b9ca-020199162a79.png) Features or Disk""},{""task"":""T2"",""task_label"":""Could this be a disk viewed edge-on?"",""value"":""![feature_or_disk.png](https://panoptes-uploads.zooniverse.org/production/project_attached_image/f353f2f1-a47e-439d-b9ca-020199162a79.png) No""},{""task"":""T4"",""task_label"":""Is there a bar feature through the centre of the galaxy?"",""value"":""No Bar""},{""task"":""T5"",""task_label"":""Is there any sign of a spiral arm pattern?"",""value"":""Yes""},{""task"":""T6"",""task_label"":""How tightly wound do the spiral arms appear?"",""value"":""![tight_arms.png](https://panoptes-uploads.zooniverse.org/production/project_attached_image/0726dfdd-72fa-49e8-a112-439294937d5e.png) Tight""},{""task"":""T7"",""task_label"":""How many spiral arms are there?"",""value"":""![cant_tell_arms.png](https://panoptes-uploads.zooniverse.org/production/project_attached_image/e40e428f-3e73-4d40-9eff-1616a7399819.png) Cant tell""},{""task"":""T8"",""task_label"":""How prominent is the central bulge, compared with the rest of the galaxy?"",""value"":""![no_bulge.png](https://panoptes-uploads.zooniverse.org/production/project_attached_image/75c872f6-7198-4b15-b663-8b88cb5c4d4b.png) No bulge""},{""task"":""T11"",""task_label"":""Is the galaxy currently merging, or is there any sign of tidal debris?"",""value"":""Neither""},{""task"":""T10"",""task_label"":""Do you see any of these rare features in the image?"",""value"":[]}]","{""15715879"":{""retired"":null,""ra"":319.11521779916546,""dec"":-0.826509379829966,""mag.g"":13.674222230911255,""mag.i"":12.560198307037354,""mag.r"":12.938228249549866,""mag.u"":15.10558009147644,""mag.z"":12.32387661933899,""nsa_id"":189862.0,""redshift"":0.019291512668132782,""mag.abs_r"":-20.916738510131836,""mag.faruv"":16.92647397518158,""petroflux"":5388.59814453125,""petroth50"":13.936717987060547,""mag.nearuv"":16.298240423202515,""petrotheta"":28.682878494262695,""absolute_size"":11.334824080956198}}",15715879'


@pytest.fixture()
def extract_row_loaded():
    """extract_row as it should appear in memory"""
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


def test_load_extract_row(extract_row, extract_row_loaded):
    expected_result = extract_row_loaded  # rename for clarity
    result = panoptes_extract_to_json.load_extract_row(extract_row)
    assert len(result.keys() - expected_result.keys()) == 0  # same keys
    for key, value in result.items():
        assert expected_result[key] == value
