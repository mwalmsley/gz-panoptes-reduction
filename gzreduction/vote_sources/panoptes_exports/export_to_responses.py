import logging
import json

import pandas as pd


def explode_annotations(df, exclude_tasks=None):
    df = df.copy()
    # df is classification export
    df['annotations'] = df['annotations'].apply(lambda x: json.loads(x))
    
    # .explode() preserves the index, so this index says which initial index contributed to which row
    # will use this to join the non-exploded columns later
    expected_index = df['annotations'].explode().index

    # this fully explodes the dataframe, but resets the index...
    exploded = pd.json_normalize(df['annotations'].explode())
    # so re-attach the index
    exploded = exploded.set_index(expected_index)

    # TODO doubt I need created_at
    cols_to_copy = ['id_str', 'user_id', 'classification_id', 'created_at', 'subject_ids', 'workflow_version'] 
    # now join on index to copy columns from initial dataframe
    exploded = exploded.join(df[cols_to_copy], how='left')
    assert len(exploded) > len(df)

    if exclude_tasks:  # e.g. [T10, T12]
        for task in exclude_tasks:
            exploded = exploded[exploded['task'] != task]

    return exploded


def clean_exploded_annotations(df, schema):
    cleaned = df.apply(lambda x: clean_response(x, schema=schema), axis=1)
    cleaned = cleaned.drop_duplicates(subset=['classification_id', 'task', 'value'])
    cleaned = cleaned.reset_index(drop=True)
    return cleaned

# def explode_classification(classification):
#     """Convert a single line in Panoptes export to list of responses
    
#     Args:
#         classification (dict): single Panoptes classification e.g. single API response

#     Returns:
#         list: of dicts of form {time, user, subject, question, answer}
#     """
#     annotation_data = classification['annotations']
#     flat_df = pd.DataFrame(annotation_data)  # put each [task, value] pair on a row
#     # WARNING export needs subject_id -> subject_ids
#      # also need to update response_to_line_header
#     columns_to_copy = ['user_id', 'classification_id', 'created_at', 'subject_ids', 'workflow_version'] 
#     for col in columns_to_copy:
#         flat_df[col] = classification[col]  # record the (same) user/subject/metadata on every row
#     # return [row.to_dict() for _, row in flat_df.iterrows()]
#     return flat_df


# def is_multiple_choice(response):
#     try:
#         return response['multiple_choice'] == True  # also handles = None -> False
#     except KeyError:  # for exports where this field doesn't yet exist
#         multiple_choice = {
#             'Do you see any of these rare features in the image?',
#             'Do you see any of these rare features?',
#         }
#         return response['task_label'] in multiple_choice


# def is_null_classification(response):
#     return response['value'] is None


def clean_response(response, schema):
    """Clean a single response
    For response values: remove trailing spaces, set lowercase and remove markdown, remove null responses
    Rename question.raw_name to question.name and answer.raw_name to answer.name, for all questions and answers
    Note: the order of operation is important for response values to match the schema.
    
    Args:
        response (pd.Series): of form {time, user, subject, question, answer}
        schema (Schema):  definition object for questions and answers

    Returns:
        response (pd.Series): as above
    """
    if not_null_or_space_string(response['value']):
             # remove trailing spaces, lowercase, remove markdown
        response['value'] = sanitise_string(response['value'])



    # check again, because it might now be null
    if not_null_or_space_string(response['value']):
        response = rename_response(response, schema)
        return response

    # if we made the response null, return a None (filter outside, slightly awkward)
    return None


def not_null_or_space_string(x):
    if x is None:
        return False
    if type(x) is not str:
        raise TypeError(x)
    return bool(x.strip(' '))  # True if string has any characters


def rename_response(response, schema):
    """
    Rename all questions ('task') and answers ('value') according to schema
    If question or task is not found in schema, log critical failure but rely on external checks to raise Exception
    Response should have already had markdown removed, else will log critical failure

    Args:
        response (pd.Series): of form {'task': raw_question_name, 'value': raw_answer_value}
        schema (Schema): definition to use for renaming. Must include Question and Answer objects.

    Returns:
        (pd.Series) of form {'task': new_question_name, 'value': new_answer_value}
    """
    response = response.copy()  # avoid mutating
    try:
        del response['task_label']  # longform task text, not needed
    except KeyError:
        pass

    try:
        question = schema.get_question_from_raw_name(response['task'])
    except IndexError:
        logging.critical('Question "{}" not found in schema'.format(response['task']))
        return None
    response['task'] = question.name

    # print(response['value'])

    try:
        answer = question.get_answer_from_raw_name(response['value'])
    except IndexError:
        logging.critical('Answer "{}" of type {} not found in schema for question "{}" '.format(
            response['value'], type(response['value']), response['task']))
        return None
    response['value'] = answer.name

    return response


def sanitise_string(string):
    """
    Remove ' and leading/trailing space, then remove any leading image markdown, make lowercase
    TODO ' removal is currently broken/untested
    Args:
        string (str): string to be edited

    Returns:
        (str) original string without leading/trailing space or leading markdown
    """
    if type(string) == str:
        clean_string = string.strip("'").lstrip(' ').rstrip(' ').lower()
        return remove_image_markdown(clean_string)
    else:
        logging.warning('{} is not string'.format(string))
        return string


def remove_image_markdown(string):
    """
    Remove leading image markdown by bracket detection
    Assumes string is in one of two forms:
        1) [name](url.png) some text after a space
        2) some text without markdown
    Args:
        string (str): string which may include leading image markdown

    Returns:
        (str) string with any leading image markdown removed
    """
    if string is None:
        return ''
    else:
        if '[' in string:  # might have markdown beginning string
            if '[' in string.split()[0]:  # first block is markdown image
                return string[list(string).index(')') + 1:].lstrip()  # first closing parenthesis ends md
        else:
            return string
