
import logging
import json
import datetime

import pandas as pd

from gzreduction import settings
from gzreduction.schemas.dr5_schema import dr5_schema


def preprocess_classifications(
        classifications,
        schema,
        start_date=None,
        save_loc=None):
    """
    Convert a raw Panoptes classification extract into flattened and cleaned subjects

    Args:
        classifications (pd.DataFrame): Panoptes classifications export: [user, subject, {response to T0, etc}]
        schema (Schema):  definition object for questions and answers
        start_date (datetime.pyi): (optional) if not None, filter out Panoptes classifications before this date
        save_loc (str): (optional) if not None, save Panoptes votes to this location

    Returns:
        (pd.DataFrame) votes with rows=classifications, columns=question_answer, values=True/False. 1 vote per row.
    """

    classifications['created_at'] = pd.to_datetime(classifications['created_at'])
    if start_date:  # e.g. live date of public GZ DR5
        classifications = classifications[classifications['created_at'] >= start_date]
        logging.debug('{} Panoptes classifications after {}'.format(len(classifications), start_date))
    else:
        logging.warning('No Panoptes classification start date selected')

    subject_question_table = flatten_raw_classifications(classifications)
    clean_sq_table = clean_flat_table(subject_question_table, schema)

    if save_loc is not None:
        clean_sq_table.to_csv(save_loc, index=False)
        logging.info('Saved {} Panoptes flat classifications to {}'.format(len(clean_sq_table), save_loc))

    return clean_sq_table


def flatten_raw_classifications(classifications, save_loc=None):
    """
    Panoptes classifications are exported as [user, subject, {response to T0, response to T1, etc}]
    Flatten this into rows of [user, subject, response]
    This part doesn't scale well with dataset size.

    Args:
        classifications (pd.DataFrame): Panoptes classifications export: [user, subject, {response to T0, etc}]

    Returns:
        (pd.DataFrame): Classifications flattened into simple table of [user, subject, question, response]
    """

    all_flat_data = []
    classifications['annotations'] = classifications['annotations'].apply(load_annotation)

    for row_n, classification in classifications.iterrows():
        flat_df = pd.DataFrame(classification['annotations'], dtype=str)  # put each [task, value] pair on a row
        for col in ['user_id', 'classification_id', 'created_at', 'workflow_version']:
            flat_df[col] = classification[col]  # record the (same) user/subject/metadata on every row
        flat_df['subject_id'] = classification['subject_ids']  # rename subject_ids in the process
        all_flat_data.append(flat_df)

    # stick together the responses of all users and all subjects
    flat_classifications = pd.concat(all_flat_data).reset_index(drop=True)  # concat messes with the index
    flat_classifications['value'].fillna(value='', inplace=True)

    logging.debug(flat_classifications['task'].value_counts())
    logging.debug(flat_classifications['value'].value_counts())

    logging.info('Panoptes votes loaded: {}'.format(len(flat_classifications)))

    if save_loc is not None:
        flat_classifications.to_csv(save_loc)

    return flat_classifications


def load_annotation(annotation_str):
    """
    Convert annotation JSON
    Remove known multiple choice questions and filter out 'None' responses
    Warning: coupled to accurate knowledge of which questions are multiple choice
    Args:
        annotation_str (str): of form "{{task:T0, task_label:smooth v featured, 'response':(img)[img.png] smooth}, etc}"

    Returns:
        (list) of form "{{task:T0, task_label:smooth v featured, 'response': smooth}, etc}", markdown and MC removed
    """

    annotation_list = json.loads(annotation_str)  # load as list of dicts

    multiple_choice = {
        'Do you see any of these rare features in the image?',
        'Do you see any of these rare features?',
    }
    return list(filter(lambda x: (x['value'] is not None) & (x['task_label'] not in multiple_choice), annotation_list))


def not_null_or_space_string(series):
    return series.str.strip(' ').astype(bool)


def clean_flat_table(input_df, schema):
    """
    For response values: remove trailing spaces, set lowercase and remove markdown, remove null responses
    Rename question.raw_name to question.name and answer.raw_name to answer.name, for all questions and answers
    Note: the order of operation is important for response values to match the schema.
    Args:
        input_df (pd.DataFrame): Classifications flattened into simple table of [user, subject, question, response]
        schema (Schema):  definition object for questions and answers

    Returns:
        (pd.DataFrame) copy of input_df with cleaned response values and renamed questions and answers
    """
    df = input_df.copy()  # avoid mutating
    df['value'] = df['value'].apply(sanitise_string)  # remove trailing spaces, lowercase, remove markdown

    before_filtering_null = len(df)
    df = df[not_null_or_space_string(df['value'])]
    logging.debug('Removed {} null classifications'.format(before_filtering_null - len(df)))

    df = df.apply(rename_flat_table_row, schema=schema, axis=1)

    return df


def rename_flat_table_row(row, schema):
    """
    Rename all questions ('task') and answers ('value') according to schema
    If question or task is not found in schema, log critical failure but rely on external checks to raise Exception
    When used with .apply, this allows the full df to be checked for bad question/answer values before exiting.

    Args:
        row (pd.Series): of form {'task': raw_question_name, 'value': raw_answer_value}
        schema (Schema): definition to use for renaming. Must include Question and Answer objects.

    Returns:
        (pd.Series) of form {'task': new_question_name, 'value': new_answer_value}
    """

    try:
        question = schema.get_question_from_raw_name(row['task'])
    except IndexError:
        logging.critical('Question "{}" not found in schema'.format(row['task']))
        return None
    row['task'] = question.name

    try:
        answer = question.get_answer_from_raw_name(row['value'])
    except IndexError:
        logging.critical('Answer {} of type {} not found in schema for question {} '.format(
            row['value'], type(row['value']), row['task']))
        return None
    row['value'] = answer.name

    return row


def sanitise_string(string):
    """
    Remove ' and leading/trailing space, then remove any leading image markdown
    ' removal is currently broken/untested
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
    Remove leading image markdown by bracket detectionm
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


def subject_question_table_to_votes(df, question_col, answer_col, schema, save_loc=None):
    """
    Transform a 'flat' table of [classification-subject-question-answer] into votes by column
    Panoptes classifications are exported as [user, subject, {response to T0, response to T1, etc}]

    Args:
        df (pd.DataFrame): rows of classification-subject-question-answer, single column of answer value (plus metadata)
        question_col (str): dataframe column listing questions (probably 'task')
        answer_col (str): dataframe column listing answers (probably 'value')
        schema (Schema): definition object for questions and answers
        save_loc (str): (optional) if not None, save Panoptes votes to this location

    Returns:
        (pd.DataFrame) votes with rows=classifications, columns=question_answer, values=True/False. 1 vote per row.
    """

    all_question_dfs = []
    for question in schema.questions:
        logging.debug('Filtering for question: {}'.format(question.name))
        question_df = df[df[question_col] == question.name].dropna(how='all', axis=1)
        assert len(question_df) != 0
        logging.debug('Answers: {}'.format(question_df['value'].value_counts()))

        answers_as_columns = pd.get_dummies(question_df[answer_col])

        # rename answer columns to count columns as some answers are identical for diff. questions
        answers = question.answers
        answer_cols = map(lambda x: x.name, answers)
        count_cols = map(lambda x: question.get_count_column(x), answers)
        answers_as_columns.rename(columns=dict(zip(answer_cols, count_cols)), inplace=True)

        questions_and_answers = pd.concat([question_df, answers_as_columns], axis=1)
        all_question_dfs.append(questions_and_answers)

    # recombine
    votes = pd.concat(all_question_dfs, axis=0).fillna(0).reset_index(drop=True)
    votes['created_at'] = pd.to_datetime(votes['created_at'])

    if save_loc is not None:
        votes.to_csv(save_loc, index=False)
        logging.info('Saved {} Panoptes votes to {}'.format(len(votes), save_loc))

    return votes


if __name__ == '__main__':

    logging.basicConfig(
        filename='panoptes_extract_to_votes.log',
        format='%(asctime)s %(message)s',
        filemode='w',
        level=logging.DEBUG)

    classifications_loc = settings.panoptes_old_style_classifications_loc
    subjects_loc = settings.panoptes_old_style_subjects_loc

    dtypes = {
        'workflow_id': str
    }
    parse_dates = ['created_at']
    nested_classifications = pd.read_csv(classifications_loc, dtype=dtypes, parse_dates=parse_dates)
    logging.debug('Loaded {} raw classifications'.format(len(nested_classifications)))
    nested_classifications = nested_classifications[nested_classifications['workflow_id'] == '6122']
    assert not nested_classifications.empty

    # make flat table of classifications. Basic use-agnostic view. Fast to append. Slow to run!
    flat_classifications = preprocess_classifications(
        nested_classifications,
        dr5_schema,
        start_date=datetime.datetime(year=2018, month=3, day=15),  # public launch
        save_loc=settings.panoptes_flat_classifications
    )

    # turn flat table into columns of votes. Standard analysis view (Ouroborous also)
    votes = subject_question_table_to_votes(
        flat_classifications, 
        question_col='task', 
        answer_col='value', 
        schema=dr5_schema,
        save_loc=settings.panoptes_votes_loc)
