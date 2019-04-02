import logging
import os

import pandas as pd

from gzreduction.panoptes import panoptes_to_responses
from gzreduction import settings
from gzreduction.schemas.dr5_schema import dr5_schema


def get_votes(df, question_col, answer_col, schema, save_loc=None):
    """
    Transform responses (one response per row) into votes (one user per row, columns as votes)
    
    Args:
        df (pd.DataFrame): rows of responses, in the form user-subject-question-answer
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
        if len(question_df) == 0:
            raise ValueError(
                'No answers found for task col "{}" and question {}'.format(
                    question_col, 
                    question.name
                )
            )
        logging.debug('Answers: {}'.format(question_df['value'].value_counts()))

        answers_as_columns = pd.get_dummies(question_df[answer_col])
        # if any answers are missing (i.e. no responses with that answer), add with 0's
        answers_without_responses = set(question.get_answer_names()) - set(answers_as_columns.columns.values)
        if answers_without_responses:
            logging.warning('Missing responses for {}, filling in with 0s'.format(answers_without_responses))
            for missing_answer in answers_without_responses:
                answers_as_columns[missing_answer] = 0

        # rename answer columns to count columns (i.e. '{question}_{answer}_count') 
        # as some answers are identical for diff. questions
        answers = question.answers
        answer_cols = map(lambda x: x.name, answers)
        count_cols = map(lambda x: question.get_count_column(x), answers)
        answers_as_columns.rename(columns=dict(zip(answer_cols, count_cols)), inplace=True)
        # stick columns for the existence of each answer on to left hand side of df, values as binary indicators
        questions_and_answers = pd.concat([question_df, answers_as_columns], axis=1)
        all_question_dfs.append(questions_and_answers)

    # recombine
    votes = pd.concat(all_question_dfs, axis=0).fillna(0).reset_index(drop=True)
    votes['created_at'] = pd.to_datetime(votes['created_at'])

    if save_loc is not None:
        votes.to_csv(save_loc, index=False)
        logging.info('Saved {} Panoptes votes to {}'.format(len(votes), save_loc))

    return votes


def get_shard_locs(shard_dir):
    candidates = [os.path.join(shard_dir, name) for name in os.listdir(response_dir)]
    return list(filter(lambda x: 'part' in x and not x.endswith('.crc'), candidates))


def join_response_shards(shard_dir):
    response_locs = get_shard_locs(shard_dir)  # Spark outputs many headerless shards, not one csv
    header = panoptes_to_responses.response_to_line_header()  # avoid duplication of schema
    
    response_dfs = [pd.read_csv(loc, index_col=False, header=None, names=header) for loc in response_locs]
    logging.info('response shards: {}'.format(len(response_dfs)))
    responses = pd.concat(response_dfs).reset_index(drop=True)
    logging.info('responses: {}'.format(len(responses)))
    logging.info(responses.iloc[0])
    return responses


if __name__ == '__main__':

    logging.basicConfig(
        filename='responses_to_votes.log',
        format='%(asctime)s %(message)s',
        filemode='w',
        level=logging.DEBUG)

    response_dir = settings.panoptes_flat_classifications
    responses = join_response_shards(response_dir)

    # turn flat table into columns of votes. Standard analysis view (Ouroborous also)
    votes = get_votes(
        responses, 
        question_col='task', 
        answer_col='value', 
        schema=dr5_schema,
        save_loc=settings.panoptes_votes_loc)
