
import logging

import pandas as pd


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
    # turn flat table into columns of votes. Standard analysis view (Ouroborous also)
    votes = subject_question_table_to_votes(
        flat_classifications, 
        question_col='task', 
        answer_col='value', 
        schema=dr5_schema,
        save_loc=settings.panoptes_votes_loc)
