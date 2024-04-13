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
        logging.info('Filtering for question: {}'.format(question.name))
        question_df = df[df[question_col] == question.name].dropna(how='all', axis=1)
        if len(question_df) == 0:
            logging.critical(
                'No answers found for question "{}" (task col: "{}"). Making some up to avoid crashing!'.format(
                    question_col, 
                    question.name
                )
            )
            for answer in question.answers:
                question_df = question_df.append(
                    {question_col: question.name, answer_col: answer.name, 'created_at': pd.Timestamp.now(tz='utc')},
                    ignore_index=True)
            # print(question_df)
        

        logging.info('Answers: {}'.format(question_df[answer_col].value_counts()))

        answers_as_columns = pd.get_dummies(question_df[answer_col]).astype(int)
        # if any answers are missing (i.e. no responses with that answer), add with 0's
        answers_without_responses = set(question.get_answer_names()) - set(answers_as_columns.columns.values)
        if answers_without_responses:
            logging.warning('No-one ever gave the answers {}, filling in with 0s'.format(answers_without_responses))
            for missing_answer in answers_without_responses:
                answers_as_columns[missing_answer] = 0

        # rename answer columns to count columns (i.e. '{question}_{answer}_count') 
        # as some answers are identical for diff. questions
        answers = question.answers
        answer_cols = [x.name for x in answers]  # e.g. 'smooth'
        count_cols = [question.get_count_column(x) for x in answers] # e.g. 'smooth-or-featured_smooth
        answers_as_columns.rename(columns=dict(zip(answer_cols, count_cols)), inplace=True)
        # stick columns for the existence of each answer on to left hand side of df, values as binary indicators
        questions_and_answers = pd.concat([question_df, answers_as_columns], axis=1)
        # questions_and_answers[answers_as_columns] = questions_and_answers[answers_as_columns].fillna(0).astype(int)
        # avoid duplicate columns
        del questions_and_answers[question_col]
        del questions_and_answers[answer_col]
        questions_and_answers.loc[:, count_cols] = questions_and_answers.loc[:, count_cols].astype(int)
        # print(questions_and_answers)
        all_question_dfs.append(questions_and_answers)

    # recombine
    # this works but is high memory complexity as total df has every column for every question
    # votes = pd.concat(all_question_dfs, axis=0).fillna(0).reset_index(drop=True)
    
    # this is more memory efficient but slower
    votes = all_question_dfs[0]
    for q_n, q_df in enumerate(all_question_dfs[1:]):
        logging.info('Merging question {} of {}'.format(q_n, len(all_question_dfs)))
        votes = votes.merge(q_df, on=['created_at', 'classification_id', 'user_id', 'subject_id'], how='outer')


    votes = votes.fillna(0)
        
    votes['created_at'] = pd.to_datetime(votes['created_at'], utc=True)

    # del votes[question_col]
    # del votes['value']

    if save_loc is not None:
        votes.to_csv(save_loc, index=False)
        logging.info('Saved {} Panoptes votes to {}'.format(len(votes), save_loc))

    # first 4 columns are id fields, rest are counts
    # set values of 4th-or-more columns to int
    for col in votes.columns[4:]:
        votes[col] = votes[col].astype(int)

    print(votes.head(20))

    return votes
