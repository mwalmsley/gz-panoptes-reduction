
import logging

import numpy as np
import pandas as pd

from sklearn import preprocessing

from gzreduction.deprecated.uncertainty import uncertainty


def reduced_votes_to_predictions(df, schema, save_loc):
    """
    Calculate predicted answers and uncertainty from reduced vote counts.
    Args:
        df (pd.DataFrame) rows of subject, columns of question_answer counts. Includes total votes and vote fractions.
        schema (Schema): definition object for questions and answers
        save_loc (str): if not None, save predictions here

    Returns:
        (pd.DataFrame) df with prediction, prediction_conf, vote_fraction_min, vote_fraction_max columns added
    """
    predictions = get_predictions(df, schema)
    encoded_predictions = encode_answers(predictions, schema)

    if save_loc is not None:
        logging.info('Saved predictions to {}'.format(save_loc))
        encoded_predictions.to_csv(save_loc, index=False)

    return encoded_predictions


def get_predictions(df, schema):
    """
    For each question in schema, find:
        - prediction
        - prediction confidence
        - 80% confidence interval +/- vote fraction values for all answers

    Args:
        df (pd.DataFrame) rows of subject, columns of question_answer counts. Includes total votes and vote fractions
        schema (Schema): definition object for questions and answers

    Returns:
        (pd.DataFrame) with prediction, prediction confidence and confidence intervals added
    """

    prediction_df = df.copy()
    for question in schema.questions:
        prediction_df = get_predictions_for_question(prediction_df, question)

    return prediction_df


def get_predictions_for_question(df, question):
    """
    For the provided question, find:
        - prediction
        - prediction confidence
        - 80% confidence interval +/- vote fraction values for all answers

    Args:
        df (pd.DataFrame) rows of subject, columns of question_answer counts. Includes total votes and vote fractions
        question (Question): definition object for question

    Returns:
        (pd.DataFrame) with prediction, prediction confidence and confidence intervals added for question
    """

    df = df.drop([question.prediction, question.prediction_conf], axis=1, errors='ignore')

    relevant_count_cols = question.get_count_columns()  # get fraction cols

    most_common_answer_cols = df[relevant_count_cols].fillna(0).idxmax(axis=1).values  # first if tie
    # inverse: count column of known question to answer value
    most_common_answers = list(map(lambda x: question.get_answer_from_count_column(x).name, most_common_answer_cols))

    df[question.prediction] = most_common_answers

    total_votes_by_row = df[relevant_count_cols].sum(axis=1)
    df[question.total_votes] = total_votes_by_row

    votes_for_most_popular = df[relevant_count_cols].max(axis=1)
    df[question.prediction_conf] = votes_for_most_popular / total_votes_by_row
    # if total votes is 0, this will be na. Replace with confidence appropriate for equal likelihood.
    df[question.prediction_conf] = df[question.prediction_conf] .fillna(
        value=1./len(question.answers))

    # calculate uncertainty - disabled as binomial approximation known to have some fairly important failures e.g. on extreme vote fractions, irrelvant answers
    # for answer in question.answers:
    #     fraction_min_col = question.get_fraction_min_col(answer)
    #     fraction_max_col = question.get_fraction_max_col(answer)

    #     row_iter = [df.iloc[n].to_dict() for n in range(len(df))]

    #     votes_series = list(map(lambda x: votes_from_subject_row(x, question=question, answer=answer), row_iter))

    #     df[fraction_min_col] = list(map(lambda x: uncertainty.get_min_p_estimate(x, interval=0.8), votes_series))
    #     df[fraction_max_col] = list(map(lambda x: uncertainty.get_max_p_estimate(x, interval=0.8), votes_series))

    return df


def votes_from_subject_row(row, question, answer):
    """
    Infer volunteer votes for answer to question, using row of reduced vote counts
    Note: this is not easier pre-reduction because '0' means a vote for (potentially) another question or no response
    Note: Likely to be deprecated when more sophisticated reduction is used
    Args:
        row (pd.Series): vote counts for a subject by question_answer, including total_votes for question
        question (Question): question to find votes for
        answer (Answer): answer to find votes for

    Returns:
        (np.array): [1, 0] ints representing each vote for answer (1) or another answer within question (0)
    """
    total_votes_value = row[question.total_votes]
    count_for_answer = row[question.get_count_column(answer)]
    return np.array(([1] * int(count_for_answer)) + ([0] * (int(total_votes_value) - int(count_for_answer))))


def get_encoders(schema):
    """
    Create encoders to transform answer predictions into ints. Useful for machine learning tools.
    Args:
        schema (Schema): definition object for questions and answers

    Returns:
        (dict) of form {question.name: encoder for question (can transform response to int and vica versa)}
    """
    encoders = dict([(question.name, preprocessing.LabelEncoder()) for question in schema.questions])

    assert len(schema.get_question_names()) == len(set(schema.get_question_names()))  # questions must be uniquely named
    for question in schema.questions:
        encoders[question.name] = encoders[question.name].fit(question.get_answer_names())

    return encoders


def encode_answers(df, schema):
    """
    Create encoded predictions for every question. Useful for machine learning tools.
    Args:
        df (pd.DataFrame): reduced votes including predicted answer to each question as string value (e.g 'smooth')
        schema (Schema): definition object for questions and answers

    Returns:
        (pd.DataFrame): df with encoded prediction columns for each question (e.g. 1 )
    """
    encoders = get_encoders(schema)

    for question in schema.questions:
        df[question.prediction_encoded] = encoders[question.name].transform(df[question.prediction])

    return df
