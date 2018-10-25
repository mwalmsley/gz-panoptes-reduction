
import logging

import pandas as pd

from gzreduction.schemas.dr2_schema import dr2_schema
from gzreduction import settings


def load_decals_votes(dr1_classifications, dr2_classifications, save_loc=None):
    """
    Transform raw ouroborous DR1/DR2 extracts into unified vote table
    Remove extremely rare answers on the assumption they are from temporary workflow versions
    DR1 and DR2 classifications are saved separately for historical reasons.
    Args:
        dr1_classifications (pd.DataFrame): raw Ouroborous DR1 export
        dr2_classifications (pd.DataFrame): raw Ouroborous DR2 export
        save_loc (str): if not None, save combined votes to here

    Returns:
        (pd.DataFrame) votes from DR1/ DR2. Rows by classification, columns by question_answer, values by count [1, 0]
    """

    dr1_df = remove_rare_answers(dr1_classifications, dr2_schema)
    dr2_df = remove_rare_answers(dr2_classifications, dr2_schema)

    # we need to make sure that the questions are the same
    for question in dr2_schema.get_raw_question_names():
        dr1_answers = dr1_df[question].dropna().unique()
        dr2_answers = dr2_df[question].dropna().unique()
        assert all([answer in dr2_answers for answer in dr1_answers])
        assert all([answer in dr1_answers for answer in dr2_answers])

    df = pd.concat([dr1_df, dr2_df])

    translated_df = explode_df(df, dr2_schema)

    votes = dr2_schema.rename_df_using_schema(translated_df)

    if save_loc:
        votes.to_csv(save_loc, index=False)

    return votes


def remove_rare_answers(df, schema):
    """
    Remove extremely rare answers left over from previous workflow versions e.g. decals-0_a-3
    Args:
        df (pd.DataFrame): DR1/DR2 concatenated raw export
        schema (Schema): definition object for questions and answers

    Returns:
        (pd.DataFrame) original df with answers rarer than 0.1% per question removed
    """
    for question in schema.questions:
        value_counts = df[question.raw_name].value_counts()
        if value_counts.min() < len(df) * 0.0001:  # fewer than 1 in 10,000 most likely beta responses
            # TODO could use date filtering instead
            df = df[df[question.raw_name] != value_counts.idxmin()]
    return df


def explode_df(df, schema):
    """
    Transform ouroborous DR1/DR2 extracts into unified vote table
    Designed for Ouroborous raw data extract, after removing rare answers.
    Args:
        df (pd.DataFrame): rows by classification, columns by question (e.g. 'decals-0'), values by answer (e.g. 'a-0')
        schema (Schema): definition object for questions and answers

    Returns:
        (pd.DataFrame) votes from DR1/ DR2. Rows by classification, columns by question_answer, values by count [1, 0]
    """

    questions = schema.questions  # objects
    
    all_translated_responses = []
    for question in questions:

        unique_answers = question.get_raw_answer_names()
        count_columns = question.get_all_raw_count_columns()
        # remove if they exist already
        df = df.drop(count_columns, errors='ignore', axis=1)
        
        # get counts
        dummy_df = pd.get_dummies(df[question.raw_name])
        
        # append column names with question
        dummy_df = dummy_df.rename(columns=dict(zip(unique_answers, count_columns)))

        # make sure you add empty column if any questions have no answers
        for count_column in count_columns:
            if count_column not in dummy_df.columns.values:
                dummy_df[count_column] = 0
    
        all_translated_responses.append(dummy_df)

    # translated responses only includes answers. Add back to original df.
    return pd.concat([df] + all_translated_responses, axis=1)


if __name__ == '__main__':

    logging.basicConfig(
        filename='ouroborous_extract_to_votes.log',
        format='%(asctime)s %(message)s',
        filemode='w',
        level=logging.DEBUG)

    logging.info('Creating new DR2 predictions from {} and {}'.format(
        settings.dr1_classifications_export_loc,
        settings.dr2_classifications_export_loc
    ))
    dr1_df = pd.read_csv(settings.dr1_classifications_export_loc)
    dr2_df = pd.read_csv(settings.dr2_classifications_export_loc)
    votes = load_decals_votes(dr1_df, dr2_df, save_loc=settings.dr2_votes_loc)
    logging.info('Saved new DR2 votes to {}'.format(settings.dr2_votes_loc))
