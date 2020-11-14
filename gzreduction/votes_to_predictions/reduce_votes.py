
import logging


def reduce_all_questions(input_df, schema, save_loc=None):
    """
    Aggregate votes by subject. Calculate total votes and vote fractions.

    Args:
        input_df (pd.DataFrame) votes with rows=classifications, columns=question_answer, values=1/0. 1 vote per row.
        schema (Schema): definition object for questions and answers
        save_loc (str): (optional) if not None, save aggregated votes here

    Returns:
        (pd.DataFrame) rows of subject, columns of question_answer vote counts. Includes total votes and vote fractions.
    """

    df = input_df.copy()  # avoid mutating input_df with any extra columns

    # work out question and columns
    questions = schema.questions
    count_columns = schema.get_count_columns()

    # every column ending _count should be added
    count_agg = dict(zip(count_columns, ['sum' for n in range(len(count_columns))]))
    # will fail if df is missing a column from the schema
    reduced_df = df.groupby('subject_id').agg(count_agg).reset_index()
    
    # replace all NaN in count columns with 0
    reduced_df = reduced_df.fillna(0)

    # calculate total votes and vote fractions
    for question in questions:
        relevant_answer_cols = question.get_count_columns()  # TODO 'all' is used inconsistently
        reduced_df[question.total_votes] = reduced_df[relevant_answer_cols].sum(axis=1)

        fraction_columns = question.get_fractions_for_question()
        for n in range(len(relevant_answer_cols)):
            reduced_df[fraction_columns[n]] = reduced_df[relevant_answer_cols[n]] / reduced_df[question.total_votes]

        # replace all NaN caused by no votes for that question (rare) with 0
        reduced_df[fraction_columns] = reduced_df[fraction_columns].fillna(0)

    # check that there are no NaN values
    counts = schema.get_count_columns()
    assert not reduced_df[counts].isnull().values.any()
    fractions = schema.get_fraction_columns()
    assert not reduced_df[fractions].isnull().values.any()

    if save_loc is not None:
        reduced_df.to_csv(save_loc, index=False)
        logging.info('Saved reduced votes to {}'.format(save_loc))

    return reduced_df
