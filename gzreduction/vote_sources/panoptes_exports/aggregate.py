import pandas as pd
import functools

# non spark version of spark/aggregate.py

def responses_to_reduced_votes(flat_df: pd.DataFrame, schema, drop_people=[]):
    """Aggregate flat (subject, classification, question, answer) table into total votes etc by subject_id
    Optionally, drop classifications from person_id in 'drop_people' list

    Args:
        flat_df ([type]): [description]
        drop_people ([type], optional): [description]. Defaults to None.

    Returns:
        [type]: [description]
    """

    # should already be done but just in case
    flat_df = flat_df.drop_duplicates(subset=['classification_id', 'task', 'value'])

    # if drop_people:
    #     print(f'Removing {len(drop_people)} people. Before: {flat_df.count()} responses')
    #     flat_df = flat_df.filter(~flat_df['person_id'].isin(drop_people))  # important ~
    #     print(f'After: {flat_df.count()} responses')

    # aggregate by creating question_response pairs, grouping, pivoting, and summing
    flat_df['question_response'] = flat_df['task'] + '_' + flat_df['value']  # will use as columns
    flat_df['dummy_response'] = 1  # will use as values
    # doesn't aggregate, just reshapes
    # will fail if a user classifies everything
    df = flat_df.pivot(index=['classification_id', 'id_str'], columns='question_response', values='dummy_response').reset_index()  # switched to subject_id
    df = df.fillna(0)
    # del df['question_response']


    # add any missing question_response
    initial_cols = df.columns.values
    for col in schema.get_count_columns():
        if col not in initial_cols:  # not created in pivot as no examples given
            df[col] = 0  # create, fill with 0's


    # reorder columns
    df = df[['id_str'] + schema.get_count_columns()]

    df = df.groupby('id_str').agg('sum').reset_index()

    # now aggregate after
    for col in schema.get_count_columns():
        assert col in df.columns.values
        df[col] = df[col].astype(int)

    # calculate total responses per question
    for question in schema.questions:
        # TODO could simplify
        df[question.total_votes] = functools.reduce(lambda x, y: x + y, [df[col] for col in question.get_count_columns()])
        df[question.total_votes] = df[question.total_votes].astype(int)

    # calculate fractions per answer
    for question in schema.questions:
        for answer in question.answers:
            df[question.get_fraction_column(answer)] = df[question.get_count_column(answer)] / df[question.total_votes]  # may give nans?
        df = df.fillna(0)  # some total vote columns will be 0, but this doesn't seem to cause na?

    return df
