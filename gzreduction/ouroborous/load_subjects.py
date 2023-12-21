
import json

import pandas as pd


def load_current_subjects(loc, workflow=None, subject_set=None, save_loc=None):
    df = pd.read_csv(loc, dtype={
        'workflow_id': str,
        'subject_set_id': str
    })
    if workflow is not None:
        df = df[df['workflow_id'] == workflow]
    if subject_set is not None:
        df = df[df['subject_set_id'] == subject_set]
    df_with_metadata = split_json_str_to_columns(df, 'metadata')
    df_with_metadata['locations'] = df_with_metadata['locations'].apply(lambda x: json.loads(x)['0'])

    current_hidden_cols = list(filter(lambda x: x.startswith('!'), df_with_metadata.columns.values))
    new_cols = list(map(lambda x: x.strip('!'), current_hidden_cols))  # remove special characters for Talk

    # Renaming breaks. Do manually.
    renaming_list = list(zip(current_hidden_cols, new_cols))
    for n in range(len(current_hidden_cols)):
        old_col = renaming_list[n][0]
        new_col = renaming_list[n][1]
        df_with_metadata[new_col] = df_with_metadata[old_col]
        df_with_metadata = df_with_metadata.drop(old_col, axis=1)
    df_with_metadata = df_with_metadata.dropna(how='all', axis=1)
    # df_renamed = df_with_metadata.rename(columns=dict(zip(current_hidden_cols, new_cols)))

    df_with_metadata = df_with_metadata.rename(columns={'0': 'locations'})
    assert len(df_with_metadata) == len(df)

    if save_loc is not None:
        df_with_metadata.to_csv(save_loc)

    return df_with_metadata


def split_json_str_to_columns(input_df, json_column_name):
    """
    Expand Dataframe column of json string into many columns
    Args:
        input_df (pd.DataFrame): dataframe with json str column
        json_column_name (str): json string column name
    Returns:
        (pd.DataFrame) input dataframe with json column expanded into many columns
    """
    input_df = input_df.reset_index()  # concat cares about the index, but we want a literal join
    json_df = pd.DataFrame(list(input_df[json_column_name].apply(json.loads)))
    new_df = pd.concat([input_df, json_df], axis=1)
    assert len(new_df) == len(input_df)
    return new_df
