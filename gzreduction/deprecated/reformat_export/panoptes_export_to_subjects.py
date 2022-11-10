# don't even need Spark for this, 2G file
import json
import argparse

import numpy as np
import pandas as pd


def extract_subjects(df):
    # will not filter by workflow

    assert not any(df['subject_id'].isna())
    # drop exact duplicates
    df = df.drop_duplicates().reset_index(drop=True)
    # subjects should not repeat
    duplicated = df.duplicated(subset=['subject_id'])
    if np.any(duplicated):
        print(df['subject_id'].value_counts().value_counts())
        exit(1)
    # df = df.drop_duplicates(subset=['subject_id'], keep='first')

    df['metadata'] = df['metadata'].apply(json.loads)
    subject_df = df[['subject_id']].copy()  # one column df
    subject_df['subject_url'] = df['locations'].apply(lambda x: json.loads(x)['0'])
    assert not any(subject_df['subject_url'].isna())
    key_metadata_dicts = list(df['metadata'].apply(extract_metadata).values)
    # metadata_dicts = list(df['metadata'].values)
    metadata_df = pd.DataFrame(data=key_metadata_dicts)
    # print(len(metadata_df))
    assert len(metadata_df) == len(subject_df)
    # print(metadata_df.tail())
    # print(subject_df.tail())
    # # exit()

    # print(subject_df.index)
    # print(metadata_df.index)

    # exit()
    subject_df = pd.concat([subject_df, metadata_df], axis=1)
    print(subject_df.tail())
    assert not any(subject_df['subject_url'].isna())
    return subject_df



def extract_metadata(metadata):
    key_metadata = {}
    for current_key, new_key in [('!iauname', 'iauname'), ('#iauname', 'iauname'), ('#uploader', 'uploader'), ('#upload_date', 'upload_date'), ('#retirement_limit', 'retirement_limit')]:
        if current_key in metadata:
            if ~pd.isnull(metadata[current_key]):
                key_metadata[new_key] = metadata[current_key]
            else:
                key_metadata[new_key] = None


    return key_metadata

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--subjects',
        dest='input_dir',
        type=str,
        default='data/latest_subjects_export.csv'
    )
    parser.add_argument(
        '--output',
        dest='output_loc',
        type=str,
        default='temp/latest_subjects.parquet'
    )
    args = parser.parse_args()

    df = pd.read_csv(args.input_dir, usecols=['subject_id', 'metadata', 'locations'])

    subject_df = extract_subjects(df)

    subject_df.head()

    subject_df.to_parquet(args.output_loc, index=False)
