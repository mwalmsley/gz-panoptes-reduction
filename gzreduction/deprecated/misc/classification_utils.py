from typing import List

import pandas as pd


def get_all_duplicates(df: pd.DataFrame) -> List:
    """Find the subject ids of all subjects where the `iauname` metadata field is the same as one or more other subjects
    Note: classifications.csv only lists subjects with at least one classification, so run this often.
    
    Args:
        df (pd.DataFrame): aggregated classifications joined with subject metadata. Probably loaded from classifications.csv
    
    Returns:
        List: all subject ids where `iauname` is the same as at least one other subject
    """
    iauname_counts = df['iauname'].value_counts()
    duplicated_iaunames = iauname_counts[iauname_counts > 1].index
    subject_ids_with_duplicated_iaunames = list(df[df['iauname'].isin(duplicated_iaunames)]['subject_id'].values.astype(str))
    return subject_ids_with_duplicated_iaunames 

