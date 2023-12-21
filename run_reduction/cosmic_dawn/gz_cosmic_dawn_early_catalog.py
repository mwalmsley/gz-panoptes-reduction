import os
from urllib.parse import urlparse

import numpy as np
import pandas as pd

if __name__ == '__main__':

    repo_dir = '/home/walml/repos'

    df = pd.read_parquet(os.path.join(repo_dir, 'gz-downloads/cosmic_dawn/gz_cosmic_dawn_aggregation_2023-01-02_ortho.parquet'))
    df['id_str'] = df['id_str'].astype(int)
  
    # add file locs
    file_key = pd.read_csv(os.path.join(repo_dir, 'gz-downloads/cosmic_dawn/cosmic_dawn_file_key.csv'))
    file_key['filename'] = file_key['locations'].apply(lambda x: f'{os.path.basename(urlparse(x).path)}')
    # file_key['file_loc'] =  file_key['filename'].apply(lambda x: os.path.join(repo_dir, 'zoobot/data/example_images/cosmic_dawn', x))  # absolute path, annoying to share to cluster
    file_key['file_loc'] =  file_key['filename'].apply(lambda x: os.path.join('data/example_images/cosmic_dawn', x))  # relative path

    df = pd.merge(df, file_key[['id_str', 'file_loc']], on='id_str', how='inner')

    # add coordinates
    # HSC is so dense that galaxies often appear in each others' images. To avoid data leakage, designate fixed train/test based on right ascension
    # see Slack with James for original
    coords = pd.read_csv(os.path.join(repo_dir, 'gz-downloads/cosmic_dawn/df_18662_subjects_at_launch_v2_is_star.csv'))
    print(coords.head())
    ra_split = np.quantile(coords['ra'], q=0.7)
    coords['in_test'] = coords['ra'] >= ra_split
    print(coords['in_test'].value_counts())
    coords['id_str'] = coords['id']
    print(len(df))
    df = pd.merge(df, coords[['id_str', 'in_test', 'ra', 'dec', 'is_star']], how='inner')
    print(len(df))

    print(df.head())

    # save here
    df.to_parquet(os.path.join(repo_dir, 'gz-downloads/cosmic_dawn/gz_cosmic_dawn_aggregation_latest_ortho_with_file_locs_and_coords.parquet'), index=False)
    # also save to zoobot for finetuning
    df.to_parquet(os.path.join(repo_dir, 'zoobot/data/gz_cosmic_dawn_aggregation_latest_ortho_with_file_locs_and_coords.parquet'), index=False)
