def get_shard_locs(shard_dir):
    candidates = [os.path.join(shard_dir, name) for name in os.listdir(shard_dir)]
    return list(filter(lambda x: 'part' in x and not x.endswith('.crc'), candidates))


def join_shards(shard_dir, header):
    shard_locs = get_shard_locs(shard_dir)  # Spark outputs many headerless shards, not one csv
    assert shard_locs
    print(shard_locs)
    print(header)
    all_dfs = [pd.read_csv(loc, index_col=False, header=None, names=header) for loc in shard_locs]
    logging.info('Shards: {}'.format(len(all_dfs)))
    df = pd.concat(all_dfs).reset_index(drop=True)
    assert len(df) > 0
    logging.info('Total joined lines: {}'.format(len(df)))
    logging.info(df.iloc[0])
    return df


if __name__ == '__main__':

    logging.basicConfig(
        filename='responses_to_votes.log',
        format='%(asctime)s %(message)s',
        filemode='w',
        level=logging.DEBUG)

    response_dir = settings.panoptes_flat_classifications
    responses = join_shards(response_dir, header=panoptes_to_responses.response_to_line_header())  # avoid duplication of schema

    # turn flat table into columns of votes. Standard analysis view (Ouroborous also)
    votes = get_votes(
        responses, 
        question_col='task', 
        answer_col='value', 
        schema=dr5_schema,
        save_loc=settings.panoptes_votes_loc)
