import pandas as pd
from gzreduction.schemas.ukidss_schema import ukidss_schema
from gzreduction.ouroborous import ouroborous_extract_to_votes

from gzreduction.votes_to_predictions import reduce_votes


if __name__ == '__main__':

    schema = ukidss_schema

    caesar_loc = '/media/walml/mirror/galaxy_zoo/older_projects/weekly_dumps/2017-10-15_galaxy_zoo_ukidss_classifications.csv'
    votes_loc = '/home/walml/repos/gz-panoptes-reduction/data/ouroborous/ukidss/votes.csv'
    reduced_votes_loc = '/home/walml/repos/gz-panoptes-reduction/data/ouroborous/ukidss/reduced_votes.csv'


    df = pd.read_csv(caesar_loc)
    # df = df.sample(10000)
    votes = ouroborous_extract_to_votes.load_ukidss_votes(df, save_loc=votes_loc)
    print(votes)

    # votes = pd.read_csv(votes_loc)
    # votes = votes.sample(100000)
    reduced_votes = reduce_votes.reduce_all_questions(votes, schema, save_loc=reduced_votes_loc)\
    
    print(reduced_votes)
