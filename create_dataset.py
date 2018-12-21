"""
This scripts should be used to create new datasets.

You should change the parameters and then run:
python ./create_dataset.py
"""


from os.path import join

from gdelt_proxy.pre_processing.json import dict_to_json_file
from gdelt_proxy.pre_processing.query import query_google_BQ
from gdelt_proxy.pre_processing.run_dataset_pipeline import \
    run_dataset_pipeline
from set_up import datasets_dir, datasets_pipelines

###################
## Parameters #####
###################

##############
## 1st dataset

# desired_file_name = "gilet_jaune_30th_November_->_2nd_December"

# date_begin = "2018-11-30T00:00:00.000Z"
# date_end = "2018-12-02T23:59:00.000Z"
# minimum_distinct_source_count = 3
# confidence = 100
# limit = 100000
# filterMentionId = "gilet.*jaune"

##############
## 2nd dataset

# desired_file_name = "gilet_jaune_7th_December_->_8th_December"

# date_begin = "2018-12-07T00:00:00.000Z"
# date_end = "2018-12-08T23:59:00.000Z"
# minimum_distinct_source_count = 3
# confidence = 100
# limit = 100000
# filterMentionId = "gilet.*jaune"

##############
# 3rd dataset

# desired_file_name = "gilet_jaune_10th_December_->_11th_December"

# date_begin = "2018-12-10T00:00:00.000Z"
# date_end = "2018-12-11T23:59:00.000Z"
# minimum_distinct_source_count = 3
# confidence = 100
# limit = 100000
# filterMentionId = "gilet.*jaune"

##############
# 4th dataset

# desired_file_name = "Donald_Trump_election_8th_November_2016_->_9th_November_2016"

# date_begin = "2016-11-08T00:00:00.000Z"
# date_end = "2016-11-09T23:59:00.000Z"
# minimum_distinct_source_count = 50
# confidence = 100
# limit = 100000
# filterMentionId = "trump.*elec"

##############
# 5th dataset

desired_file_name = "Khashoggi_case_20th_October_2018_->_21th_October_2018"

date_begin = "2018-10-20T00:00:00.000Z"
date_end = "2018-10-21T23:59:00.000Z"
minimum_distinct_source_count = 20
confidence = 100
limit = 100000
filterMentionId = "khashoggi"

##################################################
## Making the query and running the pipelies  ####
##################################################

full_df = query_google_BQ(date_begin,
                          date_end,
                          minimum_distinct_source_count,
                          confidence,
                          limit,
                          filterMentionId)

if full_df.empty:
    raise Exception("The dataframe returned by Google Big Query is empty!")

# Export the unprocess dataset to the dataset dir
full_df.to_csv(join(datasets_dir, desired_file_name + '.csv'), index=False)
print(full_df.shape)

# Run pipelines
res = run_dataset_pipeline(full_df)
dict_to_json_file(res, join(datasets_pipelines, desired_file_name + '.json'))
