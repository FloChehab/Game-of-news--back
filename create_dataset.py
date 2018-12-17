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

desired_file_name = "test"

date_begin = "2018-12-10T00:00:00.000Z"
date_end = "2018-12-17T00:00:00.000Z"
minimum_distinct_source_count = 10
confidence = 100
limit = 100000
filterMentionId = "gilet"

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

# Run pipelines
res = run_dataset_pipeline(full_df)
dict_to_json_file(res, join(datasets_pipelines, desired_file_name + '.json'))
