from os import path
from os.path import exists, join
from typing import Dict, Tuple

import pandas as pd
import simplejson
from django.conf import settings

from gdelt_proxy.pre_processing.ex_task import ExTask
from gdelt_proxy.pre_processing.json import dict_to_json_file, json_to_dict
from gdelt_proxy.pre_processing.query import (query_google_BQ,
                                              query_params_to_id)
from gdelt_proxy.pre_processing.run_dataset_pipeline import \
    run_dataset_pipeline

CACHED_QUERIES_PIPELINES_DIR = join(
    settings.BASE_DIR, 'cached_queries_pipelines')


def run_pipeline(**kwargs) -> Dict:
    # First we check if a pipeline wasn't already run for that query
    cached_fp = query_params_to_id(**kwargs)
    cached_fp = join(CACHED_QUERIES_PIPELINES_DIR, cached_fp + '.json')

    if settings.CACHE_PIPELINES:
        if exists(cached_fp):
            return json_to_dict(cached_fp)

    if settings.OFF_LINE_PREPROCESSING:
        full_df = pd.read_csv(
            path.join(settings.BASE_DIR, 'datasets/ex_GBQ_res.csv'))
    else:
        full_df = query_google_BQ(**kwargs)

    # Return a flag when there is actually no data to process
    if full_df.empty:
        return dict(empty=True)

    processed = run_dataset_pipeline(full_df)

    if settings.CACHE_PIPELINES:
        dict_to_json_file(processed, cached_fp)

    return processed
