from os import path
from os.path import exists, join
from typing import Dict, Tuple

import pandas as pd
import simplejson
from django.conf import settings

from gdelt_proxy.pre_processing.ex_task import ExTask
from gdelt_proxy.pre_processing.graph_task import GraphTask
from gdelt_proxy.pre_processing.stacked_graph_task import StackedGraphTask
from gdelt_proxy.pre_processing.query import (query_google_BQ,
                                              query_params_to_id)

import json
from gdelt_proxy.pre_processing.json import dict_to_json_file


# Put your new preprocessing task in the list
TASKS = [GraphTask, StackedGraphTask]

# Check the tasks are correctly set up
test = []
for task in TASKS:
    task_name = task.task_name
    assert task_name is not None, "You forget to set the task name..."
    assert task_name not in test, "task_name already taken, change it"
    test.append(task_name)


def pre_propress(full_df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Separated function for offline test in notebook mainly

    Arguments:
        full_df {pd.DataFrame}

    Returns:
        Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame] -- [returns the 3 df the Task.run function should accept]
    """

    # convert date
    for col in ['mentionDateAdded', 'eventDateAdded']:
        full_df[col] = pd.to_datetime(full_df[col], format="%Y%m%d%H%M%S")

    event_cols = [col for col in full_df.columns if 'event' in col.lower()]
    mention_cols = [col for col in full_df.columns if 'mention' in col.lower()]

    events_df = full_df[event_cols] \
        .groupby(by='eventId') \
        .first() \
        .reset_index()

    mentions_df = full_df[['eventId'] + mention_cols]

    return full_df, events_df, mentions_df


def run_dataset_pipeline(full_df):
    full_df, events_df, mentions_df = pre_propress(full_df)

    res = dict()
    for task in TASKS:
        res[task.task_name] = task.run(full_df, events_df, mentions_df)

    return res
