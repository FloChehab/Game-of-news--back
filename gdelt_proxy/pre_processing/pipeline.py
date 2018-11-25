from typing import Dict, Tuple
from gdelt_proxy.pre_processing.query import query_google_BQ
from gdelt_proxy.pre_processing.ex_task import ExTask
from django.conf import settings
import pandas as pd
from os import path

# Put your new preprocessing task in the list
TASKS = [ExTask]

# Check the tasks are correctly set up
test = []
for task in TASKS:
    print(task)
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

    event_cols = [col for col in full_df.columns if 'event' in col.lower()]
    mention_cols = [col for col in full_df.columns if 'mention' in col.lower()]

    events_df = full_df[event_cols] \
        .groupby(by='eventId') \
        .first() \
        .reset_index()

    mentions_df = full_df[['eventId'] + mention_cols]

    # df_source_id is a dataframe with two columns:
    #   - MentionIdentifier (the url to the article)
    #   - mentionId a unique number for each MentionIdentifier (takes less storage)
    # df_mentions_id = full_df.MentionIdentifier \
    #     .value_counts() \
    #     .reset_index()['index'] \
    #     .reset_index() \
    #     .rename(columns={'index': 'MentionIdentifier', 'level_0': 'mentionId'})

    # df_GBQ = full_df \
    #     .merge(df_mentions_id, on='MentionIdentifier') \
    #     .drop('MentionIdentifier', axis=1)

    return full_df, events_df, mentions_df


def run_pipeline(**kwargs) -> Dict:
    if settings.OFF_LINE_PREPROCESSING:
        full_df = pd.read_csv(
            path.join(settings.BASE_DIR, 'datasets/ex_GBQ_res.csv'))
    else:
        full_df = query_google_BQ(**kwargs)

    full_df, events_df, mentions_df = pre_propress(full_df)

    res = dict()
    for task in TASKS:
        res[task.task_name] = task.run(full_df, events_df, mentions_df)

    return res
