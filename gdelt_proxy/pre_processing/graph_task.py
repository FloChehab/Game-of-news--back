import pandas as pd
from typing import Dict
from gdelt_proxy.pre_processing.abstract_task import Task, pandasDfToDict


class GraphTask(Task):
    """Abstract class that handles one pipeline task.
    """

    # Holds the task name, should be overrided in subclasses
    task_name = "graph"

    @classmethod
    def run(cls,
            full_df: pd.DataFrame,
            events_df: pd.DataFrame,
            mentions_df: pd.DataFrame) -> Dict:

        best_events = events_df \
            .sort_values(by='eventSourceCount', ascending=False) \
            .head(5)

        best_mentions = mentions_df[mentions_df.eventId.isin(
            best_events.eventId)]

        sourceIds = best_mentions.mentionSourceName.value_counts() \
            .reset_index() \
            .reset_index() \
            .rename(columns={'level_0': 'sourceId',
                             'index': 'sourceName'}) \
            .drop('mentionSourceName', axis=1)

        final = best_mentions.merge(sourceIds, left_on='mentionSourceName', right_on='sourceName') \
            .groupby(by=['eventId', 'sourceId']).mean()

        data = {}
        for g, r in final.iterrows():
            eventId, sourceId = g
            if eventId not in data.keys():
                data[eventId] = {}
            data[eventId][sourceId] = round(r.mentionDocTone, 2)

        return dict(data=data,
                    mappingIdSource=sourceIds.to_dict()['sourceName'])
