import pandas as pd
from typing import Dict
from gdelt_proxy.pre_processing.abstract_task import Task, pandasDfToDict


class ExTask(Task):
    """Abstract class that handles one pipeline task.
    """

    # Holds the task name, should be overrided in subclasses
    task_name = "ExampleTask"

    @classmethod
    def run(cls,
            full_df: pd.DataFrame,
            events_df: pd.DataFrame,
            mentions_df: pd.DataFrame) -> Dict:

        best_events = events_df \
            .sort_values(by='eventSourceCount', ascending=False) \
            .head(10)

        corresponding_mentions = mentions_df[mentions_df.eventId.isin(best_events.eventId)] \
            .sort_values(by=['eventId', 'mentionDateAdded'])

        return dict(best_events=pandasDfToDict(best_events),
                    mentions=pandasDfToDict(corresponding_mentions))
