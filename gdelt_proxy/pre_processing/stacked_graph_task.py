import pandas as pd
from typing import Dict
from gdelt_proxy.pre_processing.abstract_task import Task, pandasDfToDict


class StackedGraphTask(Task):
    """Handles data for the stacked graph.
    """

    # Holds the task name, should be overrided in subclasses
    task_name = "StackedGraphTask"

    # Config
    dt_round = 'H'
    num_dom_outlets = 10

    @classmethod
    def run(cls,
            full_df: pd.DataFrame,
            events_df: pd.DataFrame,
            mentions_df: pd.DataFrame) -> Dict:

        streamg_df = StackedGraphTask.process_streamgraph_data(mentions_df)

        data = dict(
            streamgraph=streamg_df.to_dict(orient='records')
        )

        return data

    @classmethod
    def process_streamgraph_data(cls, mentions_df):
        """"""
        mentions = mentions_df[
            ['eventId', 'mentionDateAdded', 'mentionSourceName']] \
            .assign(roundedMentionDate=mentions_df.mentionDateAdded
                    .dt.round(cls.dt_round)) \
            .drop(columns={'mentionDateAdded'}) \
            .drop_duplicates()

        num_other_sources = mentions \
            .groupby(['roundedMentionDate', 'eventId']) \
            .count() \
            .apply(lambda x: x - 1) \
            .rename(columns={'mentionSourceName': 'numOtherSources'}) \
            .reset_index()

        outlet_degree = mentions \
            .merge(num_other_sources, on=['roundedMentionDate', 'eventId']) \
            .drop(columns={'eventId'}) \
            .groupby(['roundedMentionDate', 'mentionSourceName']) \
            .sum() \
            .reset_index()

        dom_outlets = outlet_degree \
            .groupby('mentionSourceName') \
            .sum() \
            .reset_index() \
            .sort_values(by='numOtherSources', ascending=False) \
            .head(cls.num_dom_outlets) \
            .mentionSourceName

        pivot_table = \
            outlet_degree[outlet_degree.mentionSourceName.isin(dom_outlets)] \
            .pivot(
                index='roundedMentionDate',
                columns='mentionSourceName',
                values='numOtherSources'
            ).fillna(0) \
            .astype(int) \
            .reset_index()

        stacked_graph_data = pivot_table \
            .assign(mentionInterval=pivot_table.roundedMentionDate
                    .astype(str)) \
            .drop(columns={'roundedMentionDate'})

        return stacked_graph_data
