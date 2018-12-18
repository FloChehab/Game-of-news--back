import pandas as pd
from typing import Dict
from gdelt_proxy.pre_processing.abstract_task import Task


class StackedGraphTask(Task):
    """Handles data for the stacked graph.
    """

    # Holds the task name, should be overrided in subclasses
    task_name = "stackedGraph"

    # Config
    dt_round = 'H'
    num_dom_outlets = 10
    dom_outlets = None

    @classmethod
    def run(cls,
            full_df: pd.DataFrame,
            events_df: pd.DataFrame,
            mentions_df: pd.DataFrame) -> Dict:

        max_date = full_df.eventDateAdded.max()

        mentions = full_df \
            .drop(columns=['mentionSourceName']) \
            .assign(mentionSourceName=full_df.mentionSourceName.str
                    .split('.', n=1, expand=True).iloc[:, 0]) \
            .loc[full_df.mentionDateAdded <= max_date] \
            .loc[:, ['eventId', 'mentionSourceName']] \
            .assign(roundedMentionDate=full_df.mentionDateAdded
                    .dt.floor(cls.dt_round)) \
            .drop_duplicates()

        mentions_with_tone = mentions \
            .assign(positiveTone=full_df['mentionDocTone']
                    - full_df['eventAvgTone'] > 0)

        dates = mentions.roundedMentionDate \
            .astype(str) \
            .sort_values() \
            .unique() \
            .tolist()

        return dict(
            dates=dates,
            streamgraph=StackedGraphTask.process_streamgraph_data(mentions),
            drilldown=StackedGraphTask.process_drilldown_data(
                mentions_with_tone)
        )

    @staticmethod
    def calculate_num_other_sources(mentions, with_tone=False):
        g = ['roundedMentionDate', 'eventId']

        if with_tone:
            g.append('positiveTone')

        return mentions \
            .groupby(g) \
            .count() \
            .apply(lambda x: x-1) \
            .rename(columns={'mentionSourceName': 'numOtherSources'}) \
            .reset_index()

    @staticmethod
    def calculate_outlet_degree(mentions, num_other_sources, with_tone=False):
        merged = mentions \
            .merge(num_other_sources, on=['roundedMentionDate', 'eventId'])
        d = ['eventId']
        g = ['roundedMentionDate', 'mentionSourceName']

        if with_tone:
            merged = merged.assign(agreeing=merged.positiveTone_x == merged
                                   .positiveTone_y)
            d.extend(['positiveTone_x', 'positiveTone_y'])
            g.append('agreeing')

        return merged.drop(columns=d).groupby(g).sum().reset_index()

    @classmethod
    def calculate_dom_outlets(cls, outlet_degree):
        cls.dom_outlets = outlet_degree \
            .groupby('mentionSourceName') \
            .sum() \
            .reset_index() \
            .sort_values(by='numOtherSources', ascending=False) \
            .head(cls.num_dom_outlets) \
            .mentionSourceName

    @staticmethod
    def prepare_pivot_table(pivot_table):
        pivot_table = pivot_table \
            .fillna(0) \
            .astype(int) \
            .reset_index()

        return pivot_table \
            .assign(mentionInterval=pivot_table.roundedMentionDate
                    .astype(str)) \
            .drop(columns=['roundedMentionDate'])

    @classmethod
    def process_streamgraph_data(cls, mentions):
        num_other_sources = \
            StackedGraphTask.calculate_num_other_sources(mentions)
        outlet_degree = \
            StackedGraphTask.calculate_outlet_degree(mentions,
                                                     num_other_sources)
        StackedGraphTask.calculate_dom_outlets(outlet_degree)
        pivot_table = \
            outlet_degree[outlet_degree.mentionSourceName.isin(
                cls.dom_outlets)] \
            .pivot_table(
                index='roundedMentionDate',
                columns='mentionSourceName',
                values='numOtherSources'
            )

        streamgraph_data = StackedGraphTask.prepare_pivot_table(pivot_table)

        return streamgraph_data.to_dict(orient='records')

    @classmethod
    def process_drilldown_data(cls, mentions):
        num_other_sources = \
            StackedGraphTask.calculate_num_other_sources(mentions, True)
        outlet_degree = \
            StackedGraphTask.calculate_outlet_degree(mentions,
                                                     num_other_sources,
                                                     True)
        pivot_table = \
            outlet_degree[outlet_degree.mentionSourceName.isin(
                cls.dom_outlets)] \
            .pivot_table(
                index=['mentionSourceName', 'roundedMentionDate'],
                columns='agreeing',
                values='numOtherSources'
            )

        drilldown_data = StackedGraphTask.prepare_pivot_table(pivot_table)

        return drilldown_data.groupby('mentionSourceName') \
            .apply(lambda x: x[['mentionInterval', True, False]]
                   .to_dict(orient='records')) \
            .to_dict()
