import pandas as pd
from typing import Dict
from gdelt_proxy.pre_processing.abstract_task import Task, pandasDfToDict
import numpy as np

NB_SITES = 40


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

        cols = ['eventId', 'mentionSourceName']

        # If an outlet has spoken multiple times about the same event, we average all the mentions tone
        mentions_cleaned = mentions_df[cols+['mentionDocTone']] \
            .groupby(by=cols) \
            .mean() \
            .rename(columns={'mentionDocTone': 'avgTone'}) \
            .reset_index()

        # reduce data size
        mentions_cleaned.avgTone = mentions_cleaned.avgTone.round(2)

        # list of the source we will consider
        top_sources = mentions_cleaned.mentionSourceName.value_counts().head(NB_SITES)

        mentions_filtered = mentions_cleaned[mentions_cleaned.mentionSourceName.isin(
            top_sources.index)]

        # retreive the first url
        mentions_with_first_url = mentions_df[cols + ['mentionIdentifier']] \
            .groupby(by=cols) \
            .first() \
            .reset_index()

        mentions_filtered = mentions_filtered.merge(
            mentions_with_first_url, on=cols)

        # nasty cross product, yes.
        mentions_cross = mentions_filtered.merge(
            mentions_filtered, on='eventId')

        # remove the rows that count the same sources...
        mentions_cross = mentions_cross[mentions_cross.mentionSourceName_x !=
                                        mentions_cross.mentionSourceName_y]

        # We need to switch certain rows to make sure the source in
        # mentionSourceName_x is < than the one in mentionSourceName_y
        mentions_cross_cleaned = mentions_cross.copy()
        mask = mentions_cross.mentionSourceName_y < mentions_cross.mentionSourceName_x
        l1 = ['mentionSourceName_x', 'mentionSourceName_y',
              'avgTone_x', 'avgTone_y']
        l2 = ['mentionSourceName_y', 'mentionSourceName_x',
              'avgTone_y', 'avgTone_x']
        # switch rows
        for col1, col2 in zip(l1, l2):
            mentions_cross_cleaned.loc[mask,
                                       col1] = mentions_cross.loc[mask, col2]
        # now we can drop duplicates : half the data to transfer
        cols_avg = ['avgTone_x', 'avgTone_y']
        cols_sources = ['mentionSourceName_x', 'mentionSourceName_y']
        mentions_cross_cleaned.drop_duplicates(
            ['eventId'] + cols_sources, inplace=True)

        final = dict()

        for i, r in mentions_cross_cleaned.iterrows():
            eventId = r.eventId
            source1 = r.mentionSourceName_x
            source2 = r.mentionSourceName_y
            avgTone1 = r.avgTone_x
            avgTone2 = r.avgTone_y
            url1 = r.mentionIdentifier_x
            url2 = r.mentionIdentifier_y
            if source1 not in final.keys():
                final[source1] = dict()
            if source2 not in final[source1].keys():
                final[source1][source2] = dict()

            final[source1][source2][eventId] = dict(
                avgTone1=avgTone1,
                avgTone2=avgTone2,
                url1=url1,
                url2=url2)

        # Now we can store the average tone also

        mentions_cross_agg = mentions_cross_cleaned.copy()[
            cols_avg + cols_sources]

        mentions_cross_agg['toneDist'] = (
            mentions_cross_agg.avgTone_x - mentions_cross_agg.avgTone_y).abs()
        mentions_cross_agg.toneDist = mentions_cross_agg.toneDist.round(2)
        mentions_cross_agg = mentions_cross_agg.drop(cols_avg, axis=1)

        tmp = mentions_cross_agg.groupby(by=cols_sources) \
            .agg([np.ma.count, np.mean], axis=1) \
            .reset_index()

        # Clear the multi index
        tmp.columns = pd.Index(
            ['source1', 'source2', 'eventsSharedCount', 'meanToneDist'])
        tmp.meanToneDist = tmp.meanToneDist.round(2)
        tmp.eventsSharedCount = tmp.eventsSharedCount.astype(int)

        for i, r in tmp.iterrows():
            source1 = r.source1
            source2 = r.source2
            meanToneDist = r.meanToneDist
            eventsSharedCount = r.eventsSharedCount
            final[source1][source2]['meanToneDist'] = meanToneDist
            final[source1][source2]['eventsSharedCount'] = eventsSharedCount

        other = dict()
        other['maxToneDist'] = tmp.meanToneDist.max()
        other['maxSharedEventsCount'] = tmp.eventsSharedCount.max()

        # Compute aggregated values on nodes
        nodes = mentions_filtered.groupby(by='mentionSourceName') \
            .agg([np.ma.count, np.mean], axis=1) \
            .reset_index() \
            .set_index('mentionSourceName')[['avgTone']]

        nodes.columns = ['mentionnedEventsCount', 'avgTone']
        nodes = nodes.sort_values(by='mentionnedEventsCount', ascending=False)
        nodes.avgTone = nodes.avgTone.round(2)  # Size optimization
        nodes.mentionnedEventsCount = nodes.mentionnedEventsCount.astype(int)

        return dict(edgesData=final,
                    nodes=nodes.to_dict('index'),
                    other=other)
