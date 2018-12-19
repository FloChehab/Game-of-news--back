import pandas as pd
from google.cloud import bigquery
from datetime import datetime
import json

DATE_BEGIN_DEFAULT = "2018-11-21T00:00:00.000Z"
DATE_END_DEFAULT = "2018-11-22T00:00:00.000Z"
MINIMUM_DISTINCT_SOURCE_COUNT_DEFAULT = 10
CONFIDENCE_DEFAULT = 100
LIMIT_DEFAULT = 100000
FILTER_MENTION_ID_DEFAULT = ""

D_FROM_TS = '%Y-%m-%dT%H:%M:%S.%fZ'
D_TO_INT_GDELT = "%Y%m%d%H%M%S"


def query_google_BQ(date_begin=DATE_BEGIN_DEFAULT,
                    date_end=DATE_END_DEFAULT,
                    minimum_distinct_source_count=MINIMUM_DISTINCT_SOURCE_COUNT_DEFAULT,
                    confidence=CONFIDENCE_DEFAULT,
                    limit=LIMIT_DEFAULT,
                    filterMentionId=FILTER_MENTION_ID_DEFAULT) -> pd.DataFrame:

    d_begin = datetime.strptime(date_begin, D_FROM_TS)
    date_begin_part = datetime(d_begin.year, d_begin.month, d_begin.day)
    d_begin_int = d_begin.strftime(D_TO_INT_GDELT)

    d_end = datetime.strptime(date_end, D_FROM_TS)
    d_end_int = d_end.strftime(D_TO_INT_GDELT)

    filterMentionIdStr = ""
    if filterMentionId != "":
        filterMentionIdStr = 'AND REGEXP_CONTAINS(MentionIdentifier, r"{}")'.format(
            filterMentionId)

    sql_query = """
    WITH
    # Filterd mentions
    mentionsFiltered AS (
        SELECT DISTINCT
            GLOBALEVENTID 
        FROM
            `gdelt-bq.gdeltv2.eventmentions_partitioned`
        WHERE
            _PARTITIONTIME >= "{date_begin_part}"
            AND _PARTITIONTIME < "{date_end}"
            AND EventTimeDate >= {d_begin_int}
            AND EventTimeDate <= {d_end_int}
            {filterMentionId}),
    # events contains the interesting fields from the events table
    events AS (
        SELECT
            A.GLOBALEVENTID AS GLOBALEVENTID,
            DATEADDED,
            Actor1Name,
            Actor2Name,
            GoldsteinScale
        FROM
            `gdelt-bq.gdeltv2.events_partitioned` A
        INNER JOIN
            mentionsFiltered B
        ON
            A.GLOBALEVENTID = B.GLOBALEVENTID
        WHERE
            _PARTITIONTIME >= "{date_begin_part}"
            AND _PARTITIONTIME < "{date_end}"),
    # mentions contains the interesting fields from the mentions table
    # We limit to mentions with a high confidence
    mentions AS (
        SELECT
            GLOBALEVENTID,
            MentionTimeDate,
            MentionSourceName,
            MentionIdentifier,
            MentionDocTone
        FROM
            `gdelt-bq.gdeltv2.eventmentions_partitioned`
        WHERE
            _PARTITIONTIME >= "{date_begin_part}"
            AND _PARTITIONTIME < "{date_end}"
            AND Confidence >= {confidence}
    ),
    # We focus on events that have been shared by a minimum number of different sources
    bestEvents AS (
        SELECT
            GLOBALEVENTID,
            COUNT(DISTINCT MentionSourceName) distinctSourceCount,
            AVG(MentionDocTone) avgDocTone
        FROM
            mentions
        GROUP BY
            GLOBALEVENTID
        HAVING
            distinctSourceCount >= {minimum_distinct_source_count} ),
    # Find all the corresponding mentions
    mentionsSelection AS (
        SELECT
            bestEvents.GLOBALEVENTID GLOBALEVENTID,
            MentionTimeDate,
            MentionSourceName,
            MentionIdentifier,
            MentionDocTone,
            distinctSourceCount,
            avgDocTone
        FROM
            bestEvents
        INNER JOIN
            mentions
        ON
            bestEvents.GLOBALEVENTID = mentions.GLOBALEVENTID
        WHERE 
            MentionTimeDate >= {d_begin_int}
            AND MentionTimeDate <= {d_end_int})
    # Final selection
    SELECT
        events.GLOBALEVENTID eventId,
        MentionTimeDate mentionDateAdded,
        MentionSourceName mentionSourceName,
        MentionIdentifier mentionIdentifier,
        MentionDocTone mentionDocTone,
        distinctSourceCount eventSourceCount,
        avgDocTone eventAvgTone,
        DATEADDED eventDateAdded,
        Actor1Name eventActor1,
        Actor2Name eventActor2,
        GoldsteinScale eventGoldsteingScale
    FROM
        mentionsSelection
    INNER JOIN
        events
    ON
        mentionsSelection.GLOBALEVENTID = events.GLOBALEVENTID
    ORDER BY
        eventSourceCount
    DESC
    LIMIT
        {limit}
    """.format(date_begin=date_begin,
               date_end=date_end,
               date_begin_part=date_begin_part.isoformat(),
               d_begin_int=d_begin_int,
               d_end_int=d_end_int,
               minimum_distinct_source_count=minimum_distinct_source_count,
               confidence=confidence,
               limit=limit,
               filterMentionId=filterMentionIdStr)

    client = bigquery.Client()
    query_job = client.query(sql_query)
    results = query_job.result()
    return results.to_dataframe()


def query_params_to_id(date_begin=DATE_BEGIN_DEFAULT,
                       date_end=DATE_END_DEFAULT,
                       minimum_distinct_source_count=MINIMUM_DISTINCT_SOURCE_COUNT_DEFAULT,
                       confidence=CONFIDENCE_DEFAULT,
                       limit=LIMIT_DEFAULT,
                       filterMentionId=FILTER_MENTION_ID_DEFAULT) -> str:
    return "{db}|{de}|{mdsc}|{conf}|{limit}|{fmi}".format(db=date_begin,
                                                          de=date_end,
                                                          mdsc=minimum_distinct_source_count,
                                                          conf=confidence,
                                                          limit=limit,
                                                          fmi=filterMentionId)
