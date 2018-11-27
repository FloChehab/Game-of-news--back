import pandas as pd
from google.cloud import bigquery


def query_google_BQ(date_begin="2018-11-21T00:00:00.000Z",
                    date_end="2018-11-22T00:00:00.000Z",
                    minimum_distinct_source_count=10,
                    confidence=100,
                    limit=1000000) -> pd.DataFrame:

    sql_query = """
    WITH
    # events contains the interesting fields from the events table
    events AS (
        SELECT
            GLOBALEVENTID,
            DATEADDED,
            Actor1Name,
            Actor2Name,
            GoldsteinScale
        FROM
            `gdelt-bq.gdeltv2.events_partitioned`
        WHERE
            _PARTITIONTIME >= "{date_begin}"
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
            _PARTITIONTIME >= "{date_begin}"
            AND _PARTITIONTIME < "{date_end}"
            AND Confidence >= {confidence} ),
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
            bestEvents.GLOBALEVENTID = mentions.GLOBALEVENTID )
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
    LIMIT
        {limit}
    """.format(date_begin=date_begin,
               date_end=date_end,
               minimum_distinct_source_count=minimum_distinct_source_count,
               confidence=confidence,
               limit=limit)

    client = bigquery.Client()
    query_job = client.query(sql_query)
    results = query_job.result()
    return results.to_dataframe()
