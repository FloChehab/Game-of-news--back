import pandas as pd
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from gdelt_proxy.schemas import EVENTS_SCHEMA, MENTIONS_SCHEMA

MENTIONS_CSV_FILTER = [
    "eventId",
    "dateEventAdded",
    "mentionType",
    "mentionSourceName",
    "mentionId",
    "confidence",
    "mentionDocTone",
]

EVENTS_CSV_FILTER = [
    "eventId",
    "eventDate",
    # "actor1Code",
    "actor1Name",
    # "actor1CountryCode",
    # "actor2Code",
    "actor2Name",
    # "actor2CountryCode",
    "goldsteinScale",
    "avgTone",
]

# for key in MENTIONS_CSV_FILTER:
#     if key not in MENTIONS_SCHEMA.keys():
#         print(key)
#         raise Exception()

# for key in EVENTS_CSV_FILTER:
#     if key not in EVENTS_SCHEMA.keys():
#         print(key)
#         raise Exception()


def read_mentions(date_str, translation):
    url = "http://data.gdeltproject.org/gdeltv2/{}{}.mentions.CSV.zip".format(
        date_str, ".translation" if translation else "")

    df = pd.read_csv(
        url, sep='\t', header=None,
        usecols=MENTIONS_CSV_FILTER,
        names=list(MENTIONS_SCHEMA.keys()),
        dtype=dict(MENTIONS_SCHEMA))

    df = df[df.mentionType == 1]
    df.drop(['mentionType'], inplace=True, axis=1)
    return df


def read_events(date_str, translation):
    url = "http://data.gdeltproject.org/gdeltv2/{}{}.export.CSV.zip".format(
        date_str, ".translation" if translation else "")

    return pd.read_csv(
        url, sep='\t', header=None,
        usecols=EVENTS_CSV_FILTER,
        names=list(EVENTS_SCHEMA.keys()),
        dtype=dict(EVENTS_SCHEMA))


@csrf_exempt
def merge_view(request, date_str):
    mentions = read_mentions(date_str, True)
    events = read_events(date_str, True)

    merged = mentions.merge(events, on='eventId')
    data = merged.to_dict(orient='split')
    data.pop('index')
    return JsonResponse({'data': data})
