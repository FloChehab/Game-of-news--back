import datetime
from os.path import exists, join

import simplejson
from django.conf import settings
from django.http import Http404, HttpResponse
from django.urls import re_path
from django.views.decorators.csrf import csrf_exempt
from google.cloud import bigquery

from gdelt_proxy.merge_view import merge_view
from gdelt_proxy.pre_processing.json import json_dumps, json_to_dict
from gdelt_proxy.pre_processing.pipeline import run_pipeline


class JsonResponse(HttpResponse):
    """
        JSON response | there is one in django
        But not good for handling nan
    """

    def __init__(self, content, content_type='application/json', status=None):
        super(JsonResponse, self).__init__(
            content=json_dumps(content),
            content_type=content_type,
            status=status,
        )


@csrf_exempt
def query_view(request):
    """View that returns the json dict fith the preprocessed data 
        corresponding to the GBQ parameters in the request    
    """

    request_param = dict()
    if len(request.body) != 0:
        request_param = simplejson.loads(request.body)
    return JsonResponse(run_pipeline(**request_param))


@csrf_exempt
def dataset_view(request, dataset_name):
    """View that return a preprocessed dataset
        Dataset_name should countain '.json'

    Raises:
        Http404 -- If file note found
    """

    fp = join(settings.BASE_DIR, 'datasets_pipelines', dataset_name)
    if exists(fp):
        return JsonResponse(json_to_dict(fp))
    else:
        raise Http404("Dataset doesn't exist on server")


@csrf_exempt
def gbq_active(request):
    """View to know if GBQ connection is working on the server
    """

    if settings.OFF_LINE_PREPROCESSING:
        return JsonResponse(dict(active=False, msg="Deactivated in site config"))

    try:
        c = bigquery.Client()
        return JsonResponse(dict(active=True))
    except:
        return JsonResponse(dict(active=False, msg="Couldn't connect to Google Big Query"))


@csrf_exempt
def server_active(request):
    """Stupid view to ping the server
    """

    return JsonResponse(dict(active=True))


urlpatterns = [
    re_path('query', query_view),
    re_path(r'dataset/(?P<dataset_name>.*)', dataset_view),
    re_path('is_gbq_active', gbq_active),
    re_path('is_server_active', server_active)
]
