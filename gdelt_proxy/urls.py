"""gdelt_proxy URL Configuration

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/2.0/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""

import datetime
import simplejson

from django.conf import settings
from django.http import HttpResponse
from django.urls import re_path
from django.views.decorators.csrf import csrf_exempt
from google.cloud import bigquery
from proxy.views import proxy_view

from gdelt_proxy.merge_view import merge_view
from gdelt_proxy.pre_processing.pipeline import run_pipeline


class JsonResponse(HttpResponse):
    """
        JSON response | there is one in django
        But not good for handling nan
    """
    @classmethod
    def datetime_handler(cls, x):
        if isinstance(x, datetime.datetime):
            return x.isoformat()  # TODO might need to change this
        raise TypeError("Unknown type")

    def __init__(self, content, content_type='application/json', status=None):
        super(JsonResponse, self).__init__(
            content=simplejson.dumps(
                content, ignore_nan=True, default=self.datetime_handler),
            content_type=content_type,
            status=status,
        )


@csrf_exempt
def my_proxy_view(request, url):
    remoteurl = 'http://data.gdeltproject.org/' + url
    return proxy_view(request, remoteurl)


@csrf_exempt
def query_view(request):
    request_param = dict()
    if len(request.body) != 0:
        request_param = simplejson.loads(request.body)
    return JsonResponse(run_pipeline(**request_param))


@csrf_exempt
def gbq_active(request):
    if settings.OFF_LINE_PREPROCESSING:
        return JsonResponse(dict(active=False, msg="Deactivated in site config"))

    try:
        c = bigquery.Client()
        return JsonResponse(dict(active=True))
    except:
        return JsonResponse(dict(active=False, msg="Couldn't connect to Google Big Query"))


urlpatterns = [
    re_path(r'proxy/(?P<url>.*)', my_proxy_view),
    re_path(r'proxy-merge/(?P<date_str>.*)', merge_view),
    re_path('query', query_view),
    re_path('is_gbq_active', gbq_active)
]
