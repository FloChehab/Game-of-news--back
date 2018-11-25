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

from django.http import JsonResponse
from django.urls import re_path
from django.views.decorators.csrf import csrf_exempt
from proxy.views import proxy_view

from gdelt_proxy.merge_view import merge_view
from gdelt_proxy.pre_processing.pipeline import run_pipeline


@csrf_exempt
def my_proxy_view(request, url):
    remoteurl = 'http://data.gdeltproject.org/' + url
    return proxy_view(request, remoteurl)


@csrf_exempt
def query_view(request):
    # TODO take parameters into account
    return JsonResponse(run_pipeline())


urlpatterns = [
    re_path(r'proxy/(?P<url>.*)', my_proxy_view),
    re_path(r'proxy-merge/(?P<date_str>.*)', merge_view),
    re_path('query', query_view)
]
