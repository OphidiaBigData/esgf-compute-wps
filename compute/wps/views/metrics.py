#! /usr/bin/env python

import prometheus_client
from django import http
from django.db.models import Sum
from django.views.decorators.csrf import ensure_csrf_cookie
from django.views.decorators.http import require_http_methods

from . import common
from wps import metrics
from wps import models
from wps import WPSError

@require_http_methods(['GET'])
@ensure_csrf_cookie
def metrics_view(request):
    response = prometheus_client.generate_latest(metrics.WPS)

    return http.HttpResponse(response, content_type=prometheus_client.CONTENT_TYPE_LATEST)
