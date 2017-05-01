#! /usr/bin/env python

import os
import json
import uuid
from contextlib import closing

import cdms2
import celery
import cwt
from celery import shared_task
from celery.utils.log import get_task_logger
from django.conf import settings as global_settings
from cwt.wps_lib import metadata

from wps import models
from wps import wps_xml
from wps import settings

__all__ = ['REGISTRY', 'register_process', 'get_process', 'CWTBaseTask', 'handle_output']

logger = get_task_logger(__name__)

REGISTRY = {}

def get_process(name):
    try:
        return REGISTRY[name]
    except KeyError:
        raise Exception('Process {} does not exist'.format(name))

def register_process(name):
    def wrapper(func):
        REGISTRY[name] = func

        return func

    return wrapper

if global_settings.DEBUG:
    @register_process('wps.demo')
    @shared_task
    def demo(variables, operations, domains):
        logger.info('Operations {}'.format(operations))

        logger.info('Domains {}'.format(domains))

        logger.info('Variables {}'.format(variables))

        return cwt.Variable('file:///demo.nc', 'tas').parameterize()

def int_or_float(value):
    try:
        return int(value)
    except ValueError:
        pass

    try:
        return float(value)
    except ValueError:
        return None

class Status(object):
    def __init__(self, job):
        self.job = job
        self.message = None
        self.percent = 0

    @classmethod
    def from_job_id(cls, job_id):
        try:
            job = models.Job.objects.get(pk=job_id)
        except models.Job.DoesNotExist:
            job = None

        return cls(job)

    def update(self, message=None, percent=None):
        if message is not None:
            self.message = message

        if percent is not None:
            self.percent = percent

        logger.info('Update status {} {} %'.format(self.message, self.percent))

        if self.job is not None:
            self.job.update_progress(self.message, self.percent)

class CWTBaseTask(celery.Task):
    def initialize(self, **kwargs):
        cwd = kwargs.get('cwd')

        if cwd is not None:
            os.chdir(cwd)

        return Status.from_job_id(kwargs.get('job_id'))

    def load(self, variables, domains, operations):
        v = dict((x, cwt.Variable.from_dict(y)) for x, y in variables.iteritems())

        d = dict((x, cwt.Domain.from_dict(y)) for x, y in domains.iteritems())

        for var in v.values():
            var.resolve_domains(d)

        o = dict((x, cwt.Process.from_dict(y)) for x, y in operations.iteritems())

        for op in o.values():
            op.resolve_inputs(v, o)

        if op.domain is not None:
            op.domain = d[op.domain]

        return v, d, o

    def build_domain(self, inp, domain, var_name):
        temporal = (0, len(inp[var_name]), 1)
        spatial = {}

        if domain is not None:
            axes = dict((x.id, x) for x in inp[var_name].getAxisList())

            for dim in domain.dimensions:
                if dim.name == 'time' or (dim.name in axes and axes[dim.name].isTime()):
                    if dim.crs == cwt.INDICES:
                        temporal = (dim.start, dim.end, dim.step)
                    elif dim.crs == cwt.VALUES:
                        start, stop = axes[dim.name].mapInterval((dim.start, dim.end))

                        temporal = (start, stop-1, dim.step)
                    else:
                        raise Exception('Unknown CRS value {}'.format(dim.crs))
                else:
                    if dim.crs == cwt.INDICES:
                        spatial[dim.name] = slice(dim.start, dim.end, dim.step)
                    elif dim.crs == cwt.VALUES:
                        start, stop = axes[dim.name].mapInterval((dim.start, dim.end))

                        spatial[dim.name] = slice(start, stop, dim.step)
                    else:
                        raise Exception('Unknown CRS value {}'.format(dim.crs))

        return temporal, spatial

    def op_by_id(self, name, operations):
        try:
            return [x for x in operations.values() if x.identifier == name][0]
        except IndexError:
            raise Exception('Could not find operation {}'.format(name))

    def generate_local_output(self, name=None):
        if name is None:
            name = '{}.nc'.format(uuid.uuid4())

        path = os.path.join(settings.OUTPUT_LOCAL_PATH, name)

        return path

    def generate_output(self, local_path, **kwargs):
        if kwargs.get('local') is None:
            out_name = local_path.split('/')[-1]

            output = settings.OUTPUT_URL.format(file_name=out_name)
        else:
            output = 'file://{}'.format(local_path)

        return output

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        try:
            job = models.Job.objects.get(pk=kwargs['job_id'])
        except KeyError:
            raise Exception('Job id was not passed to the task')
        except models.Job.DoesNotExist:
            raise Exception('Job {} does not exist'.format(kwargs['job_id']))

        job.failed(str(exc))

@shared_task(bind=True, base=CWTBaseTask)
def handle_output(self, variable, **kwargs):
    self.initialize(**kwargs)

    job_id = kwargs.get('job_id')

    try:
        job = models.Job.objects.get(pk=job_id)
    except models.Job.DoesNotExist:
        raise Exception('Job does not exist {}'.format(job_id))

    job.succeeded(json.dumps(variable))