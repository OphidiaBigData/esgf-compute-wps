#! /usr/bin/env python

import os
import uuid
from contextlib import closing
from contextlib import nested

import cdms2
import cwt
import dask.array as da
import numpy as np
from celery import shared_task
from celery.utils.log import get_task_logger
from dask import delayed

from wps import models
from wps import settings
from wps.processes import cwt_shared_task
from wps.processes import register_process

from wps import ophidia_wps

from PyOphidia import client

from pprint import pprint

logger = get_task_logger('wps.processes.ophidia')

__all__ = ['max' , "min" , "subset" ]

@register_process('OPHIDIA.max')
@cwt_shared_task()
def max(self, variables, operations, domains, **kwargs):
    job, status = self.initialize(credentials=True, **kwargs)

    v, d, o = self.load(variables, domains, operations)

    op = self.op_by_id('OPHIDIA.max', o)
        
    pprint("v = {}".format( v))
    pprint("d = {}".format( d))
    pprint("o = {}".format( o))
    pprint("op = {}".format( op))
     
    ophclient = client.Client(settings.OPH_USER,settings.OPH_PASSWD,settings.OPH_HOSTNAME,settings.OPH_PORT)
    #ophclient = client.Client(kwargs.get('username'),settings.OPH_PASSWD,settings.OPH_HOSTNAME,settings.OPH_PORT)

    workflow_name = ophidia_wps.parse_workflow_name('OPHIDIA.max') # call only one workflow
    
    subset_dims, subset_filters , subset_type = ophidia_wps.parse_workflow_domain(d)
    
    var_name = reduce(lambda x, y: x if x == y else None, [x.var_name for x in op.inputs])
    
    importnc_uri = variables.itervalues().next()["uri"]
    
    out_name = uuid.uuid4()
           
    params = [var_name, importnc_uri , settings.OPH_EXPORT_PATH , out_name , subset_dims, subset_filters , subset_type]
    
    lastResponse = ophclient.wsubmit(workflow_name , *params)

    out_path = settings.THREDDS_DAP_URL.format(file_name=out_name)
    
    print("out_path" + out_path)

    out_var = cwt.Variable(out_path, var_name)

    return out_var.parameterize()

@register_process('OPHIDIA.min')
@cwt_shared_task()
def min(self, variables, operations, domains, **kwargs):
    job, status = self.initialize(credentials=True, **kwargs)

    v, d, o = self.load(variables, domains, operations)

    op = self.op_by_id('OPHIDIA.min', o)
        
    pprint("v = {}".format( v))
    pprint("d = {}".format( d))
    pprint("o = {}".format( o))
    pprint("op = {}".format( op))
    
    ophclient = client.Client(settings.OPH_USER,settings.OPH_PASSWD,settings.OPH_HOSTNAME,settings.OPH_PORT)
    #ophclient = client.Client(kwargs.get('username'),settings.OPH_PASSWD,settings.OPH_HOSTNAME,settings.OPH_PORT)
    
    workflow_name = ophidia_wps.parse_workflow_name('OPHIDIA.min') # call only one workflow
    
    subset_dims, subset_filters , subset_type = ophidia_wps.parse_workflow_domain(d)
    
    var_name = reduce(lambda x, y: x if x == y else None, [x.var_name for x in op.inputs])
    
    importnc_uri = variables.itervalues().next()["uri"]
    
    out_name = uuid.uuid4()
           
    params = [var_name, importnc_uri , settings.OPH_EXPORT_PATH , out_name , subset_dims, subset_filters , subset_type]
    
    lastResponse = ophclient.wsubmit(workflow_name , *params)

    out_path = settings.THREDDS_DAP_URL.format(file_name=out_name)
    
    print("out_path" + out_path)

    out_var = cwt.Variable(out_path, var_name)

    return out_var.parameterize()

@register_process('OPHIDIA.subset')
@cwt_shared_task()
def subset(self, variables, operations, domains, **kwargs):
    job, status = self.initialize(credentials=True, **kwargs)

    v, d, o = self.load(variables, domains, operations)

    op = self.op_by_id('OPHIDIA.subset', o)
        
    pprint("v = {}".format( v))
    pprint("d = {}".format( d))
    pprint("o = {}".format( o))
    pprint("op = {}".format( op))

    ophclient = client.Client(settings.OPH_USER,settings.OPH_PASSWD,settings.OPH_HOSTNAME,settings.OPH_PORT)
    #ophclient = client.Client(kwargs.get('username'),settings.OPH_PASSWD,settings.OPH_HOSTNAME,settings.OPH_PORT)
    
    workflow_name = ophidia_wps.parse_workflow_name('OPHIDIA.subset') # only one oph_workflow per request
    
    subset_dims, subset_filters, subset_type = ophidia_wps.parse_workflow_domain(d)
    
    var_name = reduce(lambda x, y: x if x == y else None, [x.var_name for x in op.inputs])
    
    importnc_uri = v.itervalues().next().uri
    
    print(importnc_uri)
    
    out_name = uuid.uuid4()

    params = [ var_name, importnc_uri , settings.OPH_EXPORT_PATH , out_name , subset_dims, subset_filters , subset_type ]
    
    ophclient.wsubmit(workflow_name , *params)
    
    out_path = settings.THREDDS_DAP_URL.format(file_name=out_name)
    
    out_var = cwt.Variable(out_path, var_name)

    return out_var.parameterize()
