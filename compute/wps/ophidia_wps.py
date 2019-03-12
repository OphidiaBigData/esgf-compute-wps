#! /usr/bin/env python

import logging
import json
import re
import sys
from pprint import pprint
from wps import settings


__all__ = ['parse_workflow_name' , 'parse_workflow_domain']

logger = logging.getLogger('ophidia_wps_parser.parse_workflow_parameters')

def parse_workflow_name(identifier): 
    # gets a workflow name from the identifier

    w = identifier.split('.')[1]
    
    workflow = settings.OPH_WORKFLOW_NAME.format(wname=w)

    return workflow

def parse_workflow_domain(domains): 
    dimensions = domains.itervalues().next().dimensions # dimensioni di UN SOLO dominio
    
    subset_dims, subset_filters, subset_types = '', '', ''
   
    for x in dimensions:
        subset_dims += x.name + '|'
        if "values" in x.crs.name:
            subset_types += "coord|"
            subset_filters += str(x.start) + ':' + str(x.end) + '|'
        else:
            subset_types += "index|"
            subset_filters += str(x.start) + ':' + str(x.step) + ':' + str(x.end) + '|'
        
    subset_dims = subset_dims[:-1] # remove last char , i.e. | 
    subset_filters = subset_filters[:-1]
    subset_types = subset_types[:-1]
    
    return subset_dims, subset_filters, subset_types 
