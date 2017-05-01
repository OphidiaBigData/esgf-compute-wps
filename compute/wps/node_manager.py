#! /usr/bin/env python

from __future__ import absolute_import
from __future__ import unicode_literals

import datetime
import json
import logging
import os
import random
import re
import string
import tempfile
import time

import cwt
import django
import redis
from celery import group
from cwt import wps_lib
from lxml import etree
from myproxy.client import MyProxyClient

from wps import models
from wps import settings
from wps import tasks
from wps.auth import oauth2
from wps.auth import openid
from wps.processes import get_process

logger = logging.getLogger(__name__)

URN_AUTHORIZE = 'urn:esg:security:oauth:endpoint:authorize'
URN_ACCESS = 'urn:esg:security:oauth:endpoint:access'
URN_RESOURCE = 'urn:esg:security:oauth:endpoint:resource'
URN_MPC = 'urn:esg:security:myproxy-service'

class NodeManagerError(Exception):
    pass

class NodeManagerWPSError(Exception):
    def __init__(self, exc_type, message):
        self.exc_report = wps_lib.ExceptionReport(settings.VERSION)

        self.exc_report.add_exception(exc_type, message)

class NodeManager(object):

    def create_user(self, service, openid_response, username, certs, **extra):
        """ Create a new user. """
        user, created = models.User.objects.get_or_create(username=username)

        if not hasattr(user, 'auth'):
            auth = models.Auth(user=user)

            auth.openid = openid_response
            auth.type = service
            auth.cert = ''.join(certs)
            auth.api_key = ''.join(random.choice(string.ascii_letters+string.digits) for _ in xrange(64))
            auth.extra = json.dumps(extra)

            auth.save()
        else:
            user.auth.type = service
            user.auth.cert = ''.join(certs)
            user.auth.extra = json.dumps(extra)

            user.auth.save()

        return user.auth.api_key

    def auth_mpc(self, oid_url, username, password):
        oid = openid.OpenID.retrieve_and_parse(oid_url)

        mpc_service = oid.find(URN_MPC)

        g = re.match('socket://(.*):(.*)', mpc_service.uri)
        
        if g is None:
            raise Exception('Failed to parse MyProxyClient endpoint')

        host, port = g.groups()

        m = MyProxyClient(hostname=host, caCertDir=settings.CA_PATH)

        c = m.logon(username, password, bootstrap=True)

        api_key = self.create_user('myproxyclient', oid.response, username, c, password=password)

        return api_key

    def auth_oauth2(self, oid_url):
        oid = openid.OpenID.retrieve_and_parse(oid_url)

        auth_service = oid.find(URN_AUTHORIZE)

        cert_service = oid.find(URN_RESOURCE)

        redirect_url, state = oauth2.get_authorization_url(auth_service.uri, cert_service.uri)

        session = {
                   'oauth_state': state,
                   'openid': oid_url,
                   'openid_response': oid.response
                  }

        return redirect_url, session

    def auth_oauth2_callback(self, oid_url, oid_response, query, state):
        oid = openid.OpenID.parse(oid_response)

        token_service = oid.find(URN_ACCESS)

        cert_service = oid.find(URN_RESOURCE)

        request_url = '{}?{}'.format(settings.OAUTH2_CALLBACK, query)

        token = oauth2.get_token(token_service.uri, request_url, state)

        cert, key, new_token = oauth2.get_certificate(token, token_service.uri, cert_service.uri)

        api_key = self.create_user('oauth2', oid.response, oid_url, ''.join([cert, key]), token=new_token)

        return api_key

    def get_parameter(self, params, name):
        """ Gets a parameter from a django QueryDict """

        # Case insesitive
        temp = dict((x.lower(), y) for x, y in params.iteritems())

        if name.lower() not in temp:
            logger.info('Missing required parameter %s', name)

            raise NodeManagerWPSError(wps_lib.MissingParameterValue, name)

        return temp[name.lower()]

    def get_status(self, job_id):
        """ Get job status. """
        try:
            job = models.Job.objects.get(pk=job_id)
        except models.Job.DoesNotExist:
            raise NodeManagerError('Job {0} does not exist'.format(job_id))

        return job.latest.xml()

    def get_capabilities(self):
        """ Retrieves WPS GetCapabilities. """
        try:
            server = models.Server.objects.get(host='default')
        except models.Server.DoesNotExist:
            raise Exception('Default server does not exist')

        if server.capabilities == '':
            raise Exception('Capabilities has not been populated yet')

        return server.capabilities

    def describe_process(self, identifier):
        """ Retrieves WPS DescribeProcess. """
        try:
            process = models.Process.objects.get(identifier=identifier)
        except models.Process.DoesNotExist:
            raise Exception('Process "{}" does not exist.'.format(ientifier))

        return process.description

    def execute_local(self, user, job, identifier, data_inputs):
        o, d, v = cwt.WPS.parse_data_inputs(data_inputs)

        op_by_id = lambda x: [y for y in o if y.identifier == x][0]

        op = op_by_id(identifier)

        operations = dict((x.name, x.parameterize()) for x in o)

        domains = dict((x.name, x.parameterize()) for x in d)

        variables = dict((x.name, x.parameterize()) for x in v)

        process = get_process(identifier)

        uid, temp = tempfile.mkstemp()

        chain = tasks.setup_auth.s(user_id=user.id, temp=temp, cwd='/tmp', job_id=job.id)

        chain = (chain | process.si(variables, operations, domains, cwd='/tmp', job_id=job.id))

        chain = (chain | tasks.handle_output.s(job_id=job.id))
        
        chain = (chain | tasks.cleanup_auth.si(temp=temp, cwd='/tmp', job_id=job.id))

        chain()

    def execute_cdas2(self, job, identifier, data_inputs):
        instances = models.Instance.objects.all()

        if len(instances) == 0:
            job.failed()

            raise Exception('There are no CDAS2 instances available')

        with closing(create_socket(instances[0].host, instances[0].request, zmq.PUSH)) as request:
            request.send(str('{2}!execute!{0}!{1}'.format(identifier, data_inputs, job.id)))

    def execute(self, user, identifier, data_inputs):
        """ WPS execute operation """
        try:
            process = models.Process.objects.get(identifier=identifier)
        except models.Process.DoesNotExist:
            raise Exception('Process "{}" does not exist.'.format(ientifier))

        server = models.Server.objects.get(host='default')

        job = models.Job(server=server)

        job.save()

        job.set_report(identifier)

        if process.backend == 'local':
            self.execute_local(user, job, identifier, data_inputs)
        elif process.backend == 'CDAS2':
            self.execute_cdas2(job, identifier, data_inputs)
        else:
            job.failed()

            raise Exception('Process backend "{}" is unknown'.format(process.backend))

        return job.report

    def handle_get(self, params):
        """ Handle an HTTP GET request. """
        logger.info('Received GET request %s', params)
        
        request = self.get_parameter(params, 'request')

        service = self.get_parameter(params, 'service')

        api_key = params.get('api_key')

        operation = request.lower()

        identifier = None

        data_inputs = None

        if operation == 'describeprocess':
            identifier = self.get_parameter(params, 'identifier')
        elif operation == 'execute':
            identifier = self.get_parameter(params, 'identifier')

            data_inputs = self.get_parameter(params, 'datainputs')

        return api_key, operation, identifier, data_inputs

    def handle_post(self, data, params):
        """ Handle an HTTP POST request. 

        NOTE: we only support execute requests as POST for the moment
        """
        logger.info('Received POST request %s', data)

        try:
            request = wps_lib.ExecuteRequest.from_xml(data)
        except etree.XMLSyntaxError:
            logger.exception('Failed to parse xml request')

            raise Exception('POST request only supported for Execure operation')

        # Build to format [variable=[];domain=[];operation=[]]
        data_inputs = '[{0}]'.format(
                ';'.join('{0}={1}'.format(x.identifier, x.data.value)
                    for x in request.data_inputs))

        # CDAS doesn't like single quotes
        data_inputs = data_inputs.replace('\'', '\"')

        api_key = params.get('api_key')

        return api_key, 'execute', request.identifier, data_inputs

    def handle_request(self, request):
        """ Convert HTTP request to intermediate format. """
        if request.method == 'GET':
            return self.handle_get(request.GET)
        elif request.method == 'POST':
            return self.handle_post(request.body, request.GET)
