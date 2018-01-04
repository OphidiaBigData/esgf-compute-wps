#! /usr/bin/env python

import collections
import hashlib
import json
import math
import time
import os
import re
import uuid
from contextlib import nested
from datetime import datetime
from functools import partial

import cdms2
import celery
import cwt
from cdms2 import MV2 as MV
from celery import shared_task
from celery.utils.log import get_task_logger
from django.conf import settings as global_settings
from django.db.models import Sum
from openid.consumer import discover
from OpenSSL import crypto

from wps import models
from wps import settings
from wps import WPSError
from wps.auth import oauth2
from wps.auth import openid

__all__ = [
    'REGISTRY',
    'register_process',
    'get_process',
    'CWTBaseTask', 
    'cwt_shared_task',
    'FAILURE',
    'RETRY',
    'SUCCESS',
    'ALL',
    'AccessError',
    'InvalidShapeError',
]

FAILURE = 1
RETRY = 2
SUCCESS = 4
ALL = FAILURE | RETRY | SUCCESS

CERT_DATE_FMT = '%Y%m%d%H%M%SZ'

counter = 0

logger = get_task_logger('wps.processes.process')

REGISTRY = {}

URN_AUTHORIZE = 'urn:esg:security:oauth:endpoint:authorize'
URN_RESOURCE = 'urn:esg:security:oauth:endpoint:resource'

class InvalidShapeError(WPSError):
    def __init__(self, shape):
        msg = 'Read invalid shape {shape}'

        super(InvalidShapeError, self).__init__(msg, shape=shape)

class AccessError(WPSError):
    def __init__(self, url, reason):
        msg = 'Access error "{url}": {reason}'

        super(AccessError, self).__init__(msg, url=url, reason=reason)

class CertificateError(WPSError):
    def __init__(self, user, reason):
        msg = 'Certificate error for user "{username}": {reason}'

        super(CertificateError, self).__init__(msg, username=user.username, reason=reason)

def get_process(name):
    """ Returns a process.

    Retrieves a process from a global registry.

    Args:
        name: A string name of the process.

    Returns:
        A method that has been wrapped as a celery task.

    Raises:
        Exception: A process of name does not exist.
    """
    try:
        return REGISTRY[name]
    except KeyError:
        raise WPSError('Process {name} does not exist', name=name)

def register_process(name, abstract=''):
    """ Process decorator.

    Registers a process with the global dictionary.

    @register_process('Demo')
    def demo(a, b, c):
        return a+b+c

    Args:
        name: A unique string name for the process.
    """
    def wrapper(func):
        REGISTRY[name] = func

        func.abstract = abstract

        return func

    return wrapper

def int_or_float(value):
    try:
        return int(value)
    except ValueError:
        pass

    try:
        return float(value)
    except ValueError:
        return None

class CWTBaseTask(celery.Task):
    """ Compute Working Team (CWT) base celery task.

    This class provides common functionality for all CWT WPS Processes.

    *Note*
    This class is only instantiated once thus any state is shared between 
    all celery task decorators it is passed to.
    """

    def initialize(self, user_id, job_id, credentials=False):
        """ Initialize task.

        **kwargs has the following known values:
            job_id: An integer key of the current job.
            user_id: An integer key of the jobs user.
            cwd: A string path to change the current working directory to.
        
        Args:
            credentials: A boolean requesting credentials for the current task.
            **kwargs: A dict containing additional arguments.
        """
        try:
            user = models.User.objects.get(pk=user_id)
        except models.User.DoesNotExist:
            raise WPSError('Unknown user with ID "{id}" does not exist', id=user_id)

        try:
            job = models.Job.objects.get(pk=job_id)
        except models.Job.DoesNotExist:
            raise WPSError('Job with ID "{id}" does not exist', id=job_id)

        if credentials:
            self.load_certificate(user)

        return user, job

    def status(self, job, message, percent=0):
        job.update_status('[{}] {}'.format(self.request.id, message), percent)        

    def get_job(self, kwargs):
        job_id = kwargs.get('job_id')

        try:
            job = models.Job.objects.get(pk=job_id)
        except models.Job.DoesNotExist:
            raise WPSError('Job with ID "{id}" does not exist', id=job_id)

        return job
    
    def check_certificate(self, user):
        """ Check user certificate.

        Loads current certificate and checks if it has valid date range.

        Args:
            user: User object.

        Return:
            returns true if valid otherwise false.
        """
        if user.auth.cert == '':
            raise CertificateError(user, 'Missing certificate')

        try:
            cert = crypto.load_certificate(crypto.FILETYPE_PEM, user.auth.cert)
        except Exception as e:
            raise CertificateError(user, 'Loading certificate')

        before = datetime.strptime(cert.get_notBefore(), CERT_DATE_FMT)

        after = datetime.strptime(cert.get_notAfter(), CERT_DATE_FMT)

        now = datetime.now()

        if now >= after:
            return False

        if now <= before:
            return False

        return True

    def refresh_certificate(self, user):
        """ Refresh user certificate

        Will try to refresh a users certificate if authenticated using OAuth2.

        Args:
            user: User object.

        Return:
            returns new certificate
        """
        logger.info('Refreshing user certificate')

        if user.auth.type == 'myproxyclient':
            raise CertificateError(user, 'MyProxyClient certificate has expired')

        url, services = discover.discoverYadis(user.auth.openid_url)

        auth_service = openid.find_service_by_type(services, URN_AUTHORIZE)

        cert_service = openid.find_service_by_type(services, URN_RESOURCE)

        try:
            extra = json.loads(user.auth.extra)
        except ValueError as e:
            raise WPSError('Missing OAuth2 state, try authenticating with OAuth2 again')

        if 'token' not in extra:
            raise WPSError('Missing OAuth2 token, try authenticating with OAuth2 again')

        cert, key, new_token = oauth2.get_certificate(extra['token'], auth_service.server_url, cert_service.server_url)

        logger.info('Retrieved certificate and new token')

        extra['token'] = new_token

        user.auth.extra = json.dumps(extra)

        user.auth.cert = ''.join([cert, key])

        user.auth.save()

        return user.auth.cert

    def load_certificate(self, user):
        """ Loads a user certificate.

        First the users certificate is checked and refreshed if needed. It's
        then written to disk and the processes current working directory is 
        set, allowing calls to NetCDF library to use the certificate.

        Args:
            user: User object.
        """
        if not self.check_certificate(user):
            self.refresh_certificate(user)

        user_path = os.path.join(settings.USER_TEMP_PATH, str(user.id))

        if not os.path.exists(user_path):
            os.makedirs(user_path)

            logger.info('Created user directory {}'.format(user_path))

        os.chdir(user_path)

        logger.info('Changed working directory to {}'.format(user_path))

        cert_path = os.path.join(user_path, 'cert.pem')

        with open(cert_path, 'w') as outfile:
            outfile.write(user.auth.cert)

        logger.info('Wrote user certificate')

        dods_cookies_path = os.path.join(user_path, '.dods_cookies')

        if os.path.exists(dods_cookies_path):
            os.remove(dods_cookies_path)

            logger.info('Removed stale dods_cookies file')

        dodsrc_path = os.path.join(user_path, '.dodsrc')

        if not os.path.exists(dodsrc_path):
            with open(dodsrc_path, 'w') as outfile:
                outfile.write('HTTP.COOKIEJAR=.dods_cookies\n')
                outfile.write('HTTP.SSL.CERTIFICATE={}\n'.format(cert_path))
                outfile.write('HTTP.SSL.KEY={}\n'.format(cert_path))
                outfile.write('HTTP.SSL.CAPATH={}\n'.format(settings.CA_PATH))
                outfile.write('HTTP.SSL.VERIFY=0\n')

            logger.info('Wrote .dodsrc file {}'.format(dodsrc_path))

    def load(self, parent_variables, variables, domains, operation):
        """ Load a processes inputs.

        Loads each value into their associated container class.

        Args:
            variables: A dict mapping names of Variables to their representations.
            domains: A dict mapping names of Domains to their representations.
            operations: A dict mapping names of Processes to their representations.

        Returns:
            A tuple of 3 dictionaries. Each dictionary maps unqiue names to an
            object of their respective container type.
        """
        if isinstance(parent_variables, dict):
            variables.update(parent_variables)
        elif isinstance(parent_variables, list):
            for parent in parent_variables:
                if isinstance(parent, dict):
                    variables.update(parent)

        v = dict((x, cwt.Variable.from_dict(y)) for x, y in variables.iteritems())

        d = dict((x, cwt.Domain.from_dict(y)) for x, y in domains.iteritems())

        o = cwt.Process.from_dict(operation)

        return v, d, o

    def generate_output_path(self, name=None):
        """ Format the file path for a local output. """
        if name is None:
            name = uuid.uuid4()

        filename = '{}.nc'.format(name)

        return os.path.join(settings.LOCAL_OUTPUT_PATH, filename)

    def generate_output_url(self, local_path, **kwargs):
        """ Format the file path for a remote output. """
        if kwargs.get('local', None) is None:
            out_name = local_path.split('/')[-1]

            if settings.DAP:
                output = settings.DAP_URL.format(file_name=out_name)
            else:
                output = settings.OUTPUT_URL.format(file_name=out_name)
        else:
            if 'file://' in local_path:
                output = local_path
            else:
                output = 'file://{}'.format(local_path)

        return output

    def slice_to_str(self, s):
        """ Format a slice. """
        fmt = '{}:{}:{}'

        if isinstance(s, (tuple, list)):
            if len(s) == 2:
                slice_str = fmt.format(s[0], s[1], 1)
            elif len(s) >= 3:
                slice_str = fmt.format(s[0], s[1], s[2])
        elif isinstance(s, slice):
            slice_str = fmt.format(s.start, s.stop, s.step)
        else:
            raise WPSError('Failed to convert slice to string, unknown type "{type}"', type=type(s))
        
        return slice_str

    def generate_cache_map(self, file_map, var_map, domain_map, job):
        """ Generates a cache map.

        Iterate over files checking if the file subset is located in the cache.
        If the file exists in the cache we replace it in the original file_map.
        If the file does not exist the Cache object is added to the dict.

        Args:
            file_map: A dict mapping urls to cdms2.FileObj.
            var_map: A dict mapping urls to string variable names.
            domain_map: A dict mapping urls to dicts describing ROI.
            job: A Job object.

        Returns:
            returns a dict mapping urls to Cache objects.
        """
        caches = {}
        required_space = 0

        self.status(job, 'Checking for cached inputs')

        for url in domain_map.keys():
            var_name = var_map[url]

            temporal, spatial = domain_map[url]

            # get rid if file if its not contained in the domain
            if temporal is None or temporal.stop == 0:
                logger.debug('File "{}" is not contained in the domain'.format(url))

                file_map[url].close()

                del file_map[url]

                continue

            cache, exists = self.check_cache(url, var_name, temporal, spatial)

            if exists:
                file_map[url].close()

                try:
                    file_map[url] = cdms2.open(cache.local_path)
                except cdms2.CDMSError as e:
                    raise AccessError(url, e.message)

                domain_map[url] = (slice(0, file_map[url][var_name].getTime().shape[0], 1), spatial)

                logger.debug('File has been cached updated files dict')
            else:
                required_space += cache.estimate_size()

                caches[url] = cache

        self.free_cache(required_space)

        return caches

    def free_cache(self, required_space):
        freed_space = 0.0
        to_remove = []
        used_space = models.Cache.objects.all().aggregate(Sum('size'))['size__sum']

        if used_space < settings.CACHE_GB_MAX_SIZE:
            logger.info('No need to free space only "{}" of "{}" used'.format(used_space, settings.CACHE_GB_MAX_SIZE))

            return 0

        cache_entries = models.Cache.objects.order_by('-accessed_date')

        for entry in cache_entries:
            freed_space += float(entry.size)

            to_remove.append(entry)

            if freed_space >= required_space:
                break

        for entry in to_remove:
            logger.info('Removing "{}" to free "{}" GB'.format(entry.local_path, entry.size))

            if os.path.exists(entry.local_path):
                os.remove(entry.local_path)

            entry.delete()

        return freed_space

    def download(self, input_file, var_name, base_units, temporal, spatial, cache_file, out_file, post_process, job):
        """ Downloads file. 

        This iterates over the list of partitions. For each chunk the following
        occurs: If a cache_file is passed, the data is written. Next the chunk
        has its time axis remapped to base_units. If post_processing is provided
        then its applied to the data before it's written to the output otherwise
        it is written unmodified.

        Args:
            input_file: A cdms2.FileObj.
            var_name: A string variable name.
            base_units: A string with the units to remap the time axis.
            temporal: A list or tuple of slice objects.
            spatial: A dict mapping axis to slices.
            cache_file: A cdms2.FileObj. Can be None if not caching file.
            out_file: A cdms2.FileObj to write data to.
            post_process: A function to apply to data before writing to output.
                Can be None if no post processing is required.
            job: A Job object.
        """
        steps = len(temporal)

        for i, time_slice in enumerate(temporal):
            data = input_file(var_name, time=time_slice, **spatial)

            if any(x == 0 for x in data.shape):
                raise InvalidShapeError(data.shape)

            if cache_file is not None:
                cache_file.write(data, id=var_name)

            data.getTime().toRelativeTime(base_units)

            if post_process is None:
                out_file.write(data, id=var_name)
            else:
                out_file.write(post_process(data), id=var_name)

            percent = (i+1)*100/steps

            self.status(job, 'Retrieved chunk {} to {} from {}'.format(time_slice.start, time_slice.stop, base_units), percent)

    def open_variables(self, variables):
        file_map = collections.OrderedDict()
        file_var_map = {}

        try:
            for var in variables:
                file_map[var.uri] = cdms2.open(var.uri)

                file_var_map[var.uri] = var.var_name
        except cdms2.CDMSError as e:
            for f in file_map.values():
                f.close()

            raise AccessError(var.uri, e.message)

        return file_map, file_var_map

    def sort_variables_by_time(self, variables):
        input_dict = {}
        time_pattern = '.*_(\d+)-(\d+)\.nc'

        for i, v in enumerate(variables):
            result = re.search(time_pattern, v.uri)

            if result is None:
                raise WPSError('Error parsing time range from filename "{name}"', name=v.uri)

            groups = result.groups()

            start, _ = groups

            input_dict[start] = v

        sorted_keys = sorted(input_dict.keys())

        return [input_dict[x] for x in sorted_keys]

    def process_variable(self, variables, domain, job, process, num_inputs, **kwargs):
        axes = kwargs.get('axes', None)
        grid = kwargs.get('grid', None)
        regridTool = kwargs.get('regridTool', None)
        regridMethod = kwargs.get('regridMethod', None)

        variables = self.sort_variables_by_time(variables)

        if num_inputs is not None:
            variables = variables[:num_inputs]

        file_map, file_var_map = self.open_variables(variables)

        var_name = file_var_map.values()[0]

        with nested(*file_map.values()):
            if num_inputs > 1:
                shapes = set(x[var_name].shape for x in file_map.values())

                if len(shapes) > 1:
                    logger.info('Inputs invalid shapes "{}"'.format(shapes))

                    raise WPSError('Mismatching shapes for input files')

            self.status(job, 'Mapping domain to source files')

            domain_map = self.map_domain(file_map.values(), file_var_map, domain)

            logger.info('Domain map: {}'.format(domain_map))

            if len(domain_map) == 0:
                raise WPSError('Domain does not map to input files')

            # Disable caching when averaging over time axis,
            # since we pull chunks a shape like (120, 20, 480) and average over
            # the first index resulting in (20, 480) we overwrite out cache 
            # data each time result in a invalid file with shape (20, 480)
            if 'time' in axes:
                cache_map = {}

                self.status(job, 'Disabling caching')
                
                logger.info('Disabling cache')
            else:
                cache_map = self.generate_cache_map(file_map, file_var_map, domain_map, job)

                logger.info('Cache map: {}'.format(cache_map))

            self.status(job, 'Partitioning domain')

            partition_map = self.generate_partitions(domain_map, axes=axes)

            logger.info('Partition map: {}'.format(partition_map))

            output_path = self.generate_output_path()

            cache_file_map = {}

            try:
                for url in cache_map.keys():
                    cache_file_map[url] = cdms2.open(cache_map[url].local_path, 'w')
            except cdms2.CDMSError as e:
                for item in cache_file_map.values():
                    itme.close()

                raise AccessError(url, e.message)
            
            try:
                with cdms2.open(output_path, 'w') as outfile, nested(*cache_file_map.values()):
                    url = domain_map.keys()[0]

                    temporal, spatial = partition_map[url]

                    if isinstance(temporal, (list, tuple)):
                        self.status(job, 'Averaging over spatial axes "{}"'.format(axes))

                        count = len(temporal)

                        for i, time_slice in enumerate(temporal, 1):
                            data_list = []

                            for url in file_map.keys():
                                data = file_map[url](var_name, time=time_slice, **spatial)

                                self.status(job, 'Retrieved data slice {}'.format(data.shape), i*100/count)

                                if any(x == 0 for x in data.shape):
                                    raise InvalidShapeError(data.shape)

                                if url in cache_file_map:
                                    logger.debug('Writing cache file "{}" with data "{}"'.format(url, data.shape))

                                    cache_file_map[url].write(data, id=var_name)

                                if grid is not None:
                                    logger.debug('Regridding tool={} method={} grid={}'.format(regridTool, regridMethod, grid))

                                    data = data.regrid(grid, regridTool=regridTool, regridMethod=regridMethod)

                                data_list.append(data)

                            if len(data_list) == 1:
                                for axis in axes:
                                    axis_index = data.getAxisIndex(axis)

                                    if axis_index == -1:
                                        raise WPSError('Unknown axis name "{name}"', name=axis)

                                    data = process(data, axis=axis_index)

                                outfile.write(data, id=var_name) 
                            else:
                                outfile.write(process(data_list), id=var_name)
                    elif isinstance(spatial, (list, tuple)):
                        result = []

                        self.status(job, 'Averaging over time axis')

                        count = len(spatial)

                        for i, spatial_slice in enumerate(spatial, 1):
                            data_list = []

                            for url in file_map.keys():
                                data = file_map[url](var_name, time=temporal, **spatial_slice)

                                self.status(job, 'Retrieved data slice {}'.format(data.shape), i*100/count)

                                if any(x == 0 for x in data.shape):
                                    raise InvalidShapeError(data.shape)

                                if url in cache_file_map:
                                    logger.debug('Writing cache file "{}" with data "{}"'.format(url, data.shape))

                                    cache_file_map[url].write(data, id=var_name)

                                if grid is not None:
                                    logger.debug('Regridding tool={} method={} grid={}'.format(regridTool, regridMethod, grid))

                                    data = data.regrid(grid, regridTool=regridTool, regridMethod=regridMethod)

                                data_list.append(data)

                            if len(data_list) == 1:
                                time_axis = data.getTime()

                                time_axis_index = data.getAxisIndex(time_axis.id)

                                result.append(process(data_list[0], axis=time_axis_index))
                            else:
                                result.append(process(data_list))

                        self.status(job, 'Concatenating chunks', 100)

                        outfile.write(MV.concatenate(result), id=var_name)
                    else:
                        raise WPSError('Invalid partition map')

                for url in cache_file_map.keys():
                    cache_map[url].size = os.stat(cache_map[url].local_path).st_size / 1073741824.0

                    cache_map[url].save()
            except WPSError:
                raise
            finally:
                for item in cache_file_map.values():
                    item.close()

        return output_path
            
    def retrieve_variable(self, variables, domain, job, output_path=None, post_process=None, **kwargs):
        """ Retrieve a variable.

        The input files are opened and the domain is mapped over the files.
        Each input file is checked if its in the cache. Then non-cached files
        are downloaded and cached. Each chunk of data that is read is passed
        to post_process if provided.

        Args:
            variables: A List or tuple of cwt.Variable objects
            domain: A cwt.Domain describing the ROI
            job: A Job object.
            output_path: A string path that the data will be written.
            post_process: A function handle to call after each chunk is read.
        """
        files = collections.OrderedDict()
        file_var_map = {}

        # Open the input files
        try:
            for var in variables:
                files[var.uri] = cdms2.open(var.uri)

                file_var_map[var.uri] = var.var_name
        except cdms2.CDMSError as e:
            for f in files.values():
                f.close()

            raise AccessError(var.uri, e.message)

        self.status(job, 'Mapping domain to input files')

        # Map the domain
        domain_map = self.map_domain(files.values(), file_var_map, domain)

        if len(domain_map) == 0:
            raise WPSError('Invalid domain no input files are included')

        logger.info('Mapped domain {}'.format(domain_map))

        cache_map = self.generate_cache_map(files, file_var_map, domain_map, job)

        logger.info('Files to be cached "{}"'.format(cache_map.keys()))

        partition_map = self.generate_partitions(domain_map)

        logger.info('Partition map "{}"'.format(partition_map))

        var_name = file_var_map[domain_map.keys()[0]]

        base_units = files.values()[0][var_name].getTime().units

        output_path = self.generate_output_path()

        self.status(job, 'Retrieving files')

        # Download chunked files
        with cdms2.open(output_path, 'w') as out_file:
            for url in domain_map.keys():
                self.status(job, 'Processing input "{}"'.format(url))

                cache_file = None

                if url in cache_map:
                    logger.info('Opening cache file "{}"'.format(cache_map[url].local_path))

                    try:
                        cache_file = cdms2.open(cache_map[url].local_path, 'w')
                    except cdms2.CDMSError as e:
                        raise AccessError(url, e.message)

                temporal, spatial = partition_map[url]

                self.download(files[url], var_name, base_units, temporal, spatial, cache_file, out_file, post_process, job)

                files[url].close()

                self.status(job, 'Finished retrieving "{}"'.format(url), 100)

                if cache_file is not None:
                    cache_file.close()

                    cache_map[url].size = os.stat(cache_map[url].local_path).st_size / 1073741824.0

                    cache_map[url].save()

                    logger.info('Updating cache entry with file size "{}" GB'.format(cache_map[url].size))

        return output_path

    def generate_partitions(self, domain_map, axes=None, **kwargs):
        """ Generates a partition map from a domain map.

        Args:
            domain_map: A dict whose keys are urls and values are dicts describing
                an ROI.
            **kwargs: A dict containing extra parameters. Check description for
                known values.

        Returns:
            returns a dict mapping urls to partition map.
        """
        partitions = {}

        if axes is None:
            axes = ['lat']

        for url in domain_map.keys():
            logger.debug('Partitioning input "{}"'.format(url))

            axis = axes[0]

            temporal, spatial = domain_map[url]

            if axis in ('time', 't'):
                chunks = []

                # Could make a smarter decision on which axis to slice by
                # data alginment
                part_axis = spatial.keys()[0]

                part_slice = spatial.get(part_axis)

                step = 20

                for begin in xrange(part_slice.start, part_slice.stop, step):
                    end = min(begin+step, part_slice.stop)

                    spatial_dict = {part_axis: slice(begin, end)}

                    for k, v in spatial.iteritems():
                        if k != part_axis:
                            spatial_dict[k] = slice(v.start, v.stop, 1)

                    chunks.append(spatial_dict)    

                partitions[url] = (temporal, chunks)
            else:
                chunks = []

                tstart, tstop, tstep = temporal.start, temporal.stop, temporal.step

                diff = tstop - tstart

                step = min(diff, settings.PARTITION_SIZE)

                for begin in xrange(tstart, tstop, step):
                    end = min(begin + step, tstop)

                    chunks.append(slice(begin, end, tstep))

                partitions[url] = (chunks, spatial)

        return partitions

    def generate_dimension_id(self, var_name, temporal, spatial):
        """ Create a string describing the dimensions.

        Args:
            temporal: A slice over temporal axis.
            spatial: A dict mapping axis names to slices.

        Returns:
            A string unique to the dimensions.
        """

        dim_id = '{}!'.format(var_name)

        if temporal is None:
            dim_id += 'time:None'
        else:
            dim_id += 'time:{}'.format(self.slice_to_str(temporal))

        for name in sorted(spatial.keys()):
            dim_id += '|{}:{}'.format(name, self.slice_to_str(spatial[name]))

        return dim_id

    def generate_cache_name(self, uri, temporal, spatial):
        """ Create a cacheaable name.

        Args:
            uri: A string file uri.
            temporal: A slice over the temporal axis.
            spatial: A dict mapping axis names to slices.

        Returns:
            A unique cacheable name.
        """
        m = hashlib.sha256()

        m.update(uri)

        m.update(self.slice_to_str(temporal))

        for k in ['latitude', 'lat', 'y']:
            if k in spatial:
                m.update(self.slice_to_str(spatial[k]))

        for k in ['longitude', 'lon', 'x']:
            if k in spatial:
                m.update(self.slice_to_str(spatial[k]))

        return m.hexdigest()

    def check_cache(self, uri, var_name, temporal, spatial):
        """ Check cache for a file.

        Create a unique identifier using the URI of the input and the domain
        of the input it covers. If the file exists in the cache we validate
        it by checking the variable name and its shape. If the cache file is
        invalid, it is removed and a new one is created.

        Args:
            uri: A string path to the input.
            var_name: A string variable name in the input.
            temporal: A slice for the temporal axis.
            spatial: A dict of slices for the spatial axis.

        Returns:
            A FileVariable from CDMS2.
        """
        logger.info('Checking cache for file {} with domain {} {}'.format(uri, temporal, spatial))

        uid = self.generate_cache_name(uri, temporal, spatial)

        dimensions = self.generate_dimension_id(var_name, temporal, spatial)

        logger.info('Generated uid "{}" dimensions "{}"'.format(uid, dimensions))

        cached, created = models.Cache.objects.get_or_create(uid=uid, dimensions=dimensions)

        exists = False

        if created:
            cached.uid = uid

            cached.url = uri

            cached.save()
        else:
            local_path = cached.local_path

            if os.path.exists(local_path):
                try:
                    cached_file = cdms2.open(local_path)
                except cdms2.CDMSError as e:
                    raise AccessError(local_path, e.message)
                else:
                    # Validate the cached file
                    with cached_file as cached_file:
                        if var_name in cached_file.variables:
                            time = cached_file[var_name].getTime().shape[0]

                            expected_time = temporal.stop - temporal.start

                            if time != expected_time:
                                logger.info('Cached file is invalid due to differing time, expecting {} got {} time steps'.format(expected_time, time))

                                os.remove(local_path)
                            else:
                                logger.info('Cached file is valid')

                                exists = True
            else:
                logger.warning('Cached file "{}" does not exist on disk'.format(cached.url))

                exists = False

        return cached, exists

    def map_time_axis(self, axis, dim):
        """ Map a dimension to an axis.

        Attempts to map a Dimension to a CoordinateAxis.

        Args:
            axis: A CoordinateAxis object.
            dim: A Dimension object.
            clamp_upper: A boolean indicating whether to include the last value.

        Raises:
            Exception: Could not map the Dimension to CoordinateAxis.
        """
        if dim.crs == cwt.INDICES:
            n = axis.shape[0]

            axis_slice = slice(dim.start, min(n, dim.end), dim.step)
        elif dim.crs == cwt.VALUES:
            try:
                start, stop = axis.mapInterval((dim.start, dim.end))
            except Exception:
                axis_slice = None
            else:
                axis_slice = slice(start, stop, dim.step)
        elif dim.crs == cwt.CRS('timestamps'):
            try:
                start, stop = axis.mapInterval((str(dim.start), str(dim.end)))
            except Exception:
                axis_slice = None
            else:
                axis_slice = slice(start, stop, dim.step)
        else:
            raise WPSError('Unknown CRS "{name}"', name=dim.crs)

        logger.info('Mapped dimension {} to slice {}'.format(dim, axis_slice))

        return axis_slice

    def map_domain(self, files, file_var_map, domain):
        """ Maps a domain to a set of files.

        Sort files by their first time value. For each file we determine which
        portion of the domain belongs to the file. A dict mapping URLs to these
        domains is returned.

        Args:
            files: A list or tuple of cdms2 FileObj.
            file_var_map: A dict of urls mapped to a variable name.
            domain: A cwt.Domain describing the ROI.

        Returns:
            returns an OrderedDict whose key is a URL and value is a dict
            describing the portion of domain contained in the URL.
        """
        domains = collections.OrderedDict()

        logger.info('Mapping domain {}'.format(domain))

        if all(x[file_var_map[x.id]].getTime() != None for x in files):
            files = sorted(files, key=lambda x: x[file_var_map[x.id]].getTime().asComponentTime()[0])

        file_obj = files[0]

        var_name = file_var_map[file_obj.id]

        if file_obj[var_name].getTime() != None:
            base_units = file_obj[var_name].getTime().units
        else:
            base_units = None

        for file_obj in files:
            var_name = file_var_map[file_obj.id]
            file_header = file_obj[var_name]
            temporal = None
            spatial = {}
            skip = False

            if domain is None:
                if file_header.getTime() == None:
                    temproal = None
                else:
                    temporal = slice(0, file_header.getTime().shape[0], 1)

                axes = file_header.getAxisList()

                for axis in axes:
                    if axis.isTime():
                        continue

                    spatial[axis.id] = (axis[0], axis[-1])
            else:
                for dim in domain.dimensions:
                    axis_index = file_header.getAxisIndex(dim.name)

                    if axis_index == -1:
                        continue
                        #raise Exception('Axis {} does not exist in "{}"'.format(dim.name, file_obj.id))

                    axis = file_header.getAxis(axis_index)

                    if axis.isTime():
                        clone_axis = axis.clone()

                        clone_axis.toRelativeTime(base_units)

                        temporal = self.map_time_axis(clone_axis, dim)

                        if temporal is None:
                            logger.info('Skipping "{}" not included in domain'.format(file_obj.id))

                            skip = True

                            break

                        if dim.crs == cwt.INDICES:
                            dim.start -= temporal.start

                            dim.end -= (temporal.stop-temporal.start)+temporal.start
                    else:
                        start, stop = axis.mapInterval((dim.start, dim.end))

                        spatial[axis.id] = slice(start, stop, dim.step)

            if not skip:
                domains[file_obj.id] = (temporal, spatial)

        return domains

    def generate_grid(self, operation, variables, domains):
        """ Generate a grid.

        Args:
            operation: A Process object.
            variables: A dict mapping variable names to their respective object.
            domains: A dict mapping domain names to their respective objects.
        
        Returns:
            A tuple of 3 values. A TransientVariable containing the grid,
            a string of the tool, and a string of the method.
        """
        gridder = operation.parameters.get('gridder')

        grid = None
        tool = None
        method = None

        if gridder is not None:
            tool = gridder.tool

            method = gridder.method

            logger.info('Using {} of regridder {}'.format(method, tool))

            if isinstance(gridder.grid, (str, unicode)):
                if gridder.grid in variables:
                    v = variables[gridder.grid]

                    logger.info('Loading grid from {}'.format(v.uri))

                    self.grid_file = cdms2.open(v.uri)

                    grid = self.grid_file[v.var_name].getGrid()
                elif gridder.grid in domains:
                    d = domains[gridder.grid]
                    
                    lat = d.get_dimension(('latitude', 'lat', 'y'))

                    lon = d.get_dimension(('longitude', 'lon', 'x'))

                    lat_val = [int_or_float(x) for x in [lat.start, lat.end, lat.step]]

                    lon_val = [int_or_float(x) for x in [lon.start, lon.end, lon.step]]
                   
                    grid = cdms2.createUniformGrid(
                                                   lat_val[0], lat_val[1]-lat_val[0], lat_val[2],
                                                   lon_val[0], lon_val[1]-lon_val[0], lon_val[2])

                    logger.info('Created uniform grid for {}'.format(d))
                else:
                    grid_type, arg = gridder.grid.split('~')

                    if grid_type == 'gaussian':
                        grid = cdms2.createGaussianGrid(int(arg))

                        logger.info('Created gaussian grid {}'.format(arg))
                    elif grid_type == 'uniform':
                        lat_step, lon_step = arg.split('x')

                        lat_step = int_or_float(lat_step)

                        lon_step = int_or_float(lon_step)

                        grid = cdms2.createUniformGrid(90.0, 180/lat_step, -lat_step, 0.0, 360/lon_step, lon_step)

                        logger.info('Created uniform grid {}x{}'.format(lat_step, lon_step))

        return grid, str(tool), str(method)

    def __can_publish(self, pub_type):
        publish = getattr(self, 'PUBLISH', None)

        if publish is None:
            return False

        return (publish & pub_type) > 0

    def on_retry(self, exc, task_id, args, kwargs, einfo):
        """ Handle a retry. """
        if not self.__can_publish(RETRY):
            return

        try:
            job = self.get_job(kwargs)
        except Exception:
            logger.exception('Failed to retrieve job')
        else:
            job.retry(exc)

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        """ Handle a failure. """
        if not self.__can_publish(FAILURE):
            return

        try:
            job = self.get_job(kwargs)
        except Exception:
            logger.exception('Failed to retrieve job')
        else:
            job.failed(str(exc))

    def on_success(self, retval, task_id, args, kwargs):
        """ Handle a success. """
        if not self.__can_publish(SUCCESS):
            return

        try:
            job = self.get_job(kwargs)
        except Exception:
            logger.exception('Failed to retrieve job')
        else:
            job.succeeded(json.dumps(retval.values()[0]))

# Define after CWTBaseTask is declared
cwt_shared_task = partial(shared_task,
                          bind=True,
                          base=CWTBaseTask,
                          autoretry_for=(AccessError,),
                          retry_kwargs={'max_retries': 3})
