#!/usr/bin/env python
# Copyright 2012, Julius Seporaitis
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


__author__ = "Julius Seporaitis"
__email__ = "julius@seporaitis.net"
__copyright__ = "Copyright 2012, Julius Seporaitis"
__license__ = "Apache 2.0"
__version__ = "2.0.0"

import logging
from urlparse import urlparse

import boto
import yum
import yum.config
import yum.Errors
import yum.plugins
from yum.yumRepo import YumRepository


__all__ = ['requires_api_version', 'plugin_type', 'CONDUIT',
           'config_hook', 'postreposetup_hook']

requires_api_version = '2.5'
plugin_type = yum.plugins.TYPE_CORE
CONDUIT = None


def config_hook(conduit):
    yum.config.RepoConf.s3_enabled = yum.config.BoolOption(False)


def postreposetup_hook(conduit):
    """Plugin initialization hook. Setup the S3 repositories."""

    repos = conduit.getRepos()

    for repo in repos.listEnabled():
        if isinstance(repo, YumRepository) and repo.s3_enabled:
            new_repo = S3Repository(repo.id, repo.baseurl)
            new_repo.name = repo.name
            # new_repo.baseurl = repo.baseurl
            new_repo.mirrorlist = repo.mirrorlist
            new_repo.basecachedir = repo.basecachedir
            new_repo.gpgcheck = repo.gpgcheck
            new_repo.gpgkey = repo.gpgkey
            new_repo.proxy = repo.proxy
            new_repo.enablegroups = repo.enablegroups
            if hasattr(repo, 'priority'):
                new_repo.priority = repo.priority
            if hasattr(repo, 'base_persistdir'):
                new_repo.base_persistdir = repo.base_persistdir
            if hasattr(repo, 'metadata_expire'):
                new_repo.metadata_expire = repo.metadata_expire

            repos.delete(repo.id)
            repos.add(new_repo)


class S3Repository(YumRepository):
    """Repository object for Amazon S3, using IAM Roles."""


    def __init__(self, repoid, baseurl):
        super(S3Repository, self).__init__(repoid)
        self.logger = logging.getLogger("yum.S3Repository")
        self.logger.debug(baseurl)
        self.s3 = boto.connect_s3()
        self.s3url = self._format_baseurl(baseurl)
        urlpieces = urlparse(self.s3url)
        self.logger.debug(urlpieces)
        self.s3basepath = urlpieces.path
        self.bucket = self.s3.get_bucket(urlpieces.netloc.split('.')[0])
        self.grabber = None
        self.enable()

    def _format_baseurl(self,url):
        rc = None
        if isinstance(url, basestring):
            rc = url
        else:
            if len(url) != 1:
                raise yum.plugins.PluginYumExit("s3iam: must have only one baseurl")
            else:
                rc = url.pop()

        # Ensure urljoin doesn't ignore base path:
        if not rc.endswith('/'):
            rc += '/'

	return rc

    @property
    def grabfunc(self):
        raise NotImplementedError("grabfunc called, when it shouldn't be!")

    @property
    def grab(self):
        if not self.grabber:
            self.grabber = S3Grabber(self)
        return self.grabber

    def getbucket(self):
        self.logger.debug(self.bucket)
        return self.bucket

    def geturl(self):
	return self.s3url

    def getKeyPath(self,key):
	rc = '{base}{key}'.format(base=self.s3basepath, key=key)
	self.logger.debug('getKeyPath()={rc}'.format(rc=rc))
	return rc


class S3Grabber(object):

    def __init__(self, repo):
        """Initialize file grabber.
        Note: currently supports only single repo.baseurl. So in case of a list
              only the first item will be used.
        """
        self.logger = logging.getLogger("yum.S3Grabber")
        self.repo = repo
        if isinstance(repo, basestring):
            self.baseurl = repo
        else:
            self.baseurl = repo.geturl()
        
    def _getpath(self, url):
        path = urlparse(url).path
        if path.startswith('/'):
            path = path[1:]
        self.logger.debug(path)
        return path

    def _getbucket(self):
        return self.repo.getbucket()

    def urlgrab(self, url, filename=None, **kwargs):
        """urlgrab(url) copy the file to the local filesystem."""
        s3_key_name = self.repo.getKeyPath(self._getpath(url))
        key = self._getbucket().get_key(s3_key_name)

        if filename is None:
            filename = s3_key_name

        key.get_contents_to_filename(filename)
        return filename

    def urlopen(self, url, **kwargs):
        """urlopen(url) open the remote file and return a file object."""
        s3_key_name = self._getpath(url)
        return self._getbucket().get_key(s3_key_name)

    def urlread(self, url, limit=None, **kwargs):
        """urlread(url) return the contents of the file as a string.
        :param url:
        :param limit:
        :param kwargs:
        """
        s3_key_name = self._getpath(url)
        key = self._getbucket().get_key(s3_key_name)
        return key.get_contents_as_string()

