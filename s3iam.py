#!/usr/bin/env python
# Copyright 2012, Julius Seporaitis
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
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


import urllib2
from urlparse import urlparse
import time
import hashlib
import hmac
import json
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
        s3 = boto.connect_s3();
        self.bucket = s3.get_bucket(urlparse(baseurl).netloc.split('.')[0])
        self.baseurl = baseurl
        self.grabber = None
        self.enable()

    @property
    def grabfunc(self):
        raise NotImplementedError("grabfunc called, when it shouldn't be!")

    @property
    def grab(self):
        if not self.grabber:
            self.grabber = S3Grabber(self)
        return self.grabber


class S3Grabber(object):

    def __init__(self, repo):
        """Initialize file grabber.
        Note: currently supports only single repo.baseurl. So in case of a list
              only the first item will be used.
        """
        if isinstance(repo, basestring):
            self.baseurl = repo
        else:
            if len(repo.baseurl) != 1:
                raise yum.plugins.PluginYumExit("s3iam: repository '{0}' "
                                                "must have only one "
                                                "'baseurl' value" % repo.id)
            else:
                self.baseurl = repo.baseurl[0]
        # Ensure urljoin doesn't ignore base path:
        if not self.baseurl.endswith('/'):
            self.baseurl += '/'

    def _getpath(self, url):
        path = urlparse(url).path
        if path.startswith('/'):
            path = path[1:]
        return path

    def _getbucket(self):
        return self.repo.getbucket()

    def urlgrab(self, url, filename=None, **kwargs):
        """urlgrab(url) copy the file to the local filesystem."""
        s3_key_name = _getpath(url)
        key = self._getbucket().get_key(s3_key_name)

        if filename is None:
            filename = s3_key_name

        key.get_contents_to_filename(filename)
        return filename

    def urlopen(self, url, **kwargs):
        """urlopen(url) open the remote file and return a file object."""
        return self.bucket.get_key(s3_key_name)
        return urllib2.urlopen(self._request(url))

    def urlread(self, url, limit=None, **kwargs):
        """urlread(url) return the contents of the file as a string."""
        return urllib2.urlopen(self._request(url)).read()

