# Copyright 2012 Nextdoor.com, Inc.
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

"""ndServiceRegistry Zookeeper Client Library

Copyright 2012 Nextdoor Inc."""

__author__ = 'matt@nextdoor.com (Matt Wise)'

import logging
from kazoo.client import KazooClient
from ndServiceRegistry.funcs import rate_limiter

# Our default variables
from version import __version__ as VERSION


class ZookeeperClient(KazooClient):
    """Shim-layer that provides some safety controls"""

    @rate_limiter(targetAvgTimeBetweenCalls=1)
    def retry(self, *args, **kwargs):
        return super(ZookeeperClient, self).retry(*args, **kwargs)

    @rate_limiter(targetAvgTimeBetweenCalls=1)
    def get(self, *args, **kwargs):
        return super(ZookeeperClient, self).get(*args, **kwargs)

    @rate_limiter(targetAvgTimeBetweenCalls=10, numCallsToAverage=30)
    def set(self, *args, **kwargs):
        return super(ZookeeperClient, self).set(*args, **kwargs)

    @rate_limiter(targetAvgTimeBetweenCalls=10, numCallsToAverage=30)
    def create(self, *args, **kwargs):
        return super(ZookeeperClient, self).create(*args, **kwargs)

    @rate_limiter(targetAvgTimeBetweenCalls=10, numCallsToAverage=30)
    def delete(self, *args, **kwargs):
        return super(ZookeeperClient, self).delete(*args, **kwargs)
