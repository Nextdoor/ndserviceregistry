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

"""nd_service_registry Zookeeper Client Library

Copyright 2012 Nextdoor Inc."""

__author__ = 'matt@nextdoor.com (Matt Wise)'

import functools
import time
import logging

from kazoo.client import KazooClient

# Get a default logger
log = logging.getLogger(__name__)


class ZookeeperClient(KazooClient):
    """Shim-layer that provides some safety controls"""
    def __init__(self, *args, **kwargs):
        """Initialize the rate limiter code, then handle the default init"""

        # Iniitialize our rate limiter
        self.set_rate_limiter()

        # Finish object initialization
        super(ZookeeperClient, self).__init__(*args, **kwargs)

    def set_rate_limiter(self, time=0, calls=0):
        """Set rate limiting settings.

        args:
            time: Number of seconds to average between ZK requests (int)
            calls: Number of calls to determine averages over (int)
        """

        # Make sure that if the values are None or False, reset them
        if not isinstance(time, int):
            time=0
        if not isinstance(calls, int):
            calls=0

        log.debug('Set up rate limiting. Max %s avg between last %s calls.' %
                  (calls, time))
        self.previous_calls = []
        self.num_calls_to_average_over = calls
        self.target_avg_between_calls = time

    def rate_limiter(func):
        """Provides a decorator for rate-limiting function.

        The time between decorated function calls will be averaged over the
        last self.num_calls_to_average_over calls. A dynamically generated
        'sleep' time will be applied to new calls if the overall rate is
        higher than configured.

        The rate-limiting is blocking. Your code will not complete until
        the sleep timer is finished.
        """

        @functools.wraps(func)
        def _rate_limited_function(self, *args, **kwargs):
            # Start out assuming we will not need to throttle
            throttle = False

            # Have there even been enough calls to trigger the timing check?
            if ((self.num_calls_to_average_over > 0) and
               (len(self.previous_calls) > self.num_calls_to_average_over)):
                # Get the time elapsed between now and the last
                # 'self.num_calls_to_average_over' calls. eg. If
                # self.num_calls_to_average_over is 10, check how
                # much time has elapsed between this call, and the
                # call 10-calls ago.
                elapsed = (time.time() -
                    self.previous_calls[-self.num_calls_to_average_over])

                # Now get a rough average amount of time between the calls
                avg_between_calls = elapsed / self.num_calls_to_average_over
                log.debug('[%s] Avg time between last %s calls: %s, target: %s'
                          % (func.__name__, self.num_calls_to_average_over,
                             avg_between_calls, self.target_avg_between_calls))

                # If the average time between each call is less than the
                # self.target_avg_between_calls then we trigger throttling.
                if avg_between_calls < self.target_avg_between_calls:
                    log.warning('[%s] Too little time between last %s calls '
                                'for func. Throttling.' % (func.__name__,
                                self.num_calls_to_average_over))
                    throttle = True

            # If we're going to throttle, determine how long to wait
            if throttle:
                sleep = self.target_avg_between_calls - avg_between_calls
                log.debug('[%s] Sleeping: %s' % (func.__name__, sleep))
                self.handler.sleep_func(sleep)

            # Now go ahead and run our function
            log.debug('[%s] Calling with args[%s] and kwargs[%s]' %
                      (func.__name__, args, kwargs))

            ret = func(self, *args, **kwargs)

            # Record that we ran the function
            self.previous_calls.append(time.time())
            log.debug('[%s] Last %s calls: %s' %
                      (func.__name__, self.num_calls_to_average_over,
                       self.previous_calls[-self.num_calls_to_average_over:]))

            # Keep only the last num_calls_to_average_over+1 in the
            # previous_calls list so that we keep its size reasonable.
            calls_to_keep = self.num_calls_to_average_over + 1
            self.previous_calls = self.previous_calls[-calls_to_keep:]

            return ret
        return _rate_limited_function

    @rate_limiter
    def retry(self, *args, **kwargs):
        return super(ZookeeperClient, self).retry(*args, **kwargs)

    @rate_limiter
    def get(self, *args, **kwargs):
        return super(ZookeeperClient, self).get(*args, **kwargs)

    @rate_limiter
    def set(self, *args, **kwargs):
        return super(ZookeeperClient, self).set(*args, **kwargs)

    @rate_limiter
    def create(self, *args, **kwargs):
        return super(ZookeeperClient, self).create(*args, **kwargs)

    @rate_limiter
    def delete(self, *args, **kwargs):
        return super(ZookeeperClient, self).delete(*args, **kwargs)


class KazooFilter(logging.Filter):
    """Filters out certain Kazoo messages that we do not want to see."""
    def filter(self, record):
        retval = True

        # Filter out the PING messages
        if record.getMessage().lower().find('ping') > -1:
            retval = False

        return retval
