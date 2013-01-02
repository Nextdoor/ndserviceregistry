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

"""Exceptions for nd_service_registry

Copyright 2012 Nextdoor Inc."""

__author__ = 'matt@nextdoor.com (Matt Wise)'


class ServiceRegistryException(Exception):

    """ServiceParty Exception Object"""

    def __init__(self, e):
        self.value = e

    def __str__(self):
        return self.value


class NoConnection(ServiceRegistryException):
    """Any time the backend service is unavailable for an action."""


class ReadOnly(ServiceRegistryException):
    """Thrown when a Write operation is attempted while in Read Only mode."""
