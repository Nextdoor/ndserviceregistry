#!/usr/bin/env python
# Copyright 2013 Nextdoor.com, Inc.
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

"""
A CLI tool for interacting with ZooKeeper using ndserviceregistry.
"""

__author__ = 'me@ryangeyer.com (Ryan J. Geyer)'

import sys
import gflags
import logging

# Import the possible sub commands. If I were a more experienced Python programmer
# I'd dymanically load the module with reflection, but I got stuck trying to do that
# elegantly.
from nd_service_registry.bin.ndsr.get import Get
from nd_service_registry import KazooServiceRegistry
from nd_service_registry.exceptions import ServiceRegistryException

log = logging.getLogger(__name__)

gflags.DEFINE_string('server', None, "Server:Port string eg. 'localhost:2181'", short_name='s')
gflags.DEFINE_string('username', None, 'Your Zookeeper username', short_name='u')
gflags.DEFINE_string('password', None, 'Your Zookeeper password', short_name='p')
gflags.DEFINE_bool('quiet', False, "When set, ndsr will not print out any useful status messages, but will only output \
                                   the results of the command.", short_name='q')
gflags.DEFINE_integer('loglevel', logging.INFO, "The python logging level in integer form. 50-0 in increments of 10 \
                                                descending from CRITICAL to NOTSET")
gflags.DEFINE_string('outputformat', 'yaml', 'The desired output format for queries.  One of (yaml,json)')
gflags.DEFINE_bool('data', False, "Show data associated with each node path listed (if exists)", short_name='d')
gflags.DEFINE_bool('recursive', False, "Recursively list all children", short_name='r')

FLAGS = gflags.FLAGS


def main(argv):
    """Determines which subcommand is being called, dynamically instantiates the correct class and executes it
    """
    command = argv[1]
    capd_command = command.capitalize()
    if capd_command in ['Get']:
        # This works because we've already imported the class above.
        class_ = globals()[capd_command]
        instance = class_()
        log.info("Connecting to server %s" % FLAGS.server)
        nd = KazooServiceRegistry(server=FLAGS.server)
        if FLAGS.username is not None and FLAGS.password is not None:
            nd.set_username(FLAGS.username)
            nd.set_password(FLAGS.password)
        instance.set_ndsr(nd)
        print instance.execute(argv, FLAGS)


def console_entry_point():
    """A console entry point publicized by setuptools.  Parses flags, sets up logging, and executes the command
    """
    try:
        FLAGS.UseGnuGetOpt(True)
        argv = FLAGS(sys.argv)
    except gflags.FlagsError, e:
        print "%s\nUsage: %s get [<path>] ARGS\n%s" % (e, sys.argv[0], FLAGS)
        sys.exit(1)

    try:
        if not FLAGS.quiet:
            root_logger = logging.getLogger()
            root_logger.setLevel(FLAGS.loglevel)
            stdout_handler = logging.StreamHandler(sys.stdout)
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            stdout_handler.setFormatter(formatter)
            root_logger.addHandler(stdout_handler)

        main(argv)
    except ServiceRegistryException, e:
        # Would recommend that Zookeeper/Kazoo specific error codes be passed through
        # so the exit code could be better evaluated by tools consuming ndsr
        sys.exit(e.value)

if __name__ == '__main__':
    console_entry_point()