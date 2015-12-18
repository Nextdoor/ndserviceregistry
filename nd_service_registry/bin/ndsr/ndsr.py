#!/usr/bin/env python
# Copyright 2014 Nextdoor.com, Inc.
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
from __future__ import absolute_import
from __future__ import print_function

import gflags
import logging
import sys

from nd_service_registry.bin.ndsr import get
from nd_service_registry import exceptions
import nd_service_registry

__author__ = 'me@ryangeyer.com (Ryan J. Geyer)'

log = logging.getLogger(__name__)

gflags.DEFINE_string('server', 'localhost:2181',
                     "Server:Port string eg. 'localhost:2181'",
                     short_name='s')
gflags.DEFINE_string('username', None, 'Your Zookeeper username',
                     short_name='u')
gflags.DEFINE_string('password', None, 'Your Zookeeper password',
                     short_name='p')
gflags.DEFINE_bool('quiet', False,
                   "When set, ndsr will not print out any useful status \
                   messages, but will only output the results of the command.",
                   short_name='q')
gflags.DEFINE_integer('loglevel', logging.INFO,
                      "The python logging level in integer form. 50-0 in \
                      increments of 10 descending from CRITICAL to NOTSET")
gflags.DEFINE_string('outputformat', 'yaml',
                     'The desired output format for queries.  One of \
                     (yaml,json)')
gflags.DEFINE_bool('data', False,
                   "Show data associated with each node listed (if exists)",
                   short_name='d')
gflags.DEFINE_bool('recursive', False, "Recursively list all children",
                   short_name='r')

FLAGS = gflags.FLAGS

# Mapping of valid commands to their classes
COMMANDS = {'Get': get.Get}

# TODO (wenbin): add Put command for write data to zookeeper. Need an argument
# to specify whether or not the node is ephemeral.


def main(argv):
    """Determines which subcommand is being called, dynamically instantiates
    the correct class and executes it
    """
    command = 'get'
    if len(argv) > 1:
        command = argv[1]
    command = command.capitalize()
    if command in COMMANDS:
        log.info("Connecting to server %s" % FLAGS.server)

        # Instantiate the KazooServiceRegistry() object, and ensure that
        # we disable the normal rate-limiting that occurs so that the
        # CLI tool returns back as quickly as possible.
        nd = nd_service_registry.KazooServiceRegistry(server=FLAGS.server,
                                                      rate_limit_calls=0,
                                                      rate_limit_time=0)

        if FLAGS.username is not None and FLAGS.password is not None:
            nd.set_username(FLAGS.username)
            nd.set_password(FLAGS.password)
        # This works because we've already imported the class above.
        command_class = COMMANDS[command]
        instance = command_class(nd)
        print(instance.execute(argv, FLAGS))
    else:
        sys.stderr.write("Unknown command %s" % command)
        exit(1)


def console_entry_point():
    """A console entry point publicized by setuptools.  Parses flags, sets up
    logging, and executes the command
    """
    try:
        FLAGS.UseGnuGetOpt(True)
        argv = FLAGS(sys.argv)
    except gflags.FlagsError as e:
        print("%s\nUsage: %s [get] [<path>] ARGS\n%s" % (e,
                                                         sys.argv[0],
                                                         FLAGS))
        sys.exit(1)

    try:
        if not FLAGS.quiet:
            root_logger = logging.getLogger()
            root_logger.setLevel(FLAGS.loglevel)
            stdout_handler = logging.StreamHandler(sys.stdout)
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            stdout_handler.setFormatter(formatter)
            root_logger.addHandler(stdout_handler)

        main(argv)
    except exceptions.ServiceRegistryException as e:
        # Would recommend that Zookeeper/Kazoo specific error codes be passed
        # through so the exit code could be better evaluated by tools consuming
        # ndsr
        sys.exit(e.value)

if __name__ == '__main__':
    console_entry_point()
