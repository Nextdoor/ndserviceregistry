#!/usr/bin/env python
""" Copyright 2014 Nextdoor.com, Inc.

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.

A CLI tool for interacting with ZooKeeper using ndserviceregistry.

__author__ = 'me@ryangeyer.com (Ryan J. Geyer)'
"""


from __future__ import print_function
import argparse
import logging
import os
import sys

from nd_service_registry.bin.ndsr import get
from nd_service_registry import exceptions
import nd_service_registry

log = logging.getLogger(__name__)


# Mapping of valid commands to their classes
COMMANDS = {'Get': get.Get}

# TODO (wenbin): add Put command for write data to zookeeper. Need an argument
# to specify whether or not the node is ephemeral.


def parse_arguments():
    parser = argparse.ArgumentParser(
        prog=os.path.basename(sys.argv[0]),
        usage='%(prog)s [get] [<path>] [<flags>]',
        description=('A CLI tool for interacting with ZooKeeper using '
                     'ndserviceregistry.'),
    )

    parser.add_argument('--server', '-s', type=str, default='localhost:2181',
                        help='Server:Port string eg. \'localhost:2181\'')
    parser.add_argument('--username', '-u', type=str, default=None,
                        help='Your Zookeeper username')
    parser.add_argument('--password', '-u', type=str, default=None,
                        help='Your Zookeeper password')
    parser.add_argument('--quiet', '-q', type=bool, default=False,
                        help=('When set, ndsr will not print out any useful '
                              'status messages, but will only output the '
                              'results of the command.'))
    parser.add_argument('--loglevel', type=int, default=logging.INFO,
                        help=('The python logging level in integer form. '
                              '50-0 in increments of 10 descending from '
                              'CRITICAL to NOTSET'))
    parser.add_argument('--outputformat', type=str, default='yaml',
                        help=('The desired output format for queries.  One of '
                              '(yaml,json)'))
    parser.add_argument('--data', '-d', type=bool, default=False,
                        help=('Show data associated with each node listed '
                              '(if exists)'))
    parser.add_argument('--recursive', '-r', type=bool, default=False,
                        help='Recursively list all children')
    parser.add_argument('pos_args', type=str, nargs='?', default='',
                        help='Positional args: \'get\' or \'get\' and path.')

    return vars(parser.parse_args())


def main(args):
    """Determines which subcommand is being called, dynamically instantiates
    the correct class and executes it
    """
    command = 'get'
    if args['pos_args']:
        command = args['pos_args'][0]
    command = command.capitalize()
    if command in COMMANDS:
        log.info('Connecting to server {server}'.format(**args))

        # Instantiate the KazooServiceRegistry() object, and ensure that
        # we disable the normal rate-limiting that occurs so that the
        # CLI tool returns back as quickly as possible.
        nd = nd_service_registry.KazooServiceRegistry(server=args['server'],
                                                      rate_limit_calls=0,
                                                      rate_limit_time=0)

        if args['username'] is not None and args['password'] is not None:
            nd.set_username(args['username'])
            nd.set_password(args['password'])
        # This works because we've already imported the class above.
        command_class = COMMANDS[command]
        instance = command_class(nd)
        print(instance.execute(args))
    else:
        sys.stderr.write('Unknown command %s' % command)
        exit(1)


def console_entry_point():
    """A console entry point publicized by setuptools.  Parses flags, sets up
    logging, and executes the command
    """
    args = parse_arguments()
    args['UseGnuGetOpt'] = True

    try:
        if not args['quiet']:
            root_logger = logging.getLogger()
            root_logger.setLevel(args['loglevel'])
            stdout_handler = logging.StreamHandler(sys.stdout)
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            stdout_handler.setFormatter(formatter)
            root_logger.addHandler(stdout_handler)

        main(args)
    except exceptions.ServiceRegistryException as e:
        # Would recommend that Zookeeper/Kazoo specific error codes be passed
        # through so the exit code could be better evaluated by tools consuming
        # ndsr
        sys.exit(e.value)

if __name__ == '__main__':
    console_entry_point()
