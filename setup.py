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

import os
import shutil
import subprocess

from distutils.command.clean import clean
from distutils.command.sdist import sdist
from setuptools import setup

PACKAGE = 'nd_service_registry'
__version__ = None
execfile(os.path.join(PACKAGE, 'version.py'))  # set __version__


class SourceDistHook(sdist):

    def run(self):
        with open('version.rst', 'w') as f:
            f.write(':Version: %s\n' % __version__)
        shutil.copy('README.rst', 'README')
        sdist.run(self)
        os.unlink('MANIFEST')
        os.unlink('README')
        os.unlink('version.rst')


class CleanHook(clean):

    def run(self):
        clean.run(self)

        def maybe_rm(path):
            if os.path.exists(path):
                shutil.rmtree(path)
        if self.all:
            maybe_rm('nd_service_registry.egg-info')
            maybe_rm('dist')


setup(
    name='nd_service_registry',
    version=__version__,
    description='Nextdoor ServiceRegistry module for interacting with Apache Zookeeper.',
    long_description=open('README.rst').read(),
    author='Matt Wise',
    author_email='matt@nextdoor.com',
    url='https://github.com/Nextdoor/ndserviceregistry',
    download_url='http://pypi.python.org/pypi/nd_service_registry#downloads',
    license='Apache License, Version 2.0',
    keywords='zookeeper apache zk',
    obsoletes='ndServiceRegistry',
    packages=[PACKAGE],
    install_requires=[
        'kazoo>=1.1',
        'setuptools',
    ],
    classifiers=[
        'Development Status :: 4 - Beta',
        'Topic :: Software Development',
        'License :: OSI Approved :: Apache Software License',
        'Intended Audience :: Developers',
        'Programming Language :: Python',
        'Operating System :: POSIX',
        'Natural Language :: English',
    ],
    cmdclass={'sdist': SourceDistHook, 'clean': CleanHook},
)
