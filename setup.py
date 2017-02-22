from __future__ import absolute_import
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
import sys
import shutil

from distutils.command.clean import clean
from distutils.command.sdist import sdist
from setuptools import Command
from setuptools import setup
from setuptools import find_packages

PACKAGE = 'nd_service_registry'
__version__ = None
with open(os.path.join(PACKAGE, 'version.py')) as f:
    src = f.read()
code = compile(src, os.path.join(PACKAGE, 'version.py'), 'exec')
exec(code)  # set __version__


def maybe_rm(path):
    """Simple method for removing a file/dir if it exists"""
    if os.path.exists(path):
        try:
            shutil.rmtree(path)
        except:
            os.remove(path)


class Pep8Command(Command):
    description = 'Pep8 Lint Checks'
    user_options = []

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        # Don't import the pep8 module until now because setup.py needs to be
        # able to install Pep8 if its missing.
        import pep8
        pep8style = pep8.StyleGuide(parse_argv=True, config_file='pep8.cfg')
        report = pep8style.check_files([PACKAGE])
        if report.total_errors:
            sys.exit('ERROR: Pep8 failed with exit %d errors' %
                     report.total_errors)


class PyflakesCommand(Command):
    description = 'Pyflakes Checks'
    user_options = []

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        # Don't import the pyflakes code until now because setup.py needs to be
        # able to install Pyflakes if its missing. This localizes the import to
        # only after the setuptools code has run and verified everything is
        # installed.
        from pyflakes import api
        from pyflakes import reporter

        # Run the Pyflakes check against our package and check its output
        val = api.checkRecursive([PACKAGE], reporter._makeDefaultReporter())
        if val > 0:
            sys.exit('ERROR: Pyflakes failed with exit code %d' % val)


class UnitTestCommand(Command):
    description = 'Run unit tests'
    user_options = []
    args = [PACKAGE,
            '--with-coverage',
            '--cover-package=%s' % PACKAGE,
            '-v']

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        import nose
        maybe_rm('.coverage')
        val = nose.run(argv=self.args)

        if not val:
            sys.exit('ERROR:Tests failed')


class IntegrationTestCommand(UnitTestCommand):
    description = 'Run full integration tests and unit tests'
    args = [PACKAGE,
            '--with-coverage',
            '--cover-package=%s' % PACKAGE,
            '-v',
            '--include=integration']


class CleanHook(clean):
    def run(self):
        clean.run(self)

        maybe_rm('%s.egg-info' % PACKAGE)
        maybe_rm('dist')
        maybe_rm('.coverage')
        maybe_rm('version.rst')
        maybe_rm('MANIFEST')


class SourceDistHook(sdist):
    def run(self):
        with open('version.rst', 'w') as f:
            f.write(':Version: %s\n' % __version__)
        shutil.copy('README.md', 'README')
        sdist.run(self)
        os.unlink('MANIFEST')
        os.unlink('README')
        os.unlink('version.rst')


setup(
    name=PACKAGE,
    version=__version__,
    description='Nextdoor Service Registry Module',
    long_description=open('README.md').read(),
    author='Nextdoor Engineering',
    author_email='eng@nextdoor.com',
    url='https://github.com/Nextdoor/ndserviceregistry',
    download_url='http://pypi.python.org/pypi/%s#downloads' % PACKAGE,
    license='Apache License, Version 2.0',
    keywords='zookeeper apache zk',
    obsoletes='ndServiceRegistry',
    packages=find_packages(),
    test_suite='nose.collector',
    tests_require=open('requirements.test.txt').readlines(),
    setup_requires=open('requirements.txt').readlines(),
    install_requires=open('requirements.txt').readlines(),
    entry_points={
        'console_scripts': [
            'ndsr = nd_service_registry.bin.ndsr.ndsr:console_entry_point'
        ],
    },
    classifiers=[
        'Development Status :: 4 - Beta',
        'Topic :: Software Development',
        'License :: OSI Approved :: Apache Software License',
        'Intended Audience :: Developers',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 3',
        'Operating System :: POSIX',
        'Natural Language :: English',
    ],
    cmdclass={
        'sdist': SourceDistHook,
        'clean': CleanHook,
        'pep8': Pep8Command,
        'pyflakes': PyflakesCommand,
        'integration': IntegrationTestCommand,
        'test': UnitTestCommand,
    },
)
