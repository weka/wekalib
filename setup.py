# Copyright (C) 2021 WekaIO
#
# This library is free software; you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public
# License as published by the Free Software Foundation, version 2.1.
#
# This library is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public
# License along with this library; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA

import os
import platform
from setuptools import setup, find_packages
from platform import python_version

import versioneer


cmdclass = versioneer.get_cmdclass()

setup(name='wekalib',
      version=versioneer.get_version(),
      cmdclass=cmdclass,
      description='Python Weka JRPC-API utility library',
      long_description=open('README.md').read(),
      author='Vince Fleming',
      author_email='vince@weka.io',
      url="https://github.com/weka/wekalib",
      license='LGPLv2.1',
      packages=find_packages(
          '.', exclude=('embedded_server', 'embedded_server.*',
                        'tests', 'tests.*',
                        '*.tests', '*.tests.*')
      ),
      install_requires=[
          'python-dateutil~=2.8.2', 'urllib3~=1.26.8'
      ],
      classifiers=[
          'Development Status :: 5 - Production/Stable',
          'License :: OSI Approved :: GNU Lesser General Public License v2 (LGPLv2)',
          'Intended Audience :: Developers',
          'Operating System :: OS Independent',
          'Programming Language :: Python',
          'Programming Language :: Python :: 3',
          'Programming Language :: Python :: 3.4',
          'Programming Language :: Python :: 3.5',
          'Programming Language :: Python :: 3.6',
          'Programming Language :: Python :: 3.7',
          'Programming Language :: Python :: 3.8',
          'Programming Language :: Python :: 3.9',
          'Topic :: Software Development :: Libraries',
          'Topic :: Software Development :: Libraries :: Python Modules',
          'Operating System :: POSIX :: Linux',
          'Operating System :: POSIX :: BSD',
          'Operating System :: Microsoft :: Windows',
          'Operating System :: MacOS :: MacOS X',
      ],
)