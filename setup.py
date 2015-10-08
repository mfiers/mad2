#!/usr/bin/env python

import sys
from setuptools import setup, find_packages

description = "create & track file metadata"

with open("VERSION") as F:
    version = F.read().strip()

    
entry_points = {
    'console_scripts': [
        'mad = mad2.cli.main:dispatch',
#        'sha1p = mad2.cli.sha1p:dispatch',
#        'qdsum = mad2.cli.qdsum:dispatch'
    ]}

setup(name='mad2',
      version=version,
      description=description,
      author='Mark Fiers',
      zip_safe=False,
      author_email='mark.fiers42@gmail.com',
      entry_points=entry_points,
      include_package_data=True,
      url='http://mfiers.github.com/mad2',
      packages=find_packages(),
      install_requires=[
          'Leip',
          'Fantail',
          'humanize',
          'Jinja2',
          'lockfile',
          'pytimeparse',
          'python-dateutil',
          'pymongo',
          'colorlog',
          'pytimeparse',
          'arrow',
          'termcolor',
          'readline',
          'toMaKe',
          'iso8601',
          ],
      classifiers=[
          'Development Status :: 4 - Beta',
          'Environment :: Console',
          'Intended Audience :: Developers',
          'Operating System :: OS Independent',
          'Programming Language :: Python',
          'Programming Language :: Python :: 3',
          'Programming Language :: Python :: 3.2',
          'Programming Language :: Python :: 3.3',
          'Programming Language :: Python :: 3.4',
      ]
      )
