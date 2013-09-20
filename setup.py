#!/usr/bin/env python

from setuptools import setup, find_packages

DESCRIPTION = """
file metadata tagger
"""

entry_points = {
    'console_scripts': [
        'mad = mad2.cli.main:dispatch'
        ]}

setup(name='mad2',
      version='0.0.21',
      description=DESCRIPTION,
      author='Mark Fiers',
      author_email='mark.fiers42@gmail.com',
      entry_points = entry_points,
      include_package_data=True,
      url='http://mfiers.github.com/mad2',
      packages=find_packages(),
      install_requires=[
                'Leip',
                'Yaco',
                'xlrd',
                'Jinja2'
                ],
      classifiers = [
        'Development Status :: 4 - Beta',
        'Environment :: Console',
        'Intended Audience :: Developers',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        ]
     )
