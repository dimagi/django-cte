from __future__ import absolute_import
from __future__ import unicode_literals

import os
import re
from io import open

from setuptools import find_packages, setup


def get_version(filename):
    path = os.path.join(os.path.dirname(__file__), filename)
    with open(path, encoding="utf-8") as handle:
        content = handle.read()
    return re.search(r'__version__ = "([^"]+)"', content).group(1)


def read_md(filename):
    path = os.path.join(os.path.dirname(__file__), filename)
    with open(path, encoding='utf-8') as handle:
        return handle.read()


setup(
    name='django-cte',
    version=get_version('django_cte/__init__.py'),
    description='Common Table Expressions (CTE) for Django',
    long_description=read_md('README.md'),
    long_description_content_type='text/markdown',
    maintainer='Daniel Miller',
    maintainer_email='millerdev@gmail.com',
    url='https://github.com/dimagi/django-cte',
    license='BSD License',
    packages=find_packages(exclude=['tests']),
    include_package_data=True,
    install_requires=['django>=2.2,<4.1'],
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Environment :: Web Environment',
        'Framework :: Django',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Framework :: Django',
        'Framework :: Django :: 2',
        'Framework :: Django :: 2.2',
        'Framework :: Django :: 3',
        'Framework :: Django :: 3.0',
        'Framework :: Django :: 3.1',
        'Framework :: Django :: 3.2',
        'Framework :: Django :: 4',
        'Framework :: Django :: 4.0',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ],
)
