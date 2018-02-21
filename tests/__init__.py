from __future__ import absolute_import
from __future__ import unicode_literals

import os

import django

# django setup must occur before importing models
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "tests.settings")
django.setup()

from .django_setup import init_db, destroy_db  # noqa


def setup():
    """Initialize database for nosetests"""
    init_db()


def teardown():
    destroy_db()
