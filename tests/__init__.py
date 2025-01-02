from __future__ import absolute_import
from __future__ import unicode_literals

import os

import django
from unmagic import fixture

# django setup must occur before importing models
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "tests.settings")
django.setup()

from .django_setup import init_db, destroy_db  # noqa


@fixture(autouse=__name__, scope="package")
def test_db():
    init_db()
    yield
    destroy_db()
