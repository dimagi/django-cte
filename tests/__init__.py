import os
import warnings
from contextlib import contextmanager

import django
from unmagic import fixture

# django setup must occur before importing models
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "tests.settings")
django.setup()

from .django_setup import init_db, destroy_db  # noqa


@fixture(autouse=__file__, scope="package")
def test_db():
    with ignore_v1_warnings():
        init_db()
    yield
    destroy_db()


@contextmanager
def ignore_v1_warnings():
    msg = (
        r"CTE(Manager|QuerySet) is deprecated.*"
        r"|"
        r"Use `django_cte\.with_cte\(.*\)` instead\."
        r"|"
        r"Use `django_cte\.CTE(\.recursive)?` instead\."
    )
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", message=msg, category=DeprecationWarning)
        yield
