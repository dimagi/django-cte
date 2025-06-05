from unmagic import fixture

from .. import ignore_v1_warnings


@fixture(autouse=__file__)
def ignore_v1_deprecations():
    with ignore_v1_warnings():
        yield


@fixture(autouse=__file__, scope="class")
def ignore_v1_deprecations_in_class_setup():
    with ignore_v1_warnings():
        yield


with ignore_v1_warnings():
    from . import models  # noqa: F401
