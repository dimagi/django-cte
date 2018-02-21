from __future__ import absolute_import
from __future__ import unicode_literals

import sys

from django.db import connection

from .models import Region, Order

is_initialized = False


def init_db():
    global is_initialized
    if is_initialized:
        return
    is_initialized = True

    # replace sys.stdout for prompt to delete database
    old_stdout = sys.stdout
    sys.stdout = sys.__stdout__
    try:
        connection.creation.create_test_db(verbosity=0)
    finally:
        sys.stdout = old_stdout

    setup_data()


def destroy_db():
    connection.creation.destroy_test_db(verbosity=0)


def setup_data():
    regions = {None: None}
    for name, parent in [
        ("sun", None),
        ("mercury", "sun"),
        ("venus", "sun"),
        ("earth", "sun"),
        ("moon", "earth"),
        ("mars", "sun"),
        ("deimos", "mars"),
        ("phobos", "mars"),
        ("proxima centauri", None),
        ("bernard's star", None),
    ]:
        region = Region(name=name, parent=regions[parent])
        region.save()
        regions[name] = region

    for region, amount in [
        ("sun", 1000),
        ("mercury", 10),
        ("mercury", 11),
        ("mercury", 12),
        ("venus", 20),
        ("venus", 21),
        ("venus", 22),
        ("venus", 23),
        ("earth", 30),
        ("earth", 31),
        ("earth", 32),
        ("earth", 33),
        ("moon", 1),
        ("moon", 2),
        ("moon", 3),
        ("mars", 40),
        ("mars", 41),
        ("mars", 42),
    ]:
        order = Order(amount=amount, region=regions[region])
        order.save()
