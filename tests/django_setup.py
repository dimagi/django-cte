from django.db import connection

from .models import KeyPair, Region, Order

is_initialized = False


def init_db():
    global is_initialized
    if is_initialized:
        return
    is_initialized = True

    connection.creation.create_test_db(verbosity=0, autoclobber=True)

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
        ("proxima centauri b", "proxima centauri"),
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
        ("proxima centauri", 2000),
        ("proxima centauri b", 10),
        ("proxima centauri b", 11),
        ("proxima centauri b", 12),
    ]:
        order = Order(amount=amount, region=regions[region])
        order.save()

    for key, value, parent in [
        ("level 1", 1, None),
        ("level 2", 1, "level 1"),
        ("level 2", 2, "level 1"),
        ("level 3", 1, "level 2"),
    ]:
        parent = parent and KeyPair.objects.filter(key=parent).first()
        KeyPair.objects.create(key=key, value=value, parent=parent)
