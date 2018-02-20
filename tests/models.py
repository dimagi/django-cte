from __future__ import absolute_import
from __future__ import unicode_literals

from django.db.models import (
    CASCADE,
    Model,
    AutoField,
    ForeignKey,
    IntegerField,
    TextField,
)

from django_cte import CTEManager


class Region(Model):
    objects = CTEManager()
    name = TextField(primary_key=True)
    parent = ForeignKey("self", null=True, on_delete=CASCADE)


class Order(Model):
    objects = CTEManager()
    id = AutoField(primary_key=True)
    region = ForeignKey(Region, on_delete=CASCADE)
    amount = IntegerField(default=0)
