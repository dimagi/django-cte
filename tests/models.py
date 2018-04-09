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

from django_cte import CTEManager, CTEQuerySet


class LT40QuerySet(CTEQuerySet):

    def lt40(self):
        return self.filter(amount__lt=40)


class LT40Manager(CTEManager):

    def get_queryset(self):
        return LT40QuerySet(model=self.model, using=self._db)


class Region(Model):
    objects = CTEManager()
    name = TextField(primary_key=True)
    parent = ForeignKey("self", null=True, on_delete=CASCADE)


class Order(Model):
    objects = LT40Manager.from_queryset(LT40QuerySet)()
    id = AutoField(primary_key=True)
    region = ForeignKey(Region, on_delete=CASCADE)
    amount = IntegerField(default=0)
