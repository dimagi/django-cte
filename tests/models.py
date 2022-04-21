from __future__ import absolute_import
from __future__ import unicode_literals

from django.db.models import (
    CASCADE,
    Model,
    AutoField,
    CharField,
    ForeignKey,
    IntegerField,
    TextField,
)

from django_cte import CTEManager, CTEQuerySet


class LT40QuerySet(CTEQuerySet):

    def lt40(self):
        return self.filter(amount__lt=40)


class LT30QuerySet(CTEQuerySet):

    def lt30(self):
        return self.filter(amount__lt=30)


class LT25QuerySet(CTEQuerySet):

    def lt25(self):
        return self.filter(amount__lt=25)


class LTManager(CTEManager):
    pass


class Region(Model):
    objects = CTEManager()
    name = TextField(primary_key=True)
    parent = ForeignKey("self", null=True, on_delete=CASCADE)

    class Meta:
        db_table = "region"


class Order(Model):
    objects = CTEManager()
    id = AutoField(primary_key=True)
    region = ForeignKey(Region, on_delete=CASCADE)
    amount = IntegerField(default=0)

    class Meta:
        db_table = "orders"


class OrderFromLT40(Order):
    class Meta:
        proxy = True
    objects = CTEManager.from_queryset(LT40QuerySet)()


class OrderLT40AsManager(Order):
    class Meta:
        proxy = True
    objects = LT40QuerySet.as_manager()


class OrderCustomManagerNQuery(Order):
    class Meta:
        proxy = True
    objects = LTManager.from_queryset(LT25QuerySet)()


class OrderCustomManager(Order):
    class Meta:
        proxy = True
    objects = LTManager()


class KeyPair(Model):
    objects = CTEManager()
    key = CharField(max_length=32)
    value = IntegerField(default=0)
    parent = ForeignKey("self", null=True, on_delete=CASCADE)

    class Meta:
        db_table = "keypair"
