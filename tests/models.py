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
from django.db.models.query import QuerySet

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


class LT40Manager(CTEManager):
    # This is a semi broke manager but allows testing of the documented
    # use of get_queryset. It is broken by ignoring the queryset passed
    # to it via the static method from_queryset
    def get_queryset(self):
        return LT40QuerySet(model=self.model, using=self._db)


class LTManager(CTEManager):
    pass


class Region(Model):
    objects = CTEManager()
    name = TextField(primary_key=True)
    parent = ForeignKey("self", null=True, on_delete=CASCADE)


class Order(Model):
    objects = CTEManager()
    id = AutoField(primary_key=True)
    region = ForeignKey(Region, on_delete=CASCADE)
    amount = IntegerField(default=0)


class OrderFromLT40(Order):
    class Meta:
        proxy = True
    objects = CTEManager.from_queryset(LT40QuerySet)()


class OrderCustomLimitedManagerNQuery(Order):
    """
    This was the original documented manager setup a custom
    manager and queryset.

    This is fine if the model isn't going to be in another
    python module where you have full controll over the managers
    and querysets.
    """
    class Meta:
        proxy = True
    objects = LT40Manager.from_queryset(LT40QuerySet)()


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
