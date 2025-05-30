from django.db.models import Manager

from django_cte import CTEManager, CTEQuerySet

from ..models import (
    KeyPair as V2KeyPair,
    Order as V2Order,
    Region as V2Region,
    User,  # noqa: F401
)


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


class V1Region(V2Region):
    objects = CTEManager()

    class Meta:
        proxy = True


Region = V1Region


class V1Order(V2Order):
    objects = CTEManager()

    class Meta:
        proxy = True


Order = V1Order


class V1OrderFromLT40(Order):
    class Meta:
        proxy = True
    objects = CTEManager.from_queryset(LT40QuerySet)()


class V1OrderLT40AsManager(Order):
    class Meta:
        proxy = True
    objects = LT40QuerySet.as_manager()


class V1OrderCustomManagerNQuery(Order):
    class Meta:
        proxy = True
    objects = LTManager.from_queryset(LT25QuerySet)()


class V1OrderCustomManager(Order):
    class Meta:
        proxy = True
    objects = LTManager()


class V1OrderPlainManager(Order):
    class Meta:
        proxy = True
    objects = Manager()


class V1KeyPair(V2KeyPair):
    objects = CTEManager()

    class Meta:
        proxy = True


KeyPair = V1KeyPair
OrderCustomManager = V1OrderCustomManager
OrderCustomManagerNQuery = V1OrderCustomManagerNQuery
OrderFromLT40 = V1OrderFromLT40
OrderLT40AsManager = V1OrderLT40AsManager
OrderPlainManager = V1OrderPlainManager
