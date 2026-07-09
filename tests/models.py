import django

from django.db.models import (
    AutoField,
    CASCADE,
    CharField,
    ForeignKey,
    IntegerField,
    Manager,
    Model,
    QuerySet,
    SlugField,
    TextField,
)


class LT40QuerySet(QuerySet):

    def lt40(self):
        return self.filter(amount__lt=40)


class LT25QuerySet(QuerySet):

    def lt25(self):
        return self.filter(amount__lt=25)


class Region(Model):
    name = TextField(primary_key=True)
    parent = ForeignKey("self", null=True, on_delete=CASCADE)

    class Meta:
        db_table = "region"


class User(Model):
    id = AutoField(primary_key=True)
    name = TextField()

    class Meta:
        db_table = "user"


class Order(Model):
    id = AutoField(primary_key=True)
    region = ForeignKey(Region, on_delete=CASCADE)
    amount = IntegerField(default=0)
    user = ForeignKey(User, null=True, on_delete=CASCADE)

    class Meta:
        db_table = "orders"


class OrderFromLT40(Order):
    class Meta:
        proxy = True
    objects = Manager.from_queryset(LT40QuerySet)()


class OrderCustomManagerNQuery(Order):
    class Meta:
        proxy = True
    objects = Manager.from_queryset(LT25QuerySet)()


class KeyPair(Model):
    key = CharField(max_length=32)
    value = IntegerField(default=0)
    parent = ForeignKey("self", null=True, on_delete=CASCADE)

    class Meta:
        db_table = "keypair"


class WithDBColumn(Model):
    id = AutoField(db_column="uid", primary_key=True)
    parent = ForeignKey("self", db_column="pid", null=True, on_delete=CASCADE)


if django.VERSION >= (5, 2):
    from django.db.models import CompositePrimaryKey


    class Site(Model):
        name = SlugField()


    class WithCompositePK(Model):
        pk = CompositePrimaryKey("site_id", "username")
        site = ForeignKey(Site, on_delete=CASCADE)
        username = CharField(max_length=32)
