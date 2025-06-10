from django.db.models import (
    CASCADE,
    Manager,
    Model,
    QuerySet,
    AutoField,
    CharField,
    ForeignKey,
    IntegerField,
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
