from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

import operator
import django
from django.db.models.expressions import F, Value
from django.db.models import IntegerField
from unittest import skipIf
from django.test import TestCase
from .models import Order


int_field = IntegerField()


class TestDjangoFixesWithCTE(TestCase):

    @skipIf(django.VERSION < (4, 2), "Not fixed in django until 4.2")
    def test_union_with_values_list_and_order_on_annotation(self):
        # This is a 99% a clone of same test in DJANGO from #31496
        # tests/queries/test_qs_combinators.py:
        #   QuerySetSetOperationTests.test_union_with_values_list_and_order_on_annotation
        # this is to ensure that our code changes to the handling of columns
        # names doesn't case a regression on model with a CTEQuery sets
        qs1 = Order.objects.annotate(
            annotation=Value(-1),
            multiplier=F("annotation"),
        ).filter(amount__gte=36)
        qs2 = Order.objects.annotate(
            annotation=Value(2),
            multiplier=F("annotation"),
        ).filter(amount__lte=35)
        uq1 = qs1.union(qs2).order_by(
                "annotation", "amount").values_list("amount", flat=True)
        self.assertSequenceEqual(
            uq1,
            [40, 41, 42, 1000, 2000, 1, 2, 3, 10, 11, 12,
             20, 21, 22, 23, 30, 31, 32, 33],
        )
        uq2 = (
            qs1.union(qs2)
            .order_by(
                F("annotation") * F("multiplier"),
                "amount",
            )
            .values("amount")
        )
        self.assertQuerySetEqual(
            uq2,
            [40, 41, 42, 1000, 2000, 1, 2, 3, 10, 11, 12,
             20, 21, 22, 23, 30, 31, 32, 33],
            operator.itemgetter("amount"),
        )
        print(uq1.query)
        print(uq2.query)
