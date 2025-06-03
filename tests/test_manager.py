from django.db.models.expressions import F
from django.test import TestCase

from django_cte import CTE, with_cte

from .models import (
    OrderFromLT40,
    OrderCustomManagerNQuery,
    LT40QuerySet,
)


class TestCTE(TestCase):

    def test_cte_queryset_with_from_queryset(self):
        self.assertEqual(type(OrderFromLT40.objects.all()), LT40QuerySet)

        cte = CTE(
            OrderFromLT40.objects
            .annotate(region_parent=F("region__parent_id"))
            .filter(region__parent_id="sun")
        )
        orders = with_cte(
            cte,
            select=cte.queryset()
            .lt40()  # custom queryset method
            .order_by("region_id", "amount")
        )
        print(orders.query)

        data = [(x.region_id, x.amount, x.region_parent) for x in orders]
        self.assertEqual(data, [
            ("earth", 30, "sun"),
            ("earth", 31, "sun"),
            ("earth", 32, "sun"),
            ("earth", 33, "sun"),
            ('mercury', 10, 'sun'),
            ('mercury', 11, 'sun'),
            ('mercury', 12, 'sun'),
            ('venus', 20, 'sun'),
            ('venus', 21, 'sun'),
            ('venus', 22, 'sun'),
            ('venus', 23, 'sun'),
        ])

    def test_cte_queryset_with_custom_queryset(self):
        cte = CTE(
            OrderCustomManagerNQuery.objects
            .annotate(region_parent=F("region__parent_id"))
            .filter(region__parent_id="sun")
        )
        orders = with_cte(
            cte,
            select=cte.queryset()
            .lt25()  # custom queryset method
            .order_by("region_id", "amount")
        )
        print(orders.query)

        data = [(x.region_id, x.amount, x.region_parent) for x in orders]
        self.assertEqual(data, [
            ('mercury', 10, 'sun'),
            ('mercury', 11, 'sun'),
            ('mercury', 12, 'sun'),
            ('venus', 20, 'sun'),
            ('venus', 21, 'sun'),
            ('venus', 22, 'sun'),
            ('venus', 23, 'sun'),
        ])

    def test_cte_queryset_with_deferred_loading(self):
        cte = CTE(
            OrderCustomManagerNQuery.objects.order_by("id").only("id")[:1]
        )
        orders = with_cte(cte, select=cte)
        print(orders.query)

        self.assertEqual([x.id for x in orders], [1])
